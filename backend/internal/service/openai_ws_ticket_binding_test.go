package service

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestOpenAIWSTicketBinding(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	ctx := logger.IntoContext(context.Background(), zap.New(core))
	now := time.Now()
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}, nil)
	account := ticketTestAccount(41)
	ticket := &openAICodexTicket{Model: "gpt-6-astra", State: fakeCodexTicketState(292), Length: 292, CapturedAt: now, ExpiresAt: now.Add(time.Hour)}
	svc.storeOpenAICodexTicket(ctx, account, ticket)
	var binding openAIWSTicketBinding
	headers := http.Header{}
	require.NoError(t, svc.applyOpenAICodexTicket(ctx, account, ticket.Model, headers, &binding))
	require.Equal(t, ticket.State, headers.Get(openAICodexTurnStateHeader))
	require.Equal(t, ticket.ExpiresAt, binding.expiresAt)
	check := func(b openAIWSTicketBinding, model string, at time.Time, blocked bool) {
		t.Helper()
		err := svc.checkOpenAIWSTicket(ctx, account, b, model, 2, at)
		if !blocked {
			require.NoError(t, err)
			return
		}
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusTryAgainLater, closeErr.StatusCode())
		require.NotContains(t, err.Error(), ticket.State)
	}
	check(binding, ticket.Model, now, false)
	check(binding, ticket.Model, ticket.ExpiresAt.Add(-time.Nanosecond), false)
	check(binding, ticket.Model, ticket.ExpiresAt, true)
	entry := logs.All()[0]
	require.Equal(t, "openai.websocket_ticket_reconnect_required", entry.Message)
	require.Equal(t, "handshake_ticket_expired", entry.ContextMap()["reason"])
	require.EqualValues(t, account.ID, entry.ContextMap()["account_id"])
	require.Contains(t, entry.ContextMap(), "handshake_ticket_expires_at")
	require.NotContains(t, fmt.Sprint(entry.ContextMap()), ticket.State)
	check(binding, "gpt-5.6-sol", now, true)
	check(binding, "gpt-5.5", now, false)
	check(openAIWSTicketBinding{}, ticket.Model, now, true)
	refreshed := *ticket
	refreshed.CapturedAt = now.Add(time.Minute)
	refreshed.ExpiresAt = now.Add(2 * time.Hour)
	svc.storeOpenAICodexTicket(ctx, account, &refreshed)
	check(binding, ticket.Model, ticket.ExpiresAt, true)
	var newBinding openAIWSTicketBinding
	require.NoError(t, svc.applyOpenAICodexTicket(ctx, account, ticket.Model, http.Header{}, &newBinding))
	check(newBinding, ticket.Model, ticket.ExpiresAt, false)
	svc.openaiCodexTickets.Delete(openAICodexTicketKey(account.ID, ticket.Model))
	check(binding, ticket.Model, now, true)
	svc.cfg.Gateway.OpenAICodexTicket.FailClosed = false
	check(binding, ticket.Model, ticket.ExpiresAt, false)
	svc.cfg.Gateway.OpenAICodexTicket.FailClosed = true
	svc.cfg.Gateway.OpenAICodexTicket.Enabled = false
	check(binding, ticket.Model, ticket.ExpiresAt, false)
	svc.cfg.Gateway.OpenAICodexTicket.Enabled = true
	account.Type = AccountTypeAPIKey
	check(binding, ticket.Model, ticket.ExpiresAt, false)
}

// Exercise the real relay: a later rejected frame must neither reach upstream
// nor replay the retained initial request, for text and binary clients alike.
func TestPassthroughLifecycle_TicketUnavailable(t *testing.T) {
	for _, plan := range []string{"plus", "team"} {
		t.Run(plan, func(t *testing.T) { testPassthroughLifecycleTicketUnavailable(t, plan) })
	}
}

func testPassthroughLifecycleTicketUnavailable(t *testing.T, plan string) {
	for _, messageType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
		for _, scenario := range []string{"missing", "mapped_model", "session_model", "downgrade"} {
			t.Run(messageType.String()+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				upstream := newStagedPassthroughConn()
				cfg := passthroughLifecycleConfig()
				cfg.Gateway.OpenAIWS.OAuthEnabled = true
				cfg.Gateway.OpenAICodexTicket = config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}
				svc := newPassthroughLifecycleService(cfg, upstream)
				account := ticketTestAccount(41)
				account.Concurrency = 1
				account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
				account.Credentials["plan_type"] = plan
				length := openAICodexTicketTargetLength(account, 292)
				svc.storeOpenAICodexTicket(ctx, account, &openAICodexTicket{Model: "gpt-6-astra", State: fakeCodexTicketState(length), Length: length, ExpiresAt: time.Now().Add(time.Hour)})
				var slots, successfulTurns atomic.Int32
				server, serverErr := startPassthroughLifecycleServerWithHooks(t, ctx, svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
					return &OpenAIWSIngressHooks{
						BeforeTurn: func(int) error { slots.Store(1); return nil },
						AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
							slots.Store(0)
							if result != nil && err == nil {
								successfulTurns.Add(1)
							}
						},
						MapRequestModel: func(turn int, model string) (string, error) {
							if turn > 1 && scenario == "mapped_model" {
								return "gpt-5.6-sol", nil
							}
							return model, nil
						},
					}
				})
				defer server.Close()
				client := dialPassthroughLifecycleClientWithPayload(t, server, `{"type":"response.create","model":"gpt-6-astra"}`)
				defer client.CloseNow()
				select {
				case <-upstream.writes:
				case err := <-serverErr:
					t.Fatalf("initial relay failed: %v", err)
				case <-time.After(3 * time.Second):
					t.Fatal("initial request not forwarded")
				}
				responseModel := "gpt-6-astra"
				if scenario == "downgrade" {
					responseModel = "gpt-5.6-luna"
				}
				upstream.Send(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_first","model":%q,"usage":{"input_tokens":1,"output_tokens":1}}}`, responseModel))
				_, err := readPassthroughLifecycleFrame(t, client, 3*time.Second)
				require.NoError(t, err)
				if scenario == "missing" {
					svc.openaiCodexTickets.Delete(openAICodexTicketKey(account.ID, "gpt-6-astra"))
				}
				writeCtx, cancelWrite := context.WithTimeout(ctx, 3*time.Second)
				if scenario == "session_model" {
					require.NoError(t, client.Write(writeCtx, messageType, []byte(`{"type":"session.update","session":{"model":"gpt-5.6-sol"}}`)))
					requirePassthroughUpstreamWrite(t, upstream, 3*time.Second)
				}
				require.NoError(t, client.Write(writeCtx, messageType, []byte(`{"type":"response.create"}`)))
				cancelWrite()
				_, err = readPassthroughLifecycleFrame(t, client, 3*time.Second)
				require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(err))
				select {
				case err := <-serverErr:
					var closeErr *OpenAIWSClientCloseError
					require.ErrorAs(t, err, &closeErr)
				case <-time.After(3 * time.Second):
					t.Fatal("relay did not exit")
				}
				require.Empty(t, upstream.writes)
				require.Zero(t, slots.Load())
				require.EqualValues(t, 1, successfulTurns.Load())
			})
		}
	}
}

func TestPassthroughLifecycle_TicketModelDeselected(t *testing.T) {
	for _, messageType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
		t.Run(messageType.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upstream := newStagedPassthroughConn()
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAICodexTicket = config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}
			svc := newPassthroughLifecycleService(cfg, upstream)
			repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{}}}
			svc.settingService = NewSettingService(repo, cfg)
			account := ticketTestAccount(41)
			account.Concurrency = 1
			account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
			svc.storeOpenAICodexTicket(ctx, account, &openAICodexTicket{Model: "gpt-5.6-sol", State: fakeCodexTicketState(292), Length: 292, ExpiresAt: time.Now().Add(time.Hour)})
			var slots, successfulTurns atomic.Int32
			server, serverErr := startPassthroughLifecycleServerWithHooks(t, ctx, svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
				return &OpenAIWSIngressHooks{
					BeforeTurn: func(int) error { slots.Store(1); return nil },
					AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
						slots.Store(0)
						if result != nil && err == nil {
							successfulTurns.Add(1)
						}
					},
					MapRequestModel: func(_ int, _ string) (string, error) { return "gpt-5.6-sol", nil },
				}
			})
			defer server.Close()
			client := dialPassthroughLifecycleClientWithPayload(t, server, `{"type":"response.create","model":"client-alias"}`)
			defer client.CloseNow()
			requirePassthroughUpstreamWrite(t, upstream, 3*time.Second)
			// Change the selection while the first request is already in flight.
			setTicketModels(t, svc.settingService, repo, "gpt-6-astra")
			for turn := 1; turn <= 2; turn++ {
				upstream.Send(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d","model":"gpt-5.6-luna","usage":{"input_tokens":1,"output_tokens":1}}}`, turn))
				_, err := readPassthroughLifecycleFrame(t, client, 3*time.Second)
				require.NoError(t, err)
				if turn == 1 {
					writeCtx, stop := context.WithTimeout(ctx, 3*time.Second)
					err = client.Write(writeCtx, messageType, []byte(`{"type":"response.create"}`))
					stop()
					require.NoError(t, err)
					requirePassthroughUpstreamWrite(t, upstream, 3*time.Second)
				}
			}
			require.Eventually(t, func() bool { return successfulTurns.Load() == 2 }, time.Second, time.Millisecond)
			client.CloseNow()
			cancel()
			select {
			case <-serverErr:
			case <-time.After(3 * time.Second):
				t.Fatal("relay did not exit")
			}
			require.Empty(t, upstream.writes)
			require.Zero(t, slots.Load())
			require.EqualValues(t, 2, successfulTurns.Load())
			require.Zero(t, svc.codexTicketVersion(account, "gpt-5.6-sol"))
		})
	}
}
