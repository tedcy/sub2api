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
	for _, messageType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
		for _, scenario := range []string{"missing", "mapped_model", "session_model"} {
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
				svc.storeOpenAICodexTicket(ctx, account, &openAICodexTicket{Model: "gpt-6-astra", State: fakeCodexTicketState(292), Length: 292, ExpiresAt: time.Now().Add(time.Hour)})
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
				upstream.Send(`{"type":"response.completed","response":{"id":"resp_first","model":"gpt-6-astra","usage":{"input_tokens":1,"output_tokens":1}}}`)
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
