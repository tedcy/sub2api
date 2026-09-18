package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func ticketProbeSSE(model string) string {
	return fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"model\":%q,\"status\":\"completed\"}}\n\n", model)
}

type revocationTestRepo struct {
	AccountRepository
	extra map[string]any
	fail  bool
	mu    sync.Mutex
}

func (r *revocationTestRepo) UpdateExtra(_ context.Context, _ int64, extra map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("database unavailable")
	}
	if r.extra == nil {
		r.extra = map[string]any{}
	}
	for k, v := range extra {
		r.extra[k] = v
	}
	return nil
}

func TestCodexTicketDowngradeRules(t *testing.T) {
	for _, tc := range []struct {
		sent, observed string
		want           bool
	}{
		{"gpt-6-astra", "gpt-5.6-luna", true}, {" GPT-5.6-SOL ", " GPT-5.6-LUNA ", true},
		{"gpt-6-astra", "gpt-5.6-sol", false}, {"gpt-5.5", "gpt-5.6-luna", false},
		{"gpt-6-astra", "", false}, {"alias", "gpt-5.6-luna", false},
	} {
		require.Equal(t, tc.want, isCodexTicketDowngrade(tc.sent, tc.observed))
	}
}

func TestCodexTicketRevocationLifecycle(t *testing.T) {
	for _, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
		t.Run(model, func(t *testing.T) {
			ctx := context.Background()
			svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}, nil)
			repo := &revocationTestRepo{}
			svc.accountRepo = repo
			account := ticketTestAccount(41)
			ticket := &openAICodexTicket{Model: model, State: fakeCodexTicketState(292), Length: 292, ExpiresAt: time.Now().Add(time.Hour)}
			svc.storeOpenAICodexTicket(ctx, account, ticket)
			// Keep a stale database snapshot to ensure revoked tickets cannot resurrect.
			account.Extra = map[string]any{openAICodexTicketExtraKey(model): ticket}
			var binding openAIWSTicketBinding
			require.NoError(t, svc.applyOpenAICodexTicket(ctx, account, model, http.Header{}, &binding))
			observer := svc.ticketModelObserver(ctx, account, model)
			repo.fail = true
			observer.Observe("gpt-5.6-luna", false)
			observer.Observe(model, true) // Later declarations cannot undo evidence.
			require.Nil(t, svc.lookupOpenAICodexTicket(account, model))
			require.Error(t, svc.checkOpenAIWSTicket(ctx, account, binding, model, 2, time.Now()))
			replacement := *ticket
			svc.storeOpenAICodexTicket(ctx, account, &replacement, 1)
			require.Nil(t, svc.lookupOpenAICodexTicket(account, model))
			repo.fail = false
			svc.retryCodexTicketRevocations(ctx)
			persisted := *account
			b, err := json.Marshal(repo.extra)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(b, &persisted.Extra))
			restarted := ticketTestService(t, svc.cfg.Gateway.OpenAICodexTicket, nil)
			require.Nil(t, restarted.lookupOpenAICodexTicket(&persisted, model))
			status := OpenAICodexTicketStatuses(&persisted, svc.cfg.Gateway.OpenAICodexTicket, time.Now())
			for _, item := range status {
				if item.Model == model {
					require.Equal(t, "upstream_model_downgrade", item.RevocationReason)
					require.True(t, item.Blocked)
				}
			}
			// The in-flight probe started before revocation cannot publish.
			svc.storeOpenAICodexTicket(ctx, account, &replacement, 0)
			require.Nil(t, svc.lookupOpenAICodexTicket(account, model))
			svc.storeOpenAICodexTicket(ctx, account, &replacement, 1)
			require.NotNil(t, svc.lookupOpenAICodexTicket(account, model))
			require.Error(t, svc.checkOpenAIWSTicket(ctx, account, binding, model, 2, time.Now()))
			var fresh openAIWSTicketBinding
			require.NoError(t, svc.applyOpenAICodexTicket(ctx, account, model, http.Header{}, &fresh))
			require.NoError(t, svc.checkOpenAIWSTicket(ctx, account, fresh, model, 1, time.Now()))
			svc.revokeCodexTicket(ctx, account, model, "gpt-5.6-luna", 0)
			require.NotNil(t, svc.lookupOpenAICodexTicket(account, model))
		})
	}
}

func TestCodexTicketRecoveryRequiresOnlyValid292(t *testing.T) {
	for _, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
		for _, body := range []string{
			ticketProbeSSE("gpt-5.6-luna"), ticketProbeSSE(""), "data: {}\n\n",
			"", "data: {\"type\":\"response.failed\"}\n\n",
		} {
			ctx := context.Background()
			h := http.Header{}
			h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(body))}}
			svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true, HarvestProxyURL: "http://proxy.example.com:8080"}, upstream)
			account := ticketTestAccount(41)
			svc.revokeCodexTicket(ctx, account, model, "gpt-5.6-luna", 0)
			require.Nil(t, svc.lookupOpenAICodexTicket(account, model))
			svc.probeOnceOpenAICodexTicket(ctx, account, model)
			require.NotNil(t, svc.lookupOpenAICodexTicket(account, model))
			require.False(t, svc.openAICodexTicketBlocksAccount(account, model))
			// A later business downgrade revokes the newly recovered generation.
			svc.ticketModelObserver(ctx, account, model).Observe("gpt-5.6-luna", false)
			require.True(t, svc.openAICodexTicketBlocksAccount(account, model))
		}
	}
}

func TestCodexTicketHTTPResponsePaths(t *testing.T) {
	for _, route := range []string{"json", "sse", "passthrough_json", "chat_buffered", "messages_buffered", "ws_http_bridge"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			account := ticketTestAccount(41)
			account.Concurrency = 1
			model := "gpt-5.6-sol"
			sse := "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-5.6-luna\"}}\n\n" + "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n" + ticketProbeSSE(model)
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}
			svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}, &httpUpstreamRecorder{resp: resp})
			svc.cfg.Gateway.MaxLineSize = defaultMaxLineSize
			svc.storeOpenAICodexTicket(ctx, account, &openAICodexTicket{Model: model, State: fakeCodexTicketState(292), Length: 292, ExpiresAt: time.Now().Add(time.Hour)})
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			payload := []byte(`{"model":"gpt-5.6-sol","input":"hello"}`)
			_, err := svc.buildUpstreamRequest(ctx, c, account, payload, "token", true, "", false)
			require.NoError(t, err)
			switch route {
			case "json", "passthrough_json":
				resp.Header.Set("Content-Type", "application/json")
				resp.Body = io.NopCloser(strings.NewReader(`{"id":"resp_test","model":"gpt-5.6-luna","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`))
				if route == "json" {
					_, err = svc.handleNonStreamingResponse(ctx, resp, c, account, "client-alias", model)
				} else {
					_, err = svc.handleNonStreamingResponsePassthrough(ctx, resp, c, account, "client-alias", model)
				}
			case "sse":
				_, err = svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "client-alias", model)
			case "chat_buffered":
				_, err = svc.handleChatBufferedStreamingResponse(resp, c, account, "client-alias", model, model, time.Now())
			case "messages_buffered":
				_, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, account, "client-alias", model, model, time.Now())
			case "ws_http_bridge":
				_, err = svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "token", payload, len(payload), model, "", "", "", "", 1, func([]byte) error { return nil }, map[string]uint64{model: 0})
			}
			require.NoError(t, err)
			require.Nil(t, svc.lookupOpenAICodexTicket(account, model))
			_, err = svc.buildUpstreamRequest(ctx, c, account, payload, "token", true, "", false)
			require.ErrorIs(t, err, ErrOpenAICodexTicketUnavailable)
		})
	}
}

func TestCodexTicketConcurrentRevocation(t *testing.T) {
	ctx := context.Background()
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}, nil)
	account := ticketTestAccount(41)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); svc.revokeCodexTicket(ctx, account, "gpt-6-astra", "gpt-5.6-luna", 0) }()
	}
	wg.Wait()
	require.EqualValues(t, 1, svc.codexTicketVersion(account, "gpt-6-astra"))
	require.EqualValues(t, 0, svc.codexTicketVersion(account, "gpt-5.6-sol"))
}

func TestCodexTicketPoolCompatibility(t *testing.T) {
	account := ticketTestAccount(41)
	bound := openAIWSTicketBinding{model: "gpt-6-astra", expiresAt: time.Now().Add(time.Hour)}
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
	old := normalizeOpenAIWSHandshakeCompatibility(account, h, bound)
	bound.version++
	require.NotEqual(t, old, normalizeOpenAIWSHandshakeCompatibility(account, h, bound))
	bound.version--
	bound.expiresAt = bound.expiresAt.Add(time.Hour)
	require.NotEqual(t, old, normalizeOpenAIWSHandshakeCompatibility(account, h, bound))
}

func TestCodexTicketPooledWSRevocation(t *testing.T) {
	ctx := context.Background()
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAICodexTicket = config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}
	conn := &openAIWSCaptureConn{events: [][]byte{[]byte(`{"type":"response.completed","response":{"id":"resp_revoke","model":"gpt-5.6-luna","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}`)}}
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: conn})
	svc := &OpenAIGatewayService{cfg: cfg, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool}
	account := ticketTestAccount(41)
	account.Concurrency = 1
	account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeCtxPool}
	svc.storeOpenAICodexTicket(ctx, account, &openAICodexTicket{Model: "gpt-6-astra", State: fakeCodexTicketState(292), Length: 292, ExpiresAt: time.Now().Add(time.Hour)})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	result, err := svc.Forward(ctx, c, account, []byte(`{"model":"gpt-6-astra","stream":false,"instructions":"help","input":[{"role":"user","content":"hi"}]}`))
	require.NoError(t, err)
	require.True(t, result.OpenAIWSMode)
	require.Nil(t, svc.lookupOpenAICodexTicket(account, "gpt-6-astra"))
}
