package service

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type attemptsBlockingUpstream struct {
	HTTPUpstream
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (u *attemptsBlockingUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls.Add(1)
	close(u.started)
	<-u.release
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
	return &http.Response{StatusCode: 200, Header: h}, nil
}

func TestCodexProbeAttemptsSingleflightAndLateCompletion(t *testing.T) {
	u := &attemptsBlockingUpstream{started: make(chan struct{}), release: make(chan struct{})}
	cfg := config.OpenAICodexTicketConfig{Enabled: true, HarvestProxyURL: "http://proxy.test:80"}
	s := ticketTestService(t, cfg, u)
	a := ticketTestAccount(1)
	model := openAICodexTicketDefaultModel
	done := make(chan struct{})
	go func() { defer close(done); s.probeOnceOpenAICodexTicket(context.Background(), a, model) }()
	select {
	case <-u.started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not start")
	}
	joined := s.openaiCodexTicketFlight.DoChan(openAICodexTicketKey(a.ID, model), func() (any, error) { t.Error("duplicate flight ran"); return nil, nil })
	require.EqualValues(t, 1, s.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
	s.revokeCodexTicket(context.Background(), a, model, "gpt-5.6-luna", 0)
	require.Zero(t, s.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
	close(u.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not finish")
	}
	require.True(t, (<-joined).Shared)
	require.EqualValues(t, 1, u.calls.Load())
	require.Zero(t, s.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
	require.Nil(t, s.lookupOpenAICodexTicket(a, model))
}

func TestCodexProbeAttemptsExpiryDuringProbe(t *testing.T) {
	cfg := config.OpenAICodexTicketConfig{Enabled: true}
	s := ticketTestService(t, cfg, nil)
	a := ticketTestAccount(1)
	model := openAICodexTicketDefaultModel
	now := time.Now()
	a.Extra = map[string]any{openAICodexTicketExtraKey(model): &openAICodexTicket{State: fakeCodexTicketState(292), ExpiresAt: now.Add(-time.Second)}}
	s.recordCodexTicketProbe(a, model, now.Add(-2*time.Second))
	require.EqualValues(t, 1, s.OpenAICodexTicketStatuses(a, cfg, now.Add(-2*time.Second))[0].ProbeAttempts)
	// Successful old-round completion can save a ticket, but not resurrect its count.
	ticket := &openAICodexTicket{Model: model, State: fakeCodexTicketState(292), Length: 292, ExpiresAt: now.Add(time.Hour)}
	require.True(t, s.storeOpenAICodexTicket(context.Background(), a, ticket))
	require.Zero(t, s.OpenAICodexTicketStatuses(a, cfg, now)[0].ProbeAttempts)
	s.recordCodexTicketProbe(a, model, now)
	require.EqualValues(t, 1, s.OpenAICodexTicketStatuses(a, cfg, now)[0].ProbeAttempts)
}

func TestCodexProbeAttemptsRounds(t *testing.T) {
	cfg := config.OpenAICodexTicketConfig{Enabled: true, Models: []string{openAICodexTicketDefaultModel, openAICodexTicketDefaultSolModel}}
	svc := ticketTestService(t, cfg, nil)
	a := ticketTestAccount(1)
	now := time.Now()
	count := func(at time.Time) uint64 { return svc.OpenAICodexTicketStatuses(a, cfg, at)[0].ProbeAttempts }
	require.Zero(t, count(now))
	svc.recordCodexTicketProbe(a, openAICodexTicketDefaultModel, now)
	require.EqualValues(t, 1, count(now))
	require.Zero(t, svc.OpenAICodexTicketStatuses(a, cfg, now)[1].ProbeAttempts)
	require.Zero(t, svc.OpenAICodexTicketStatuses(ticketTestAccount(2), cfg, now)[0].ProbeAttempts)
	ticket := &openAICodexTicket{Model: openAICodexTicketDefaultModel, State: fakeCodexTicketState(292), Length: 292, CapturedAt: now, ExpiresAt: now.Add(time.Hour), Attempts: 1}
	require.True(t, svc.storeOpenAICodexTicket(context.Background(), a, ticket))
	require.EqualValues(t, 1, count(now))
	svc.recordCodexTicketProbe(a, ticket.Model, now.Add(time.Minute))
	refresh := *ticket
	refresh.ExpiresAt = now.Add(2 * time.Hour)
	require.True(t, svc.storeOpenAICodexTicket(context.Background(), a, &refresh))
	require.EqualValues(t, 2, count(ticket.ExpiresAt))
	require.Zero(t, count(refresh.ExpiresAt))
	svc.recordCodexTicketProbe(a, ticket.Model, refresh.ExpiresAt)
	require.EqualValues(t, 1, count(refresh.ExpiresAt))
	require.EqualValues(t, 1, count(refresh.ExpiresAt.Add(time.Second)))
	// Stale account snapshots cannot repeatedly reset the new round.
	a.Extra = map[string]any{openAICodexTicketExtraKey(ticket.Model): ticket}
	require.EqualValues(t, 1, count(refresh.ExpiresAt.Add(time.Second)))
	restarted := ticketTestService(t, cfg, nil)
	require.Zero(t, restarted.OpenAICodexTicketStatuses(a, cfg, now)[0].ProbeAttempts)
	require.Equal(t, 1, ticket.Attempts)
}

func TestCodexProbeAttemptsRevocationAndPersistence(t *testing.T) {
	cfg := config.OpenAICodexTicketConfig{Enabled: true}
	svc := ticketTestService(t, cfg, nil)
	repo := &revocationTestRepo{}
	svc.accountRepo = repo
	a := ticketTestAccount(1)
	model := openAICodexTicketDefaultModel
	svc.recordCodexTicketProbe(a, model, time.Now())
	svc.revokeCodexTicket(context.Background(), a, model, "gpt-5.6-luna", 0)
	require.Zero(t, svc.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
	svc.recordCodexTicketProbe(a, model, time.Now())
	ticket := &openAICodexTicket{Model: model, State: fakeCodexTicketState(292), Length: 292, ExpiresAt: time.Now().Add(time.Hour)}
	require.False(t, svc.storeOpenAICodexTicket(context.Background(), a, ticket, 0))
	require.EqualValues(t, 1, svc.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
	require.True(t, svc.storeOpenAICodexTicket(context.Background(), a, ticket, 1))
	require.EqualValues(t, 1, svc.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
	b, err := json.Marshal(repo.extra)
	require.NoError(t, err)
	require.NotContains(t, string(b), "probe_attempts")
	require.NotContains(t, string(b), "probeExpiry")
}

func TestCodexProbeAttemptsDispatch(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status, length int
		err            error
	}{
		{"success", 200, 292, nil}, {"312", 200, 312, nil}, {"http_error", 503, 0, nil}, {"timeout", 0, 0, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(tc.length))
			u := &httpUpstreamRecorder{resp: &http.Response{StatusCode: tc.status, Header: h}, err: tc.err}
			cfg := config.OpenAICodexTicketConfig{Enabled: true, HarvestProxyURL: "http://proxy.test:80"}
			s := ticketTestService(t, cfg, u)
			a := ticketTestAccount(1)
			s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
			require.Len(t, u.requests, 1)
			require.EqualValues(t, 1, s.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
		})
	}
	for _, reason := range []string{"proxy", "token", "disabled", "cancelled"} {
		t.Run(reason, func(t *testing.T) {
			cfg := config.OpenAICodexTicketConfig{Enabled: true, HarvestProxyURL: "http://proxy.test:80"}
			a := ticketTestAccount(1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch reason {
			case "proxy":
				cfg.HarvestProxyURL = ""
			case "token":
				a.Credentials = nil
			case "disabled":
				cfg.Enabled = false
			case "cancelled":
				cancel()
			}
			u := &httpUpstreamRecorder{}
			s := ticketTestService(t, cfg, u)
			s.probeOnceOpenAICodexTicket(ctx, a, openAICodexTicketDefaultModel)
			require.Empty(t, u.requests)
			cfg.Enabled = true
			require.Zero(t, s.OpenAICodexTicketStatuses(a, cfg, time.Now())[0].ProbeAttempts)
		})
	}
}
