package service

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type ticketGuardRepo struct {
	AccountRepository
	extra map[string]any
	apply bool
	err   error
	sets  int
}

func (r *ticketGuardRepo) SetOpenAICodexTicketErrorIfTokenMatches(context.Context, int64, string, string, string) (bool, error) {
	r.sets++
	return r.apply, r.err
}
func (r *ticketGuardRepo) UpdateExtra(_ context.Context, _ int64, extra map[string]any) error {
	if r.err != nil {
		return r.err
	}
	if r.extra == nil {
		r.extra = map[string]any{}
	}
	maps.Copy(r.extra, extra)
	return nil
}

func ticketGuardService(t *testing.T, u HTTPUpstream) *OpenAIGatewayService {
	return ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, HarvestProxyURL: "http://proxy.test:80"}, u)
}

func TestCodexTicketGuardUnauthorizedRecovery(t *testing.T) {
	u := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 401, Header: http.Header{}}}
	s := ticketGuardService(t, u)
	r := &ticketGuardRepo{apply: true}
	s.accountRepo = r
	a := ticketTestAccount(1)
	model := openAICodexTicketDefaultModel
	s.probeOnceOpenAICodexTicket(context.Background(), a, model)
	s.probeOnceOpenAICodexTicket(context.Background(), a, model)
	s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultSolModel)
	require.Len(t, u.requests, 1)
	require.Equal(t, 1, r.sets)
	require.True(t, s.openAICodexTicketTokenInvalid(a))
	require.EqualValues(t, 1, s.OpenAICodexTicketStatuses(a, s.openAICodexTicketConfig(), time.Now())[0].ProbeAttempts)
	// Persisted hashes survive restart, without carrying token strings into extra.
	b, err := json.Marshal(r.extra)
	require.NoError(t, err)
	var extra map[string]any
	require.NoError(t, json.Unmarshal(b, &extra))
	reloaded := *a
	reloaded.Extra = extra
	restarted := ticketGuardService(t, u)
	require.True(t, restarted.openAICodexTicketTokenInvalid(&reloaded))
	reloaded.Credentials = maps.Clone(a.Credentials)
	reloaded.Credentials["access_token"] = "new-token"
	require.False(t, s.openAICodexTicketTokenInvalid(&reloaded))
	require.False(t, restarted.openAICodexTicketTokenInvalid(&reloaded))
	// A different account is unaffected.
	require.False(t, s.openAICodexTicketTokenInvalid(ticketTestAccount(2)))
	logs, err := s.OpenAICodexTicketLogs(context.Background(), a, model, time.Now())
	require.NoError(t, err)
	require.Len(t, logs.Entries, 2)
	require.Equal(t, "token_invalid", logs.Entries[1].Reason)
	require.True(t, logs.Status.TokenInvalid)
	require.Equal(t, 401, logs.Entries[1].HTTPStatus)
}

func TestCodexTicketGuardStale401AndFailedPersistence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		apply   bool
		err     error
		stopped bool
	}{
		{"reauthorized", false, nil, false}, {"write_failure", false, errors.New("database unavailable"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := ticketGuardService(t, nil)
			s.accountRepo = &ticketGuardRepo{apply: tc.apply, err: tc.err}
			a := ticketTestAccount(1)
			s.stopOpenAICodexTicketHarvestOnUnauthorized(context.Background(), a, "cached-token")
			require.Equal(t, tc.stopped, s.openAICodexTicketTokenInvalid(a))
			require.Equal(t, tc.stopped, s.lookupOpenAICodexTicketTokenInvalidation(a).matches("cached-token"))
		})
	}
}

func TestCodexTicketGuardQuotaPauseRecovery(t *testing.T) {
	now := time.Now()
	for _, window := range []string{"primary", "secondary"} {
		t.Run(window, func(t *testing.T) {
			h := http.Header{}
			h.Set("x-codex-"+window+"-used-percent", "100")
			h.Set("x-codex-"+window+"-window-minutes", "300")
			h.Set("x-codex-"+window+"-reset-after-seconds", "3600")
			u := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 429, Header: h}}
			s := ticketGuardService(t, u)
			r := &ticketGuardRepo{apply: true}
			s.accountRepo = r
			a := ticketTestAccount(1)
			s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
			s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultSolModel)
			require.Len(t, u.requests, 1)
			require.True(t, s.openAICodexTicketHarvestPaused(a, now))
			require.False(t, s.isOpenAIAccountRuntimeBlocked(a), "quota probe must not install business cooldown")
			require.Nil(t, a.RateLimitResetAt)
			require.Equal(t, 0, r.sets)
			restarted := ticketGuardService(t, nil)
			reloaded := *a
			b, err := json.Marshal(r.extra)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(b, &reloaded.Extra))
			require.True(t, restarted.openAICodexTicketHarvestPaused(&reloaded, now))
			require.False(t, s.openAICodexTicketHarvestPaused(a, now.Add(2*time.Hour)))
			// Fresh trusted usage resumes; stale snapshot cannot override the pause.
			a.Extra = map[string]any{"codex_5h_used_percent": 10.0, "codex_usage_updated_at": now.Add(-time.Hour).Format(time.RFC3339Nano)}
			require.True(t, s.openAICodexTicketHarvestPaused(a, now))
			a.Extra["codex_usage_updated_at"] = time.Now().Add(time.Second).Format(time.RFC3339Nano)
			require.False(t, s.openAICodexTicketHarvestPaused(a, now))
		})
	}
}

func TestCodexTicketGuardUnknown429KeepsProbing(t *testing.T) {
	u := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 429, Header: http.Header{}}}
	s := ticketGuardService(t, u)
	a := ticketTestAccount(1)
	for i := 0; i < 2; i++ {
		s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
	}
	require.Len(t, u.requests, 2)
	require.False(t, s.openAICodexTicketHarvestPaused(a, time.Now()))
	reset := time.Now().Add(time.Hour)
	a.RateLimitResetAt = &reset
	s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
	require.Len(t, u.requests, 2)
}

func TestCodexTicketLogsBoundedIsolatedAndSafe(t *testing.T) {
	var store openAICodexTicketLogStore
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				store.append(1, "MODEL", OpenAICodexTicketLogEntry{Event: "started"})
			}
		}()
	}
	wg.Wait()
	entries := store.snapshot(1, "model")
	require.Len(t, entries, OpenAICodexTicketLogLimit)
	for i := 1; i < len(entries); i++ {
		require.Greater(t, entries[i].ID, entries[i-1].ID)
	}
	entries[0].Event = "mutated"
	require.Equal(t, "started", store.snapshot(1, "model")[0].Event)
	require.Empty(t, store.snapshot(2, "model"))
	require.Empty(t, store.snapshot(1, "other"))
	for i := int64(2); i <= openAICodexTicketLogMaxStreams+1; i++ {
		store.append(i, "model", OpenAICodexTicketLogEntry{})
	}
	require.Len(t, store.streams, openAICodexTicketLogMaxStreams)
	require.Empty(t, store.snapshot(1, "model"))
	u := &httpUpstreamRecorder{err: errors.New("secret-key http://user:password@proxy.test sensitive-ticket")}
	s := ticketGuardService(t, u)
	a := ticketTestAccount(1)
	s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
	logs, err := s.OpenAICodexTicketLogs(context.Background(), a, openAICodexTicketDefaultModel, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(logs)
	require.NoError(t, err)
	for _, secret := range []string{"secret-key", "password", "proxy.test", "sensitive-ticket"} {
		require.NotContains(t, string(b), secret)
	}
	require.Equal(t, "request_error", logs.Entries[1].Reason)
	_, err = s.OpenAICodexTicketLogs(context.Background(), a, "unknown", time.Now())
	require.ErrorIs(t, err, ErrOpenAICodexTicketLogModel)
	require.Len(t, u.requests, 1, "reading logs must not probe")
}

func TestCodexTicketGuardBeforeDispatchDoesNotCount(t *testing.T) {
	u := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}}}
	s := ticketGuardService(t, u)
	a := ticketTestAccount(1)
	s.stopOpenAICodexTicketHarvestOnUnauthorized(context.Background(), a, "tok")
	_, _, err := s.fireOpenAICodexTicketProbe(context.Background(), a, "tok", openAICodexTicketDefaultSolModel, "http://proxy.test:80", time.Second)
	require.ErrorIs(t, err, errOpenAICodexTicketHarvestPaused)
	require.Empty(t, u.requests)
	require.Empty(t, s.openaiCodexTicketLogs.snapshot(a.ID, openAICodexTicketDefaultSolModel))
	require.Zero(t, s.OpenAICodexTicketStatuses(a, s.openAICodexTicketConfig(), time.Now())[1].ProbeAttempts)
}

func TestCodexTicketQuotaPauseMissingResetHasBound(t *testing.T) {
	now := time.Now()
	a := ticketTestAccount(1)
	p := &openAICodexTicketHarvestPause{ObservedAt: now, Usage: map[string]any{"codex_5h_used_percent": 100.0}}
	require.True(t, p.active(a, now))
	require.False(t, p.active(a, now.Add(5*time.Hour)))
	// Identity-conflicting usage is not evidence of recovery.
	a.Extra = map[string]any{"codex_5h_used_percent": 0.0, "chatgpt_account_id": "other"}
	require.True(t, p.active(a, now))
}

func TestCodexTicketLogsSaveFailureIsNotSuccess(t *testing.T) {
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
	u := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: h}}
	s := ticketGuardService(t, u)
	s.accountRepo = &ticketGuardRepo{err: errors.New("unavailable")}
	a := ticketTestAccount(1)
	model := openAICodexTicketDefaultModel
	s.probeOnceOpenAICodexTicket(context.Background(), a, model)
	entries := s.openaiCodexTicketLogs.snapshot(a.ID, model)
	require.Len(t, entries, 3)
	require.Equal(t, "received", entries[1].Event)
	require.Equal(t, "discarded", entries[2].Event)
	require.EqualValues(t, 1, entries[2].Attempt)
	require.Nil(t, s.lookupOpenAICodexTicket(a, model))
}

type ticketGuardConcurrentUpstream struct {
	HTTPUpstream
	started chan struct{}
	release chan struct{}
}

func (u *ticketGuardConcurrentUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	var body struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}
	if strings.Contains(body.Model, "astra") {
		close(u.started)
		<-u.release
		h := http.Header{}
		h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
		return &http.Response{StatusCode: 200, Header: h}, nil
	}
	return &http.Response{StatusCode: 401, Header: http.Header{}}, nil
}

func TestCodexTicketGuardLateSuccessCannotUndo401(t *testing.T) {
	u := &ticketGuardConcurrentUpstream{started: make(chan struct{}), release: make(chan struct{})}
	s := ticketGuardService(t, u)
	a := ticketTestAccount(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
	}()
	select {
	case <-u.started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe not started")
	}
	s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultSolModel)
	close(u.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe not finished")
	}
	require.True(t, s.openAICodexTicketTokenInvalid(a))
	entries := s.openaiCodexTicketLogs.snapshot(a.ID, openAICodexTicketDefaultModel)
	s.probeOnceOpenAICodexTicket(context.Background(), a, openAICodexTicketDefaultModel)
	require.Len(t, s.openaiCodexTicketLogs.snapshot(a.ID, openAICodexTicketDefaultModel), len(entries))
}
