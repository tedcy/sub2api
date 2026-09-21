package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func setTicketModels(t *testing.T, settings *SettingService, repo *codexTicketSettingRepo, models ...string) {
	t.Helper()
	if models == nil {
		models = []string{}
	}
	b, err := json.Marshal(models)
	require.NoError(t, err)
	repo.values[SettingKeyOpenAICodexTicketModels] = string(b)
	settings.InvalidateOpenAICodexTicketModelsCache()
}

func TestCodexTicketModelsSettingsCache(t *testing.T) {
	ctx := context.Background()
	repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{}}}
	s := NewSettingService(repo, &config.Config{})
	fallback := s.defaultCodexTicketModels()
	require.Equal(t, []string{"gpt-6-astra", "gpt-5.6-sol"}, s.GetOpenAICodexTicketModels(ctx, fallback))
	setTicketModels(t, s, repo, " GPT-5.6-SOL ", "gpt-5.6-sol")
	require.Equal(t, []string{"gpt-5.6-sol"}, s.GetOpenAICodexTicketModels(ctx, fallback))
	copy := s.GetOpenAICodexTicketModels(ctx, fallback)
	copy[0] = "changed"
	require.Equal(t, []string{"gpt-5.6-sol"}, s.GetOpenAICodexTicketModels(ctx, fallback))
	repo.err = errors.New("database unavailable")
	s.InvalidateOpenAICodexTicketModelsCache()
	require.Equal(t, []string{"gpt-5.6-sol"}, s.GetOpenAICodexTicketModels(ctx, fallback))
	repo.err = nil
	setTicketModels(t, s, repo)
	require.Empty(t, s.GetOpenAICodexTicketModels(ctx, fallback))
	require.NotNil(t, s.GetOpenAICodexTicketModels(ctx, fallback))
	rebuilt := NewSettingService(repo, &config.Config{})
	require.Empty(t, rebuilt.GetOpenAICodexTicketModels(ctx, fallback))
}

func TestCodexTicketModelsIndependentGatesAndObservers(t *testing.T) {
	for _, selected := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
		t.Run(selected, func(t *testing.T) {
			ctx := context.Background()
			svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}, nil)
			repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{}}}
			svc.settingService = NewSettingService(repo, svc.cfg)
			setTicketModels(t, svc.settingService, repo, selected)
			account := ticketTestAccount(41)
			for _, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
				wantBlocked := model == selected
				require.Equal(t, wantBlocked, svc.openAICodexTicketBlocksAccount(account, model))
				require.Equal(t, wantBlocked, svc.applyOpenAICodexTicket(ctx, account, model, http.Header{}) != nil)
				require.Equal(t, wantBlocked, svc.checkOpenAIWSTicket(ctx, account, openAIWSTicketBinding{}, model, 2, time.Now()) != nil)
				for _, format := range []string{"json", "sse", "chat", "messages", "ws"} {
					observer := svc.ticketModelObserver(ctx, account, model)
					switch format {
					case "messages":
						observer.ObserveAnthropic([]byte(`{"message":{"model":"gpt-5.6-luna"}}`))
					case "chat", "json":
						observer.ObserveOpenAI([]byte(`{"model":"gpt-5.6-luna"}`), "")
					default:
						observer.ObserveOpenAI([]byte(`{"type":"response.completed","response":{"model":"gpt-5.6-luna"}}`), "response.completed")
					}
					require.Equal(t, "gpt-5.6-luna", observer.Model())
					require.Equal(t, wantBlocked, svc.codexTicketState(account, model).Revoked)
				}
			}
			statuses := svc.OpenAICodexTicketStatuses(account, svc.cfg.Gateway.OpenAICodexTicket, time.Now())
			require.Len(t, statuses, 1)
			require.Equal(t, selected, statuses[0].Model)
			// Turning gating off never removes the normal observer/audit result.
			svc.cfg.Gateway.OpenAICodexTicket.FailClosed = false
			require.False(t, svc.openAICodexTicketBlocksAccount(account, selected))
			require.NoError(t, svc.checkOpenAIWSTicket(ctx, account, openAIWSTicketBinding{}, selected, 3, time.Now()))
		})
	}
}

func TestCodexTicketModelsTogglePreservesRevocationAndLogs(t *testing.T) {
	ctx := context.Background()
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true}, nil)
	svc.accountRepo = &revocationTestRepo{}
	repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{}}}
	svc.settingService = NewSettingService(repo, svc.cfg)
	account, model := ticketTestAccount(41), "gpt-5.6-sol"
	lateObserver := svc.ticketModelObserver(ctx, account, model)
	setTicketModels(t, svc.settingService, repo, "gpt-6-astra")
	lateObserver.Observe("gpt-5.6-luna", true)
	require.Zero(t, svc.codexTicketVersion(account, model))
	setTicketModels(t, svc.settingService, repo, model)
	svc.ticketModelObserver(ctx, account, model).Observe("gpt-5.6-luna", true)
	require.EqualValues(t, 1, svc.codexTicketVersion(account, model))
	svc.openaiCodexTicketLogs.append(account.ID, model, OpenAICodexTicketLogEntry{Event: "miss"})
	setTicketModels(t, svc.settingService, repo, "gpt-6-astra")
	require.False(t, svc.openAICodexTicketBlocksAccount(account, model))
	logs, err := svc.OpenAICodexTicketLogs(ctx, account, model, time.Now())
	require.NoError(t, err)
	require.Len(t, logs.Entries, 1)
	require.Nil(t, logs.Status)
	setTicketModels(t, svc.settingService, repo, model)
	require.True(t, svc.openAICodexTicketBlocksAccount(account, model))
	require.EqualValues(t, 1, svc.codexTicketVersion(account, model))
}

func TestCodexTicketModelsProbeSelectionAndInflightDiscard(t *testing.T) {
	ctx := context.Background()
	repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{}}}
	settings := NewSettingService(repo, &config.Config{})
	calls := 0
	upstream := &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
		calls++
		setTicketModels(t, settings, repo, "gpt-6-astra")
		return codexTicketResponse(), nil
	}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true, HarvestProxyURL: "http://proxy.example.com:8080"}, upstream)
	svc.settingService = settings
	svc.accountRepo = &revocationTestRepo{}
	account := ticketTestAccount(41)
	setTicketModels(t, settings, repo, "gpt-6-astra")
	svc.probeOnceOpenAICodexTicket(ctx, account, "gpt-5.6-sol")
	_, _, err := svc.fireOpenAICodexTicketProbe(ctx, account, "test-token", "gpt-5.6-sol", "http://proxy.example.com:8080", time.Second)
	require.ErrorIs(t, err, errOpenAICodexTicketModelDisabled)
	require.Zero(t, calls)
	require.Zero(t, svc.codexTicketState(account, "gpt-5.6-sol").probeAttempts)
	setTicketModels(t, settings, repo, "gpt-5.6-sol")
	svc.probeOnceOpenAICodexTicket(ctx, account, "gpt-5.6-sol")
	require.Equal(t, 1, calls)
	require.Nil(t, svc.lookupOpenAICodexTicket(account, "gpt-5.6-sol"))
	require.EqualValues(t, 1, svc.codexTicketState(account, "gpt-5.6-sol").probeAttempts)
	setTicketModels(t, settings, repo, "gpt-5.6-sol")
	// Recovery remains header-driven, regardless of the response model.
	upstream.do = func(*http.Request) (*http.Response, error) { return codexTicketResponse(), nil }
	svc.probeOnceOpenAICodexTicket(ctx, account, "gpt-5.6-sol")
	require.NotNil(t, svc.lookupOpenAICodexTicket(account, "gpt-5.6-sol"))
}
