package service

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCodexTicketTeamTargetLength(t *testing.T) {
	for _, tc := range []struct {
		plan             string
		configured, want int
	}{
		{"team", 292, 332}, {" TEAM ", 318, 332}, {"self_serve_business_prolite", 0, 332},
		{"Self-Serve Business\tProlite", 292, 332}, {"\u00a0TEAM\u00a0", 292, 332},
		{"plus", 0, 292}, {"", 292, 292}, {"unknown", 0, 292}, {"business", 292, 292}, {"free", 318, 318},
	} {
		a := ticketTestAccount(41)
		a.Credentials["plan_type"] = tc.plan
		require.Equal(t, tc.want, openAICodexTicketTargetLength(a, tc.configured), tc.plan)
	}
	require.Equal(t, 292, openAICodexTicketTargetLength(nil, 0))
}

func TestCodexTicketTeamProbeCacheAndGates(t *testing.T) {
	for _, plan := range []string{"team", "self_serve_business_prolite", "plus"} {
		for _, size := range []int{292, 312, 332} {
			t.Run(plan+"/"+strconv.Itoa(size), func(t *testing.T) {
				ctx := context.Background()
				a := ticketTestAccount(41)
				a.Credentials["plan_type"] = plan
				upstream := &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
					resp := codexTicketResponse()
					resp.Header.Set(openAICodexTurnStateHeader, fakeCodexTicketState(size))
					return resp, nil
				}}
				cfg := config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true, HarvestProxyURL: "http://proxy.example.com:8080"}
				svc := ticketTestService(t, cfg, upstream)
				repo := &revocationTestRepo{}
				svc.accountRepo = repo
				model := "gpt-5.6-sol"
				target := openAICodexTicketTargetLength(a, 292)
				valid := size == target
				svc.probeOnceOpenAICodexTicket(ctx, a, model)
				require.Equal(t, !valid, svc.openAICodexTicketBlocksAccount(a, model))
				var binding openAIWSTicketBinding
				h := http.Header{}
				require.Equal(t, !valid, svc.applyOpenAICodexTicket(ctx, a, model, h, &binding) != nil)
				require.Equal(t, !valid, svc.checkOpenAIWSTicket(ctx, a, binding, model, 2, time.Now()) != nil)
				logs, err := svc.OpenAICodexTicketLogs(ctx, a, model, time.Now())
				require.NoError(t, err)
				require.Equal(t, target, logs.TargetLength)
				require.EqualValues(t, 1, logs.Status.ProbeAttempts)
				require.Equal(t, valid, logs.Status.Ready)
				if valid {
					require.Len(t, h.Get(openAICodexTurnStateHeader), target)
					// A fresh service must hydrate the same ticket from persisted extra.
					a.Extra = repo.extra
					fresh := ticketTestService(t, cfg, nil)
					require.False(t, fresh.openAICodexTicketBlocksAccount(a, model))
					ticket := svc.lookupOpenAICodexTicket(a, model)
					require.Error(t, svc.checkOpenAIWSTicket(ctx, a, binding, model, 2, ticket.ExpiresAt))
					statuses := svc.OpenAICodexTicketStatuses(a, cfg, ticket.ExpiresAt)
					for _, status := range statuses {
						if status.Model == model {
							require.False(t, status.Ready)
						}
					}
				}
				// Wrong-length persistent and cached tickets must not bypass the gate.
				wrong := 292
				if target == 292 {
					wrong = 332
				}
				ticket := &openAICodexTicket{Model: model, State: fakeCodexTicketState(wrong), Length: wrong, ExpiresAt: time.Now().Add(time.Hour)}
				a.Extra = map[string]any{openAICodexTicketExtraKey(model): ticket}
				fresh := ticketTestService(t, cfg, nil)
				fresh.openaiCodexTickets.Store(openAICodexTicketKey(a.ID, model), ticket)
				require.True(t, fresh.openAICodexTicketBlocksAccount(a, model))
				fresh.cfg.Gateway.OpenAICodexTicket.FailClosed = false
				require.NoError(t, fresh.applyOpenAICodexTicket(ctx, a, model, http.Header{}))
				fresh.cfg.Gateway.OpenAICodexTicket.FailClosed = true
				fresh.cfg.Gateway.OpenAICodexTicket.Enabled = false
				require.False(t, fresh.openAICodexTicketBlocksAccount(a, model))
			})
		}
	}
}

func TestCodexTicketTeamModelSelectionAndRecovery(t *testing.T) {
	ctx := context.Background()
	a := ticketTestAccount(41)
	a.Credentials["plan_type"] = "team"
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true, Models: []string{"gpt-6-astra"}}, nil)
	svc.accountRepo = &revocationTestRepo{}
	svc.ticketModelObserver(ctx, a, "gpt-5.6-sol").Observe("gpt-5.6-luna", true)
	require.Zero(t, svc.codexTicketVersion(a, "gpt-5.6-sol"))
	require.False(t, svc.openAICodexTicketBlocksAccount(a, "gpt-5.6-sol"))
	model := "gpt-6-astra"
	ticket := &openAICodexTicket{Model: model, State: fakeCodexTicketState(332), Length: 332, ExpiresAt: time.Now().Add(time.Hour)}
	require.True(t, svc.storeOpenAICodexTicket(ctx, a, ticket))
	var old openAIWSTicketBinding
	require.NoError(t, svc.applyOpenAICodexTicket(ctx, a, model, http.Header{}, &old))
	svc.ticketModelObserver(ctx, a, model).Observe("gpt-5.6-luna", true)
	require.True(t, svc.openAICodexTicketBlocksAccount(a, model))
	require.False(t, svc.storeOpenAICodexTicket(ctx, a, ticket, 0))
	require.True(t, svc.storeOpenAICodexTicket(ctx, a, ticket, 1))
	require.Error(t, svc.checkOpenAIWSTicket(ctx, a, old, model, 2, time.Now()))
	var fresh openAIWSTicketBinding
	require.NoError(t, svc.applyOpenAICodexTicket(ctx, a, model, http.Header{}, &fresh))
	require.NoError(t, svc.checkOpenAIWSTicket(ctx, a, fresh, model, 1, time.Now()))
}
