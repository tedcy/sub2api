package service

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// This server-managed marker stops synthetic probes without installing an
// account/model cooldown. Business requests still observe upstream rate limits.
const OpenAICodexTicketHarvestPauseExtraKey = openAICodexTicketExtraKeyPrefix + "harvest_pause"

type openAICodexTicketHarvestPause struct {
	ObservedAt time.Time      `json:"observed_at"`
	Usage      map[string]any `json:"usage"`
}

func parseOpenAICodexTicketHarvestPause(account *Account) *openAICodexTicketHarvestPause {
	if !isOpenAICodexTicketAccount(account) {
		return nil
	}
	raw := account.Extra[OpenAICodexTicketHarvestPauseExtraKey]
	if pause, ok := raw.(*openAICodexTicketHarvestPause); ok {
		return pause
	}
	if raw == nil {
		return nil
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var pause openAICodexTicketHarvestPause
	if json.Unmarshal(payload, &pause) != nil || pause.ObservedAt.IsZero() {
		return nil
	}
	return &pause
}

func (pause *openAICodexTicketHarvestPause) active(account *Account, now time.Time) bool {
	if pause == nil || pause.ObservedAt.IsZero() || !isOpenAICodexTicketAccount(account) {
		return false
	}
	original5h, original7d := openAICanonicalQuotaWindows(pause.Usage, now)
	current5h, current7d := openAICanonicalQuotaWindows(account.Extra, now)
	observedUsageAt := parseSchedulingResetAt(pause.Usage["codex_usage_updated_at"])
	currentUsageAt := parseSchedulingResetAt(account.Extra["codex_usage_updated_at"])
	// An older scheduler snapshot must not undo a newly observed 429. Missing
	// timestamps remain compatible with accounts whose usage was stored manually.
	useCurrent := openAICodexSnapshotIdentityTrusted(account) &&
		(observedUsageAt == nil || (currentUsageAt != nil && !currentUsageAt.Before(*observedUsageAt)))
	activeWindow := func(original, current openAICanonicalQuotaWindow, maxAge time.Duration) bool {
		if !original.hasUsed || !(original.usedPercent >= 100) || original.reset || !now.Before(pause.ObservedAt.Add(maxAge)) {
			return false
		}
		return !useCurrent || !current.hasUsed || (current.usedPercent >= 100 && !current.reset)
	}
	return activeWindow(original5h, current5h, 5*time.Hour) || activeWindow(original7d, current7d, 7*24*time.Hour)
}

func (s *OpenAIGatewayService) openAICodexTicketHarvestPaused(account *Account, now time.Time) bool {
	pause := parseOpenAICodexTicketHarvestPause(account)
	if s != nil && account != nil {
		if raw, ok := s.openaiCodexTicketQuotaPauses.Load(account.ID); ok {
			local, _ := raw.(*openAICodexTicketHarvestPause)
			if local != nil && (pause == nil || local.ObservedAt.After(pause.ObservedAt)) {
				pause = local
			}
		}
	}
	return pause.active(account, now)
}

func (s *OpenAIGatewayService) codexTicketHarvestSkipReason(account *Account, now time.Time) string {
	if s.openAICodexTicketTokenInvalid(account) {
		return "token_invalid"
	}
	if s.openAICodexTicketHarvestPaused(account, now) {
		return "quota_exhausted"
	}
	if account.IsRateLimited() {
		return "rate_limited"
	}
	return ""
}

func (s *OpenAIGatewayService) pauseOpenAICodexTicketHarvestOnQuota(ctx context.Context, account *Account, headers http.Header) {
	if s == nil || !isOpenAICodexTicketAccount(account) {
		return
	}
	now := time.Now()
	usage := make(map[string]any)
	if openAICodexSnapshotIdentityTrusted(account) {
		for key, value := range account.Extra {
			if key == "codex_usage_updated_at" || strings.HasPrefix(key, "codex_5h_") || strings.HasPrefix(key, "codex_7d_") ||
				strings.HasPrefix(key, "codex_primary_") || strings.HasPrefix(key, "codex_secondary_") {
				usage[key] = value
			}
		}
		// A partial response may advance the snapshot time without updating both
		// windows. Anchor inherited countdowns before merging the new headers.
		for _, window := range []string{"5h", "7d", "primary", "secondary"} {
			if resetAt, ok := openAICodexWindowResetAt(usage, window); ok {
				usage["codex_"+window+"_reset_at"] = resetAt.UTC().Format(time.RFC3339Nano)
			}
		}
	}
	if snapshot := ParseCodexRateLimitHeaders(headers); snapshot != nil {
		updates := buildCodexUsageExtraUpdates(snapshot, now)
		for _, window := range []string{"5h", "7d"} {
			if _, hasUsed := updates["codex_"+window+"_used_percent"]; !hasUsed {
				continue
			}
			// New usage without a reset must not inherit an expired cached reset.
			delete(usage, "codex_"+window+"_reset_at")
			delete(usage, "codex_"+window+"_reset_after_seconds")
		}
		maps.Copy(usage, updates)
		usage["codex_usage_updated_at"] = now.UTC().Format(time.RFC3339Nano)
	}
	pause := &openAICodexTicketHarvestPause{ObservedAt: now, Usage: usage}
	if !pause.active(account, now) {
		return
	}
	s.openaiCodexTicketQuotaPauses.Store(account.ID, pause)
	if s.accountRepo == nil {
		return
	}
	// Persist only the probe marker. Updating the global usage snapshot here can
	// preemptively auto-pause an account before a real request reaches upstream.
	updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.accountRepo.UpdateExtra(updateCtx, account.ID, map[string]any{
		OpenAICodexTicketHarvestPauseExtraKey: pause,
	}); err != nil {
		logger.L().Warn("openai_codex_ticket quota pause persist failed", zap.Int64("account_id", account.ID), zap.Error(err))
	}
}
