package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// Keep the marker in the server-managed ticket namespace so account edits
// preserve it and API responses and exports redact the credential fingerprints.
const OpenAICodexTicketTokenInvalidExtraKey = openAICodexTicketExtraKeyPrefix + "token_invalid"

var errOpenAICodexTicketTokenInvalid = errors.New("codex ticket harvest stopped after HTTP 401")

type openAICodexTicketErrorSetter interface {
	SetOpenAICodexTicketErrorIfTokenMatches(ctx context.Context, id int64, accountToken, rejectedToken, errorMsg string) (bool, error)
}

type openAICodexTicketTokenInvalidation struct {
	ObservedAt        time.Time `json:"observed_at"`
	RejectedTokenHash string    `json:"rejected_token_hash"`
	AccountTokenHash  string    `json:"account_token_hash"`
}

func openAICodexTicketTokenHash(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (invalidation *openAICodexTicketTokenInvalidation) matches(token string) bool {
	if invalidation == nil || invalidation.ObservedAt.IsZero() || invalidation.RejectedTokenHash == "" {
		return false
	}
	hash := openAICodexTicketTokenHash(token)
	return hash != "" && (hash == invalidation.RejectedTokenHash || hash == invalidation.AccountTokenHash)
}

func parseOpenAICodexTicketTokenInvalidation(account *Account) *openAICodexTicketTokenInvalidation {
	if !isOpenAICodexTicketAccount(account) {
		return nil
	}
	raw := account.Extra[OpenAICodexTicketTokenInvalidExtraKey]
	if invalidation, ok := raw.(*openAICodexTicketTokenInvalidation); ok {
		return invalidation
	}
	if raw == nil {
		return nil
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var invalidation openAICodexTicketTokenInvalidation
	if json.Unmarshal(payload, &invalidation) != nil || invalidation.ObservedAt.IsZero() || invalidation.RejectedTokenHash == "" {
		return nil
	}
	return &invalidation
}

func (s *OpenAIGatewayService) lookupOpenAICodexTicketTokenInvalidation(account *Account) *openAICodexTicketTokenInvalidation {
	invalidation := parseOpenAICodexTicketTokenInvalidation(account)
	if s != nil && account != nil {
		if raw, ok := s.openaiCodexTicketInvalidTokens.Load(account.ID); ok {
			local, _ := raw.(*openAICodexTicketTokenInvalidation)
			if local != nil && (invalidation == nil || local.ObservedAt.After(invalidation.ObservedAt)) {
				invalidation = local
			}
		}
	}
	return invalidation
}

func (s *OpenAIGatewayService) openAICodexTicketTokenInvalid(account *Account) bool {
	return isOpenAICodexTicketAccount(account) && s.lookupOpenAICodexTicketTokenInvalidation(account).matches(account.GetOpenAIAccessToken())
}

func (s *OpenAIGatewayService) stopOpenAICodexTicketHarvestOnUnauthorized(ctx context.Context, account *Account, token string) {
	if s == nil || !isOpenAICodexTicketAccount(account) || strings.TrimSpace(token) == "" {
		return
	}
	// Serialize marker writes and the runtime block with account recovery. A
	// delayed response must neither replace a newer stop nor re-block recovery.
	mu := s.openAIAccountRuntimeBlockLock(account.ID)
	mu.Lock()
	defer mu.Unlock()
	// The provider can return a refreshed or cached token without updating the
	// caller's account snapshot. Recognize both until credentials actually change.
	invalidation := &openAICodexTicketTokenInvalidation{
		ObservedAt:        time.Now().UTC(),
		RejectedTokenHash: openAICodexTicketTokenHash(token),
		AccountTokenHash:  openAICodexTicketTokenHash(account.GetOpenAIAccessToken()),
	}
	previous, tracked := s.openaiCodexTicketInvalidTokens.Swap(account.ID, invalidation)
	if !s.markOpenAICodexTicketAccountErrorLocked(ctx, account, token) {
		if tracked {
			s.openaiCodexTicketInvalidTokens.Store(account.ID, previous)
		} else {
			s.openaiCodexTicketInvalidTokens.Delete(account.ID)
		}
		return
	}
	if s.accountRepo == nil {
		return
	}
	updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.accountRepo.UpdateExtra(updateCtx, account.ID, map[string]any{
		OpenAICodexTicketTokenInvalidExtraKey: invalidation,
	}); err != nil {
		logger.L().Warn("openai_codex_ticket token invalidation persist failed", zap.Int64("account_id", account.ID), zap.Error(err))
	}
}

func (s *OpenAIGatewayService) markOpenAICodexTicketAccountErrorLocked(ctx context.Context, account *Account, token string) bool {
	if s.accountRepo != nil {
		errorCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		const message = "令牌失效，已停止打票"
		var err error
		if repo, ok := s.accountRepo.(openAICodexTicketErrorSetter); ok {
			var applied bool
			applied, err = repo.SetOpenAICodexTicketErrorIfTokenMatches(errorCtx, account.ID, account.GetOpenAIAccessToken(), token, message)
			if err == nil && !applied {
				return false
			}
		} else {
			err = s.accountRepo.SetError(errorCtx, account.ID, message)
		}
		if err != nil {
			logger.L().Warn("openai_codex_ticket set account error failed", zap.Int64("account_id", account.ID), zap.Error(err))
		}
	}
	_, _ = s.blockAccountSchedulingLocked(account, time.Time{}, "codex_ticket_401")
	return true
}
