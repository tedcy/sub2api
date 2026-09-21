package service

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type openAICodexTicketRevocation struct {
	Version uint64    `json:"version"`
	Revoked bool      `json:"revoked"`
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at,omitempty"`
}

type openAICodexTicketState struct {
	sync.Mutex
	openAICodexTicketRevocation
	dirty     bool
	accountID int64
	model     string
	// Memory-only round state. The expiry watermark prevents stale snapshots
	// from resetting a round twice; probe completion never writes the count.
	probeAttempts uint64
	probeExpiry   time.Time
	probeExpired  bool
}

func codexTicketRevocationKey(model string) string {
	return "codex_turn_ticket_revocation:" + normalizeOpenAICodexTicketModel(model)
}

func parseCodexTicketRevocation(account *Account, model string) (r openAICodexTicketRevocation) {
	if account != nil {
		b, _ := json.Marshal(account.Extra[codexTicketRevocationKey(model)])
		_ = json.Unmarshal(b, &r)
	}
	return
}

func (s *OpenAIGatewayService) codexTicketState(account *Account, model string) *openAICodexTicketState {
	model = normalizeOpenAICodexTicketModel(model)
	initial := &openAICodexTicketState{openAICodexTicketRevocation: parseCodexTicketRevocation(account, model), accountID: account.ID, model: model}
	actual, _ := s.openaiCodexTicketStates.LoadOrStore(openAICodexTicketKey(account.ID, model), initial)
	return actual.(*openAICodexTicketState)
}

func (s *OpenAIGatewayService) codexTicketVersion(account *Account, model string) uint64 {
	state := s.codexTicketState(account, model)
	state.Lock()
	defer state.Unlock()
	return state.Version
}

func isCodexTicketDowngrade(sent, observed string) bool {
	sent = normalizeOpenAICodexTicketModel(sent)
	return (sent == openAICodexTicketDefaultModel || sent == openAICodexTicketDefaultSolModel) && strings.EqualFold(strings.TrimSpace(observed), "gpt-5.6-luna")
}

// Caller holds the per-account/model lock. Publish recovery only after success.
func (s *OpenAIGatewayService) persistCodexTicketState(ctx context.Context, state *openAICodexTicketState, ticket *openAICodexTicket, rev openAICodexTicketRevocation) bool {
	if s.accountRepo == nil {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.accountRepo.UpdateExtra(ctx, state.accountID, map[string]any{
		codexTicketRevocationKey(state.model):  rev,
		openAICodexTicketExtraKey(state.model): ticket,
	}); err != nil {
		logger.FromContext(ctx).Warn("openai_codex_ticket persist failed", zap.Int64("account_id", state.accountID), zap.String("model", state.model), zap.Uint64("version", rev.Version), zap.Error(err))
		return false
	}
	return true
}

func (s *OpenAIGatewayService) revokeCodexTicket(ctx context.Context, account *Account, sent, observed string, version uint64) {
	if !isOpenAICodexTicketAccount(account) || !s.openAICodexTicketGatedModelContext(ctx, sent) || !isCodexTicketDowngrade(sent, observed) {
		return
	}
	state := s.codexTicketState(account, sent)
	state.Lock()
	defer state.Unlock()
	// Duplicate/late responses cannot revoke a newly validated generation.
	if state.Revoked || state.Version != version {
		return
	}
	state.Version++
	state.Revoked = true
	state.Reason = "upstream_model_downgrade"
	state.At = time.Now()
	state.probeAttempts = 0
	state.probeExpired = true
	s.openaiCodexTickets.Delete(openAICodexTicketKey(account.ID, state.model))
	state.dirty = true
	logger.FromContext(ctx).Warn("openai_codex_ticket revoked", zap.Int64("account_id", account.ID), zap.String("sent_model", state.model), zap.String("response_model", strings.TrimSpace(observed)), zap.Uint64("version", state.Version))
	state.dirty = !s.persistCodexTicketState(context.WithoutCancel(ctx), state, nil, state.openAICodexTicketRevocation)
}

func (s *OpenAIGatewayService) retryCodexTicketRevocations(ctx context.Context) {
	if s == nil {
		return
	}
	s.openaiCodexTicketStates.Range(func(_, value any) bool {
		state := value.(*openAICodexTicketState)
		state.Lock()
		if state.dirty {
			state.dirty = !s.persistCodexTicketState(ctx, state, nil, state.openAICodexTicketRevocation)
		}
		state.Unlock()
		return ctx.Err() == nil
	})
}

func (s *OpenAIGatewayService) ticketModelObserver(ctx context.Context, account *Account, model string) *upstreamResponseModelObserver {
	o := &upstreamResponseModelObserver{}
	if !isOpenAICodexTicketAccount(account) {
		return o
	}
	version := s.codexTicketVersion(account, model)
	var once sync.Once
	o.onModel = func(observed string) {
		if isCodexTicketDowngrade(model, observed) {
			once.Do(func() { s.revokeCodexTicket(ctx, account, model, observed, version) })
		}
	}
	return o
}

func (s *OpenAIGatewayService) bindTicketModelObserver(ctx context.Context, c *gin.Context, account *Account, model string) {
	if c != nil {
		c.Set(upstreamResponseModelObserverContextKey, s.ticketModelObserver(ctx, account, model))
	}
}
