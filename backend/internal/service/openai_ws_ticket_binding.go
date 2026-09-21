package service

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	coderws "github.com/coder/websocket"
	"go.uber.org/zap"
)

// A refreshed cache ticket cannot renew an already established WS handshake.
// Keep only the identity and deadline, never the secret turn-state value.
type openAIWSTicketBinding struct {
	version   uint64
	model     string
	expiresAt time.Time
}

func (s *OpenAIGatewayService) checkOpenAIWSTicket(ctx context.Context, account *Account, binding openAIWSTicketBinding, model string, turn int, now time.Time) error {
	model = normalizeOpenAICodexTicketModel(model)
	if !isOpenAICodexTicketAccount(account) || !s.openAICodexTicketEnabledContext(ctx) || !s.openAICodexTicketConfig().FailClosed || !s.openAICodexTicketGatedModel(model) {
		return nil
	}
	reason := ""
	switch {
	case binding.version != s.codexTicketVersion(account, model):
		reason = "handshake_ticket_revoked"
	case binding.model != model:
		reason = "handshake_ticket_model_mismatch"
	case !now.Before(binding.expiresAt):
		reason = "handshake_ticket_expired"
	case !s.lookupOpenAICodexTicket(account, model).valid(now, openAICodexTicketTargetLength(account, s.openAICodexTicketConfig().TargetLength)):
		reason = "current_ticket_unavailable"
	}
	if reason == "" {
		return nil
	}
	logger.FromContext(ctx).Info("openai.websocket_ticket_reconnect_required",
		zap.Int64("account_id", account.ID), zap.Int("turn", turn),
		zap.String("model", model), zap.String("reason", reason),
		zap.Time("handshake_ticket_expires_at", binding.expiresAt))
	return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "model ticket unavailable for this connection, please reconnect", ErrOpenAICodexTicketUnavailable)
}
