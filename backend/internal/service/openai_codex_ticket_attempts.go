package service

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// All round helpers run under the existing account/model lock. Expiry is the
// round identity: repeated reads of the same expired ticket cannot clear new
// attempts. Revocation uses the existing version barrier for in-flight results.
func (state *openAICodexTicketState) observeProbeTicket(ticket *openAICodexTicket) {
	if ticket != nil && ticket.Version == state.Version && ticket.ExpiresAt.After(state.probeExpiry) {
		state.probeExpiry = ticket.ExpiresAt
		state.probeExpired = false
	}
}

func (state *openAICodexTicketState) expireProbeRound(now time.Time) {
	if !state.probeExpired && !state.probeExpiry.IsZero() && !now.Before(state.probeExpiry) {
		state.probeAttempts = 0
		state.probeExpired = true
	}
}

func (s *OpenAIGatewayService) syncProbeRound(state *openAICodexTicketState, account *Account, now time.Time) {
	if !state.Revoked {
		state.observeProbeTicket(parseOpenAICodexTicketFromAny(account.ID, state.model, account.Extra[openAICodexTicketExtraKey(state.model)]))
		if raw, ok := s.openaiCodexTickets.Load(openAICodexTicketKey(account.ID, state.model)); ok {
			ticket, _ := raw.(*openAICodexTicket)
			state.observeProbeTicket(ticket)
		}
	}
	state.expireProbeRound(now)
}

func (s *OpenAIGatewayService) recordCodexTicketProbe(account *Account, model string, now time.Time) {
	state := s.codexTicketState(account, model)
	state.Lock()
	defer state.Unlock()
	s.syncProbeRound(state, account, now)
	state.probeAttempts++
}

// OpenAICodexTicketStatuses enriches the existing summary from this gateway's
// memory, never from persisted Attempts. It also observes natural expiry when
// the harvester has not started the next probe yet.
func (s *OpenAIGatewayService) OpenAICodexTicketStatuses(account *Account, cfg config.OpenAICodexTicketConfig, now time.Time) []OpenAICodexTicketStatus {
	statuses := OpenAICodexTicketStatuses(account, cfg, now)
	if s == nil {
		return statuses
	}
	for i := range statuses {
		state := s.codexTicketState(account, statuses[i].Model)
		state.Lock()
		s.syncProbeRound(state, account, now)
		statuses[i].ProbeAttempts = state.probeAttempts
		state.Unlock()
	}
	return statuses
}
