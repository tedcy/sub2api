package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

func normalizeCodexTicketModels(models []string) ([]string, error) {
	out := make([]string, 0, len(models))
	for _, model := range models {
		model = normalizeOpenAICodexTicketModel(model)
		if model != openAICodexTicketDefaultModel && model != openAICodexTicketDefaultSolModel {
			return nil, fmt.Errorf("unsupported ticket model: %q", model)
		}
		if !slices.Contains(out, model) {
			out = append(out, model)
		}
	}
	return out, nil
}

func (s *SettingService) defaultCodexTicketModels() []string {
	if s != nil && s.cfg != nil && len(s.cfg.Gateway.OpenAICodexTicket.Models) > 0 {
		return slices.Clone(s.cfg.Gateway.OpenAICodexTicket.Models)
	}
	return []string{openAICodexTicketDefaultModel, openAICodexTicketDefaultSolModel}
}

type cachedOpenAICodexTicketModels struct {
	models    []string
	expiresAt int64
}

// Missing settings retain yaml/env defaults; an explicit [] must remain empty.
func (s *SettingService) GetOpenAICodexTicketModels(ctx context.Context, fallback []string) []string {
	if s == nil || s.settingRepo == nil {
		return slices.Clone(fallback)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if cached, ok := s.openAICodexTicketModelsCache.Load().(*cachedOpenAICodexTicketModels); ok && time.Now().UnixNano() < cached.expiresAt {
		return slices.Clone(cached.models)
	}
	ch := s.openAICodexTicketModelsSF.DoChan(SettingKeyOpenAICodexTicketModels, func() (any, error) {
		if cached, ok := s.openAICodexTicketModelsCache.Load().(*cachedOpenAICodexTicketModels); ok && time.Now().UnixNano() < cached.expiresAt {
			return cached.models, nil
		}
		dbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		raw, err := s.settingRepo.GetValue(dbCtx, SettingKeyOpenAICodexTicketModels)
		models := slices.Clone(fallback)
		if err == nil && raw != "" {
			err = json.Unmarshal([]byte(raw), &models)
			if err == nil {
				models, err = normalizeCodexTicketModels(models)
			}
		}
		ttl := openAICodexTicketEnabledCacheTTL
		if err != nil && !errors.Is(err, ErrSettingNotFound) {
			ttl = time.Second
			models = slices.Clone(fallback)
			if cached, ok := s.openAICodexTicketModelsCache.Load().(*cachedOpenAICodexTicketModels); ok {
				models = cached.models
			}
		}
		s.openAICodexTicketModelsCache.Store(&cachedOpenAICodexTicketModels{models: models, expiresAt: time.Now().Add(ttl).UnixNano()})
		return models, nil
	})
	select {
	case <-ctx.Done():
		return slices.Clone(fallback)
	case result := <-ch:
		if models, ok := result.Val.([]string); ok {
			return slices.Clone(models)
		}
		return slices.Clone(fallback)
	}
}

func (s *SettingService) InvalidateOpenAICodexTicketModelsCache() {
	if s == nil {
		return
	}
	s.openAICodexTicketModelsSF.Forget(SettingKeyOpenAICodexTicketModels)
	if cached, ok := s.openAICodexTicketModelsCache.Load().(*cachedOpenAICodexTicketModels); ok {
		s.openAICodexTicketModelsCache.Store(&cachedOpenAICodexTicketModels{models: cached.models})
	}
}
