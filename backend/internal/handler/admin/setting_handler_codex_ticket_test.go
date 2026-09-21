package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSettingsCodexTicketProxyWriteReadAndHotReload(t *testing.T) {
	key := service.SettingKeyOpenAICodexTicketHarvestProxyURL
	oldProxy := "http://user:old-secret@old.example.com:8080"
	newProxy := "socks5h://user:new-secret@new.example.com:1080"
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{key: oldProxy})
	require.Equal(t, oldProxy, h.settingService.GetOpenAICodexTicketHarvestProxyURL(context.Background()))
	rec := doUpdateSettings(t, h, map[string]any{key: newProxy}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, newProxy, repo.values[key])
	require.Equal(t, newProxy, h.settingService.GetOpenAICodexTicketHarvestProxyURL(context.Background()))
	require.NotContains(t, rec.Body.String(), "new-secret")
	require.Contains(t, rec.Body.String(), `"openai_codex_ticket_harvest_proxy_configured":true`)
	// Omission, empty input and the masked GET value all preserve the real secret.
	for _, body := range []map[string]any{{"site_name": "updated"}, {key: ""}, {key: service.MaskProxyURL(newProxy)}} {
		rec = doUpdateSettings(t, h, body, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, newProxy, repo.values[key])
	}
	get := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(get)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/settings", nil)
	h.GetSettings(c)
	require.Equal(t, http.StatusOK, get.Code)
	require.NotContains(t, get.Body.String(), "new-secret")
	require.Contains(t, get.Body.String(), "new.example.com")
}

func TestSettingsCodexTicketRejectInvalidProxyWithoutLeakingPassword(t *testing.T) {
	key := service.SettingKeyOpenAICodexTicketHarvestProxyURL
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{key: "http://previous.example.com:8080"})
	rec := doUpdateSettings(t, h, map[string]any{key: "ftp://user:invalid-secret@proxy.example.com:21"}, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "invalid-secret")
	require.Equal(t, "http://previous.example.com:8080", repo.values[key])
}

func TestSettingsCodexTicketModelsWriteReadAndValidation(t *testing.T) {
	key := service.SettingKeyOpenAICodexTicketModels
	enabledKey := service.SettingKeyOpenAICodexTicketEnabled
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{})
	before, err := h.settingService.GetAllSettings(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-6-astra", "gpt-5.6-sol"}, before.OpenAICodexTicketModels)
	// Prime runtime cache to exercise save invalidation.
	h.settingService.GetOpenAICodexTicketModels(context.Background(), before.OpenAICodexTicketModels)
	rec := doUpdateSettings(t, h, map[string]any{enabledKey: true, key: []string{" GPT-6-ASTRA ", "gpt-6-astra"}}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.JSONEq(t, `["gpt-6-astra"]`, repo.values[key])
	require.Contains(t, rec.Body.String(), `"openai_codex_ticket_models":["gpt-6-astra"]`)
	require.Equal(t, []string{"gpt-6-astra"}, h.settingService.GetOpenAICodexTicketModels(context.Background(), before.OpenAICodexTicketModels))
	rec = doUpdateSettings(t, h, map[string]any{"site_name": "unchanged models"}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.JSONEq(t, `["gpt-6-astra"]`, repo.values[key])
	for _, invalid := range [][]string{{}, {"gpt-5.6-luna"}, {""}} {
		rec = doUpdateSettings(t, h, map[string]any{key: invalid}, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.JSONEq(t, `["gpt-6-astra"]`, repo.values[key])
	}
	rec = doUpdateSettings(t, h, map[string]any{enabledKey: false, key: []string{}}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.JSONEq(t, `[]`, repo.values[key])
	rec = doUpdateSettings(t, h, map[string]any{enabledKey: true}, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	after, err := h.settingService.GetAllSettings(context.Background())
	require.NoError(t, err)
	require.Empty(t, after.OpenAICodexTicketModels)
	require.False(t, after.OpenAICodexTicketEnabled)
}
