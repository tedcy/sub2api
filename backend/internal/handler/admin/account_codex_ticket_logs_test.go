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

type codexLogsAdminStub struct {
	service.AdminService
	account *service.Account
}

func (s *codexLogsAdminStub) GetAccount(context.Context, int64) (*service.Account, error) {
	return s.account, nil
}

func TestAccountCodexTicketLogsValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, path string
		account    *service.Account
		gateway    bool
		code       int
	}{
		{"valid", "41?model=gpt-6-astra", &service.Account{ID: 41, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}, true, 200},
		{"bad_id", "bad?model=gpt-6-astra", nil, true, 400},
		{"missing_model", "41", nil, true, 400},
		{"not_found", "41?model=gpt-6-astra", nil, true, 404},
		{"unsupported", "41?model=gpt-6-astra", &service.Account{ID: 41, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey}, true, 400},
		{"unknown_model", "41?model=unknown", &service.Account{ID: 41, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}, true, 400},
		{"unavailable", "41?model=gpt-6-astra", &service.Account{ID: 41, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}, false, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &AccountHandler{adminService: &codexLogsAdminStub{account: tc.account}}
			if tc.gateway {
				h.SetCodexTicketGateway(&service.OpenAIGatewayService{})
			}
			router := gin.New()
			router.GET("/accounts/:id", h.GetCodexTicketLogs)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/accounts/"+tc.path, nil))
			require.Equal(t, tc.code, w.Code)
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			if tc.code == 200 {
				require.Contains(t, w.Body.String(), `"entries":[]`)
				require.Contains(t, w.Body.String(), `"limit":200`)
			}
		})
	}
}
