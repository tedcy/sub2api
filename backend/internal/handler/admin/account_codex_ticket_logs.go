package admin

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// GetCodexTicketLogs reads the current process's bounded account/model history.
// GET /api/v1/admin/accounts/:id/codex-ticket-logs?model=...
func (h *AccountHandler) GetCodexTicketLogs(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || accountID <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	model := strings.TrimSpace(c.Query("model"))
	if model == "" || len(model) > 256 {
		response.BadRequest(c, "Invalid model")
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), accountID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if account == nil {
		response.NotFound(c, "Account not found")
		return
	}
	if !account.IsOpenAIOAuthLike() || account.IsShadow() {
		response.BadRequest(c, "Account does not support Codex tickets")
		return
	}
	provider := h.codexTicketGateway
	if provider == nil {
		response.Error(c, http.StatusServiceUnavailable, "Codex ticket logs are unavailable")
		return
	}
	logs, err := provider.OpenAICodexTicketLogs(c.Request.Context(), account, model, time.Now())
	if errors.Is(err, service.ErrOpenAICodexTicketLogModel) {
		response.BadRequest(c, "Model is not configured for Codex tickets")
		return
	}
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, logs)
}
