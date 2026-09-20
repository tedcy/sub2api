package repository

import (
	"context"
	"errors"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// SetOpenAICodexTicketErrorIfTokenMatches stops an account after a ticket request
// rejects its token, without overwriting a concurrent reauthorization or disable.
// The stored token may still be the account snapshot's token when the request used
// a refreshed token from cache, so either observed token is an eligible match.
func (r *accountRepository) SetOpenAICodexTicketErrorIfTokenMatches(
	ctx context.Context,
	id int64,
	accountToken, rejectedToken, errorMsg string,
) (bool, error) {
	if r == nil || r.sql == nil {
		return false, errors.New("account repository SQL executor is not configured")
	}
	accountToken = strings.TrimSpace(accountToken)
	rejectedToken = strings.TrimSpace(rejectedToken)
	if accountToken == "" && rejectedToken == "" {
		return false, nil
	}

	result, err := r.sql.ExecContext(ctx, `
		WITH updated AS (
		UPDATE accounts AS a
		SET status = $1,
			error_message = $2,
			schedulable = FALSE,
			updated_at = NOW()
		WHERE a.id = $3
			AND a.deleted_at IS NULL
			AND a.platform = $4
			AND a.type IN ($5, $6)
			AND a.parent_account_id IS NULL
			AND (a.status = $7 OR (a.status = $1 AND a.error_message = $2))
			AND NULLIF(BTRIM(a.credentials->>'access_token'), '') IS NOT NULL
			AND BTRIM(a.credentials->>'access_token') IN ($8, $9)
		RETURNING a.id
		)
		INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)
		SELECT $10, updated.id, NULL, NULL FROM updated
	`,
		service.StatusError,
		errorMsg,
		id,
		service.PlatformOpenAI,
		service.AccountTypeOAuth,
		service.AccountTypeSetupToken,
		service.StatusActive,
		accountToken,
		rejectedToken,
		service.SchedulerOutboxEventAccountChanged,
	)
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected == 0 {
		return false, err
	}
	r.syncSchedulerAccountSnapshotDetached(ctx, id)
	return true, nil
}
