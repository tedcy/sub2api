package repository

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAccountRepository_SetOpenAICodexTicketErrorIfTokenMatches_GuardsAccountAndToken(t *testing.T) {
	exec := &recordingSQLExecutor{result: rowsAffectedResult(0)}
	repo := newAccountRepositoryWithSQL(nil, exec, nil)

	applied, err := repo.SetOpenAICodexTicketErrorIfTokenMatches(
		context.Background(), 42, "  account-snapshot-token  ", "\nrejected-cached-token\t", "令牌失效，已停止打票",
	)

	require.NoError(t, err)
	require.False(t, applied)
	require.Len(t, exec.execQueries, 1, "account change and conditional outbox insert must be atomic")
	query := normalizeSQLWhitespace(exec.execQueries[0])
	require.Contains(t, query, "WITH updated AS ( UPDATE accounts AS a")
	require.Contains(t, query, "SET status = $1, error_message = $2, schedulable = FALSE, updated_at = NOW()")
	require.Contains(t, query, "WHERE a.id = $3 AND a.deleted_at IS NULL")
	require.Contains(t, query, "a.platform = $4 AND a.type IN ($5, $6)")
	require.Contains(t, query, "a.parent_account_id IS NULL", "shadow accounts inherit their parent's credentials")
	require.Contains(t, query, "(a.status = $7 OR (a.status = $1 AND a.error_message = $2))",
		"a late 401 must not overwrite a disabled account or another error")
	require.Contains(t, query, "NULLIF(BTRIM(a.credentials->>'access_token'), '') IS NOT NULL",
		"an absent or empty stored token must never match an empty input")
	require.Contains(t, query, "BTRIM(a.credentials->>'access_token') IN ($8, $9)",
		"a late 401 must not overwrite credentials replaced after the upstream request started")
	require.Contains(t, query, "INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)")
	require.Contains(t, query, "SELECT $10, updated.id, NULL, NULL FROM updated",
		"no match must not enqueue an account change")
	require.Equal(t, []any{
		service.StatusError, "令牌失效，已停止打票", int64(42), service.PlatformOpenAI,
		service.AccountTypeOAuth, service.AccountTypeSetupToken, service.StatusActive,
		"account-snapshot-token", "rejected-cached-token", service.SchedulerOutboxEventAccountChanged,
	}, exec.execArgs[0])
}

func TestAccountRepository_SetOpenAICodexTicketErrorIfTokenMatches_Result(t *testing.T) {
	dbErr := errors.New("account update failed")
	rowsErr := errors.New("rows affected failed")
	tests := []struct {
		name    string
		result  sql.Result
		execErr error
		want    bool
		wantErr error
	}{
		{name: "changed account and wrote outbox", result: sqlmock.NewResult(0, 1), want: true},
		{name: "concurrent token or status change has no match", result: sqlmock.NewResult(0, 0)},
		{name: "database failure", execErr: dbErr, wantErr: dbErr},
		{name: "rows affected failure", result: sqlmock.NewErrorResult(rowsErr), wantErr: rowsErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			expectation := mock.ExpectExec(`WITH updated AS \(.*UPDATE accounts AS a.*INSERT INTO scheduler_outbox.*FROM updated`).
				WithArgs(service.StatusError, "令牌失效，已停止打票", int64(42),
					service.PlatformOpenAI, service.AccountTypeOAuth, service.AccountTypeSetupToken,
					service.StatusActive, "account-token", "rejected-token", service.SchedulerOutboxEventAccountChanged)
			if tt.execErr != nil {
				expectation.WillReturnError(tt.execErr)
			} else {
				expectation.WillReturnResult(tt.result)
			}
			repo := newAccountRepositoryWithSQL(nil, db, nil)

			applied, err := repo.SetOpenAICodexTicketErrorIfTokenMatches(
				context.Background(), 42, "account-token", "rejected-token", "令牌失效，已停止打票",
			)

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, applied)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestAccountRepository_SetOpenAICodexTicketErrorIfTokenMatches_EmptyTokensSkipMutation(t *testing.T) {
	exec := &recordingSQLExecutor{result: rowsAffectedResult(1)}
	repo := newAccountRepositoryWithSQL(nil, exec, nil)

	applied, err := repo.SetOpenAICodexTicketErrorIfTokenMatches(context.Background(), 42, " \n\t", "\r ", "expired")

	require.NoError(t, err)
	require.False(t, applied)
	require.Empty(t, exec.execQueries)
}

func TestAccountRepository_SetOpenAICodexTicketErrorIfTokenMatches_RequiresSQLExecutor(t *testing.T) {
	for _, repo := range []*accountRepository{nil, {}} {
		applied, err := repo.SetOpenAICodexTicketErrorIfTokenMatches(context.Background(), 42, "token", "token", "expired")
		require.Error(t, err)
		require.False(t, applied)
	}
}
