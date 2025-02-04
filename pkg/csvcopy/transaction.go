package csvcopy

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

type Transaction struct {
	loc Location
}

type TransactionRowState string

const (
	TransactionRowStatePending   TransactionRowState = "pending"
	TransactionRowStateCompleted TransactionRowState = "completed"
	TransactionRowStateFailed    TransactionRowState = "failed"
)

type TransactionRow struct {
	FileID        string
	StartRow      int64
	RowCount      int
	ByteOffset    int
	ByteLen       int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	State         TransactionRowState
	FailureReason *string
}

func newTransaction(loc Location) Transaction {
	return Transaction{
		loc: loc,
	}
}

func (tr Transaction) setPending(ctx context.Context, conn *sqlx.Conn) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy (
		file_id, start_row, row_count, byte_offset, byte_len,
		created_at, updated_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), 'pending', NULL)
	`
	_, err := conn.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen)
	return err
}

func (tr Transaction) setCompleted(ctx context.Context, tx *sqlx.Tx) error {
	sql := `
	UPDATE timescaledb_parallel_copy
	SET state = 'completed', failure_reason = NULL, updated_at = NOW()
	WHERE file_id = $1 AND start_row = $2
	`
	_, err := tx.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow)
	return err
}

func (tr Transaction) setFailed(ctx context.Context, conn *sqlx.Conn, reason string) error {
	sql := `
	UPDATE timescaledb_parallel_copy
	SET state = 'failed', failure_reason = $1, updated_at = NOW()
	WHERE file_id = $2 AND start_row = $3
	`
	_, err := conn.ExecContext(ctx, sql, reason, tr.loc.FileID, tr.loc.StartRow)
	return err
}

func (tr Transaction) get(ctx context.Context, conn *sqlx.Conn) (TransactionRow, error) {
	row := TransactionRow{}

	err := conn.QueryRowContext(ctx, `
		SELECT file_id, start_row, row_count, byte_offset, byte_len, created_at, updated_at, state, failure_reason
		FROM timescaledb_parallel_copy
		WHERE file_id = $1 AND start_row = $2
		LIMIT 1
	`, tr.loc.FileID, tr.loc.StartRow).Scan(
		&row.FileID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.UpdatedAt, &row.State, &row.FailureReason,
	)

	if err != nil {
		return row, err
	}

	return row, nil
}

func ensureTransactionTable(ctx context.Context, conn *sqlx.Conn) error {
	sql := `
	CREATE TABLE IF NOT EXISTS timescaledb_parallel_copy (
		id SERIAL PRIMARY KEY,
		file_id TEXT NOT NULL,
		start_row BIGINT NOT NULL,
		row_count INT NOT NULL,
		byte_offset INT NOT NULL,
		byte_len INT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
		state TEXT NOT NULL DEFAULT 'pending',
		failure_reason TEXT DEFAULT NULL
	);`
	_, err := conn.ExecContext(ctx, sql)
	return err
}
