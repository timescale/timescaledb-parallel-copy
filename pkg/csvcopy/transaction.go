package csvcopy

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

type transaction struct {
	loc Location
}

type transactionRowState string

const (
	transactionRowStatePending   transactionRowState = "pending"
	transactionRowStateCompleted transactionRowState = "completed"
	transactionRowStateFailed    transactionRowState = "failed"
)

type transactionRow struct {
	FileID        string
	StartRow      int64
	RowCount      int
	ByteOffset    int
	ByteLen       int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	State         transactionRowState
	FailureReason *string
}

// newTransaction creates a new transaction for the given fileID starting at row 0
func newTransaction(fileID string) transaction {
	return transaction{
		loc: Location{
			FileID: fileID,
		},
	}
}

// newTransaction creates a new transaction for the given fileID starting at given location.
func newTransactionAt(loc Location) transaction {
	return transaction{
		loc: loc,
	}
}

func (tr transaction) setPending(ctx context.Context, conn *sqlx.Conn) error {
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

func (tr transaction) setCompleted(ctx context.Context, tx *sqlx.Tx) error {
	sql := `
	UPDATE timescaledb_parallel_copy
	SET state = 'completed', failure_reason = NULL, updated_at = NOW()
	WHERE file_id = $1 AND start_row = $2
	`
	_, err := tx.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow)
	return err
}

func (tr transaction) setFailed(ctx context.Context, conn *sqlx.Conn, reason string) error {
	sql := `
	UPDATE timescaledb_parallel_copy
	SET state = 'failed', failure_reason = $1, updated_at = NOW()
	WHERE file_id = $2 AND start_row = $3
	`
	_, err := conn.ExecContext(ctx, sql, reason, tr.loc.FileID, tr.loc.StartRow)
	return err
}

// get returns the row stats for the current transaction
func (tr transaction) get(ctx context.Context, conn *sqlx.Conn) (*transactionRow, error) {
	row := &transactionRow{}

	err := conn.QueryRowContext(ctx, `
		SELECT file_id, start_row, row_count, byte_offset, byte_len, created_at, updated_at, state, failure_reason
		FROM timescaledb_parallel_copy
		WHERE file_id = $1 AND start_row = $2
		LIMIT 1
	`, tr.loc.FileID, tr.loc.StartRow).Scan(
		&row.FileID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.UpdatedAt, &row.State, &row.FailureReason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return row, err
	}

	return row, nil
}

// next returns the next transaction in the sequence.
// If it returns nil, it means there is no next transaction
func (tr transaction) next(ctx context.Context, conn *sqlx.Conn) (*transaction, error) {
	row := transactionRow{}

	err := conn.QueryRowContext(ctx, `
		SELECT file_id, start_row, row_count, byte_offset, byte_len, created_at, updated_at, state, failure_reason
		FROM timescaledb_parallel_copy
		WHERE file_id = $1 AND start_row > $2
		ORDER BY start_row ASC
		LIMIT 1
	`, tr.loc.FileID, tr.loc.StartRow).Scan(
		&row.FileID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.UpdatedAt, &row.State, &row.FailureReason,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	next := newTransactionAt(Location{
		FileID:     row.FileID,
		StartRow:   row.StartRow,
		RowCount:   row.RowCount,
		ByteOffset: row.ByteOffset,
		ByteLen:    row.ByteLen,
	})

	return &next, nil
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
