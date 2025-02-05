package csvcopy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Transaction struct {
	loc Location
}

type transactionRowState string

const (
	transactionRowStatePending   transactionRowState = "pending"
	transactionRowStateCompleted transactionRowState = "completed"
	transactionRowStateFailed    transactionRowState = "failed"
)

type TransactionRow struct {
	FileID        string
	StartRow      int64
	RowCount      int
	ByteOffset    int
	ByteLen       int
	CreatedAt     time.Time
	State         transactionRowState
	FailureReason *string
}

// LoadTransaction creates a new transaction for the given fileID starting at row 0
func LoadTransaction(ctx context.Context, conn *sqlx.Conn, fileID string) (*Transaction, *TransactionRow, error) {
	row, err := getTransactionRow(ctx, conn, fileID, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load transaction, %w", err)
	}
	return newTransactionAt(Location{
		FileID:     row.FileID,
		StartRow:   row.StartRow,
		RowCount:   row.RowCount,
		ByteOffset: row.ByteOffset,
		ByteLen:    row.ByteLen,
	}), row, nil
}

// newTransaction creates a new transaction for the given fileID starting at given location.
func newTransactionAt(loc Location) *Transaction {
	return &Transaction{
		loc: loc,
	}
}

func (tr Transaction) setProcessing(ctx context.Context, tx *sqlx.Tx) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy (
		file_id, start_row, row_count, byte_offset, byte_len,
		created_at,  state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), 'processing', NULL)
	`
	_, err := tx.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen)
	return err
}

func (tr Transaction) setCompleted(ctx context.Context, tx *sqlx.Tx) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy (
		file_id, start_row, row_count, byte_offset, byte_len,
		created_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), 'completed', NULL)
	`
	_, err := tx.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen)
	return err
}

func (tr Transaction) setFailed(ctx context.Context, conn *sqlx.Conn, reason string) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy (
		file_id, start_row, row_count, byte_offset, byte_len,
		created_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), 'failed', $6)
	`
	_, err := conn.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen, reason)
	return err
}

// Get returns the row stats for the current transaction
func (tr Transaction) Get(ctx context.Context, conn *sqlx.Conn) (*TransactionRow, error) {
	return getTransactionRow(ctx, conn, tr.loc.FileID, tr.loc.StartRow)
}

func getTransactionRow(ctx context.Context, conn *sqlx.Conn, fileID string, startRow int64) (*TransactionRow, error) {
	row := &TransactionRow{}

	err := conn.QueryRowContext(ctx, `
		SELECT file_id, start_row, row_count, byte_offset, byte_len, created_at, state, failure_reason
		FROM timescaledb_parallel_copy
		WHERE file_id = $1 AND start_row = $2
		LIMIT 1
	`, fileID, startRow).Scan(
		&row.FileID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.State, &row.FailureReason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return row, err
	}

	return row, nil
}

// Next returns the Next transaction in the sequence.
// If it returns nil, it means there is no Next transaction
func (tr Transaction) Next(ctx context.Context, conn *sqlx.Conn) (*Transaction, *TransactionRow, error) {
	row := TransactionRow{}

	err := conn.QueryRowContext(ctx, `
		SELECT file_id, start_row, row_count, byte_offset, byte_len, created_at, state, failure_reason
		FROM timescaledb_parallel_copy
		WHERE file_id = $1 AND start_row > $2
		ORDER BY start_row ASC
		LIMIT 1
	`, tr.loc.FileID, tr.loc.StartRow).Scan(
		&row.FileID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.State, &row.FailureReason,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	next := newTransactionAt(Location{
		FileID:     row.FileID,
		StartRow:   row.StartRow,
		RowCount:   row.RowCount,
		ByteOffset: row.ByteOffset,
		ByteLen:    row.ByteLen,
	})

	return next, &row, nil
}

func ensureTransactionTable(ctx context.Context, conn *sqlx.Conn) error {
	sql := `
	CREATE TABLE IF NOT EXISTS timescaledb_parallel_copy (
		file_id TEXT NOT NULL,
		start_row BIGINT NOT NULL,
		row_count BIGINT NOT NULL,
		byte_offset BIGINT NOT NULL,
		byte_len BIGINT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		state TEXT NOT NULL DEFAULT 'pending',
		failure_reason TEXT DEFAULT NULL,
		UNIQUE (file_id, start_row)
	);`
	_, err := conn.ExecContext(ctx, sql)
	return err
}
