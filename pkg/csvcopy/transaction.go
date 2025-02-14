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
	transactionRowStateCompleted transactionRowState = "completed"
	transactionRowStateFailed    transactionRowState = "failed"
)

type TransactionRow struct {
	ImportID      string
	StartRow      int64
	RowCount      int
	ByteOffset    int
	ByteLen       int
	CreatedAt     time.Time
	State         transactionRowState
	FailureReason *string
}

// LoadTransaction creates a new transaction for the given fileID starting at row 0
func LoadTransaction(ctx context.Context, conn sqlx.QueryerContext, fileID string) (*Transaction, *TransactionRow, error) {
	row, err := getTransactionRow(ctx, conn, fileID, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load transaction, %w", err)
	}
	return newTransactionAt(Location{
		ImportID:   row.ImportID,
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

func (tr Transaction) setCompleted(ctx context.Context, conn sqlx.ExecerContext) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy_transactions (
		import_id, start_row, row_count, byte_offset, byte_len,
		created_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), 'completed', NULL)
	`
	_, err := conn.ExecContext(ctx, sql, tr.loc.ImportID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen)
	return err
}

func (tr Transaction) setFailed(ctx context.Context, conn sqlx.ExecerContext, reason string) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy_transactions (
		import_id, start_row, row_count, byte_offset, byte_len,
		created_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), 'failed', $6)
	`
	_, err := conn.ExecContext(ctx, sql, tr.loc.ImportID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen, reason)
	return err
}

// Get returns the row stats for the current transaction
func (tr Transaction) Get(ctx context.Context, conn sqlx.QueryerContext) (*TransactionRow, error) {
	return getTransactionRow(ctx, conn, tr.loc.ImportID, tr.loc.StartRow)
}

func getTransactionRow(ctx context.Context, conn sqlx.QueryerContext, fileID string, startRow int64) (*TransactionRow, error) {
	row := &TransactionRow{}

	err := conn.QueryRowxContext(ctx, `
		SELECT import_id, start_row, row_count, byte_offset, byte_len, created_at, state, failure_reason
		FROM timescaledb_parallel_copy_transactions
		WHERE import_id = $1 AND start_row = $2
		LIMIT 1
	`, fileID, startRow).Scan(
		&row.ImportID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.State, &row.FailureReason,
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
func (tr Transaction) Next(ctx context.Context, conn sqlx.QueryerContext) (*Transaction, *TransactionRow, error) {
	row := TransactionRow{}

	err := conn.QueryRowxContext(ctx, `
		SELECT import_id, start_row, row_count, byte_offset, byte_len, created_at, state, failure_reason
		FROM timescaledb_parallel_copy_transactions
		WHERE import_id = $1 AND start_row > $2
		ORDER BY start_row ASC
		LIMIT 1
	`, tr.loc.ImportID, tr.loc.StartRow).Scan(
		&row.ImportID, &row.StartRow, &row.RowCount, &row.ByteOffset, &row.ByteLen, &row.CreatedAt, &row.State, &row.FailureReason,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	next := newTransactionAt(Location{
		ImportID:   row.ImportID,
		StartRow:   row.StartRow,
		RowCount:   row.RowCount,
		ByteOffset: row.ByteOffset,
		ByteLen:    row.ByteLen,
	})

	return next, &row, nil
}

func ensureTransactionTable(ctx context.Context, connString string) error {
	dbx, err := connect(connString)
	if err != nil {
		return fmt.Errorf("failed to connect to database, %w", err)
	}
	defer dbx.Close()

	connx, err := dbx.Connx(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to database, %w", err)
	}
	defer connx.Close()

	sql := `
	CREATE TABLE IF NOT EXISTS timescaledb_parallel_copy_transactions (
		import_id TEXT NOT NULL,
		start_row BIGINT NOT NULL,
		row_count BIGINT NOT NULL,
		byte_offset BIGINT NOT NULL,
		byte_len BIGINT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		state TEXT NOT NULL,
		failure_reason TEXT DEFAULT NULL,
		PRIMARY KEY (import_id, start_row)
	);

	-- Index for efficient lookups and ordering by import_id and start_row
	-- Used when finding the next batch to process for a specific import
	CREATE INDEX IF NOT EXISTS idx_transactions_import_start
	ON timescaledb_parallel_copy_transactions (import_id, start_row);
	`

	_, err = connx.ExecContext(ctx, sql)
	return err
}
