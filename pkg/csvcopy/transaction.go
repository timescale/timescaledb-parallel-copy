package csvcopy

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
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
func LoadTransaction(ctx context.Context, conn sqlx.QueryerContext, fileID string) (*Transaction, *TransactionRow, error) {
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

func (tr Transaction) setCompleted(ctx context.Context, conn sqlx.ExecerContext) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy (
		file_id, start_row, row_count, byte_offset, byte_len,
		created_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), 'completed', NULL)
	`
	_, err := conn.ExecContext(ctx, sql, tr.loc.FileID, tr.loc.StartRow, tr.loc.RowCount, tr.loc.ByteOffset, tr.loc.ByteLen)
	return err
}

func (tr Transaction) setFailed(ctx context.Context, conn sqlx.ExecerContext, reason string) error {
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
func (tr Transaction) Get(ctx context.Context, conn sqlx.QueryerContext) (*TransactionRow, error) {
	return getTransactionRow(ctx, conn, tr.loc.FileID, tr.loc.StartRow)
}

func getTransactionRow(ctx context.Context, conn sqlx.QueryerContext, fileID string, startRow int64) (*TransactionRow, error) {
	row := &TransactionRow{}

	err := conn.QueryRowxContext(ctx, `
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
func (tr Transaction) Next(ctx context.Context, conn sqlx.QueryerContext) (*Transaction, *TransactionRow, error) {
	row := TransactionRow{}

	err := conn.QueryRowxContext(ctx, `
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

//go:embed migrations/*
var migrations embed.FS

const DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

func ensureTransactionTable(connString string) error {
	dbx, err := connect(connString)
	if err != nil {
		return fmt.Errorf("failed to connect to database, %w", err)
	}

	defer dbx.Close()
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to load migration source, %w", err)
	}

	instance, err := pgx.WithInstance(dbx.DB, &pgx.Config{
		MigrationsTable:       "timescaledb_parallel_copy_migrations",
		StatementTimeout:      10 * time.Second,
		MultiStatementMaxSize: DefaultMultiStatementMaxSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create target db instance, %w", err)
	}

	m, err := migrate.NewWithInstance("embed", source, "target_db", instance)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance, %w", err)
	}

	err = m.Up()
	if err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return nil
		}
		return err
	}
	return nil
}
