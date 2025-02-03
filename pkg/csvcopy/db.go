package csvcopy

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// connect returns a SQLX database corresponding to the provided connection
// string/URL, env variables, and any provided overrides.
func connect(connStr string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("pgx/v5", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	return db, nil
}

// copyFromLines bulk-loads data using the given copyCmd. lines must provide a
// set of complete lines of CSV data, including the end-of-line delimiters.
// Returns the number of rows inserted.
func copyFromLines(ctx context.Context, conn *sql.Conn, lines io.Reader, copyCmd string) (int64, error) {
	var rowCount int64
	// pgx requires us to use the low-level API for a raw COPY FROM operation.
	err := conn.Raw(func(driverConn interface{}) error {
		// Unfortunately there are three layers to unwrap here: the stdlib.Conn,
		// the pgx.Conn, and the pgconn.PgConn.
		pg := driverConn.(*stdlib.Conn).Conn().PgConn()

		result, err := pg.CopyFrom(ctx, lines, copyCmd)
		if err != nil {
			return err
		}

		rowCount = result.RowsAffected()
		return nil
	})
	return rowCount, err
}

// copyFromBatch bulk-loads data using the given copyCmd. lines must provide a
// set of complete lines of CSV data, including the end-of-line delimiters.
// Returns the number of rows inserted.
// It uses Location.FileID to insert a control row
func copyFromBatch(ctx context.Context, db *sqlx.DB, batch Batch, copyCmd string) (int64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquiring DB connection for COPY: %w", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	defer tx.Rollback()

	err = insertControlRow(ctx, tx, batch.Location)
	if err != nil {
		return 0, fmt.Errorf("failed to insert control row, %w", err)
	}

	rowCount, err := copyFromLines(ctx, conn, &batch.Data, copyCmd)
	if err != nil {
		return rowCount, err
	}

	return rowCount, tx.Commit()
}

func insertControlRow(ctx context.Context, tx *sql.Tx, location Location) error {
	sql := `
	INSERT INTO timescaledb_parallel_copy (
		file_id, start_row, row_count, byte_offset, byte_len,
		created_at, updated_at, state, failure_reason
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), 'pending', NULL)
	`
	_, err := tx.ExecContext(ctx, sql, location.FileID, location.StartRow, location.RowCount, location.ByteOffset, location.ByteLen)
	return err
}

func completedControlRow(ctx context.Context, tx *sql.Tx, location Location) error {
	sql := `
	UPDATE timescaledb_parallel_copy
	SET state = 'completed', failure_reason = NULL, updated_at = NOW()
	WHERE file_id = $1 AND start_row = $2
	`
	_, err := tx.ExecContext(ctx, sql, location.FileID, location.StartRow)
	return err
}

func failedControlRow(ctx context.Context, tx *sql.Tx, location Location, reason string) error {
	sql := `
	UPDATE timescaledb_parallel_copy
	SET state = 'failed', failure_reason = $1, updated_at = NOW()
	WHERE file_id = $2 AND start_row = $3
	`
	_, err := tx.ExecContext(ctx, sql, reason, location.FileID, location.StartRow)
	return err
}

func ensureControlTable(ctx context.Context, conn *sqlx.DB) error {
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
