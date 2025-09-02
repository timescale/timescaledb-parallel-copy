package csvcopy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

// BatchHandlerLog prints a log line that reports the error in the given batch
func BatchHandlerLog(log Logger, next BatchErrorHandler) BatchErrorHandler {
	return BatchErrorHandler(func(ctx context.Context, c *Copier, db *sqlx.Conn, batch Batch, reason error) *BatchError {
		log.Infof("Batch %d, starting at byte %d with len %d, has error: %s", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen, reason.Error())

		if next != nil {
			return next(ctx, c, db, batch, reason)
		}
		return NewErrContinue(reason)
	})
}

// BatchHandlerNoop no operation
func BatchHandlerNoop() BatchErrorHandler {
	return BatchErrorHandler(func(_ context.Context, _ *Copier, _ *sqlx.Conn, _ Batch, reason error) *BatchError {
		return NewErrContinue(reason)
	})
}

// BatchHandlerError fails the process
func BatchHandlerError() BatchErrorHandler {
	return BatchErrorHandler(func(_ context.Context, _ *Copier, _ *sqlx.Conn, _ Batch, reason error) *BatchError {
		return NewErrStop(reason)
	})
}

// generateRandomTableSuffix creates a random suffix for temporary table names
func generateRandomTableSuffix() string {
	bytes := make([]byte, 6) // 6 bytes = 12 hex characters
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func BatchConflictHandler(next BatchErrorHandler) BatchErrorHandler {
	const UniqueViolationError = "23505"

	return BatchErrorHandler(func(ctx context.Context, c *Copier, db *sqlx.Conn, batch Batch, reason error) *BatchError {
		pgerr := &pgconn.PgError{}
		if !errors.As(reason, &pgerr) {
			return next(ctx, c, db, batch, reason)
		}
		if pgerr.Code != UniqueViolationError {
			return next(ctx, c, db, batch, reason)
		}

		c.Logger.Infof("Batch %d, starting at byte %d with len %d, has conflict: %s", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen, reason.Error())
		_, err := batch.Data.Seek(0, io.SeekStart)
		if err != nil {
			return NewErrContinue(fmt.Errorf("failed to seek to start of batch data, %w", err))
		}

		// We need to create a table like the destination table
		randomSuffix := generateRandomTableSuffix()
		temporalTableName := fmt.Sprintf("tmp_batch_%s", randomSuffix)

		c.Logger.Infof("Creating temporal table %s", temporalTableName)
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", temporalTableName, c.GetFullTableName()))
		if err != nil {
			return NewErrContinue(fmt.Errorf("failed to create temporal table %s, %w", temporalTableName, err))
		}

		// Create copy command for temporary table
		tempCopyCmd := strings.Replace(c.copyCmd(), c.GetFullTableName(), temporalTableName, 1)
		rows, err := copyFromLines(ctx, db.Conn, batch.Data, tempCopyCmd)
		if err != nil {
			return NewErrContinue(fmt.Errorf("failed to copy from lines %w", err))
		}

		c.Logger.Infof("Copied %d rows to temporal table %s", rows, temporalTableName)

		// Transfer data from temp table to destination table with conflict resolution
		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s ON CONFLICT DO NOTHING", c.GetFullTableName(), temporalTableName)
		result, err := db.ExecContext(ctx, insertSQL)
		if err != nil {
			return NewErrContinue(fmt.Errorf("failed to insert from temporal table %s to %s: %w", temporalTableName, c.GetFullTableName(), err))
		}

		insertedRows, _ := result.RowsAffected()
		c.Logger.Infof("Inserted %d rows from temporal table %s to %s", insertedRows, temporalTableName, c.GetFullTableName())

		// We need to drop temporal table
		c.Logger.Infof("Dropping temporal table %s", temporalTableName)
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", temporalTableName))
		if err != nil {
			return NewErrContinue(fmt.Errorf("failed to drop temporal table %s, %w", temporalTableName, err))
		}

		return &BatchError{
			Continue:     true,
			InsertedRows: insertedRows,
			SkippedRows:  rows - insertedRows,
			Handled:      true,
		}
	})
}
