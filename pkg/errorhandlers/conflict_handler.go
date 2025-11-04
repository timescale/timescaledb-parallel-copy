package errorhandlers

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
	"github.com/timescale/timescaledb-parallel-copy/pkg/csvcopy"
)

// generateRandomTableSuffix creates a random suffix for temporary table names
func generateRandomTableSuffix() string {
	bytes := make([]byte, 6) // 6 bytes = 12 hex characters
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// ConflictHandlerConfig holds configuration for BatchConflictHandler
type ConflictHandlerConfig struct {
	Next csvcopy.BatchErrorHandler
}

// ConflictHandlerOption allows configuring the conflict handler
type ConflictHandlerOption func(*ConflictHandlerConfig)


// WithConflictHandlerNext sets the next batch error handler
func WithConflictHandlerNext(next csvcopy.BatchErrorHandler) ConflictHandlerOption {
	return func(config *ConflictHandlerConfig) {
		config.Next = next
	}
}

// BatchConflictHandler handles unique constraint violations during batch processing
// by creating temporary tables and using ON CONFLICT DO NOTHING to skip duplicates.
// This allows CSV imports to continue processing even when duplicate rows are encountered.
//
// The handler works by:
// 1. Detecting PostgreSQL unique constraint violations (error code 23505)
// 2. Creating a temporary table with the same structure as the destination
// 3. Copying the batch data to the temporary table
// 4. Using INSERT ... ON CONFLICT DO NOTHING to transfer only non-duplicate rows
// 5. Cleaning up the temporary table (automatic with PostgreSQL)
//
// If next is provided, non-unique-constraint errors are forwarded to it.
func BatchConflictHandler(options ...ConflictHandlerOption) csvcopy.BatchErrorHandler {
	config := &ConflictHandlerConfig{}
	for _, option := range options {
		option(config)
	}
	const UniqueViolationError = "23505"

	return csvcopy.BatchErrorHandler(func(ctx context.Context, c *csvcopy.Copier, db *sqlx.Conn, batch csvcopy.Batch, reason error) csvcopy.HandleBatchErrorResult {
		c.LogInfo(ctx, "BatchConflictHandler called: batch %d, byte offset %d, len %d", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen)

		pgerr := &pgconn.PgError{}
		if !errors.As(reason, &pgerr) {
			c.LogInfo(ctx, "BatchConflictHandler: error is not PostgreSQL error. Type: %T, Error: %v", reason, reason)
			if config.Next != nil {
				return config.Next(ctx, c, db, batch, reason)
			}
			return csvcopy.NewErrStop(reason)
		}

		if pgerr.Code != UniqueViolationError {
			c.LogInfo(ctx, "BatchConflictHandler: not a unique constraint violation (code %s != %s). Forwarding to next handler.", pgerr.Code, UniqueViolationError)
			if config.Next != nil {
				return config.Next(ctx, c, db, batch, reason)
			}
			return csvcopy.NewErrStop(reason)
		}

		c.LogInfo(ctx, "BatchConflictHandler: Batch %d, has conflict: %s", batch.Location.StartRow, reason.Error())
		_, err := batch.Data.Seek(0, io.SeekStart)
		if err != nil {
			return csvcopy.NewErrStop(fmt.Errorf("failed to seek to start of batch data, %w", err))
		}

		// Create a temporary table with random name (automatically cleaned up by PostgreSQL)
		randomSuffix := generateRandomTableSuffix()
		temporaryTableName := fmt.Sprintf("tmp_batch_%s", randomSuffix)

		c.LogInfo(ctx, "BatchConflictHandler: Creating temporary table %s", temporaryTableName)
		_, err = db.ExecContext(ctx, fmt.Sprintf("/* Worker-%d */ CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING DEFAULTS)", csvcopy.GetWorkerIDFromContext(ctx), temporaryTableName, c.GetFullTableName()))
		if err != nil {
			return csvcopy.NewErrStop(fmt.Errorf("failed to create temporary table %s, %w", temporaryTableName, err))
		}

		// Create copy command for temporary table
		tempCopyCmd := strings.Replace(c.CopyCmdWithContext(ctx), c.GetFullTableName(), temporaryTableName, 1)
		rows, err := csvcopy.CopyFromLines(ctx, db.Conn, batch.Data, tempCopyCmd)
		if err != nil {
			return csvcopy.NewErrStop(fmt.Errorf("failed to copy from lines %w", err))
		}

		c.LogInfo(ctx, "BatchConflictHandler: Copied %d rows to temporary table %s", rows, temporaryTableName)

		// Insert data using ON CONFLICT DO NOTHING to skip duplicates
		insertSQL := fmt.Sprintf("/* Worker-%d */ INSERT INTO %s SELECT * FROM %s ON CONFLICT DO NOTHING", csvcopy.GetWorkerIDFromContext(ctx), c.GetFullTableName(), temporaryTableName)
		result, err := db.ExecContext(ctx, insertSQL)
		if err != nil {
			return csvcopy.NewErrStop(fmt.Errorf("failed to insert from temporary table %s to %s: %w", temporaryTableName, c.GetFullTableName(), err))
		}
		insertedRows, _ := result.RowsAffected()

		c.LogInfo(ctx, "BatchConflictHandler: Processed %d rows from temporary table %s to %s", insertedRows, temporaryTableName, c.GetFullTableName())

		// No need to drop temporary table - PostgreSQL automatically cleans it up

		return csvcopy.HandleBatchErrorResult{
			Continue:     true,
			InsertedRows: insertedRows,
			SkippedRows:  rows - insertedRows,
			Handled:      true,
		}
	})
}


