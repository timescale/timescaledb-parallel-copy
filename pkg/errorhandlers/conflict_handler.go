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
	CustomFunctionName string
	Next               csvcopy.BatchErrorHandler
}

// ConflictHandlerOption allows configuring the conflict handler
type ConflictHandlerOption func(*ConflictHandlerConfig)

// WithConflictHandlerFunctionName sets a custom function name for conflict resolution
// The function should exist in the destination table's schema and have the signature:
// function_name(dest_schema text, dest_table text, temp_schema text, temp_table text) RETURNS bigint
func WithConflictHandlerFunctionName(functionName string) ConflictHandlerOption {
	return func(config *ConflictHandlerConfig) {
		config.CustomFunctionName = functionName
	}
}

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
// 4. Using INSERT ... ON CONFLICT DO NOTHING to transfer only non-duplicate rows (or custom function if specified)
// 5. Cleaning up the temporary table
//
// If next is provided, non-unique-constraint errors are forwarded to it.
// Options can be provided to customize conflict resolution behavior.
func BatchConflictHandler(options ...ConflictHandlerOption) csvcopy.BatchErrorHandler {
	config := &ConflictHandlerConfig{}
	for _, option := range options {
		option(config)
	}
	const UniqueViolationError = "23505"

	return csvcopy.BatchErrorHandler(func(ctx context.Context, c *csvcopy.Copier, db *sqlx.Conn, batch csvcopy.Batch, reason error) *csvcopy.BatchError {
		c.LogWithContext(ctx, "BatchConflictHandler called: batch %d, byte offset %d, len %d", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen)

		pgerr := &pgconn.PgError{}
		if !errors.As(reason, &pgerr) {
			c.LogWithContext(ctx, "BatchErrorHandler: error is not PostgreSQL error. Type: %T, Error: %v", reason, reason)
			if config.Next != nil {
				return config.Next(ctx, c, db, batch, reason)
			}
			return csvcopy.NewErrContinue(reason)
		}

		if pgerr.Code != UniqueViolationError {
			c.LogWithContext(ctx, "BatchErrorHandler: not a unique constraint violation (code %s != %s). Forwarding to next handler.", pgerr.Code, UniqueViolationError)
			if config.Next != nil {
				return config.Next(ctx, c, db, batch, reason)
			}
			return csvcopy.NewErrContinue(reason)
		}

		c.LogWithContext(ctx, "Batch %d, starting at byte %d with len %d, has conflict: %s", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen, reason.Error())
		_, err := batch.Data.Seek(0, io.SeekStart)
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to seek to start of batch data, %w", err))
		}

		// We need to create a table like the destination table
		randomSuffix := generateRandomTableSuffix()
		temporalTableName := fmt.Sprintf("%s.tmp_batch_%s", c.GetSchemaName(), randomSuffix)

		c.LogWithContext(ctx, "Creating temporal table %s", temporalTableName)
		_, err = db.ExecContext(ctx, fmt.Sprintf("/* Worker-%d */ CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS)", csvcopy.GetWorkerIDFromContext(ctx), temporalTableName, c.GetFullTableName()))
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to create temporal table %s, %w", temporalTableName, err))
		}

		// Create copy command for temporary table
		tempCopyCmd := strings.Replace(c.CopyCmdWithContext(ctx), c.GetFullTableName(), temporalTableName, 1)
		rows, err := csvcopy.CopyFromLines(ctx, db.Conn, batch.Data, tempCopyCmd)
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to copy from lines %w", err))
		}

		c.LogWithContext(ctx, "Copied %d rows to temporal table %s", rows, temporalTableName)

		// Check for custom function if specified
		var insertedRows int64
		if config.CustomFunctionName != "" {
			exists, err := checkCustomFunctionExists(ctx, db, c.GetSchemaName(), config.CustomFunctionName)
			if err != nil {
				return csvcopy.NewErrContinue(fmt.Errorf("failed to check for custom conflict handler %s.%s: %w", c.GetSchemaName(), config.CustomFunctionName, err))
			}
			if !exists {
				return csvcopy.NewErrContinue(fmt.Errorf("custom conflict handler %s.%s not found", c.GetSchemaName(), config.CustomFunctionName))
			}

			c.LogWithContext(ctx, "Using custom conflict handler %s.%s", c.GetSchemaName(), config.CustomFunctionName)
			// Parse temp table name to extract schema and table parts
			tempParts := strings.SplitN(temporalTableName, ".", 2)
			tempSchema := tempParts[0]
			tempTable := tempParts[1]

			insertedRows, err = callCustomConflictHandler(ctx, db, c.GetSchemaName(), config.CustomFunctionName, c.GetSchemaName(), c.GetTableName(), tempSchema, tempTable)
			if err != nil {
				return csvcopy.NewErrContinue(fmt.Errorf("custom conflict handler %s.%s failed: %w", c.GetSchemaName(), config.CustomFunctionName, err))
			}
		} else {
			// Default behavior: INSERT ... ON CONFLICT DO NOTHING
			insertSQL := fmt.Sprintf("/* Worker-%d */ INSERT INTO %s SELECT * FROM %s ON CONFLICT DO NOTHING", csvcopy.GetWorkerIDFromContext(ctx), c.GetFullTableName(), temporalTableName)
			result, err := db.ExecContext(ctx, insertSQL)
			if err != nil {
				return csvcopy.NewErrContinue(fmt.Errorf("failed to insert from temporal table %s to %s: %w", temporalTableName, c.GetFullTableName(), err))
			}
			insertedRows, _ = result.RowsAffected()
		}

		c.LogWithContext(ctx, "Processed %d rows from temporal table %s to %s", insertedRows, temporalTableName, c.GetFullTableName())

		// We need to drop temporal table
		c.LogWithContext(ctx, "Dropping temporal table %s", temporalTableName)
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", temporalTableName))
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to drop temporal table %s, %w", temporalTableName, err))
		}

		return &csvcopy.BatchError{
			Continue:     true,
			InsertedRows: insertedRows,
			SkippedRows:  rows - insertedRows,
			Handled:      true,
		}
	})
}

// checkCustomFunctionExists checks if a custom conflict handler function exists in the specified schema
func checkCustomFunctionExists(ctx context.Context, db *sqlx.Conn, schema, functionName string) (bool, error) {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON p.pronamespace = n.oid
			WHERE n.nspname = $1 AND p.proname = $2
		)`
	err := db.QueryRowContext(ctx, query, schema, functionName).Scan(&exists)
	return exists, err
}

// callCustomConflictHandler calls the user-defined conflict resolution function
func callCustomConflictHandler(ctx context.Context, db *sqlx.Conn, schema, functionName, destSchema, destTable, tempSchema, tempTable string) (int64, error) {
	query := fmt.Sprintf(`/* Worker-%d */ SELECT "%s"."%s"($1, $2, $3, $4)`, csvcopy.GetWorkerIDFromContext(ctx), schema, functionName)
	var affectedRows int64
	err := db.QueryRowContext(ctx, query, destSchema, destTable, tempSchema, tempTable).Scan(&affectedRows)
	return affectedRows, err
}
