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

// BatchConflictHandler handles unique constraint violations during batch processing
// by creating temporary tables and using ON CONFLICT DO NOTHING to skip duplicates.
// This allows CSV imports to continue processing even when duplicate rows are encountered.
//
// The handler works by:
// 1. Detecting PostgreSQL unique constraint violations (error code 23505)
// 2. Creating a temporary table with the same structure as the destination
// 3. Copying the batch data to the temporary table
// 4. Using INSERT ... ON CONFLICT DO NOTHING to transfer only non-duplicate rows
// 5. Cleaning up the temporary table
//
// If next is provided, non-unique-constraint errors are forwarded to it.
func BatchConflictHandler(next csvcopy.BatchErrorHandler) csvcopy.BatchErrorHandler {
	const UniqueViolationError = "23505"

	return csvcopy.BatchErrorHandler(func(ctx context.Context, c *csvcopy.Copier, db *sqlx.Conn, batch csvcopy.Batch, reason error) *csvcopy.BatchError {
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
			return csvcopy.NewErrContinue(fmt.Errorf("failed to seek to start of batch data, %w", err))
		}

		// We need to create a table like the destination table
		randomSuffix := generateRandomTableSuffix()
		temporalTableName := fmt.Sprintf("tmp_batch_%s", randomSuffix)

		c.Logger.Infof("Creating temporal table %s", temporalTableName)
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", temporalTableName, c.GetFullTableName()))
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to create temporal table %s, %w", temporalTableName, err))
		}

		// Create copy command for temporary table
		tempCopyCmd := strings.Replace(c.CopyCmd(), c.GetFullTableName(), temporalTableName, 1)
		rows, err := csvcopy.CopyFromLines(ctx, db.Conn, batch.Data, tempCopyCmd)
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to copy from lines %w", err))
		}

		c.Logger.Infof("Copied %d rows to temporal table %s", rows, temporalTableName)

		// Transfer data from temp table to destination table with conflict resolution
		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s ON CONFLICT DO NOTHING", c.GetFullTableName(), temporalTableName)
		result, err := db.ExecContext(ctx, insertSQL)
		if err != nil {
			return csvcopy.NewErrContinue(fmt.Errorf("failed to insert from temporal table %s to %s: %w", temporalTableName, c.GetFullTableName(), err))
		}

		insertedRows, _ := result.RowsAffected()
		c.Logger.Infof("Inserted %d rows from temporal table %s to %s", insertedRows, temporalTableName, c.GetFullTableName())

		// We need to drop temporal table
		c.Logger.Infof("Dropping temporal table %s", temporalTableName)
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