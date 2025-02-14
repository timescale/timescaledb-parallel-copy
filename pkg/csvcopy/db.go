package csvcopy

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type ErrBatchAlreadyProcessed struct {
	State *TransactionRow
}

func (e *ErrBatchAlreadyProcessed) Error() string {
	return fmt.Sprintf("batch already processed: %s", e.State.State)
}

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
	connx, err := db.Connx(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquiring DBx connection for COPY: %w", err)
	}
	defer connx.Close()

	if !batch.Location.HasImportID() {
		rowCount, err := copyFromLines(ctx, connx.Conn, &batch.Data, copyCmd)
		if err != nil {
			return rowCount, fmt.Errorf("failed to copy from lines %w", err)
		}
		return rowCount, nil
	}

	tx, err := connx.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	tr := newTransactionAt(batch.Location)

	err = tr.setCompleted(ctx, tx)
	if err != nil {
		if isDuplicateKeyError(err) {
			connx, err := db.Connx(ctx)
			if err != nil {
				return 0, fmt.Errorf("acquiring DBx connection for transaction row: %w", err)
			}
			defer connx.Close()

			trState, err := tr.Get(ctx, connx)
			if err != nil {
				return 0, fmt.Errorf("failed to get transaction row: %w", err)
			}
			return 0, &ErrBatchAlreadyProcessed{
				State: trState,
			}
		}
		return 0, fmt.Errorf("failed to insert control row, %w", err)
	}

	rowCount, err := copyFromLines(ctx, connx.Conn, &batch.Data, copyCmd)
	if err != nil {
		return rowCount, fmt.Errorf("failed to copy from lines %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return rowCount, err
	}

	return rowCount, nil
}
