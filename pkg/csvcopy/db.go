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
	connx, err := db.Connx(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquiring DBx connection for COPY: %w", err)
	}
	defer connx.Close()

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
		handleErr := handleCopyError(ctx, db, tr, err)
		if handleErr != nil {
			return 0, fmt.Errorf("failed to handle error %s, failed to copy from lines %w,", handleErr, err)
		}
		return 0, fmt.Errorf("failed to insert control row, %w", err)
	}

	rowCount, err := copyFromLines(ctx, connx.Conn, &batch.Data, copyCmd)
	if err != nil {
		handleErr := handleCopyError(ctx, db, tr, err)
		if handleErr != nil {
			return rowCount, fmt.Errorf("failed to handle error %s, failed to copy from lines %w,", handleErr, err)
		}
		return rowCount, fmt.Errorf("failed to copy from lines %w", err)
	}

	err = tx.Commit()
	if err != nil {
		handleErr := handleCopyError(ctx, db, tr, err)
		if handleErr != nil {
			return rowCount, fmt.Errorf("failed to handle error %s, failed to copy from lines %w,", handleErr, err)
		}
		return rowCount, err
	}

	return rowCount, nil
}

func handleCopyError(ctx context.Context, db *sqlx.DB, tr Transaction, copyErr error) error {
	connx, err := db.Connx(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a new connection, %w", err)
	}
	defer connx.Close()

	err = tr.setFailed(ctx, connx, copyErr.Error())
	if err != nil {
		return fmt.Errorf("failed to set state to failed, %w", err)
	}
	return nil
}
