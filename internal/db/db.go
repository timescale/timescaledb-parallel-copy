package db

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// Connect returns a SQLX database corresponding to the provided connection
// string/URL, env variables, and any provided overrides.
func Connect(connStr string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	return db, nil
}

// CopyFromLines bulk-loads data using the given copyCmd. lines must provide a
// set of complete lines of CSV data, including the end-of-line delimiters.
// Returns the number of rows inserted.
func CopyFromLines(ctx context.Context, db *sqlx.DB, lines io.Reader, copyCmd string) (int64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquiring DB connection for COPY: %w", err)
	}
	defer conn.Close()

	var rowCount int64

	// pgx requires us to use the low-level API for a raw COPY FROM operation.
	err = conn.Raw(func(driverConn interface{}) error {
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
