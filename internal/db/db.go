package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
)

// Overrideable is an interface for defining ways to override PG settings
// outside of the usual manners (through the connection string/URL or env vars).
// An example would be having specific flags that can be used to set database
// connect parameters.
type Overrideable interface {
	Override() string
}

// OverrideDBName is a type for overriding the database name used to connect.
// To use it, one casts a string of the database name as an OverrideDBName
type OverrideDBName string

func (o OverrideDBName) Override() string {
	return string(o)
}

// ErrInvalidSSLMode is the error when the provided SSL mode is not one of the
// values that PostgreSQL supports.
type ErrInvalidSSLMode struct {
	given string
}

func (e *ErrInvalidSSLMode) Error() string {
	return "invalid SSL mode: " + e.given
}

const (
	// envSSLMode is the environment variable key for SSL mode.
	envSSLMode = "PGSSLMODE"
)

var sslmodeRegex = regexp.MustCompile("sslmode=([a-zA-Z-]+)")

// determineTLS attempts to match SSL mode to a known PostgreSQL supported value.
func determineTLS(connStr string) (string, error) {
	res := sslmodeRegex.FindStringSubmatch(connStr)
	var sslmode string
	if len(res) == 2 {
		sslmode = res[1]
	} else {
		sslmode = os.Getenv(envSSLMode)
	}

	if sslmode == "" {
		return "", nil
	}

	switch sslmode {
	case "require", "disable", "allow", "prefer", "verify-ca", "verify-full":
		return sslmode, nil
	default:
		return "", &ErrInvalidSSLMode{given: sslmode}
	}
}

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
