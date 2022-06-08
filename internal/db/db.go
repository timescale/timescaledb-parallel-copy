package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
)

// minimalConnConfig is the minimal settings we need for connection. More
// unusual options are currently not supported.
type minimalConnConfig struct {
	host     string
	user     string
	password string
	db       string
	port     uint16
	sslmode  string
}

// DSN returns the PostgreSQL compatible DSN string that corresponds to mcc.
// This is expressed as a string of <key>=<value> separated by spaces.
func (mcc *minimalConnConfig) DSN() string {
	var s strings.Builder
	writeNonempty := func(key, val string) {
		if val != "" {
			_, err := s.WriteString(key + "=" + val + " ")
			if err != nil {
				panic(err)
			}
		}
	}
	writeNonempty("host", mcc.host)
	writeNonempty("user", mcc.user)
	writeNonempty("password", mcc.password)
	writeNonempty("dbname", mcc.db)
	if mcc.port != 0 {
		writeNonempty("port", strconv.FormatUint(uint64(mcc.port), 10))
	}
	writeNonempty("sslmode", mcc.sslmode)

	return strings.TrimSpace(s.String())
}

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

// parseConnStr uses an external lib (that backs pgx) to take care of parsing
// connection parameters for connecting to PostgreSQL. It handles the connStr
// being in DSN or URL form, as well as reading env vars for additional settings.
func parseConnStr(connStr string, overrides ...Overrideable) (*minimalConnConfig, error) {
	config, err := pgconn.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	sslmode, err := determineTLS(connStr)
	if err != nil {
		return nil, err
	}

	mcc := &minimalConnConfig{
		host:     config.Host,
		user:     config.User,
		password: config.Password,
		db:       config.Database,
		port:     config.Port,
		sslmode:  sslmode,
	}

	for _, o := range overrides {
		switch o.(type) {
		case OverrideDBName:
			mcc.db = o.Override()
		default:
			return nil, fmt.Errorf("unknown overrideable: %T=%s", o, o.Override())
		}
	}

	return mcc, nil

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
func Connect(connStr string, overrides ...Overrideable) (*sqlx.DB, error) {
	mcc, err := parseConnStr(connStr, overrides...)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	db, err := sqlx.Connect("pgx", mcc.DSN())
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	return db, nil
}

// CopyFromLines bulk-loads data using the given copyCmd. If the COPY statement
// uses a tab delimiter, splitChar must be set to "\t"; otherwise it is ignored.
// Returns the number of rows inserted.
func CopyFromLines(db *sqlx.DB, lines []string, copyCmd, splitChar string) (int64, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return 0, fmt.Errorf("acquiring DB connection for COPY: %w", err)
	}
	defer conn.Close()

	// Our slice of lines needs to be presented to PgConn.CopyFrom() as a
	// unified io.Reader. Joining the entire string would eat memory, so use an
	// intermediary io.Pipe instead.
	//
	// TODO: just take the bytes directly as an argument rather than converting
	// to a slice and back
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		for _, line := range lines {
			if _, err := fmt.Fprintln(w, line); err != nil {
				// If the pipe's not already closed with an error, add one for
				// further debugging. In particular we want to ensure that the
				// read half returns an error, so that the CopyFrom transaction
				// doesn't commit below.
				err = fmt.Errorf("writing to COPY pipe: %w", err)
				w.CloseWithError(err)
				return
			}
		}
	}()

	// pgx requires us to use the low-level API for a raw COPY FROM operation.
	err = conn.Raw(func(driverConn interface{}) error {
		defer r.Close()

		// Unfortunately there are three layers to unwrap here: the stdlib.Conn,
		// the pgx.Conn, and the pgconn.PgConn.
		pg := driverConn.(*stdlib.Conn).Conn().PgConn()

		_, err := pg.CopyFrom(context.Background(), r, copyCmd)
		return err
	})

	if err != nil {
		return 0, err
	}

	return int64(len(lines)), nil
}
