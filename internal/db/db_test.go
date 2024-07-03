package db_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/timescale/timescaledb-parallel-copy/internal/db"
)

// mustConnect reads the TEST_CONNINFO environment variable and attempts to
// connect to the database it points to. If the variable doesn't exist (or is
// empty), the test is skipped. If it exists but can't be used to connect, the
// test fails. The new database connection is returned.
func mustConnect(t *testing.T) *sqlx.DB {
	conninfo := os.Getenv("TEST_CONNINFO")
	if len(conninfo) == 0 {
		t.Skip("the TEST_CONNINFO environment variable must point to a running database")
	}

	d, err := db.Connect(conninfo)
	if err != nil {
		t.Fatalf("failed to connect using TEST_CONNINFO: %v", err)
	}

	return d
}

func TestCopyFromLines(t *testing.T) {
	cases := []struct {
		name     string
		columns  string
		copyCmd  string
		lines    []string
		expected [][]interface{}
	}{
		{
			name:    "simple types with basic CSV",
			columns: "(s TEXT, i INT)",
			copyCmd: `COPY test FROM STDIN WITH DELIMITER ',' CSV`,
			lines: []string{
				"a,34",
				"null,42",
				"some whitespace,-13",
				"  more\twhitespace  ,0",
				",",
			},
			expected: [][]interface{}{
				{"a", int64(34)},
				{"null", int64(42)},
				{"some whitespace", int64(-13)},
				{"  more\twhitespace  ", int64(0)},
				{nil, nil},
			},
		},
		{
			name:    "simple types with tab delimiters",
			columns: "(s TEXT, i INT)",
			copyCmd: `COPY test FROM STDIN WITH DELIMITER E'\t' CSV`,
			lines: []string{
				"a\t34",
				"b\t42",
				"\"c\td\"\t0",
			},
			expected: [][]interface{}{
				{"a", int64(34)},
				{"b", int64(42)},
				{"c\td", int64(0)},
			},
		},
		{
			name:    "quoting corner cases",
			columns: "(s TEXT)",
			copyCmd: `COPY test FROM STDIN WITH DELIMITER ',' CSV`,
			lines: []string{
				``,
				`""`,
				`"quoted,delimiter\t"`,
				`"quoted""quotes"`,
				`"multi`,
				`line column"`,
			},
			expected: [][]interface{}{
				{nil},
				{""},
				{"quoted,delimiter\\t"},
				{`quoted"quotes`},
				{"multi\nline column"},
			},
		},
		{
			name:    "copy columns",
			columns: "(a INT, s TEXT, b INT)",
			copyCmd: `COPY test(s, b) FROM STDIN WITH DELIMITER ',' CSV`,
			lines: []string{
				"a,34",
				"b,42",
			},
			expected: [][]interface{}{
				{nil, "a", int64(34)},
				{nil, "b", int64(42)},
			},
		},
		{
			name:    "NULL copy option",
			columns: "(s TEXT, i INT)",
			copyCmd: `COPY test FROM STDIN WITH DELIMITER ',' NULL 'NULL' CSV`,
			lines: []string{
				"a,1",
				"NULL,2",
				"b,NULL",
				"c,3",
			},
			expected: [][]interface{}{
				{"a", int64(1)},
				{nil, int64(2)},
				{"b", nil},
				{"c", int64(3)},
			},
		},
		{
			name:    "bytea conversion",
			columns: "(b BYTEA)",
			copyCmd: `COPY test FROM STDIN WITH DELIMITER ',' CSV`,
			lines: []string{
				`\x68656c6c6f20776f726c64`,
				`\x666f6f626172`,
			},
			expected: [][]interface{}{
				{[]byte("hello world")},
				{[]byte("foobar")},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := mustConnect(t)
			defer d.Close()

			// Create our test table.
			d.MustExec(`CREATE TABLE test` + c.columns)
			defer d.MustExec(`DROP TABLE test`)

			// Load the rows into it.
			allLines := strings.Join(append(c.lines, ""), "\n")
			num, err := db.CopyFromLines(context.Background(), d, strings.NewReader(allLines), c.copyCmd)
			if err != nil {
				t.Errorf("CopyFromLines() returned error: %v", err)
			}

			// Check the resulting table contents.
			rows, err := d.Queryx(`SELECT * FROM test;`)
			if err != nil {
				t.Fatalf("d.Queryx() failed: %v", err)
			}

			var actualData [][]interface{}
			var actualCount int64

			for rows.Next() {
				row, err := rows.SliceScan()
				if err != nil {
					t.Fatalf("rows.SliceScan() failed: %v", err)
				}

				actualData = append(actualData, row)
				actualCount++
			}

			if num != actualCount {
				t.Errorf("CopyFromLines() = %d rows, want %d", num, actualCount)
			}

			if !reflect.DeepEqual(actualData, c.expected) {
				t.Errorf("table contains unexpected contents after COPY")
				t.Logf("got:\n%v", actualData)
				t.Logf("want:\n%v", c.expected)
			}
		})
	}

	t.Run("bad COPY statements bubble up errors", func(t *testing.T) {
		d := mustConnect(t)
		defer d.Close()

		// Have plenty of data ready to write, to make sure that the internal
		// pipes are correctly maintained during an error.
		lines := bytes.Repeat([]byte{'\n'}, 10000)
		badCopy := `COPY BUT NOT REALLY`

		num, err := db.CopyFromLines(context.Background(), d, bytes.NewReader(lines), badCopy)
		if num != 0 {
			t.Errorf("CopyFromLines() reported %d new rows, want 0", num)
		}

		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) {
			t.Fatalf("CopyFromLines() returned unexpected error %#v; want type %T", err, pgerr)
		}

		const SQLSTATE_SYNTAX_ERROR = "42601"
		if pgerr.Code != SQLSTATE_SYNTAX_ERROR {
			t.Errorf("CopyFromLines() returned error %s, want %s", pgerr.Code, SQLSTATE_SYNTAX_ERROR)
		}
	})

	t.Run("timestamp COPY honors DateStyle", func(t *testing.T) {
		d := mustConnect(t)
		defer d.Close()

		// Get the connected database to ALTER.
		var dbname string
		if err := d.Get(&dbname, `SELECT current_database()`); err != nil {
			t.Fatalf("retrieving current_database(): %v", err)
		}

		// Switch to a DMY datestyle.
		dbname = pgx.Identifier{dbname}.Sanitize()
		d.MustExec(`ALTER DATABASE ` + dbname + ` SET DateStyle = 'ISO, DMY'`)

		// Reconnect after the DateStyle change; we don't want any pooled
		// connections using the old setting. Also reset the DateStyle at the
		// end of the test.
		d.Close()
		d = mustConnect(t)
		defer d.Close()
		defer d.MustExec(`ALTER DATABASE ` + dbname + ` RESET DateStyle`)

		// Create our test table.
		d.MustExec(`CREATE TABLE test(t timestamp)`)
		defer d.MustExec(`DROP TABLE test`)

		// Load the data. (Every row should have the same result.)
		cmd := `COPY test FROM STDIN WITH DELIMITER ',' CSV`
		lines := []string{
			"2000-01-02 03:04:05", // should always be interpreted YYYY-MM-DD
			"02-01-2000 03:04:05",
			"02/01/2000 03:04:05",
		}

		lineData := strings.Join(append(lines, ""), "\n")
		_, err := db.CopyFromLines(context.Background(), d, strings.NewReader(lineData), cmd)
		if err != nil {
			t.Fatalf("CopyFromLines() returned error: %v", err)
		}

		// Check the result.
		var actual []time.Time
		err = d.Select(&actual, `SELECT * FROM test`)
		if err != nil {
			t.Fatalf("reading test table: %v", err)
		}

		expected := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
		for i, stamp := range actual {
			if stamp != expected {
				t.Errorf("test row %d is %v, want %v", i, stamp, expected)
			}
		}
	})
}
