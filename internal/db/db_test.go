package db_test

import (
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgconn"
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
			num, err := db.CopyFromLines(d, c.lines, c.copyCmd)
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
		lines := make([]string, 10000)
		badCopy := `COPY BUT NOT REALLY`

		num, err := db.CopyFromLines(d, lines, badCopy)
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
}
