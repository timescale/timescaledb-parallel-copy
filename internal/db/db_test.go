package db_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

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
		delim    string
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
				{`  more\twhitespace  `, int64(0)}, // TODO incorrect?
				{nil, nil},
			},
		},
		{
			name:    "simple types with tab delimiters",
			columns: "(s TEXT, i INT)",
			copyCmd: `COPY test FROM STDIN WITH DELIMITER E'\t' CSV`,
			delim:   "\t",
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
				{"quoted,delimiter\\\\t"}, // TODO incorrect
				{`quoted"quotes`},
				{"multi\nline column"}, // TODO: this is incorrectly counted as two rows
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := mustConnect(t)
			defer d.Close()

			// Create our test table.
			d.MustExec(`CREATE TABLE test` + c.columns)
			defer d.MustExec(`DROP TABLE test`)

			// Load the rows into it.
			num, err := db.CopyFromLines(d, c.lines, c.copyCmd, c.delim)
			if err != nil {
				t.Errorf("CopyFromLines() returned error: %v", err)
			}
			if num != int64(len(c.lines)) {
				t.Errorf("CopyFromLines() = %d, want %d", num, len(c.lines))
			}

			// Check the resulting table contents.
			rows, err := d.Queryx(`SELECT * FROM test;`)
			if err != nil {
				t.Fatalf("d.Queryx() failed: %v", err)
			}

			var actual [][]interface{}
			for rows.Next() {
				row, err := rows.SliceScan()
				if err != nil {
					t.Fatalf("rows.SliceScan() failed: %v", err)
				}

				actual = append(actual, row)
			}

			if !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("table contains unexpected contents after COPY")
				t.Logf("got:\n%v", actual)
				t.Logf("want:\n%v", c.expected)
			}
		})
	}
}
