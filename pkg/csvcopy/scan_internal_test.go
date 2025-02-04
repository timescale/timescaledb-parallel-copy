package csvcopy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestCSVRowState(t *testing.T) {
	cases := []struct {
		name          string
		input         []string
		quote, escape rune // default '"'
		expectMore    bool
	}{
		{
			name: "quoted value",
			input: []string{
				`"hello there"` + "\n",
			},
			expectMore: false,
		},
		{
			name: "empty value",
			input: []string{
				`""` + "\n",
			},
			expectMore: false,
		},
		{
			name: "quoted value with escaped quote",
			input: []string{
				`"hello""there"` + "\n",
			},
			expectMore: false,
		},
		{
			name:   "quoted value with custom-escaped quote",
			escape: '\\',
			input: []string{
				`"hello\"there"` + "\n",
			},
			expectMore: false,
		},
		{
			name:   "quoted value with escaped escape",
			escape: '\\',
			input: []string{
				`"hello\\"` + "\n",
			},
			expectMore: false,
		},
		{
			name:   "quoted value with invalid escape",
			escape: '\\',
			input: []string{
				`"hello\,"` + "\n",
			},
			expectMore: false,
		},
		{
			name: "leaving a quote open",
			input: []string{
				`"hello there`,
			},
			expectMore: true,
		},
		{
			name:  "leaving a custom quote open",
			quote: '\'',
			input: []string{
				`'hello there`,
			},
			expectMore: true,
		},
		{
			name: "leaving a quote open with standard escapes",
			input: []string{
				`"hello""there`,
			},
			expectMore: true,
		},
		{
			name:   "leaving a quote open with custom quotes and escapes",
			quote:  '\'',
			escape: '\\',
			input: []string{
				`'hello\'there`,
			},
			expectMore: true,
		},
		{
			name: "splitting a quoted value across chunks",
			input: []string{
				`"hello `,
				`there"` + "\n",
			},
			expectMore: false,
		},
		{
			name: "splitting a quoted value with standard escapes",
			input: []string{
				`"foo `,
				`bar""baz"` + "\n",
			},
			expectMore: false,
		},
		{
			name:   "splitting a quoted value with custom escapes",
			escape: '\\',
			input: []string{
				`"foo `,
				`bar\"baz"` + "\n",
			},
			expectMore: false,
		},
		{
			name: "closing and reopening a quote across chunks",
			input: []string{
				`"foo `,
				`b"ar ba"z`,
			},
			expectMore: true,
		},
		{
			// This case and the next are subtly different. Escapes aren't
			// recognized until you're in a quoted value, but escapes and quotes
			// can be the same character.
			name: "closing and reopening a quote with doubled quotes",
			input: []string{
				`"foo `,
				`b"ar""ba"z`,
			},
			expectMore: true,
		},
		{
			// Unlike the last case, the backslash here receives no special
			// treatment, because it's not inside a quoted value.
			name:   "closing and reopening a quote with unquoted escape",
			escape: '\\',
			input: []string{
				`"foo `,
				`b"ar\"ba"z`,
			},
			expectMore: false,
		},
		{
			name: "splitting a standard escape sequence across chunks",
			input: []string{
				`"hello "`,
				`"there"` + "\n",
			},
			expectMore: false,
		},
		{
			name:   "splitting a custom escape sequence across chunks",
			escape: '\\',
			input: []string{
				`"hello \`,
				`"there"` + "\n",
			},
			expectMore: false,
		},
		{
			name:   "splitting a misleading escape sequence across chunks",
			escape: '\\',
			input: []string{
				// The first backslash escapes the next, causing the second
				// double-quote to close the quote, then the third double-quote
				// reopens it.
				`"hello \`,
				`\"there"` + "\n",
			},
			expectMore: true,
		},
	}

	for _, c := range cases {
		copyOpts := "CSV"

		quote := byte('"')
		if c.quote != 0 {
			quote = byte(c.quote)
			copyOpts = fmt.Sprintf("%s QUOTE '%s'",
				copyOpts, strings.ReplaceAll(string(c.quote), "'", "''"))
		}

		escape := byte('"')
		if c.escape != 0 {
			escape = byte(c.escape)
			copyOpts = fmt.Sprintf("%s ESCAPE '%s'",
				copyOpts, strings.ReplaceAll(string(c.escape), "'", "''"))
		}

		t.Run(c.name, func(t *testing.T) {
			scanner := makeCSVRowState(quote, escape)

			for _, chunk := range c.input {
				scanner.Scan([]byte(chunk))
			}

			if scanner.NeedsMore() != c.expectMore {
				t.Errorf("NeedsMore() = %v, want %v", scanner.NeedsMore(), c.expectMore)
				t.Logf("escape = %q, quote = %q", escape, quote)
				t.Logf("input data was %q", c.input)
			}
		})

		// Constructing test cases for CSV quote parsing can be tricky, since
		// the corner cases are underdocumented. If the developer provides a
		// TEST_CONNINFO, we can sanity-check by making sure a real database
		// actually rejects the unterminated test cases and accepts all the
		// others.
		t.Run(fmt.Sprintf("live DB check: %s", c.name), func(t *testing.T) {
			d := mustConnect(t)
			defer d.Close()

			d.MustExec(`CREATE TABLE csv(t text)`)
			defer d.MustExec(`DROP TABLE csv`)

			allLines := strings.Join(c.input, "")
			copyCmd := fmt.Sprintf(`COPY csv FROM STDIN WITH %s`, copyOpts)

			num, err := copyFromBatch(context.Background(), d, newBatchFromReader(strings.NewReader(allLines)), copyCmd)

			if c.expectMore {
				// If our test case claimed to be unterminated, then the DB
				// should have also complained about an unterminated row.
				expected := "22P04" // bad COPY format

				var pgerr *pgconn.PgError
				if !errors.As(err, &pgerr) {
					t.Fatalf("CopyFromLines() returned error %#v, want type %T", err, pgerr)
				}
				if pgerr.Code != expected {
					t.Errorf("database reported error code %q, want %q", pgerr.Code, expected)
				}

				return
			}

			// Otherwise, exactly one row should have been correctly inserted.
			if err != nil {
				t.Errorf("CopyFromLines() returned unexpected error: %v", err)
			}
			if num != 1 {
				t.Errorf("CopyFromLines() inserted %d rows, want %d", num, 1)
			}
		})
	}
}
