package batch_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/timescale/timescaledb-parallel-copy/internal/batch"
)

func TestScan(t *testing.T) {
	cases := []struct {
		name     string
		input    []string
		size     int
		skip     int
		limit    int64
		expected []string
	}{
		{
			name: "basic split",
			input: []string{
				"a,b,c",
				"1,2,3",
				"4,5,6",
				"7,8,9",
			},
			size: 2,
			expected: []string{
				"a,b,c\n1,2,3\n",
				"4,5,6\n7,8,9",
			},
		},
		{
			name: "leftover rows",
			input: []string{
				"a,b,c",
				"1,2,3",
				"4,5,6",
				"7,8,9",
			},
			size: 3,
			expected: []string{
				"a,b,c\n1,2,3\n4,5,6\n",
				"7,8,9",
			},
		},
		{
			name: "skipped header",
			input: []string{
				"a,b,c",
				"d,e,f",
				"1,2,3",
				"4,5,6",
				"7,8,9",
			},
			size: 2,
			skip: 2,
			expected: []string{
				"1,2,3\n4,5,6\n",
				"7,8,9",
			},
		},
		{
			name: "scan limit",
			input: []string{
				"a,b,c",
				"1,2,3",
				"4,5,6",
			},
			size:  1,
			limit: 2,
			expected: []string{
				"a,b,c\n",
				"1,2,3\n",
			},
		},
		{
			name:  "empty input",
			input: []string{},
			size:  3,
		},
		{
			name: "fully skipped input",
			input: []string{
				"a,b,c",
			},
			size: 3,
			skip: 2,
		},
		{
			name: "long lines",
			input: []string{
				strings.Repeat("1111", 4096),
				strings.Repeat("2222", 4096),
				strings.Repeat("3333", 4096),
				strings.Repeat("4444", 4096),
			},
			size: 2,
			expected: []string{
				strings.Repeat("1111", 4096) + "\n" + strings.Repeat("2222", 4096) + "\n",
				strings.Repeat("3333", 4096) + "\n" + strings.Repeat("4444", 4096),
			},
		},
		{
			name: "long lines with limit",
			input: []string{
				strings.Repeat("1111", 4096),
				strings.Repeat("2222", 4096),
				strings.Repeat("3333", 4096),
				strings.Repeat("4444", 4096),
			},
			size:  2,
			limit: 3,
			expected: []string{
				strings.Repeat("1111", 4096) + "\n" + strings.Repeat("2222", 4096) + "\n",
				strings.Repeat("3333", 4096) + "\n",
			},
		},
		{
			name: "long lines with header and limit",
			input: []string{
				strings.Repeat("1111", 4096),
				strings.Repeat("2222", 4096),
				strings.Repeat("3333", 4096),
				strings.Repeat("4444", 4096),
			},
			size:  2,
			skip:  1,
			limit: 2,
			expected: []string{
				strings.Repeat("2222", 4096) + "\n" + strings.Repeat("3333", 4096) + "\n",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rowChan := make(chan net.Buffers)
			resultChan := make(chan []string)

			// Collector for the scanned row batches.
			go func() {
				var actual []string

				for buf := range rowChan {
					actual = append(actual, string(bytes.Join(buf, nil)))
				}

				resultChan <- actual
			}()

			all := strings.Join(c.input, "\n")
			reader := strings.NewReader(all)
			opts := batch.Options{
				Size:  c.size,
				Skip:  c.skip,
				Limit: c.limit,
			}

			err := batch.Scan(reader, rowChan, opts)
			if err != nil {
				t.Fatalf("Scan() returned error: %v", err)
			}

			// Check results.
			close(rowChan)
			actual := <-resultChan

			if !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("Scan() returned unexpected batch results")
				t.Logf("got:\n%v", actual)
				t.Logf("want:\n%v", c.expected)
			}
		})
	}

	errCases := []struct {
		name string
		skip int
	}{
		{
			name: "reader errors are bubbled up",
			// no skip
		},
		{
			name: "reader errors are bubbled up during header skips",
			skip: 5,
		},
	}

	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			expected := errors.New("sentinel")
			reader := newErrReader(strings.NewReader(`
				some input
				should be discarded
			`), expected)

			rowChan := make(chan net.Buffers, 1)
			opts := batch.Options{
				Size: 50,
				Skip: c.skip,
			}

			err := batch.Scan(reader, rowChan, opts)
			if !errors.Is(err, expected) {
				t.Errorf("Scan() returned unexpected error: %v", err)
				t.Logf("want: %v", expected)
			}

			// Make sure no batches were written to the channel; we shouldn't have
			// had enough lines to fill one.
			close(rowChan)
			if len(rowChan) > 0 {
				t.Errorf("Scan() buffered unexpected data: %v", <-rowChan)
			}
		})
	}
}

// errReader is an io.Reader that returns an error on the second call to
// Read(), _and_ on all future calls to Read(). (It is nearly identical to
// iotest.TimeoutReader() except that the error is "sticky" and will never be
// cleared on a future call.)
type errReader struct {
	r   io.Reader
	err error
}

func newErrReader(r io.Reader, err error) *errReader {
	return &errReader{r, err}
}

func (e *errReader) Read(buf []byte) (int, error) {
	if e.r == nil {
		return 0, e.err
	}

	n, err := e.r.Read(buf)
	e.r = nil
	return n, err
}
