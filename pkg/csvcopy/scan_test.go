package csvcopy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func TestScan(t *testing.T) {
	cases := []struct {
		name             string
		input            []string
		size             int
		bufferSize       int
		batchSize        int
		skip             int
		limit            int64
		quote            rune // default '"'
		escape           rune // default is c.quote
		expected         []string
		expectedRowCount []int
		expectedError    string
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
			expectedRowCount: []int{
				2,
				2,
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
			expectedRowCount: []int{
				3,
				1,
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
			expectedRowCount: []int{
				2,
				1,
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
			expectedRowCount: []int{
				1,
				1,
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
			expectedRowCount: []int{
				2,
				2,
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
			expectedRowCount: []int{
				2,
				1,
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
			expectedRowCount: []int{
				2,
			},
		},
		{
			name: "quoted multi-line rows",
			input: []string{
				// row 1
				`a,b,"c`,
				`d"`,
				// row 2
				`1,"2`,
				`3",4`,
				// row 3
				`"5`,
				`6",7,8`,
				// row 4
				`7,8,"9`,
				`10"`,
			},
			size: 2,
			expected: []string{
				`a,b,"c
d"
1,"2
3",4
`,
				`"5
6",7,8
7,8,"9
10"`,
			},
			expectedRowCount: []int{
				2,
				2,
			},
		},
		{
			name: "quoted multi-line rows with skipped header lines",
			input: []string{
				// header row
				`a,b,"c`,
				`d"`,
				// row 1
				`1,"2`,
				`3",4`,
				// row 2
				`"5`,
				`6",7,8`,
				// row 3
				`7,8,"9`,
				`10"`,
			},
			size: 2,
			skip: 2, // note we skip header *lines*, not rows
			expected: []string{
				`1,"2
3",4
"5
6",7,8
`,
				`7,8,"9
10"`,
			},
			expectedRowCount: []int{
				2,
				1,
			},
		},
		{
			name:  "custom-quoted multi-line rows",
			quote: '\'',
			input: []string{
				// row 1
				`a,b,'c''`,
				`d'`,
				// row 2
				`1,'2`,
				`3',4`,
			},
			size: 2,
			expected: []string{
				`a,b,'c''
d'
1,'2
3',4`,
			},
			expectedRowCount: []int{
				2,
			},
		},
		{
			name:   "custom-escaped multi-line rows",
			escape: '\\',
			input: []string{
				// row 1
				`a,b,"c\"`,
				`d"`,
				// row 2
				`1,"2`,
				`3",4`,
			},
			size: 2,
			expected: []string{
				`a,b,"c\"
d"
1,"2
3",4`,
			},
			expectedRowCount: []int{
				2,
			},
		},
		{
			name: "buffer size is smaller than line size",
			input: []string{
				strings.Repeat("a", 4096),
				strings.Repeat("1", 4096),
				strings.Repeat("2", 4096),
				strings.Repeat("3", 4096),
			},
			size:          2,
			bufferSize:    1024,
			expectedError: bufio.ErrBufferFull.Error(),
		},
		{
			name: "batch size is smaller than buffer size",
			input: []string{
				strings.Repeat("a", 4096),
				strings.Repeat("1", 4096),
				strings.Repeat("2", 4096),
				strings.Repeat("3", 4096),
			},
			size:          2,
			batchSize:     1024,
			bufferSize:    2048,
			expectedError: "batch size (1024) is smaller than buffer size (2048)",
		},
		{
			name: "batch size is hit before line limit",
			input: []string{
				strings.Repeat("a", 4096),
				strings.Repeat("1", 4096),
				strings.Repeat("2", 4096),
				strings.Repeat("3", 4096),
			},
			size:       2,
			batchSize:  5000,
			bufferSize: 5000,
			expected: []string{
				strings.Repeat("a", 4096) + "\n",
				strings.Repeat("1", 4096) + "\n",
				strings.Repeat("2", 4096) + "\n",
				strings.Repeat("3", 4096),
			},
			expectedRowCount: []int{
				1,
				1,
				1,
				1,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rowChan := make(chan Batch)
			resultChan := make(chan []string)

			// Collector for the scanned row batches.
			go func() {
				var actual []string
				i := 0
				for buf := range rowChan {
					assert.EqualValues(t, c.expectedRowCount[i], buf.Location.RowCount, "on batch %d", i)
					actual = append(actual, string(bytes.Join(buf.data, nil)))
					i++
				}

				resultChan <- actual
			}()

			all := strings.Join(c.input, "\n")
			reader := strings.NewReader(all)
			opts := scanOptions{
				Size:           c.size,
				Skip:           c.skip,
				Limit:          c.limit,
				Quote:          byte(c.quote),
				Escape:         byte(c.escape),
				BufferByteSize: c.bufferSize,
				BatchByteSize:  c.batchSize,
			}

			err := scan(context.Background(), reader, rowChan, opts)
			if err != nil {
				if c.expectedError == "" {
					assert.NoError(t, err)
				} else {
					assert.Contains(t, err.Error(), c.expectedError)
				}
			}

			// Check results.
			close(rowChan)
			actual := <-resultChan

			assert.Equal(t, c.expected, actual)
			// if !reflect.DeepEqual(actual, c.expected) {
			// 	t.Errorf("Scan() returned unexpected batch results")
			// 	t.Logf("got:\n%q", actual)
			// 	t.Logf("want:\n%q", c.expected)
			// }
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

			rowChan := make(chan Batch, 1)
			opts := scanOptions{
				Size: 50,
				Skip: c.skip,
			}

			err := scan(context.Background(), reader, rowChan, opts)
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

func BenchmarkScan(b *testing.B) {
	benchmarks := []struct {
		name string
		line string
	}{
		// All cases are patterned off of the gov.uk Price Paid Data schema:
		//
		//    https://www.gov.uk/guidance/about-the-price-paid-data
		//
		// but note that no actual personal data has been duplicated here; the
		// entries are made up.
		{
			// Scan is complex enough that it appears we need to warm up the GC
			// before we get stable results. Otherwise, the first benchmark runs
			// artificially slowly.
			name: "warmup (disregard)",
			line: `---------------------------------------------------------------------------------------------------------------------------------`,
		},
		{
			name: "no quotes",
			line: `{5702803E-68CC-416B-BD04-2A6A04369690},1234567,2000-01-02 03:04,123 ABC,A,A,A,000,0,STREET,LOCALITY,TOWN/CITY,DISTRICT,COUNTY,Z,Y`,
		},
		{
			name: "some quotes at the beginning",
			line: `"{5702803E-68CC-416B-BD04-2A6A04369690}",1234567,2000-01-02 03:04,"123 ABC",A,A,A,000,0,STREET,LOCALITY,TOWN/CITY,DISTRICT,COUNTY,Z,Y`,
		},
		{
			name: "some quotes in the middle",
			line: `{5702803E-68CC-416B-BD04-2A6A04369690},1234567,2000-01-02 03:04,"123 ABC",A,A,A,000,0,STREET,LOCALITY,"TOWN OR CITY",DISTRICT,COUNTY,Z,Y`,
		},
		{
			name: "all quotes",
			line: `"{5702803E-68CC-416B-BD04-2A6A04369690}","1234567","2000-01-02 03:04","123 ABC","A","A","A","000","0","STREET","LOCALITY","TOWN/CITY","DISTRICT","COUNTY","Z","Y"`,
		},
		{
			name: "nothing but quotes",
			// This is basically the worst case for an IndexByte implementation.
			// It's intended as a boundary for comparison, not as a case for us
			// to actually optimize.
			line: `"""""""""""""""""""""""""""""""""""","""""","""""""""""""","""""","","","","""","","""""","""""""","""""""","""""""","""""","",""`,
		},
	}

	for _, bm := range benchmarks {
		// Real-world cases need thousands of lines per batch to perform well.
		// parallel-copy defaults to 5000, so that seems like a good number to
		// start optimizing here.
		opts := scanOptions{
			Size: 5000,
		}
		data := strings.Repeat(bm.line+"\n", opts.Size)
		reader := strings.NewReader(data)

		// Run each benchmark twice, once with standard ESCAPEs, and once with
		// custom. (The implementations diverge enough to make it worth tracking
		// both.)
		escType := "standard"

		for i := 0; i < 2; i++ {
			name := fmt.Sprintf("%s (%s escapes)", bm.name, escType)

			b.Run(name, func(b *testing.B) {
				// Make sure our output channel won't block. This relies on each
				// call to Scan() producing exactly one batch
				rowChan := make(chan Batch, b.N)
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					reader.Reset(data) // rewind to the beginning

					err := scan(context.Background(), reader, rowChan, opts)
					if err != nil {
						b.Errorf("Scan() returned unexpected error: %v", err)
					}
				}
			})

			escType = "custom"
			opts.Escape = '\\'
		}
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
