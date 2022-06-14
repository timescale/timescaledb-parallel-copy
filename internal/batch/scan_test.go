package batch_test

import (
	"bufio"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/timescale/timescaledb-parallel-copy/internal/batch"
)

func TestScan(t *testing.T) {
	cases := []struct {
		name     string
		input    []string
		size     int
		skip     int
		limit    int64
		expected [][]string
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
			expected: [][]string{
				{
					"a,b,c",
					"1,2,3",
				},
				{
					"4,5,6",
					"7,8,9",
				},
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
			expected: [][]string{
				{
					"a,b,c",
					"1,2,3",
					"4,5,6",
				},
				{
					"7,8,9",
				},
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
			expected: [][]string{
				{
					"1,2,3",
					"4,5,6",
				},
				{
					"7,8,9",
				},
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
			expected: [][]string{
				{
					"a,b,c",
				},
				{
					"1,2,3",
				},
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rowChan := make(chan *batch.Batch)
			resultChan := make(chan [][]string)

			// Collector for the scanned row batches.
			go func() {
				var actual [][]string

				for b := range rowChan {
					actual = append(actual, b.Rows)
				}

				resultChan <- actual
			}()

			all := strings.Join(c.input, "\n")
			scanner := bufio.NewScanner(strings.NewReader(all))

			err := batch.Scan(c.size, c.skip, c.limit, scanner, rowChan)
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

	t.Run("scanner errors are bubbled up", func(t *testing.T) {
		// iotest.TimeoutReader returns iotest.ErrTimeout on the second read.
		scanner := bufio.NewScanner(
			iotest.TimeoutReader(strings.NewReader(`
				some input
				should be discarded
			`)),
		)
		rowChan := make(chan *batch.Batch, 1)

		err := batch.Scan(50, 0, 0, scanner, rowChan)
		if !errors.Is(err, iotest.ErrTimeout) {
			t.Errorf("Scan() returned unexpected error: %v", err)
			t.Logf("want: %v", iotest.ErrTimeout)
		}

		// Make sure no batches were written to the channel; we shouldn't have
		// had enough lines to fill one.
		close(rowChan)
		if len(rowChan) > 0 {
			t.Errorf("Scan() buffered unexpected data: %v", (<-rowChan).Rows)
		}
	})
}
