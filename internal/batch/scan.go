package batch

import (
	"bufio"
)

type Batch struct {
	Rows []string
}

// Scan reads lines from a bufio.Scanner, each which should be in CSV format
// with a delimiter specified by --split (comma by default)
func Scan(itemsPerBatch, skip int, limit int64, scanner *bufio.Scanner, batchChan chan *Batch) error {
	rows := make([]string, 0, itemsPerBatch)
	var linesRead int64

	for i := 0; i < skip; i++ {
		scanner.Scan()
	}

	for scanner.Scan() {
		if limit != 0 && linesRead >= limit {
			break
		}

		rows = append(rows, scanner.Text())
		if len(rows) >= itemsPerBatch { // dispatch to COPY worker & reset
			batchChan <- &Batch{rows}
			rows = make([]string, 0, itemsPerBatch)
		}
		linesRead++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Finished reading input, make sure last batch goes out.
	if len(rows) > 0 {
		batchChan <- &Batch{rows}
	}

	return nil
}
