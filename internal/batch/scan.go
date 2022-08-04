package batch

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

// Options contains all the configurable knobs for Scan.
type Options struct {
	Size  int   // maximum number of rows per batch
	Skip  int   // how many header lines to skip at the beginning
	Limit int64 // total number of rows to scan after the header
}

// Scan reads all lines from an io.Reader, partitions them into net.Buffers with
// opts.Size lines each, and writes each batch to the out channel. If opts.Skip
// is greater than zero, that number of lines will be discarded from the
// beginning of the data. If opts.Limit is greater than zero, then Scan will
// stop once it has written that number of lines, across all batches, to the
// channel.
func Scan(r io.Reader, out chan<- net.Buffers, opts Options) error {
	var linesRead int64
	reader := bufio.NewReader(r)

	for skip := opts.Skip; skip > 0; {
		// The use of ReadLine() here avoids copying or buffering data that
		// we're just going to discard.
		_, isPrefix, err := reader.ReadLine()

		if err == io.EOF {
			// No data?
			return nil
		} else if err != nil {
			return fmt.Errorf("skipping header: %w", err)
		}

		if !isPrefix {
			// We pulled a full line from the buffer.
			skip--
		}
	}

	// We read a continuous stream of []byte from our buffered reader. Rather
	// than coalesce all of the incoming slices into a single contiguous buffer
	// (which would have bad memory usage and performance characteristics for
	// larger CSV datasets, and be wasted anyway as soon as the underlying
	// Postgres connection divides the data into smaller CopyData chunks), keep
	// the slices as-is and store them in net.Buffers, which is a convenient
	// io.Reader abstraction wrapped over a [][]byte.
	bufs := make(net.Buffers, 0, opts.Size)
	var bufferedLines int

	for {
		data, err := reader.ReadSlice('\n')

		switch err {
		case bufio.ErrBufferFull:
			// This is fine; add the data we have to the output and look for the
			// end of line during the next iteration.

		case io.EOF:
			// Also fine, but unlike ErrBufferFull we won't have another
			// iteration after this. We still need to handle any data that was
			// returned.

		case nil:
			// We read a full line from the input.
			bufferedLines++
			linesRead++

		default:
			return err
		}

		if len(data) > 0 {
			// ReadSlice doesn't make a copy of the data; to avoid an overwrite
			// on the next call, we need to make one now.
			buf := make([]byte, len(data))
			copy(buf, data)
			bufs = append(bufs, buf)

			if bufferedLines >= opts.Size { // dispatch to COPY worker & reset
				out <- bufs
				bufs = make(net.Buffers, 0, opts.Size)
				bufferedLines = 0
			}
		}

		// Check termination conditions.
		if err == io.EOF {
			break
		} else if opts.Limit != 0 && linesRead >= opts.Limit {
			break
		}
	}

	// Finished reading input, make sure last batch goes out.
	if len(bufs) > 0 {
		out <- bufs
	}

	return nil
}
