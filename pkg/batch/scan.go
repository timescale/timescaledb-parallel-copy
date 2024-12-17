package batch

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
)

// Options contains all the configurable knobs for Scan.
type Options struct {
	Size  int   // maximum number of rows per batch
	Skip  int   // how many header lines to skip at the beginning
	Limit int64 // total number of rows to scan after the header

	Quote  byte // the QUOTE character; defaults to '"'
	Escape byte // the ESCAPE character; defaults to QUOTE
}

// Batch represents an operation to copy data into the DB
type Batch struct {
	Data     net.Buffers
	Location Location

	// backup hold the same data as Data. It is used to rewind if something goes wrong
	// Because it copies the slice, the memory is not duplicated
	// Because we only read data, the underlaying memory is not modified neither
	backup net.Buffers
}

func NewBatch(data net.Buffers, location Location) Batch {
	b := Batch{
		Data:     data,
		Location: location,
	}
	b.snapshot()
	return b
}

func (b *Batch) snapshot() {
	b.backup = net.Buffers{}
	b.backup = append(b.backup, b.Data...)
}

// Makes data available again to read
func (b *Batch) Rewind() {
	b.Data = net.Buffers{}
	b.Data = append(b.Data, b.backup...)
}

// Location positions a batch within the original data
type Location struct {
	// StartRow represents the index of the row where the batch starts.
	// First row of the file is row 0
	// The header counts as a line
	StartRow int64
	// RowCount is the number of rows in the batch
	RowCount int
	// ByteOffset is the byte position on the original file.
	// It can be used with ReadAt to process the same batch again.
	ByteOffset int
	// ByteLen represents the number of bytes for the batch.
	// It can be used to know how big the batch is and read it accordingly
	ByteLen int
}

func NewLocation(rowsRead int64, bufferedRows int, skip int, byteOffset int, byteLen int) Location {
	return Location{
		StartRow:   rowsRead - int64(bufferedRows) + int64(skip) - 1, // Index rows starting at 0
		RowCount:   bufferedRows,
		ByteOffset: byteOffset,
		ByteLen:    byteLen,
	}
}

// Scan reads all lines from an io.Reader, partitions them into net.Buffers with
// opts.Size rows each, and writes each batch to the out channel. If opts.Skip
// is greater than zero, that number of lines will be discarded from the
// beginning of the data. If opts.Limit is greater than zero, then Scan will
// stop once it has written that number of rows, across all batches, to the
// channel.
//
// Scan expects the input to be in Postgres CSV format. Since this format allows
// rows to be split over multiple lines, the caller may provide opts.Quote and
// opts.Escape as the QUOTE and ESCAPE characters used for the CSV input.
func Scan(ctx context.Context, r io.Reader, out chan<- Batch, opts Options) error {
	var rowsRead int64
	counter := &CountReader{Next: r}
	reader := bufio.NewReader(counter)

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
			// We pulled a full row from the buffer.
			skip--
		}
	}

	quote := byte('"')
	if opts.Quote != 0 {
		quote = opts.Quote
	}

	escape := quote
	if opts.Escape != 0 {
		escape = opts.Escape
	}

	scanner := makeCSVRowState(quote, escape)

	// We read a continuous stream of []byte from our buffered reader. Rather
	// than coalesce all of the incoming slices into a single contiguous buffer
	// (which would have bad memory usage and performance characteristics for
	// larger CSV datasets, and be wasted anyway as soon as the underlying
	// Postgres connection divides the data into smaller CopyData chunks), keep
	// the slices as-is and store them in net.Buffers, which is a convenient
	// io.Reader abstraction wrapped over a [][]byte.
	bufs := make(net.Buffers, 0, opts.Size)
	var bufferedRows int

	byteStart := counter.Total() - reader.Buffered()
	for {
		eol := false

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
			eol = true

		default:
			return err
		}

		if len(data) > 0 {
			// ReadSlice doesn't make a copy of the data; to avoid an overwrite
			// on the next call, we need to make one now.
			buf := make([]byte, len(data))
			copy(buf, data)
			bufs = append(bufs, buf)

			// Figure out whether we're still inside a quoted value, in which
			// case the row hasn't ended yet even if we're at the end of a line.
			scanner.Scan(buf)
			if eol && !scanner.NeedsMore() {
				bufferedRows++
				rowsRead++
			}

			if bufferedRows >= opts.Size { // dispatch to COPY worker & reset
				byteEnd := counter.Total() - reader.Buffered()
				select {
				case out <- NewBatch(
					bufs,
					NewLocation(rowsRead, bufferedRows, opts.Skip, byteStart, byteEnd-byteStart),
				):
				case <-ctx.Done():
					return ctx.Err()
				}
				bufs = make(net.Buffers, 0, opts.Size)
				bufferedRows = 0
				byteStart = byteEnd
			}
		}

		// Check termination conditions.
		if err == io.EOF {
			break
		} else if opts.Limit != 0 && rowsRead >= opts.Limit {
			break
		}
	}

	// Finished reading input, make sure last batch goes out.
	if len(bufs) > 0 {
		byteEnd := counter.Total() - reader.Buffered()
		select {
		case out <- NewBatch(
			bufs,
			NewLocation(rowsRead, bufferedRows, opts.Skip, byteStart, byteEnd-byteStart),
		):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	log.Print("total rows ", rowsRead)

	return nil
}

// csvRowState maintains a basic parse state for the Postgres CSV format. It's
// designed to help clients maintain an accurate row count even when quoted rows
// are split over multiple lines.
type csvRowState struct {
	quote, escape byte // delimiters

	// scan state
	inQuote  bool // are we in an open quoted value?
	inEscape bool // are we (potentially) in an open escape sequence?
}

// makeCSVRowState initializes a new csvRowState with the given delimiters.
func makeCSVRowState(quote, escape byte) *csvRowState {
	return &csvRowState{
		quote:  quote,
		escape: escape,
	}
}

// Scan reads raw line data from a CSV stream. After feeding data to Scan, the
// parse state may be checked by calling NeedsMore.
//
// Note that newline characters (and whitespace characters in general) are
// significant in Postgres CSV format and MUST NOT be stripped from the stream
// that is given to Scan. (This property is met by bufio.Reader's ReadSlice
// method.)
func (c *csvRowState) Scan(buf []byte) {
	for _, b := range buf {
		// If we think the previous character might have been an escape, the
		// current character might need to be ignored.
		if c.inEscape {
			c.inEscape = false

			// Only the quote and escape delimiters can themselves be escaped.
			switch b {
			case c.quote, c.escape:
				// Okay, it was really an escape and we should ignore this.
				continue
			}

			// NB: this is the strangest corner case of the Postgres CSV
			// format. If the quote and escape delimiters are the same character
			// -- e.g. in the default case, where both are a double-quote (") --
			// and we're to this point in the code, then the last escape
			// character we saw was actually an ending quote, and we need to
			// make sure the state reflects that before continuing.
			//
			// (As a concrete example, consider a buffer with the following
			// data at the start:
			//
			//     "hello world"
			//
			// If the next character in the buffer is a double-quote, then we're
			// still inside an open quoted value, since the "" sequence is
			// replaced with a literal double-quote. But if the next character
			// is a comma, then this is a complete quoted value.)
			//
			// The Postgres code handles this case with a lookahead, but we
			// don't always have the ability to do that here, since we're
			// operating on a buffer stream.
			if c.quote == c.escape {
				c.inQuote = false
			}
		}

		// Escape sequences are only recognized inside of a quoted value;
		// otherwise the escape character has no special meaning.
		if c.inQuote && (b == c.escape) {
			c.inEscape = true
			continue
		}

		if b == c.quote {
			// We know this is an unescaped quote delimiter, so we're either
			// beginning or ending a quoted string.
			c.inQuote = !c.inQuote
		}
	}
}

// NeedsMore returns true if the current row is incomplete: the previous buffers
// given to Scan opened a quoted value that has not yet been closed.
//
// Note that even if NeedsMore returns false, that does NOT imply that the
// current position is at the end of a row. (To decide that, the caller needs to
// track whether the stream is also at the end of a line.)
func (c *csvRowState) NeedsMore() bool {
	// We don't need to check c.inEscape, because that can only be true if
	// c.inQuote is also true.
	return c.inQuote
}

// CountReader is a wrapper that counts how many bytes have been read from the given reader
type CountReader struct {
	Next  io.Reader
	total int
}

func (c *CountReader) Read(b []byte) (int, error) {
	n, err := c.Next.Read(b)
	c.total += n
	return n, err
}
func (c *CountReader) Total() int {
	return c.total
}
