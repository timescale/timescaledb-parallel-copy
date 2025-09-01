package csvcopy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/timescale/timescaledb-parallel-copy/pkg/buffer"
)

// scanOptions contains all the configurable knobs for Scan.
type scanOptions struct {
	Size          int   // maximum number of rows per batch, It may be less than this if ChunkByteSize is reached first
	Skip          int   // how many header lines to skip at the beginning
	Limit         int64 // total number of rows to scan after the header.
	BatchByteSize int   // Max byte size for a batch.

	Quote  byte // the QUOTE character; defaults to '"'
	Escape byte // the ESCAPE character; defaults to QUOTE

	// ImportID used for idempotency.
	// If the same ImportID is inserted, it will attempt to recover from a previously failed insert.
	// If data is already inserted, it is a NOOP
	ImportID string
}

// Batch represents an operation to copy data into the DB
type Batch struct {
	data     *buffer.Seekable
	Location Location
}

func newBatch(data *buffer.Seekable, location Location) Batch {
	b := Batch{
		data:     data,
		Location: location,
	}
	return b
}

// newBatchFromReader used for testing purposes
func newBatchFromReader(r io.Reader) Batch {
	b := Batch{
		data: buffer.NewSeekable([][]byte{}),
	}
	buf := make([]byte, 32*1024)

	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error reading data: %v", err)
		}

		b.Location.ByteLen += n
		// Process the data read from the buffer
		_, _ = b.data.Write(buf[:n]) // Write cannot fail, just exists to meet Writer interface
	}

	return b
}

// Location positions a batch within the original data
type Location struct {
	// ImportID used for idempotency.
	// If the same ImportID is inserted, it will attempt to recover from a previously failed insert.
	// If data is already inserted, it is a NOOP
	ImportID string
	// StartRow represents the index of the row where the batch starts.
	// First row of the file is row 0
	// The header counts as a line
	StartRow int64
	// RowCount is the number of rows in the batch
	RowCount int
	// ByteOffset is the byte position in the original file.
	// It can be used with ReadAt to process the same batch again.
	ByteOffset int
	// ByteLen represents the number of bytes for the batch.
	// It can be used to know how big the batch is and read it accordingly
	ByteLen int
}

func newLocation(importID string, rowsRead int64, bufferedRows int, skip int, byteOffset int, byteLen int) Location {
	return Location{
		ImportID:   importID,
		StartRow:   rowsRead - int64(bufferedRows) + int64(skip), // Index rows starting at 0
		RowCount:   bufferedRows,
		ByteOffset: byteOffset,
		ByteLen:    byteLen,
	}
}

func (l Location) String() string {
	return fmt.Sprintf("%s:%d", l.ImportID, l.StartRow)
}

func (l Location) HasImportID() bool {
	return l.ImportID != ""
}

// scan reads all lines from a pre-configured buffered reader, partitions them into net.Buffers with
// opts.Size rows each, and writes each batch to the out channel. If opts.Limit is greater than zero,
// then scan will stop once it has written that number of rows, across all batches, to the channel.
//
// scan expects the input to be in Postgres CSV format. Since this format allows
// rows to be split over multiple lines, the caller may provide opts.Quote and
// opts.Escape as the QUOTE and ESCAPE characters used for the CSV input.
//
// The caller is responsible for setting up the CountReader and buffered reader,
// and for skipping any headers before calling this function.
func scan(ctx context.Context, counter *CountReader, reader *bufio.Reader, out chan<- Batch, opts scanOptions) error {
	var rowsRead int64

	batchSize := 20 * 1024 * 1024 // 20 MB batch size
	if opts.BatchByteSize > 0 {
		batchSize = opts.BatchByteSize
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
	// the slices as-is and store them in our Seekable buffer, which is a convenient
	// io.Reader abstraction wrapped over a [][]byte with additional seek functionality.
	bufs := buffer.NewSeekable([][]byte{})
	var bufferedRows int

	// finishedRow is true if the current row has been fully read and counted
	finishedRow := true
	byteStart := counter.Total - reader.Buffered()

	// send the current data until the byteEnd
	send := func(byteEnd int) error {
		select {
		case out <- newBatch(
			bufs,
			newLocation(opts.ImportID, rowsRead, bufferedRows, opts.Skip, byteStart, byteEnd-byteStart),
		):
		case <-ctx.Done():
			return ctx.Err()
		}
		bufs = buffer.NewSeekable([][]byte{})
		bufferedRows = 0
		byteStart = byteEnd
		return nil
	}
	for {
		eol := false

		byteEndBeforeLine := counter.Total - reader.Buffered()

		data, err := reader.ReadSlice('\n')

		switch err {
		case bufio.ErrBufferFull:
			// If we hit buffer full, we do not have enough data to read a full row
			return fmt.Errorf("reading lines, %w. you should provably increase batch size", err)

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
			byteEnd := counter.Total - reader.Buffered()
			// Chunk will be bigger than ChunkByteSize if we append the current line. Let's send the data we have int he buffer
			if byteEnd-byteStart > batchSize {
				log.Printf("reached max batch size, sending %d rows", bufferedRows)
				err := send(byteEndBeforeLine)
				if err != nil {
					return err
				}
			}

			finishedRow = false

			_, _ = bufs.Write(data) // Write cannot fail, just exists to meet Writer interface

			// Figure out whether we're still inside a quoted value, in which
			// case the row hasn't ended yet even if we're at the end of a line.
			// TODO: This may no be a feasible scenario given that we require a full row to be in the buffer.
			scanner.Scan(data)
			if eol && !scanner.NeedsMore() {
				finishedRow = true
				bufferedRows++
				rowsRead++
			}

			if bufferedRows >= opts.Size { // dispatch to COPY worker & reset
				err := send(byteEnd)
				if err != nil {
					return err
				}
			}
		}

		// Check termination conditions.
		if err == io.EOF {
			// if we have data in the buffer and we are not at the end of a row, we need to count the last row
			// this can happen if the last row is not terminated by a newline
			if bufs.TotalSize() > 0 && !finishedRow {
				bufferedRows++
				rowsRead++
			}
			break
		} else if opts.Limit != 0 && rowsRead >= opts.Limit {
			break
		}
	}
	// Finished reading input, make sure last batch goes out.
	if bufs.HasData() {
		byteEnd := counter.Total - reader.Buffered()
		select {
		case out <- newBatch(
			bufs,
			newLocation(opts.ImportID, rowsRead, bufferedRows, opts.Skip, byteStart, byteEnd-byteStart),
		):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

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
	Reader io.Reader
	Total  int
}

func (c *CountReader) Read(b []byte) (int, error) {
	n, err := c.Reader.Read(b)
	c.Total += n
	return n, err
}

// skipLines skips the specified number of lines starting from the very beginning of the file.
func skipLines(reader *bufio.Reader, skip int) error {
	for skip > 0 {
		// The use of ReadLine() here avoids copying or buffering data that
		// we're just going to discard.
		_, isPrefix, err := reader.ReadLine()

		if err == io.EOF {
			// No data?
			return nil
		} else if err != nil {
			return fmt.Errorf("skipping line: %w", err)
		}
		if !isPrefix {
			// We pulled a full row from the buffer.
			skip--
		}
	}
	return nil
}

// parseHeaders parses the first header line and skips remaining header lines
func parseHeaders(reader *bufio.Reader, quote, escape byte, comma rune) ([]string, error) {
	// Read the first header line
	var headerLine []byte
	for {
		data, isPrefix, err := reader.ReadLine()
		if err == io.EOF {
			return []string{}, nil
		} else if err != nil {
			return nil, fmt.Errorf("reading header: %w", err)
		}

		headerLine = append(headerLine, data...)
		if !isPrefix {
			// We have a complete line
			break
		}
	}

	// Parse the CSV header line using PostgreSQL CSV format
	// (which differs from standard CSV in escape handling)
	headers, err := parsePostgreSQLCSVLine(string(headerLine), comma, quote, escape)
	if err != nil {
		return nil, fmt.Errorf("parsing header line: %w", err)
	}

	return headers, nil
}

// parsePostgreSQLCSVLine parses a CSV line using PostgreSQL CSV format rules
// This handles quote, escape, and comma characters as PostgreSQL COPY expects
func parsePostgreSQLCSVLine(line string, comma rune, quote, escape byte) ([]string, error) {
	var fields []string
	var field []byte
	var inQuote bool

	for i := 0; i < len(line); i++ {
		b := line[i]

		if inQuote {
			if b == escape && i+1 < len(line) {
				// Handle escape sequences - look ahead to see what's being escaped
				next := line[i+1]
				if next == quote || next == escape {
					// Valid escape sequence, add the escaped character
					field = append(field, next)
					i++ // Skip the next character as it's been consumed
					continue
				}
			}

			if b == quote {
				// End of quoted field
				inQuote = false
				continue
			}

			// Regular character inside quotes
			field = append(field, b)
		} else {
			if b == quote {
				// Start of quoted field
				inQuote = true
				continue
			}

			if rune(b) == comma {
				// Field separator
				fields = append(fields, string(field))
				field = field[:0]
				continue
			}

			// Regular character outside quotes
			field = append(field, b)
		}
	}

	// Add the last field
	fields = append(fields, string(field))

	if inQuote {
		return nil, fmt.Errorf("unterminated quoted field in header line")
	}

	return fields, nil
}
