// Package seekablebuffers provides a seekable wrapper around net.Buffers
// that enables retry functionality for database copy operations.
package buffer

import (
	"fmt"
	"io"
)

// Buffers contains zero or more runs of bytes to write.
//
// On certain machines, for certain types of connections, this is
// optimized into an OS-specific batch write operation (such as
// "writev").
type Seekable struct {
	buf      [][]byte
	position int64
}

var (
	_ io.WriterTo = (*Seekable)(nil)
	_ io.Reader   = (*Seekable)(nil)
	_ io.Writer   = (*Seekable)(nil)
	_ io.Seeker   = (*Seekable)(nil)
)

func NewSeekable(buf [][]byte) *Seekable {
	return &Seekable{
		buf:      buf,
		position: 0,
	}
}

func (v *Seekable) HasData() bool {
	return v.position < v.TotalSize()
}

func (v *Seekable) TotalSize() int64 {
	var size int64
	for _, b := range v.buf {
		size += int64(len(b))
	}
	return size
}

// WriteTo writes contents of the buffers to w.
//
// WriteTo implements [io.WriterTo] for [Buffers].
//
// WriteTo modifies the slice v as well as v[i] for 0 <= i < len(v),
// but does not modify v[i][j] for any i, j.
func (v *Seekable) WriteTo(w io.Writer) (n int64, err error) {
	if v.position >= v.TotalSize() {
		return 0, nil
	}

	currentPos := v.position

	for _, buf := range v.buf {
		bufLen := int64(len(buf))
		if currentPos >= bufLen {
			currentPos -= bufLen
			continue
		}

		startInBuf := currentPos
		bytesToWrite := buf[startInBuf:]

		nb, err := w.Write(bytesToWrite)
		n += int64(nb)
		if err != nil {
			v.position += n
			return n, err
		}
		currentPos = 0
	}

	v.position += n
	return n, nil
}

// Read from the buffers.
//
// Read implements [io.Reader] for [Buffers].
//
// Read modifies the slice v as well as v[i] for 0 <= i < len(v),
// but does not modify v[i][j] for any i, j.
func (v *Seekable) Read(p []byte) (n int, err error) {
	if v.position >= v.TotalSize() {
		return 0, io.EOF
	}

	remaining := len(p)
	currentPos := v.position

	for i, buf := range v.buf {
		if remaining == 0 {
			break
		}

		bufLen := int64(len(buf))
		if currentPos >= bufLen {
			currentPos -= bufLen
			continue
		}

		startInBuf := currentPos
		bytesToRead := bufLen - startInBuf
		if int64(remaining) < bytesToRead {
			bytesToRead = int64(remaining)
		}

		copied := copy(p[n:], buf[startInBuf:startInBuf+bytesToRead])
		n += copied
		remaining -= copied
		currentPos = 0

		if i == len(v.buf)-1 && n < len(p) {
			err = io.EOF
		}
	}

	v.position += int64(n)
	return n, err
}

// Write appends data to the buffer
func (v *Seekable) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Create a copy of the input data to avoid issues with caller reusing the slice
	data := make([]byte, len(p))
	copy(data, p)

	v.buf = append(v.buf, data)
	return len(p), nil
}

// WriteString appends a string to the buffer
func (v *Seekable) WriteString(s string) (n int, err error) {
	return v.Write([]byte(s))
}

// Seek sets the position for next Read or Write operation
func (v *Seekable) Seek(offset int64, whence int) (int64, error) {
	totalSize := v.TotalSize()

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = v.position + offset
	case io.SeekEnd:
		newPos = totalSize + offset
	default:
		return v.position, fmt.Errorf("invalid whence value: %d", whence)
	}

	if newPos < 0 {
		return v.position, fmt.Errorf("seek position cannot be negative: %d", newPos)
	}
	if newPos > totalSize {
		return v.position, fmt.Errorf("seek position beyond buffer size: %d > %d", newPos, totalSize)
	}

	v.position = newPos
	return v.position, nil
}
