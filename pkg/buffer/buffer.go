// Package buffer provides a seekable bufferwrapper around net.Buffers
// that enables retry functionality for database copy operations.
package buffer

import (
	"context"
	"fmt"
	"io"
)

// Pool provides a pool of Seekable buffers.
// It has a fixed capacity, allocating new Seekable buffers on demand.
type Pool struct {
	buffers    chan *Seekable
	bufferSize int
	capacity   int
	size       int
}

func NewPool(capacity int, bufferSize int) *Pool {
	return &Pool{
		buffers:    make(chan *Seekable, capacity),
		bufferSize: bufferSize,
		size:       0,
		capacity:   capacity,
	}
}

// Get returns an item from the pool, if one is available.
// If the pool has not reached its capacity, a new item is allocated.
// If no items are available, and the pool is at capacity, it blocks.
// If the context is cancelled, returns nil and the context's error.
func (b *Pool) Get(ctx context.Context) (*Seekable, error) {
	select {
	case buf := <-b.buffers:
		return buf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if b.size < cap(b.buffers) {
			b.size += 1
			buf := NewSeekable(make([]byte, 0, b.bufferSize))
			return buf, nil
		}
		select {
			case buf := <-b.buffers:
				return buf, nil
			case <-ctx.Done():
				return nil, ctx.Err()
		}
	}
}

// Put puts an item back in the pool.
func (b *Pool) Put(buf *Seekable) {
	buf.Reset()
	b.buffers <- buf
}

// Seekable is a seekable buffer
type Seekable struct {
	buf      []byte
	position int64
}

var (
	_ io.Reader = (*Seekable)(nil)
	_ io.Writer = (*Seekable)(nil)
	_ io.Seeker = (*Seekable)(nil)
)

func NewSeekable(buf []byte) *Seekable {
	return &Seekable{
		buf:      buf,
		position: 0,
	}
}

func (v *Seekable) HasData() bool {
	return v.position < v.Len()
}

func (v *Seekable) Len() int64 {
	return int64(len(v.buf))
}

// Read from the buffers.
//
// Read implements [io.Reader] for [Buffers].
//
// Read modifies the slice v as well as v[i] for 0 <= i < len(v),
// but does not modify v[i][j] for any i, j.
func (v *Seekable) Read(p []byte) (n int, err error) {
	if v.position >= int64(len(v.buf)) {
		return 0, io.EOF
	}

	remaining := min(len(p), len(v.buf)-int(v.position))

	n = copy(p, v.buf[v.position:int(v.position)+remaining])
	v.position += int64(n)

	return n, err
}

// Write appends data to the buffer
func (v *Seekable) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	capacity := min(cap(v.buf)-len(v.buf), len(p))

	n = copy(v.buf[len(v.buf):len(v.buf)+capacity], p)
	v.buf = v.buf[:len(v.buf)+capacity]

	if len(p) > capacity {
		return n, io.ErrShortWrite
	}

	return n, nil
}

// WriteString appends a string to the buffer
func (v *Seekable) WriteString(s string) (n int, err error) {
	return v.Write([]byte(s))
}

// Seek sets the position for next Read or Write operation
func (v *Seekable) Seek(offset int64, whence int) (int64, error) {
	totalSize := v.Len()

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

// Reset resets the Seekable buffer for use
func (v *Seekable) Reset() {
	v.position = 0
	v.buf = v.buf[:0]
}
