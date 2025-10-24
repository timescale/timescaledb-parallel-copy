// Package buffer provides a pool of seekable buffers.
// The seekable buffer enables retry functionality for database copy operations.
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
	size       int
}

func NewPool(capacity int, bufferSize int) *Pool {
	return &Pool{
		buffers:    make(chan *Seekable, capacity),
		bufferSize: bufferSize,
		size:       0,
	}
}

// Get returns an item from the pool, if one is available.
// If the pool has not reached its capacity, a new item is allocated.
// If no items are available, and the pool is at capacity, it blocks.
// If the context is cancelled, returns nil and the context's error.
// To return a Seekable to the pool, call Close on the Seekable.
func (b *Pool) Get(ctx context.Context) (*Seekable, error) {
	select {
	case buf := <-b.buffers:
		return buf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if b.size < cap(b.buffers) {
			b.size += 1
			buf := &Seekable{
				buf:      make([]byte, 0, b.bufferSize),
				position: 0,
				pool:     b,
			}
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

// put puts an item back in the pool.
func (b *Pool) put(buf *Seekable) {
	b.buffers <- buf
}

// Seekable is a seekable buffer
type Seekable struct {
	buf      []byte
	position int64
	pool     *Pool
}

var (
	_ io.Reader = (*Seekable)(nil)
	_ io.Writer = (*Seekable)(nil)
	_ io.Seeker = (*Seekable)(nil)
	_ io.Closer = (*Seekable)(nil)
)

func NewSeekable(buf []byte) *Seekable {
	return &Seekable{
		buf:      buf,
		position: 0,
		pool:     nil,
	}
}

func (v *Seekable) HasData() bool {
	return v.position < v.Len()
}

func (v *Seekable) Len() int64 {
	return int64(len(v.buf))
}

// Close closes the buffer, returning it to the pool.
func (v *Seekable) Close() error {
	v.position = 0
	v.buf = v.buf[:0]
	if v.pool != nil {
		v.pool.put(v)
	}
	return nil
}

// Read from the buffers.
//
// Read implements [io.Reader] for [Seekable].
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

// Seek sets the position for next Read operation
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
