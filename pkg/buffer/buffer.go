package buffer

import (
	"bytes"
	"io"
)

type Pool struct {
	buffers chan Segmented
	segmentSize int
	segmentCount int
	capacity int
	size    int
}

func NewPool(poolSize int, segmentSize, segmentCount int) *Pool {
	return &Pool{
		buffers: make(chan Segmented, poolSize),
		segmentCount: segmentCount,
		segmentSize: segmentSize,
		size: 0,
		capacity: poolSize,
	}
}

func (b *Pool) Get() Segmented  {
	select {
	case buf := <-b.buffers:
		return buf
	default:
		if b.size < b.capacity {
			buf := NewSegmented(b.segmentSize, b.segmentCount)
			return buf
		}
		buf := <-b.buffers
		return buf
	}
}

func (b *Pool) Put(buf Segmented) {
	buf.Reset()
	b.buffers <- buf
}

type Segmented struct {
	buf [][]byte
	segmentCount int
	segmentSize int
	currentSegment     int  // current segment index
	segmentOffset      int  // offset within current segment
}

func NewSegmented(segmentSize, segmentCount int) Segmented {
	buf := make([][]byte, 0, segmentCount)
	for range segmentCount {
		buf = append(buf, make([]byte, 0, segmentSize))
	}
	return Segmented{
		buf: buf,
		segmentCount: segmentCount,
		segmentSize: segmentSize,
	}
}

func (s* Segmented) Reset() {
	s.segmentOffset = 0
	s.currentSegment = 0
	for i := range s.segmentCount {
		s.buf[i] = s.buf[i][:0]
	}
}

func (s *Segmented) Write(p []byte) (n int, err error) {
	written := 0
	for len(p) > 0 {
		if s.segmentOffset >= s.segmentSize {
			// Move to next segment
			s.currentSegment++
			s.segmentOffset = 0
			if s.currentSegment >= s.segmentCount {
				return written, io.ErrShortBuffer
			}
		}

		// Write to current segment
		available := s.segmentSize - s.segmentOffset
		toWrite := len(p)
		if toWrite > available {
			toWrite = available
		}

		copy(s.buf[s.currentSegment][s.segmentOffset:s.segmentOffset+toWrite], p[:toWrite])
		s.segmentOffset += toWrite
		s.buf[s.currentSegment] = s.buf[s.currentSegment][:s.segmentOffset]
		p = p[toWrite:]
		written += toWrite
	}
	return written, nil
}

func (s* Segmented) Len() int {
	return s.currentSegment * s.segmentSize + s.segmentOffset
}

func (s* Segmented) Reader() io.Reader {
	readers := make([]io.Reader, 0, s.currentSegment)
	for i := range s.currentSegment + 1 {
		readers = append(readers, bytes.NewReader(s.buf[i]))
	}
	return io.MultiReader(readers...)
}
