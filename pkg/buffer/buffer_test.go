package buffer

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSeekAndRead(t *testing.T) {
	data := []byte("hello world")

	sb := NewSeekable(data)

	// Test initial read
	buf := make([]byte, 11)
	n, err := sb.Read(buf)
	if n != 11 || (err != nil && err != io.EOF) {
		t.Errorf("Read() = (%d, %v), expected 11 bytes", n, err)
	}
	if string(buf) != "hello world" {
		t.Errorf("Read data = %q, want %q", string(buf), "hello world")
	}

	// Test seek to beginning
	pos, err := sb.Seek(0, io.SeekStart)
	if pos != 0 || err != nil {
		t.Errorf("Seek(0, SeekStart) = (%d, %v), want (0, nil)", pos, err)
	}

	// Test read after seek to beginning
	buf = make([]byte, 11)
	n, err = sb.Read(buf)
	if n != 11 || (err != nil && err != io.EOF) {
		t.Errorf("Read after seek = (%d, %v), expected 11 bytes", n, err)
	}
	if string(buf) != "hello world" {
		t.Errorf("Read after seek = %q, want %q", string(buf), "hello world")
	}
}

func TestSeekPositions(t *testing.T) {
	data := []byte("abcdefghi")

	sb := NewSeekable(data)

	tests := []struct {
		name     string
		offset   int64
		whence   int
		expected int64
		readData string
	}{
		{"SeekStart 0", 0, io.SeekStart, 0, "abcdefghi"},
		{"SeekStart 3", 3, io.SeekStart, 3, "defghi"},
		{"SeekStart 6", 6, io.SeekStart, 6, "ghi"},
		{"SeekStart 9", 9, io.SeekStart, 9, ""}, // At end
		{"SeekCurrent -3", -3, io.SeekCurrent, 6, "ghi"},
		{"SeekCurrent -6", -6, io.SeekCurrent, 0, "abcdefghi"},
		{"SeekEnd 0", 0, io.SeekEnd, 9, ""},
		{"SeekEnd -3", -3, io.SeekEnd, 6, "ghi"},
		{"SeekEnd -9", -9, io.SeekEnd, 0, "abcdefghi"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset position for each test case to ensure predictable state
			if strings.Contains(tt.name, "SeekCurrent") {
				// For SeekCurrent tests, set up the expected starting position
				if tt.name == "SeekCurrent -3" {
					_, _ = sb.Seek(9, io.SeekStart) // Start from end
				} else if tt.name == "SeekCurrent -6" {
					_, _ = sb.Seek(6, io.SeekStart) // Start from position 6
				}
			}

			pos, err := sb.Seek(tt.offset, tt.whence)
			if pos != tt.expected || err != nil {
				t.Errorf("Seek(%d, %d) = (%d, %v), want (%d, nil)",
					tt.offset, tt.whence, pos, err, tt.expected)
			}

			// Read remaining data
			buf := make([]byte, 20)
			n, err := sb.Read(buf)
			if err != nil && err != io.EOF {
				t.Errorf("Read after seek failed: %v", err)
			}
			readData := string(buf[:n])
			if readData != tt.readData {
				t.Errorf("Read data = %q, want %q", readData, tt.readData)
			}
		})
	}
}

func TestSeekErrors(t *testing.T) {
	data := []byte("test")
	sb := NewSeekable(data)

	tests := []struct {
		name   string
		offset int64
		whence int
	}{
		{"negative position", -1, io.SeekStart},
		{"beyond end", 10, io.SeekStart},
		{"invalid whence", 0, 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sb.Seek(tt.offset, tt.whence)
			if err == nil {
				t.Errorf("Seek(%d, %d) should return error", tt.offset, tt.whence)
			}
		})
	}
}

func TestPartialReads(t *testing.T) {
	data := []byte("abcdefghijklmno")

	sb := NewSeekable(data)

	// Test reading in small chunks
	result := ""
	buf := make([]byte, 3)

	for {
		n, err := sb.Read(buf)
		if n > 0 {
			result += string(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}
	}

	expected := "abcdefghijklmno"
	if result != expected {
		t.Errorf("Partial reads result = %q, want %q", result, expected)
	}
}

func TestRetryScenario(t *testing.T) {
	// Simulate CSV data that might need retry
	csvData := bytes.Join([][]byte{
		[]byte("id,name,value\n"),
		[]byte("1,John,100\n"),
		[]byte("2,Jane,200\n"),
		[]byte("3,Bob,300\n"),
	}, []byte(""))

	sb := NewSeekable(csvData)

	// Simulate first attempt that reads some data
	attempt1 := &strings.Builder{}
	buf := make([]byte, 10) // Small buffer to simulate partial read
	n, err := sb.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("First read failed: %v", err)
	}
	attempt1.Write(buf[:n])

	// Get current position for verification
	currentPos, _ := sb.Seek(0, io.SeekCurrent)
	if currentPos != 10 {
		t.Errorf("Position after first read = %d, want 10", currentPos)
	}

	// Simulate retry after reset
	pos, err := sb.Seek(0, io.SeekStart)
	if pos != 0 || err != nil {
		t.Errorf("Reset seek failed: (%d, %v)", pos, err)
	}

	attempt2 := &strings.Builder{}
	_, err = io.Copy(attempt2, sb)
	if err != nil {
		t.Fatalf("Retry failed: %v", err)
	}

	expected := "id,name,value\n1,John,100\n2,Jane,200\n3,Bob,300\n"
	if attempt2.String() != expected {
		t.Errorf("Retry result = %q, want %q", attempt2.String(), expected)
	}

	// Verify partial read in first attempt
	if len(attempt1.String()) != 10 {
		t.Errorf("First attempt read %d bytes, want 10", len(attempt1.String()))
	}
}

func TestSeekBoundaries(t *testing.T) {
	data := []byte("abcdefghi")

	sb := NewSeekable(data)

	// Test seeking to exact buffer boundaries
	positions := []int64{0, 3, 6, 9} // Start, end of first, end of second, end

	for _, pos := range positions {
		actualPos, err := sb.Seek(pos, io.SeekStart)
		if actualPos != pos || err != nil {
			t.Errorf("Seek to boundary %d = (%d, %v), want (%d, nil)",
				pos, actualPos, err, pos)
		}

		// Verify we can read from this position
		buf := make([]byte, 1)
		n, err := sb.Read(buf)

		if pos == 9 { // At end
			if n != 0 || err != io.EOF {
				t.Errorf("Read at end pos %d = (%d, %v), want (0, EOF)", pos, n, err)
			}
		} else {
			if n != 1 || (err != nil && err != io.EOF) {
				t.Errorf("Read at pos %d = (%d, %v), want (1, nil or EOF)", pos, n, err)
			}
		}
	}
}

func TestWrite(t *testing.T) {
	// Test basic write functionality
	sb := NewSeekable(make([]byte, 0, 20))

	n, err := sb.Write([]byte("hello"))
	if n != 5 || err != nil {
		t.Errorf("Write('hello') = (%d, %v), want (5, nil)", n, err)
	}

	n, err = sb.Write([]byte(" world"))
	if n != 6 || err != nil {
		t.Errorf("Write(' world') = (%d, %v), want (6, nil)", n, err)
	}

	// Test reading back the written data
	_, err = sb.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("Seek to start failed: %v", err)
	}
	buf := make([]byte, 20)
	n, err = sb.Read(buf)
	if n != 11 || (err != nil && err != io.EOF) {
		t.Errorf("Read after write = (%d, %v), want (11, nil or EOF)", n, err)
	}
	if string(buf[:n]) != "hello world" {
		t.Errorf("Read data = %q, want %q", string(buf[:n]), "hello world")
	}
}

func TestWriteAndSeek(t *testing.T) {
	// Test writing data, seeking, and reading from different positions
	sb := NewSeekable(make([]byte, 0, 20))

	_, _ = sb.Write([]byte("abc"))
	_, _ = sb.Write([]byte("def"))
	_, _ = sb.Write([]byte("ghi"))

	// Test reading from beginning
	_, err := sb.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("Seek to start failed: %v", err)
	}
	buf := make([]byte, 3)
	n, err := sb.Read(buf)
	if n != 3 || err != nil {
		t.Errorf("Read from start = (%d, %v), want (3, nil)", n, err)
	}
	if string(buf) != "abc" {
		t.Errorf("Read data = %q, want %q", string(buf), "abc")
	}

	// Test reading from middle
	_, err = sb.Seek(3, io.SeekStart)
	if err != nil {
		t.Errorf("Seek to start failed: %v", err)
	}
	n, err = sb.Read(buf)
	if n != 3 || err != nil {
		t.Errorf("Read from middle = (%d, %v), want (3, nil)", n, err)
	}
	if string(buf) != "def" {
		t.Errorf("Read data = %q, want %q", string(buf), "def")
	}

	// Test reading from end
	_, err = sb.Seek(6, io.SeekStart)
	if err != nil {
		t.Errorf("Seek to start failed: %v", err)
	}
	n, err = sb.Read(buf)
	if n != 3 || (err != nil && err != io.EOF) {
		t.Errorf("Read from end = (%d, %v), want (3, nil or EOF)", n, err)
	}
	if string(buf) != "ghi" {
		t.Errorf("Read data = %q, want %q", string(buf), "ghi")
	}
}

func TestWriteEmptySlice(t *testing.T) {
	sb := NewSeekable(make([]byte, 0, 20))

	// Test writing empty slice
	n, err := sb.Write([]byte{})
	if n != 0 || err != nil {
		t.Errorf("Write([]) = (%d, %v), want (0, nil)", n, err)
	}

	// Test writing nil slice
	n, err = sb.Write(nil)
	if n != 0 || err != nil {
		t.Errorf("Write(nil) = (%d, %v), want (0, nil)", n, err)
	}

	// Buffer should still be empty
	buf := make([]byte, 10)
	n, err = sb.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("Read from empty buffer = (%d, %v), want (0, EOF)", n, err)
	}
}

func TestWriteToEmptyBuffer(t *testing.T) {
	sb := NewSeekable([]byte{})

	n, err := sb.Write([]byte("Hello World"))
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

// TestNewPool verifies pool initialization
func TestNewPool(t *testing.T) {
	bufferSize := 1024

	pool := NewPool(5, bufferSize)

	require.Equal(t, 0, pool.size, "expected initial size 0")
	require.Equal(t, 5, cap(pool.buffers))
}

// TestGetAllocatesNewBuffer when pool is below capacity
func TestGetAllocatesNewBuffer(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(5, 1024)

	buf, err := pool.Get(ctx)

	require.NoError(t, err)
	require.NotNil(t, buf)
	require.Equal(t, 1024, cap(buf.buf))
	require.Equal(t, 0, len(buf.buf))
}

func TestPoolCapacityMaintained(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	p := NewPool(1, 1024)
	buf, err := p.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, buf)
	buf, err = p.Get(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, buf)
}

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := NewPool(1, 1024)
	cancel()
	buf, err := p.Get(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, buf)
}

// TestGetMultipleBuffers verifies multiple allocations
func TestGetMultipleBuffers(t *testing.T) {
	ctx := context.Background()
	capacity := 3
	pool := NewPool(capacity, 512)

	buffers := make([]*Seekable, capacity)
	for i := 0; i < capacity; i++ {
		var err error
		buffers[i], err = pool.Get(ctx)
		require.NoError(t, err)
		require.NotNil(t, buffers[i])
	}

	// All buffers should be different instances
	for i := 0; i < capacity; i++ {
		for j := i + 1; j < capacity; j++ {
			require.NotSame(t, buffers[i], buffers[j])
		}
	}
}

// TestCloseReturnsBufferToPool verifies Close resets and returns buffer
func TestCloseReturnsBufferToPool(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(2, 256)

	buf, err := pool.Get(ctx)
	require.NoError(t, err)
	buf.buf = append(buf.buf, byte(42))
	buf.position = 10

	err = buf.Close()
	require.NoError(t, err)

	// Buffer should be reset
	require.Equal(t, 0, len(buf.buf))
	require.Equal(t, int64(0), buf.position)

	// Buffer should be retrievable
	retrieved, err := pool.Get(ctx)
	require.NoError(t, err)
	require.Same(t, buf, retrieved)
}

// TestConcurrentGetPut tests concurrent operations
func TestConcurrentGetPut(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(5, 512)
	numGoroutines := 20
	operationsPerGoroutine := 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				buf, err := pool.Get(ctx)
				require.NoError(t, err)
				require.NotNil(t, buf)
				buf.buf = append(buf.buf, byte(j%256))
				err = buf.Close()
				require.NoError(t, err)
			}
		}()
	}

	wg.Wait()
}

// TestGetReusesBuffers verifies that buffers are reused from pool
func TestGetReusesBuffers(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(2, 256)

	// Get and put multiple buffers
	buf1, err := pool.Get(ctx)
	require.NoError(t, err)
	buf2, err := pool.Get(ctx)
	require.NoError(t, err)
	err = buf1.Close()
	require.NoError(t, err)
	err = buf2.Close()
	require.NoError(t, err)

	// Next gets should return the same instances
	retrieved1, err := pool.Get(ctx)
	require.NoError(t, err)
	retrieved2, err := pool.Get(ctx)
	require.NoError(t, err)

	require.Same(t, retrieved1, buf1)
	require.Same(t, retrieved2, buf2)
}

// TestBufferSizeConfiguration verifies buffer size is respected
func TestBufferSizeConfiguration(t *testing.T) {
	ctx := context.Background()
	tests := []int{256, 1024, 4096}

	for _, size := range tests {
		pool := NewPool(2, size)
		buf, err := pool.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, size, cap(buf.buf))
	}
}
