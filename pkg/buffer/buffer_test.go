package buffer

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestSeekAndRead(t *testing.T) {
	data := [][]byte{
		[]byte("hello"),
		[]byte(" "),
		[]byte("world"),
	}

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
	data := [][]byte{
		[]byte("abc"),
		[]byte("def"),
		[]byte("ghi"),
	}

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
					sb.Seek(9, io.SeekStart) // Start from end
				} else if tt.name == "SeekCurrent -6" {
					sb.Seek(6, io.SeekStart) // Start from position 6
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
	data := [][]byte{[]byte("test")}
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

func TestWriteTo(t *testing.T) {
	data := [][]byte{
		[]byte("hello"),
		[]byte(" "),
		[]byte("world"),
	}

	sb := NewSeekable(data)

	// Test WriteTo from beginning
	var buf bytes.Buffer
	n, err := sb.WriteTo(&buf)
	if n != 11 || err != nil {
		t.Errorf("WriteTo() = (%d, %v), want (11, nil)", n, err)
	}
	if buf.String() != "hello world" {
		t.Errorf("WriteTo data = %q, want %q", buf.String(), "hello world")
	}

	// Test WriteTo after seek
	sb.Seek(6, io.SeekStart) // Start from "world"
	buf.Reset()
	n, err = sb.WriteTo(&buf)
	if n != 5 || err != nil {
		t.Errorf("WriteTo after seek = (%d, %v), want (5, nil)", n, err)
	}
	if buf.String() != "world" {
		t.Errorf("WriteTo after seek data = %q, want %q", buf.String(), "world")
	}
}

func TestPartialReads(t *testing.T) {
	data := [][]byte{
		[]byte("abcde"),
		[]byte("fghij"),
		[]byte("klmno"),
	}

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
	csvData := [][]byte{
		[]byte("id,name,value\n"),
		[]byte("1,John,100\n"),
		[]byte("2,Jane,200\n"),
		[]byte("3,Bob,300\n"),
	}

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
	data := [][]byte{
		[]byte("abc"),
		[]byte("def"),
		[]byte("ghi"),
	}

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
	sb := NewSeekable([][]byte{})
	
	n, err := sb.Write([]byte("hello"))
	if n != 5 || err != nil {
		t.Errorf("Write('hello') = (%d, %v), want (5, nil)", n, err)
	}
	
	n, err = sb.Write([]byte(" world"))
	if n != 6 || err != nil {
		t.Errorf("Write(' world') = (%d, %v), want (6, nil)", n, err)
	}
	
	// Test reading back the written data
	sb.Seek(0, io.SeekStart)
	buf := make([]byte, 20)
	n, err = sb.Read(buf)
	if n != 11 || (err != nil && err != io.EOF) {
		t.Errorf("Read after write = (%d, %v), want (11, nil or EOF)", n, err)
	}
	if string(buf[:n]) != "hello world" {
		t.Errorf("Read data = %q, want %q", string(buf[:n]), "hello world")
	}
}

func TestWriteString(t *testing.T) {
	sb := NewSeekable([][]byte{})
	
	n, err := sb.WriteString("test")
	if n != 4 || err != nil {
		t.Errorf("WriteString('test') = (%d, %v), want (4, nil)", n, err)
	}
	
	n, err = sb.WriteString(" string")
	if n != 7 || err != nil {
		t.Errorf("WriteString(' string') = (%d, %v), want (7, nil)", n, err)
	}
	
	// Test reading back
	sb.Seek(0, io.SeekStart)
	buf := make([]byte, 20)
	n, err = sb.Read(buf)
	if n != 11 || (err != nil && err != io.EOF) {
		t.Errorf("Read after WriteString = (%d, %v), want (11, nil or EOF)", n, err)
	}
	if string(buf[:n]) != "test string" {
		t.Errorf("Read data = %q, want %q", string(buf[:n]), "test string")
	}
}

func TestWriteAndSeek(t *testing.T) {
	// Test writing data, seeking, and reading from different positions
	sb := NewSeekable([][]byte{})
	
	sb.Write([]byte("abc"))
	sb.Write([]byte("def"))
	sb.Write([]byte("ghi"))
	
	// Test reading from beginning
	sb.Seek(0, io.SeekStart)
	buf := make([]byte, 3)
	n, err := sb.Read(buf)
	if n != 3 || err != nil {
		t.Errorf("Read from start = (%d, %v), want (3, nil)", n, err)
	}
	if string(buf) != "abc" {
		t.Errorf("Read data = %q, want %q", string(buf), "abc")
	}
	
	// Test reading from middle
	sb.Seek(3, io.SeekStart)
	n, err = sb.Read(buf)
	if n != 3 || err != nil {
		t.Errorf("Read from middle = (%d, %v), want (3, nil)", n, err)
	}
	if string(buf) != "def" {
		t.Errorf("Read data = %q, want %q", string(buf), "def")
	}
	
	// Test reading from end
	sb.Seek(6, io.SeekStart)
	n, err = sb.Read(buf)
	if n != 3 || (err != nil && err != io.EOF) {
		t.Errorf("Read from end = (%d, %v), want (3, nil or EOF)", n, err)
	}
	if string(buf) != "ghi" {
		t.Errorf("Read data = %q, want %q", string(buf), "ghi")
	}
}

func TestWriteEmpty(t *testing.T) {
	sb := NewSeekable([][]byte{})
	
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

func TestMixedWriteAndInitialData(t *testing.T) {
	// Test starting with initial data and then writing more
	initialData := [][]byte{
		[]byte("initial"),
		[]byte(" data"),
	}
	
	sb := NewSeekable(initialData)
	
	// Write additional data
	sb.Write([]byte(" plus"))
	sb.Write([]byte(" more"))
	
	// Read everything
	sb.Seek(0, io.SeekStart)
	buf := make([]byte, 30)
	n, err := sb.Read(buf)
	if err != nil && err != io.EOF {
		t.Errorf("Read error: %v", err)
	}
	
	expected := "initial data plus more"
	if string(buf[:n]) != expected {
		t.Errorf("Read data = %q, want %q", string(buf[:n]), expected)
	}
}