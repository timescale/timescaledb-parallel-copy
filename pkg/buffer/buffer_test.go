package buffer

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadWhatYouWrite(t *testing.T) {
	buffer := NewSegmented(10, 4)

	input := "hello world, this is a string"

	_, err := buffer.Write([]byte(input))
	require.NoError(t, err)

	res, err := io.ReadAll(buffer.Reader())
	require.Equal(t, input, string(res))
}
