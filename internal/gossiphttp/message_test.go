package gossiphttp

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fuzz_message ensures that any byte slice encoded as a ckit gossip message
// can be decoded back to its original form.
func Fuzz_message(f *testing.F) {
	tt := []string{"Hello, world"}
	for _, tc := range tt {
		f.Add([]byte(tc))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Ignore slices longer than the max message length.
		if len(data) > math.MaxUint16 {
			t.Skip()
		}

		var buf bytes.Buffer
		require.NoError(t, writeMessage(&buf, data))

		res, err := readMessage(&buf)
		require.NoError(t, err)
		require.Equal(t, data, res)
	})
}

func Benchmark_message(b *testing.B) {
	// We use 1024 bytes throughout the benchmarks below since messages will
	// generally be bound to the UDP MTU size (1024 bytes).

	b.Run("write", func(b *testing.B) {
		data := make([]byte, 1024)
		for i := 0; i < b.N; i++ {
			_ = writeMessage(io.Discard, data)
		}
	})

	b.Run("read", func(b *testing.B) {
		var buf bytes.Buffer
		_ = writeMessage(&buf, make([]byte, 1024))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = readMessage(bytes.NewReader(buf.Bytes()))
		}
	})
}
