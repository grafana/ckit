package gossiphttp

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
		if len(data) > MaxMessageLength {
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

func TestMessageRoundTrip(t *testing.T) {
	testMessages := [][]byte{
		bytes.Repeat([]byte("a"), math.MaxUint16),
		bytes.Repeat([]byte("b"), 100),
		bytes.Repeat([]byte("c"), math.MaxUint16+1),
		bytes.Repeat([]byte("d"), 1_000_000),
	}

	for i, message := range testMessages {
		t.Run(fmt.Sprintf("message_%d", i), func(t *testing.T) {
			var buf bytes.Buffer

			// Write message
			err := writeMessage(&buf, message)
			require.NoError(t, err)

			// Read message back
			readBuf := bytes.NewBuffer(buf.Bytes())
			readData, err := readMessage(readBuf)
			require.NoError(t, err)

			// Verify round trip
			require.Equal(t, message, readData)
		})
	}
}

func TestWriteMessageMagicByte(t *testing.T) {
	tests := []struct {
		name          string
		message       []byte
		expectedMagic byte
	}{
		{
			name:          "empty message (16-bit)",
			message:       []byte{},
			expectedMagic: magic16,
		},
		{
			name:          "small message (16-bit)",
			message:       []byte("hello"),
			expectedMagic: magic16,
		},
		{
			name:          "exactly uint16 boundary (16-bit)",
			message:       bytes.Repeat([]byte("a"), math.MaxUint16),
			expectedMagic: magic16,
		},
		{
			name:          "uint16 boundary + 1 (32-bit)",
			message:       bytes.Repeat([]byte("a"), math.MaxUint16+1),
			expectedMagic: magic32,
		},
		{
			name:          "large message (32-bit)",
			message:       bytes.Repeat([]byte("a"), 1_000_000),
			expectedMagic: magic32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := writeMessage(&buf, tt.message)
			require.NoError(t, err)

			// Get the written data
			writtenData := buf.Bytes()

			// The first byte should be the magic byte
			actualMagic := writtenData[0]
			require.Equal(t, tt.expectedMagic, actualMagic,
				"message length %d should use magic %x, got %x",
				len(tt.message), tt.expectedMagic, actualMagic)
		})
	}
}

func TestRead3ByteHeaderMessage(t *testing.T) {
	// The older versions of ckit use 3-byte header.
	header := make([]byte, 3)
	payload := []byte("hello")

	// Create the message just as the old version of ckit would.
	header[0] = magic16
	binary.BigEndian.PutUint16(header[1:], uint16(len(payload)))

	// Write the header and payload to the buffer.
	var message bytes.Buffer
	message.Write(header)
	message.Write(payload)

	// Try to read the message.
	data, err := readMessage(&message)

	require.NoError(t, err)
	require.Equal(t, payload, data)
}

func TestWrite3ByteHeaderForSmallMessages(t *testing.T) {
	// Small messages should use 3-byte header for backward compatibility.
	payload := []byte("hello")

	// Write the message using writeMessage.
	var message bytes.Buffer
	err := writeMessage(&message, payload)
	require.NoError(t, err)

	writtenData := message.Bytes()

	// Verify magic byte.
	require.Equal(t, byte(magic16), writtenData[0])

	// Verify length is encoded as uint16 (2 bytes).
	length := binary.BigEndian.Uint16(writtenData[1:3])
	require.Equal(t, uint16(len(payload)), length)

	// Verify the payload
	require.Equal(t, payload, writtenData[3:])
}
