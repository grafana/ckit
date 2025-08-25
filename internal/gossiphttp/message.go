package gossiphttp

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
)

// NOTE(rfratto): Even though our messages can store up to 4 GiB
// ([math.MaxUint32]), the effective limit on 32-bit platforms is 2 GiB
// ([math.MaxInt32]), since slices are sized with a signed integer.
//
// Since ckit supports 32-bit builds we need to determine the platform-specific
// maximum message length.
//
// To set [MaxMesasgeLength], we rely on how [math.MaxInt] is either equivalent
// to [math.MaxInt32] or [math.MaxInt64] depending on the platform:
//
//   - 32-bit: [math.MaxUint32] & [math.MaxInt32] == [math.MaxInt32]
//   - 64-bit: [math.MaxUint32] & [math.MaxInt64] == [math.MaxUint32]
//
// Only in the 32-bit case, the top bit becomes masked out, properly lowering
// the limit.
//
// This makes use of untyped constants in Go to make sure the compiler has
// enough space for computing this value.

// MaxMessageLength is the maximum length of a message that can be sent or
// received.
//
// MaxMessageLength is [math.MaxInt32] on 32-bit platforms and [math.MaxUint32]
// on 64-bit platforms.
const MaxMessageLength = math.MaxUint32 & math.MaxInt

const (
	// magic is the first byte sent with every message.
	magic      = 0xcc
	magic32    = 0xcd
	headerSize = 5 // 1 byte magic + 2 bytes length of the payload (16-bit) or 4 bytes length of the payload (32-bit)
)

var headerPool = &sync.Pool{
	New: func() any {
		return &header{
			data: make([]byte, headerSize),
		}
	},
}

type header struct {
	data []byte
}

// readMessage reads a message from an [io.Reader].
func readMessage(r io.Reader) ([]byte, error) {
	header := headerPool.Get().(*header)
	defer headerPool.Put(header)
	if _, err := io.ReadFull(r, header.data); err != nil {
		return nil, err
	}

	var (
		gotMagic   = header.data[0]
		dataLength uint32
	)

	switch gotMagic {
	case magic:
		dataLength = uint32(binary.BigEndian.Uint16(header.data[1:]))
	case magic32:
		dataLength = binary.BigEndian.Uint32(header.data[1:])
	default:
		return nil, fmt.Errorf("invalid magic (%x)", gotMagic)
	}

	if dataLength > MaxMessageLength {
		// This can be triggered on 32-bit platforms if a 64-bit sender sends more
		// than 2 GiB.
		return nil, fmt.Errorf("message length %d exceeds size limit %d", dataLength, MaxMessageLength)
	}

	data := make([]byte, dataLength)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// writeMessage writes a message to an [io.Writer].
func writeMessage(w io.Writer, message []byte) error {
	if len(message) > MaxMessageLength {
		return fmt.Errorf("message length %d exceeds size limit %d", len(message), uint32(math.MaxUint32))
	}

	header := headerPool.Get().(*header)
	defer headerPool.Put(header)

	if len(message) <= math.MaxUint16 {
		header.data[0] = magic
		binary.BigEndian.PutUint16(header.data[1:], uint16(len(message)))
	} else {
		header.data[0] = magic32
		binary.BigEndian.PutUint32(header.data[1:], uint32(len(message)))
	}

	if _, err := w.Write(header.data); err != nil {
		return err
	}

	_, err := w.Write(message)
	return err
}
