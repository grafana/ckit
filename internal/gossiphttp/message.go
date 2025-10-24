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
	magic16      = 0xcc // magic byte for 16-bit messages
	header16Size = 3    // 1 byte magic + 2 bytes length of the payload (16-bit)
	magic32      = 0xcd // magic byte for 32-bit messages
	header32Size = 5    // 1 byte magic + 4 bytes length of the payload (32-bit)
)

var headerPool = &sync.Pool{
	New: func() any {
		return &header{
			data: make([]byte, header32Size), // use the largest possible header size
		}
	},
}

type header struct {
	data []byte
}

func (h *header) write(messageLength int, destination io.Writer) error {
	if messageLength <= math.MaxUint16 {
		h.data[0] = magic16
		binary.BigEndian.PutUint16(h.data[1:3], uint16(messageLength))
		if _, err := destination.Write(h.data[0:3]); err != nil {
			return err
		}
	} else {
		h.data[0] = magic32
		binary.BigEndian.PutUint32(h.data[1:5], uint32(messageLength))
		if _, err := destination.Write(h.data[0:5]); err != nil {
			return err
		}
	}
	return nil
}

func (h *header) readFrom(r io.Reader) (dataLength int, err error) {
	// Read the minimum header size (3 bytes) to obtain the magic byte and determine the message format.
	if _, err := io.ReadFull(r, h.data[0:3]); err != nil {
		return 0, err
	}
	switch h.data[0] {
	case magic16:
		return int(binary.BigEndian.Uint16(h.data[1:3])), nil // no additional data to read, whole header consumed with a single read op
	case magic32:
		// Read the remaining 2 bytes of the large header.
		if _, err := io.ReadFull(r, h.data[3:5]); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(h.data[1:5])), nil
	default:
		return 0, fmt.Errorf("unrecognized magic byte (%x)", h.data[0])
	}
}

// readMessage reads a message from an [io.Reader].
func readMessage(r io.Reader) ([]byte, error) {
	header := headerPool.Get().(*header)
	defer headerPool.Put(header)
	dataLength, err := header.readFrom(r)
	if err != nil {
		return nil, err
	}

	if dataLength > MaxMessageLength {
		// This can be triggered on 32-bit platforms if a 64-bit sender sends more
		// than 2 GiB.
		return nil, fmt.Errorf("message length %d exceeds size limit %d", dataLength, MaxMessageLength)
	}

	data := make([]byte, dataLength)
	_, err = io.ReadFull(r, data)
	return data, err
}

// writeMessage writes a message to an [io.Writer].
func writeMessage(w io.Writer, message []byte) error {
	if len(message) > MaxMessageLength {
		return fmt.Errorf("message length %d exceeds size limit %d", len(message), uint32(math.MaxUint32))
	}

	header := headerPool.Get().(*header)
	defer headerPool.Put(header)

	err := header.write(len(message), w)
	if err != nil {
		return err
	}

	_, err = w.Write(message)
	return err
}
