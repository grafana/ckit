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
	// magic16 is the first byte sent with every message.
	magic16      = 0xcc
	magic32      = 0xcd
	header16Size = 3 // 1 byte magic + 2 bytes length of the payload (16-bit)
	header32Size = 5 // 1 byte magic + 4 bytes length of the payload (32-bit)
)

var headerPool = &sync.Pool{
	New: func() any {
		return &header{
			magic:  make([]byte, 1),
			size16: make([]byte, 2),
			size32: make([]byte, 4),
		}
	},
}

type header struct {
	magic []byte
	// Only one of size16 or size32 will be used when reading or writing
	// the header depending on the length of the message. This uses a
	// small amount of extra memory, which is negligible for most use
	// cases, but it simplifies the logic for reading and writing the header.
	size16 []byte
	size32 []byte
}

func (h *header) setLength16(length uint16) {
	h.magic[0] = magic16
	binary.BigEndian.PutUint16(h.size16, length)
}

func (h *header) setLength32(length uint32) {
	h.magic[0] = magic32
	binary.BigEndian.PutUint32(h.size32, length)
}

func (h *header) writeTo(w io.Writer) error {
	if _, err := w.Write(h.magic); err != nil {
		return err
	}
	switch h.magic[0] {
	case magic16:
		if _, err := w.Write(h.size16); err != nil {
			return err
		}
	case magic32:
		if _, err := w.Write(h.size32); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized magic byte (%x)", h.magic[0])
	}
	return nil
}

func (h *header) readFrom(r io.Reader) error {
	if _, err := io.ReadFull(r, h.magic); err != nil {
		return err
	}
	switch h.magic[0] {
	case magic16:
		if _, err := io.ReadFull(r, h.size16); err != nil {
			return err
		}
	case magic32:
		if _, err := io.ReadFull(r, h.size32); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized magic byte (%x)", h.magic[0])
	}
	return nil
}

func (h *header) dataLength() int {
	switch h.magic[0] {
	case magic16:
		return int(binary.BigEndian.Uint16(h.size16))
	case magic32:
		return int(binary.BigEndian.Uint32(h.size32))
	}
	return 0
}

// readMessage reads a message from an [io.Reader].
func readMessage(r io.Reader) ([]byte, error) {
	header := headerPool.Get().(*header)
	defer headerPool.Put(header)
	if err := header.readFrom(r); err != nil {
		return nil, err
	}

	dataLength := header.dataLength()
	if dataLength > MaxMessageLength {
		// This can be triggered on 32-bit platforms if a 64-bit sender sends more
		// than 2 GiB.
		return nil, fmt.Errorf("message length %d exceeds size limit %d", dataLength, MaxMessageLength)
	}

	data := make([]byte, dataLength)
	_, err := io.ReadFull(r, data)
	return data, err
}

// writeMessage writes a message to an [io.Writer].
func writeMessage(w io.Writer, message []byte) error {
	if len(message) > MaxMessageLength {
		return fmt.Errorf("message length %d exceeds size limit %d", len(message), uint32(math.MaxUint32))
	}

	header := headerPool.Get().(*header)
	defer headerPool.Put(header)

	if len(message) <= math.MaxUint16 {
		header.setLength16(uint16(len(message)))
	} else {
		header.setLength32(uint32(len(message)))
	}

	err := header.writeTo(w)
	if err != nil {
		return err
	}

	_, err = w.Write(message)
	return err
}
