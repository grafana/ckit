package chash

import (
	"github.com/cespare/xxhash/v2"
)

// KeyBuilder generate keys to input into a Hash. To generate a key,
// first write to the KeyBuilder, then call Key. The KeyBuilder can
// be re-used afterwards by calling Reset. KeyBuilder can not be
// used concurrently.
//
// KeyBuilder implements io.Writer.
type KeyBuilder struct {
	dig *xxhash.Digest
}

// NewKeyBuilder returns a new KeyBuilder that can generate keys.
func NewKeyBuilder() *KeyBuilder { return &KeyBuilder{dig: xxhash.New()} }

// Write appends b to kb's state. Write always returns len(b), nil.
func (kb *KeyBuilder) Write(b []byte) (n int, err error) { return kb.dig.Write(b) }

// Reset resets kb's state.
func (kb *KeyBuilder) Reset() { kb.dig.Reset() }

// Key computes the key from kb's current state.
func (kb *KeyBuilder) Key() uint64 { return kb.dig.Sum64() }

// Key is a convenience method to generate a Key for a string as an alternative
// to generating a KeyBuilder.
func Key(s string) uint64 {
	return xxhash.Sum64String(s)
}
