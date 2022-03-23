package hash

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyBuilder(t *testing.T) {
	t.Run("Generates the same hash with no change", func(t *testing.T) {
		kb := NewKeyBuilder()
		_, _ = fmt.Fprint(kb, "Testing")

		hash1 := kb.Key()
		hash2 := kb.Key()
		require.Equal(t, hash1, hash2)
	})

	t.Run("Generates new key after write", func(t *testing.T) {
		kb := NewKeyBuilder()
		beforeWrite := kb.Key()

		_, _ = fmt.Fprint(kb, "Testing")

		afterWrite := kb.Key()

		require.NotEqual(t, beforeWrite, afterWrite)
	})

	t.Run("Resets to initial state", func(t *testing.T) {
		kb := NewKeyBuilder()
		initialState := kb.Key()

		_, _ = fmt.Fprint(kb, "Testing")
		kb.Reset()

		currentState := kb.Key()
		require.Equal(t, currentState, initialState)
	})
}

func TestKeyBuilder_Key_Equivalence(t *testing.T) {
	in := "Hello, world!"

	kb := NewKeyBuilder()
	_, _ = fmt.Fprint(kb, in)

	require.Equal(t, kb.Key(), StringKey(in))
}
