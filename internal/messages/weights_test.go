package messages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWeightsMessage_RoundTrip(t *testing.T) {
	w := Weights{
		NodeName: "node-a",
		Time:     7,
		Weights:  map[uint64]uint64{1: 100, 2: 200, 3: 300},
	}

	raw, err := Encode(&w)
	require.NoError(t, err)

	buf, ty, err := Parse(raw)
	require.NoError(t, err)
	require.Equal(t, TypeWeights, ty)

	var actual Weights
	require.NoError(t, Decode(buf, &actual))
	require.Equal(t, w, actual)
}
