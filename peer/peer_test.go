package peer

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONRepresentation(t *testing.T) {
	p := Peer{
		Name:  "test-peer",
		Addr:  "localhost",
		Self:  true,
		State: StateParticipant,
	}
	var q Peer

	b, err := json.Marshal(p)
	require.NoError(t, err)

	err = json.Unmarshal(b, &q)
	require.NoError(t, err)

	require.Equal(t, p, q)
}
