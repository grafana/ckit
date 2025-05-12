package messages

import (
	"testing"

	"github.com/grafana/ckit/peer"
	"github.com/stretchr/testify/require"
)

func TestMessages(t *testing.T) {
	md := State{NodeName: "test", NewState: peer.StateParticipant}

	raw, err := Encode(&md)
	require.NoError(t, err)

	buf, ty, err := Parse(raw)
	require.NoError(t, err)
	require.Equal(t, TypeState, ty)

	var actual State
	err = Decode(buf, &actual)
	require.NoError(t, err)
	require.Equal(t, md, actual)
}

func TestMessages_Invalid(t *testing.T) {
	require.Panics(t, func() { Encode(fakeMessage{ty: TypeInvalid}) })
	require.Panics(t, func() { Encode(fakeMessage{ty: 254}) })

	md := State{NodeName: "test", NewState: peer.StateParticipant}
	raw, err := Encode(&md)
	require.NoError(t, err)

	// Force the type byte (2) to be invalid.
	raw[2] = byte(TypeInvalid)
	_, _, err = Parse(raw)
	require.EqualError(t, err, "invalid message type 0")

	raw[2] = byte(150)
	_, _, err = Parse(raw)
	require.EqualError(t, err, "invalid message type 150")

	// Restore the type back and then mess up the magic number.
	raw[2] = byte(TypeState)

	raw[1] = 0xFF
	raw[0] = 0xDD
	_, _, err = Parse(raw)
	require.EqualError(t, err, "invalid magic header ddff")
}

type fakeMessage struct {
	ty Type
}

func (fm fakeMessage) Type() Type   { return fm.ty }
func (fm fakeMessage) Name() string { return "" }
func (fm fakeMessage) Cache() bool  { return false }
