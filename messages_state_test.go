package ckit

import (
	"testing"

	"github.com/grafana/ckit/internal/messages"
	"github.com/stretchr/testify/require"
)

func TestMessages(t *testing.T) {
	md := stateMessage{NodeName: "test", NewState: PeerStateParticipant}

	raw, err := messages.Encode(&md)
	require.NoError(t, err)

	buf, ty, err := messages.Parse(raw)
	require.NoError(t, err)
	require.Equal(t, messages.TypeState, ty)

	var actual stateMessage
	err = messages.Decode(buf, &actual)
	require.NoError(t, err)
	require.Equal(t, md, actual)
}

func TestMessages_Invalid(t *testing.T) {
	require.Panics(t, func() { messages.Encode(fakeMessage{ty: messages.TypeInvalid}) })
	require.Panics(t, func() { messages.Encode(fakeMessage{ty: 254}) })

	md := stateMessage{NodeName: "test", NewState: PeerStateParticipant}
	raw, err := messages.Encode(&md)
	require.NoError(t, err)

	// Force the type byte (2) to be invalid.
	raw[2] = byte(messages.TypeInvalid)
	_, _, err = messages.Parse(raw)
	require.EqualError(t, err, "invalid message type 0")

	raw[2] = byte(150)
	_, _, err = messages.Parse(raw)
	require.EqualError(t, err, "invalid message type 150")

	// Restore the type back and then mess up the magic number.
	raw[2] = byte(messages.TypeState)

	raw[1] = 0xFF
	raw[0] = 0xDD
	_, _, err = messages.Parse(raw)
	require.EqualError(t, err, "invalid magic header ddff")
}

type fakeMessage struct {
	ty messages.Type
}

func (fm fakeMessage) Type() messages.Type                 { return fm.ty }
func (fm fakeMessage) Invalidates(m messages.Message) bool { return false }
func (fm fakeMessage) Cache() bool                         { return false }
