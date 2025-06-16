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

// TestMessages_ForwardCompatibility ensures that older versions of ckit can
// decode messages from newer versions of ckit (where message introduced new
// files) while also re-encoding those messages with the fields retained.
//
// This ensures that gossiping messages between different versions of ckit does
// not silently drop fields that were added in newer versions.
func TestMessages_ForwardCompatibility(t *testing.T) {
	type (
		oldMessage struct {
			fakeMessage

			Address string
		}

		newMessage struct {
			fakeMessage

			Address string
			Version int
		}
	)

	expect := newMessage{
		fakeMessage: newFakeMessage(TypeState),
		Address:     "127.0.0.1:80",
		Version:     3,
	}

	raw, err := Encode(expect)
	require.NoError(t, err)

	recv := oldMessage{fakeMessage: newFakeMessage(TypeState)}
	{
		buf, ty, err := Parse(raw)
		require.NoError(t, err)
		require.Equal(t, TypeState, ty)

		err = Decode(buf, &recv)
		require.NoError(t, err)
	}

	// Re-encode the message we received, but include unknown fields that
	// oldMessage didn't recognize.
	raw, err = Encode(recv)
	require.NoError(t, err)

	actual := newMessage{fakeMessage: newFakeMessage(TypeState)}
	{
		buf, ty, err := Parse(raw)
		require.NoError(t, err)
		require.Equal(t, TypeState, ty)

		err = Decode(buf, &actual)
		require.NoError(t, err)
		require.Len(t, actual.CodecMissingFields(), 0)
	}

	require.Equal(t, expect, actual)
}

type fakeMessage struct {
	ty            Type
	missingFields map[string]any
}

func newFakeMessage(ty Type) fakeMessage {
	return fakeMessage{
		ty:            ty,
		missingFields: make(map[string]any),
	}
}

func (fm fakeMessage) Type() Type   { return fm.ty }
func (fm fakeMessage) Name() string { return "" }
func (fm fakeMessage) Cache() bool  { return false }

func (fm fakeMessage) CodecMissingField(field []byte, value interface{}) bool {
	if fm.missingFields == nil {
		return false
	}
	fm.missingFields[string(field)] = value
	return true
}

func (fm fakeMessage) CodecMissingFields() map[string]interface{} { return fm.missingFields }
