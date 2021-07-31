package messages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessages(t *testing.T) {
	md := Metadata{ApplicationAddr: "127.0.0.1:8085"}

	raw, err := Encode(&md)
	require.NoError(t, err)

	buf, ty, err := Validate(raw)
	require.NoError(t, err)
	require.Equal(t, TypeMetadata, ty)

	var actual Metadata
	err = Decode(buf, &actual)
	require.NoError(t, err)
	require.Equal(t, md, actual)
}

func TestMessages_Invalid(t *testing.T) {
	require.Panics(t, func() { Encode(fakeMessage{ty: TypeInvalid}) })
	require.Panics(t, func() { Encode(fakeMessage{ty: 254}) })

	md := Metadata{ApplicationAddr: "127.0.0.1:8085"}
	raw, err := Encode(&md)
	require.NoError(t, err)

	// Force the type byte (2) to be invalid.
	raw[2] = byte(TypeInvalid)
	_, _, err = Validate(raw)
	require.EqualError(t, err, "invalid message type 0 (invalid)")

	raw[2] = byte(150)
	_, _, err = Validate(raw)
	require.EqualError(t, err, "invalid message type 150 (unknown)")

	// Restore the type back and then mess up the magic number.
	raw[2] = byte(TypeMetadata)

	raw[1] = 0xFF
	raw[0] = 0xDD
	_, _, err = Validate(raw)
	require.EqualError(t, err, "invalid magic header ddff")
}

type fakeMessage struct {
	ty Type
}

func (fm fakeMessage) Type() Type                 { return fm.ty }
func (fm fakeMessage) Invalidates(m Message) bool { return false }
func (fm fakeMessage) Cache() bool                { return false }
