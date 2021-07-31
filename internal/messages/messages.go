// Package messages enables utilities for encoding and decoding custom user
// messages using the gossip mechanism.
package messages

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/go-msgpack/codec"
)

// magicHeader is added at the start of every message.
const magicHeader uint16 = 0x1201

// Type of the message. Encoded along with the payload to be able to uniquely
// identify messages.
//
// Custom types must be registered by calling RegisterType from an init method.
type Type uint8

const (
	TypeInvalid Type = iota
	TypeMetadata
)

var knownTypes = map[Type]string{
	TypeInvalid:  "invalid",
	TypeMetadata: "metadata",
}

// String returns the registered type name.
func (ty Type) String() string {
	val, ok := knownTypes[ty]
	if !ok {
		return "unknown"
	}
	return val
}

// Registered returns true when ty has been registered.
func (ty Type) Registered() bool {
	_, ok := knownTypes[ty]
	return ok
}

// RegisterType registers a new user type ty with a given name. ty must be
// unique. Values of Type below 50 are reserved for ckit. RegisterType
// will panic if the value of ty is reserved or if ty is already in use
// by another type.
func RegisterType(ty Type, name string) {
	if uint8(ty) < 50 {
		panic("message types with values < 50 are reserved")
	}

	if _, exist := knownTypes[ty]; exist {
		panic(fmt.Sprintf("message type value %d already in use by %s", ty, ty))
	}
	knownTypes[ty] = name
}

// Message is a payload that can be sent to other peers.
type Message interface {
	// Type should return the Type of the message.
	Type() Type

	// Invalidates should return true if this Message invalidates a previous
	// Message m.
	Invalidates(m Message) bool

	// Cache should return true if this Message should be cached into the
	// local state. Messages in the local state will be synchronized with
	// peers over time, and is useful as an anti-entropy measure.
	Cache() bool
}

// Encode encodes a message with a type. The type must be a valid, registered
// type, otherwise Encode will panic. To decode the message, call Validate
// and then pass the returned buf to Decode.
//
// Msgpack is used for encoding structs.
func Encode(m Message) (raw []byte, err error) {
	ty := m.Type()
	if ty == TypeInvalid || !ty.Registered() {
		panic("ty must be a known, valid type")
	}

	buf := bytes.NewBuffer(nil)

	// Write magic header and type
	binary.Write(buf, binary.BigEndian, magicHeader)
	buf.WriteByte(uint8(ty))

	// Then add the message
	var handle codec.MsgpackHandle
	enc := codec.NewEncoder(buf, &handle)
	err = enc.Encode(m)
	return buf.Bytes(), err
}

// Validate validates an encoded buffer. Returns a buffer that can be passed
// to Decode and the type of the message. Returns an error if the type is not
// a valid, recognized type.
func Validate(raw []byte) (buf []byte, ty Type, err error) {
	if len(raw) < 3 {
		return nil, TypeInvalid, fmt.Errorf("payload too small for message")
	}

	magic := binary.BigEndian.Uint16(raw[0:2])
	if magic != magicHeader {
		return nil, TypeInvalid, fmt.Errorf("invalid magic header %x", magic)
	}

	ty = Type(raw[2])
	if ty == TypeInvalid || !ty.Registered() {
		return nil, TypeInvalid, fmt.Errorf("invalid message type %[1]d (%[1]s)", ty)
	}

	buf = raw[3:]
	return
}

// Decode decodes a message. buf should be the payload of the message retrieved
// from Validate.
func Decode(buf []byte, m Message) error {
	r := bytes.NewReader(buf)
	var handle codec.MsgpackHandle
	dec := codec.NewDecoder(r, &handle)
	return dec.Decode(m)
}
