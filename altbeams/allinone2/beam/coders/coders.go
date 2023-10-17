package coders

import (
	"reflect"

	"golang.org/x/exp/constraints"
)

// Coder represents a coder for a specific type.
type Coder[E any] interface {
	Encode(enc *Encoder, v E)
	Decode(dec *Decoder) E
}

// Codable represents types that know how to code themselves.
type Codable interface {
	Encode(enc *Encoder)
	Decode(dec *Decoder)
}

// MakeCoder is a convenience function for primitive coders access.
func MakeCoder[E any]() Coder[E] {
	var e E
	return makeCoder(reflect.TypeOf(e)).(Coder[E])
}

// makeCoder works around
func makeCoder(rt reflect.Type) any {
	switch rt.Kind() {
	case reflect.Int:
		return varintCoder[int]{}
	case reflect.Int8:
		return varintCoder[int8]{}
	case reflect.Int16:
		return varintCoder[int16]{}
	case reflect.Int32:
		return varintCoder[int32]{}
	case reflect.Int64:
		return varintCoder[int64]{}
	case reflect.Uint:
		return varintCoder[uint]{}
	case reflect.Uint8:
		return byteCoder{}
	case reflect.Uint16:
		return varintCoder[uint16]{}
	case reflect.Uint32:
		return varintCoder[uint32]{}
	case reflect.Uint64:
		return varintCoder[uint64]{}
	case reflect.Slice:
		switch rt.Elem().Kind() {
		case reflect.Uint8:
			return bytesCoder{}
		}
	}
	// Returning nil since type assertion elsewhere will provide better information
	// to the developer.
	return nil
}

type varintCoder[T constraints.Integer] struct{}

func (varintCoder[T]) Encode(enc *Encoder, v T) {
	enc.Varint(uint64(v))
}

func (varintCoder[T]) Decode(dec *Decoder) T {
	return T(dec.Varint())
}

type byteCoder struct{}

func (byteCoder) Encode(enc *Encoder, v byte) {
	enc.Byte(v)
}

func (byteCoder) Decode(dec *Decoder) byte {
	return dec.Byte()
}

type bytesCoder struct{}

func (bytesCoder) Encode(enc *Encoder, v []byte) {
	enc.Bytes(v)
}

func (bytesCoder) Decode(dec *Decoder) []byte {
	return dec.Bytes()
}
