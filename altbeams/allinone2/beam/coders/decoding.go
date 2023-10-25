package coders

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Decoder deserializes data from a byte slice data in the expected results.
type Decoder struct {
	data []byte
}

// NewDecoder instantiates a new Decoder for a given byte slice.
func NewDecoder(data []byte) *Decoder {
	return &Decoder{data}
}

// Varint encodes a variable length integer.
//
// This matches with "beam:coder:varint:v1" of the beam_runner_api.proto coders.
func (d *Decoder) Varint() uint64 {
	v, n := protowire.ConsumeVarint(d.data)
	d.Read(n)
	return v
}

// Empty returns true iff all bytes in d have been consumed.
func (d *Decoder) Empty() bool {
	return len(d.data) == 0
}

// makeDecodeError creates and returns a decoder error.
func makeDecodeError(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

// DecodeProto deserializes the value from a byte slice using proto serialization.
func (d *Decoder) DecodeProto(value proto.Message) {
	if err := proto.Unmarshal(d.Bytes(), value); err != nil {
		panic(makeDecodeError("error decoding to proto %T: %w", value, err))
	}
}

// DecodeBinaryUnmarshaler deserializes the value from a byte slice using
// UnmarshalBinary.
func (d *Decoder) DecodeBinaryUnmarshaler(value encoding.BinaryUnmarshaler) {
	if err := value.UnmarshalBinary(d.Bytes()); err != nil {
		panic(makeDecodeError("error decoding BinaryUnmarshaler %T: %w", value, err))
	}
}

// Read reads and returns n bytes from the decoder and advances the decode past
// the read bytes.
func (d *Decoder) Read(n int) []byte {
	if len := len(d.data); len < n {
		panic(makeDecodeError("unable to read #bytes: want %d got %d", n, len))
	}
	b := d.data[:n]
	d.data = d.data[n:]
	return b
}

// Uint8 decodes a value of type uint8.
func (d *Decoder) Uint8() uint8 {
	return d.Read(1)[0]
}

// Byte decodes a value of type byte.
func (d *Decoder) Byte() byte {
	return d.Uint8()
}

// Int8 decodes a value of type int8.
func (d *Decoder) Int8() int8 {
	return int8(d.Uint8())
}

// Uint16 decodes a value of type uint16.
func (d *Decoder) Uint16() uint16 {
	return binary.BigEndian.Uint16(d.Read(2))
}

// Int16 decodes a value of type int16.
func (d *Decoder) Int16() int16 {
	return int16(d.Uint16())
}

// Uint32 decodes a value of type uint32.
func (d *Decoder) Uint32() uint32 {
	return binary.BigEndian.Uint32(d.Read(4))
}

// Int32 decodes a value of type int32.
func (d *Decoder) Int32() int32 {
	return int32(d.Uint32())
}

// Rune decodes a value of type rune.
func (d *Decoder) Rune() rune {
	return d.Int32()
}

// Uint64 decodes a value of type uint64.
func (d *Decoder) Uint64() uint64 {
	return binary.BigEndian.Uint64(d.Read(8))
}

// Int64 decodes a value of type int64.
func (d *Decoder) Int64() int64 {
	return int64(d.Uint64())
}

// Uint decodes a value of type uint.
// Uint values are encoded as 64 bits.
func (d *Decoder) Uint() uint {
	return uint(d.Uint64())
}

// Int decodes a value of type int.
// Int values are encoded as 64 bits.
func (d *Decoder) Int() int {
	return int(d.Int64())
}

// Bool decodes a value of type bool.
func (d *Decoder) Bool() bool {
	if b := d.Uint8(); b == 0 {
		return false
	} else if b == 1 {
		return true
	} else {
		panic(makeDecodeError("unable to decode bool; expected {0, 1} got %v", b))
	}
}

// Float decodes a value of type float32.
func (d *Decoder) Float() float32 {
	return math.Float32frombits(d.Uint32())
}

// Double decodes a value of type float64.
func (d *Decoder) Double() float64 {
	return math.Float64frombits(d.Uint64())
}

// Complex64 decodes a value of type complex64.
func (d *Decoder) Complex64() complex64 {
	return complex(d.Float(), d.Float())
}

// Complex128 decodes a value of type complex128.
func (d *Decoder) Complex128() complex128 {
	return complex(d.Double(), d.Double())
}

// String decodes a value of type string.
func (d *Decoder) StringUtf8() string {
	return string(d.Bytes())
}

// Bytes decodes a value of type []byte.
func (d *Decoder) Bytes() []byte {
	n := d.Varint()
	return d.Read(int(n))
}

func (d *Decoder) Timestamp() time.Time {
	msec := d.Uint64()
	return time.UnixMilli((int64)(msec) + math.MinInt64)
}

// GlobalWindow encodes as no bytes, making this a no-op.
//
// This matches with "beam:coder:global_window:v1" of the beam_runner_api.proto coders.
func (d *Decoder) GlobalWindow() {
}

func (d *Decoder) Pane() PaneInfo {
	d.Read(1)
	return PaneInfo{}
}

func DecodeWindowedValueHeader[W window](d *Decoder) (time.Time, []W, PaneInfo) {
	et := d.Timestamp()
	fmt.Println(d)
	n := d.Uint32()
	fmt.Println(d)
	windows := make([]W, int(n))
	for _, w := range windows {
		w.decode(d)
	}
	fmt.Println(d)
	pane := d.Pane()
	fmt.Println(d)
	return et, windows, pane
}
