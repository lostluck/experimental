package coders

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

type Encoder struct {
	data  []byte    // Contains the serialized arguments.
	space [100]byte // Prellocated buffer to avoid allocations for small size arguments.
}

func NewEncoder() *Encoder {
	var enc Encoder
	enc.data = enc.space[:0] // Arrange to use builtin buffer
	return &enc
}

// Reset resets the Encoder to use a buffer with a capacity of at least the
// provided size. All encoded data is lost.
func (e *Encoder) Reset(n int) {
	if n <= cap(e.data) {
		e.data = e.data[:0]
	} else {
		e.data = make([]byte, 0, n)
	}
}

// Data returns the byte slice that contains the serialized arguments.
func (e *Encoder) Data() []byte {
	return e.data
}

// Grow increases the size of the encoder's data if needed. Only appends a new
// slice if there is not enough capacity to satisfy bytesNeeded.
// Returns the slice fragment that contains bytesNeeded.
func (e *Encoder) Grow(bytesNeeded int) []byte {
	n := len(e.data)
	if cap(e.data)-n >= bytesNeeded {
		e.data = e.data[:n+bytesNeeded] // Grow in place (common case)
	} else {
		// Create a new larger slice.
		e.data = append(e.data, make([]byte, bytesNeeded)...)
	}
	return e.data[n:]
}

// Bytes encodes an arg of type []byte.
// For a byte slice, we encode its length as a varint, followed by the serialized content.
// Nil slices are encoded identically to 0 length slices.
//
// This matches with "beam:coder:bytes:v1" of the beam_runner_api.proto coders.
func (e *Encoder) Bytes(arg []byte) {
	n := len(arg)
	prefix := protowire.SizeVarint(uint64(n))
	data := e.Grow(prefix + n)
	protowire.AppendVarint(data[:0], uint64(n))
	copy(data[prefix:], arg)
}

// String encodes an arg of type string.
// For a string, we encode its length as a varint, followed by the serialized content.
//
// This matches with "beam:coder:string_utf8:v1" of the beam_runner_api.proto coders.
func (e *Encoder) StringUtf8(arg string) {
	n := len(arg)
	prefix := protowire.SizeVarint(uint64(n))
	data := e.Grow(prefix + n)
	protowire.AppendVarint(data[:0], uint64(n))
	copy(data[prefix:], arg)
}

// Bool encodes an arg of type bool.
// Serialize boolean values as an uint8 that encodes either 0 (false) or 1 (true).
//
// This matches with "beam:coder:bool:v1" of the beam_runner_api.proto coders.
func (e *Encoder) Bool(arg bool) {
	if arg {
		e.Uint8(1)
	} else {
		e.Uint8(0)
	}
}

// Varint encodes a variable length integer.
//
// This matches with "beam:coder:varint:v1" of the beam_runner_api.proto coders.
func (e *Encoder) Varint(i uint64) {
	data := e.Grow(protowire.SizeVarint(i))
	protowire.AppendVarint(data[:0], i)
}

// Double encodes the floating point value as a big-endian 64-bit integer
// according to the IEEE 754 double format bit layout.
// Components: None
// This matches with "beam:coder:double:v1" of the beam_runner_api.proto coders.
func (e *Encoder) Double(arg float64) {
	binary.BigEndian.PutUint64(e.Grow(8), math.Float64bits(arg))
}

// KVs are structural.
//  "beam:coder:kv:v1" is a no-op here since it's just appending the two fields together.
// Something else will handle that, using the encoder.

// Iterable is a structural thing that, like KVs should be handled elsewhere.
// Encodes an iterable of elements.
//
// The encoding for an iterable [e1...eN] of known length N is
//
//    fixed32(N)
//    encode(e1) encode(e2) encode(e3) ... encode(eN)
//
// If the length is unknown, it is batched up into groups of size b1..bM
// and encoded as
//
//     fixed32(-1)
//     varInt64(b1) encode(e1) encode(e2) ... encode(e_b1)
//     varInt64(b2) encode(e_(b1+1)) encode(e_(b1+2)) ... encode(e_(b1+b2))
//     ...
//     varInt64(bM) encode(e_(N-bM+1)) encode(e_(N-bM+2)) ... encode(eN)
//     varInt64(0)
//
// Components: Coder for a single element.
// ITERABLE = 3 [(beam_urn) = "beam:coder:iterable:v1"];

// Timers are also structural.
// Encodes a timer containing a user key, a dynamic timer tag, a clear bit,
// a fire timestamp, a hold timestamp, the windows and the paneinfo.
// The encoding is represented as:
//   user key - user defined key, uses the component coder.
//   dynamic timer tag - a string which identifies a timer.
//   windows - uses component coders.
//   clear bit - a boolean set for clearing the timer.
//   fire timestamp - a big endian 8 byte integer representing millis-since-epoch.
//     The encoded representation is shifted so that the byte representation of
//     negative values are lexicographically ordered before the byte representation
//     of positive values. This is typically done by subtracting -9223372036854775808
//     from the value and encoding it as a signed big endian integer. Example values:
//
//     -9223372036854775808: 00 00 00 00 00 00 00 00
//                     -255: 7F FF FF FF FF FF FF 01
//                       -1: 7F FF FF FF FF FF FF FF
//                        0: 80 00 00 00 00 00 00 00
//                        1: 80 00 00 00 00 00 00 01
//                      256: 80 00 00 00 00 00 01 00
//      9223372036854775807: FF FF FF FF FF FF FF FF
//   hold timestamp - similar to the fire timestamp.
//   paneinfo - similar to the paneinfo of the windowed_value.
// Components: Coder for the key and windows.
// TIMER = 4 [(beam_urn) = "beam:coder:timer:v1"];

// Length prefix is structural.
// The length prefix is the length encoded as a Varint.
// // Components: The coder to attach a length prefix to
// LENGTH_PREFIX = 6 [(beam_urn) = "beam:coder:length_prefix:v1"];

// IntervalWindow encodes a single interval window, which is the end time as a
// beam timestamp, and a varint of the duration in milliseconds.
//
// This matches with "beam:coder:interval_window:v1" of the beam_runner_api.proto coders.
func (e *Encoder) IntervalWindow(end time.Time, dur time.Duration) {
	e.Timestamp(end)
	e.Varint(uint64(dur.Milliseconds()))
}

// GlobalWindow encodes as no bytes, making this a no-op.
//
// This matches with "beam:coder:global_window:v1" of the beam_runner_api.proto coders.
func (e *Encoder) GlobalWindow() {
}

type GWC struct{}

// Encode encodes this window.
func (GWC) Encode(enc *Encoder) {
	enc.GlobalWindow()
}

func (GWC) decode(dec *Decoder) {
	dec.GlobalWindow()
}

// EncodeWindowedValueHeader encodes the WindowedValue but not the value
//
//	Encodes an element, the windows it is in, the timestamp of the element,
//
// and the pane of the element. The encoding is represented as:
// timestamp windows pane element
//
//	timestamp - A big endian 8 byte integer representing millis-since-epoch.
//	  The encoded representation is shifted so that the byte representation
//	  of negative values are lexicographically ordered before the byte
//	  representation of positive values. This is typically done by
//	  subtracting -9223372036854775808 from the value and encoding it as a
//	  signed big endian integer. Example values:
//
//	  -9223372036854775808: 00 00 00 00 00 00 00 00
//	                  -255: 7F FF FF FF FF FF FF 01
//	                    -1: 7F FF FF FF FF FF FF FF
//	                     0: 80 00 00 00 00 00 00 00
//	                     1: 80 00 00 00 00 00 00 01
//	                   256: 80 00 00 00 00 00 01 00
//	   9223372036854775807: FF FF FF FF FF FF FF FF
//
//	windows - The windows are encoded using the beam:coder:iterable:v1
//	  format, where the windows are encoded using the supplied window
//	  coder.
//
//	pane - The first byte of the pane info determines which type of
//	  encoding is used, as well as the is_first, is_last, and timing
//	  fields. If this byte is bits [0 1 2 3 4 5 6 7], then:
//	  * bits [0 1 2 3] determine the encoding as follows:
//	      0000 - The entire pane info is encoded as a single byte.
//	             The is_first, is_last, and timing fields are encoded
//	             as below, and the index and non-speculative index are
//	             both zero (and hence are not encoded here).
//	      0001 - The pane info is encoded as this byte plus a single
//	             VarInt encoed integer representing the pane index. The
//	             non-speculative index can be derived as follows:
//	               -1 if the pane is early, otherwise equal to index.
//	      0010 - The pane info is encoded as this byte plus two VarInt
//	             encoded integers representing the pane index and
//	             non-speculative index respectively.
//	  * bits [4 5] encode the timing as follows:
//	      00 - early
//	      01 - on time
//	      10 - late
//	      11 - unknown
//	  * bit 6 is 1 if this is the first pane, 0 otherwise.
//	  * bit 7 is 1 if this is the last pane, 0 otherwise.
//
//	element - The element incoded using the supplied element coder.
//
// Components: The element coder and the window coder, in that order.
// WINDOWED_VALUE = 8 [(beam_urn) = "beam:coder:windowed_value:v1"];
func EncodeWindowedValueHeader[W window](e *Encoder, eventTime time.Time, windows []W, pane PaneInfo) {
	e.Timestamp(eventTime)
	e.Uint32(uint32(len(windows)))
	for _, w := range windows {
		w.Encode(e)
	}
	e.Pane(pane)
}

type PaneInfo struct{}

func (e *Encoder) Pane(pane PaneInfo) {
	e.Grow(1)[0] = 0x04
}

// Nullable handles the nil bit of a value.
// Wraps a coder of a potentially null value
// A Nullable Type is encoded by:
//   - A one byte null indicator, 0x00 for null values, or 0x01 for present
//     values.
//   - For present values the null indicator is followed by the value
//     encoded with it's corresponding coder.
//
// Components: single coder for the value
// NULLABLE = 17 [(beam_urn) = "beam:coder:nullable:v1"];
func (e *Encoder) Nullable(isNil bool) {
	e.Bool(isNil)
}

type window interface {
	Encode(e *Encoder)
	decode(d *Decoder)
}

// Timestamp encodes event times in the following fashion.
//
//	timestamp - A big endian 8 byte integer representing millis-since-epoch.
//	  The encoded representation is shifted so that the byte representation
//	  of negative values are lexicographically ordered before the byte
//	  representation of positive values. This is typically done by
//	  subtracting -9223372036854775808 from the value and encoding it as a
//	  signed big endian integer. Example values:
//
//	  -9223372036854775808: 00 00 00 00 00 00 00 00
//	                  -255: 7F FF FF FF FF FF FF 01
//	                    -1: 7F FF FF FF FF FF FF FF
//	                     0: 80 00 00 00 00 00 00 00
//	                     1: 80 00 00 00 00 00 00 01
//	                   256: 80 00 00 00 00 00 01 00
//	   9223372036854775807: FF FF FF FF FF FF FF FF
func (e *Encoder) Timestamp(ts time.Time) {
	unixMillis := ts.UnixMilli()
	e.Uint64((uint64)(unixMillis - math.MinInt64))
}

// Uint8 encodes an arg of type uint8.
func (e *Encoder) Uint8(arg uint8) {
	e.Grow(1)[0] = arg
}

// Byte encodes an arg of type byte.
func (e *Encoder) Byte(arg byte) {
	e.Uint8(arg)
}

// Int8 encodes an arg of type int8.
func (e *Encoder) Int8(arg int8) {
	e.Uint8(byte(arg))
}

// Uint16 encodes an arg of type uint16.
func (e *Encoder) Uint16(arg uint16) {
	binary.BigEndian.PutUint16(e.Grow(2), arg)
}

// Int16 encodes an arg of type int16.
func (e *Encoder) Int16(arg int16) {
	e.Uint16(uint16(arg))
}

// Uint32 encodes an arg of type uint32.
func (e *Encoder) Uint32(arg uint32) {
	binary.BigEndian.PutUint32(e.Grow(4), arg)
}

// Int32 encodes an arg of type int32.
func (e *Encoder) Int32(arg int32) {
	e.Uint32(uint32(arg))
}

// Rune encodes an arg of type rune.
func (e *Encoder) Rune(arg rune) {
	e.Int32(arg)
}

// Uint64 encodes an arg of type uint64.
func (e *Encoder) Uint64(arg uint64) {
	binary.BigEndian.PutUint64(e.Grow(8), arg)
}

// encoderError is the type of error passed to panic by encoding code that encounters an error.
type encoderError struct {
	err error
}

func (e encoderError) Error() string {
	if e.err == nil {
		return "encoder:"
	}
	return "encoder: " + e.err.Error()
}

func (e encoderError) Unwrap() error {
	return e.err
}

// makeEncodeError creates and returns an encoder error.
func makeEncodeError(format string, args ...interface{}) encoderError {
	return encoderError{fmt.Errorf(format, args...)}
}

// Int64 encodes an arg of type int64.
func (e *Encoder) Int64(arg int64) {
	e.Uint64(uint64(arg))
}

// Uint encodes an arg of type uint.
// Uint can have 32 bits or 64 bits based on the machine type. To simplify our
// reasoning, we encode the highest possible value.
func (e *Encoder) Uint(arg uint) {
	e.Uint64(uint64(arg))
}

// Int encodes an arg of type int.
// Int can have 32 bits or 64 bits based on the machine type. To simplify our
// reasoning, we encode the highest possible value.
func (e *Encoder) Int(arg int) {
	e.Uint64(uint64(arg))
}

// Float32 encodes an arg of type float32.
func (e *Encoder) Float(arg float32) {
	binary.BigEndian.PutUint32(e.Grow(4), math.Float32bits(arg))
}

// Complex64 encodes an arg of type complex64.
// We encode the real and the imaginary parts one after the other.
func (e *Encoder) Complex64(arg complex64) {
	e.Float(real(arg))
	e.Float(imag(arg))
}

// Complex128 encodes an arg of type complex128.
func (e *Encoder) Complex128(arg complex128) {
	e.Double(real(arg))
	e.Double(imag(arg))
}
