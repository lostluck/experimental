package coders

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

func MakeCoder[E any]() Coder[E] {
	var e E
	return makeCoder(e).(Coder[E])
}

// makeCoder works around
func makeCoder(a any) any {
	switch a.(type) {
	case int:
		return intCoder{}
	case []byte:
		return bytesCoder{}
	}
	return nil
}

type intCoder struct{}

func (intCoder) Encode(enc *Encoder, v int) {
	// TODO correctly encode/decode ints for varints.
	enc.Varint(uint64(v))
}

func (intCoder) Decode(dec *Decoder) int {
	return int(dec.Varint())
}

type bytesCoder struct{}

// TODO correctly encode/decode ints for varints.
func (bytesCoder) Encode(enc *Encoder, v []byte) {
	enc.Bytes(v)
}

func (bytesCoder) Decode(dec *Decoder) []byte {
	return dec.Bytes()
}
