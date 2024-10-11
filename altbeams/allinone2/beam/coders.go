package beam

import (
	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
)

// coderFromProto bridges the gap between the Go type, and the
// proto coder. This is necessary since sometimes a runner may adjust
// a coder, such as adding length prefixes.
func coderFromProto[E any](cs map[string]*pipepb.Coder, cid string) coders.Coder[E] {
	c, ok := cs[cid]
	if ok {
		switch c.GetSpec().GetUrn() {
		case "beam:coder:length_prefix:v1":
			ccid := c.GetComponentCoderIds()[0]
			return &lpCoder[E]{Coder: coderFromProto[E](cs, ccid)}
		case "beam:coder:windowed_value:v1":
			// Doesn't happen often, but generally from sources and sinks.
			ccid := c.GetComponentCoderIds()[0]
			return coderFromProto[E](cs, ccid)
		case "beam:coder:kv:v1",
			"beam:coder:state_backed_iterable:v1",
			"beam:coder:iterable:v1":
			// Handled by the structured coder clause below.
		}
	}
	var e E
	a := any(e)
	switch a := a.(type) {
	case structuredCoder:
		return a.makeCoder(cs, cid).(coders.Coder[E])
	}
	// Just infer primitives directly.
	return coders.MakeCoder[E]()
}

func MakeCoder[E any]() coders.Coder[E] {
	var e E
	a := any(e)
	switch a := a.(type) {
	case structuredCoder:
		return a.makeCoder(nil, "").(coders.Coder[E])
	}
	return coders.MakeCoder[E]()
}

type structuredCoder interface {
	makeCoder(cs map[string]*pipepb.Coder, cid string) any
}

var _ structuredCoder = KV[int, int]{}

func (KV[K, V]) makeCoder(cs map[string]*pipepb.Coder, cid string) any {
	c := cs[cid]
	return kvCoder[K, V]{
		KCoder: coderFromProto[K](cs, c.GetComponentCoderIds()[0]),
		VCoder: coderFromProto[V](cs, c.GetComponentCoderIds()[1]),
	}
}

type kvCoder[K, V Element] struct {
	KCoder coders.Coder[K]
	VCoder coders.Coder[V]
}

func (c kvCoder[K, V]) Encode(enc *coders.Encoder, v KV[K, V]) {
	c.KCoder.Encode(enc, v.Key)
	c.VCoder.Encode(enc, v.Value)
}

func (c kvCoder[K, V]) Decode(dec *coders.Decoder) KV[K, V] {
	return KV[K, V]{
		Key:   c.KCoder.Decode(dec),
		Value: c.VCoder.Decode(dec),
	}
}

var _ structuredCoder = Iter[int]{}

func (Iter[V]) makeCoder(cs map[string]*pipepb.Coder, cid string) any {
	c := cs[cid]
	return iterCoder[V]{
		VCoder: coderFromProto[V](cs, c.GetComponentCoderIds()[0]),
	}
}

type iterCoder[V Element] struct {
	VCoder coders.Coder[V]
}

func (c iterCoder[V]) Encode(enc *coders.Encoder, v Iter[V]) {
	panic("iterators are unencodeable")
}

func (c iterCoder[V]) Decode(dec *coders.Decoder) Iter[V] {
	n := dec.Int32()
	var cur int32
	return Iter[V]{
		source: func() (V, bool) {
			if cur >= n {
				var dummy V
				return dummy, false
			}
			cur++
			return c.VCoder.Decode(dec), true
		},
	}
}

// lpCoder takes a different coder for a type, and deals with
// length prefixes, adding them on encoding, reading them on
// decode.
type lpCoder[E Element] struct {
	Coder coders.Coder[E]
}

func (c *lpCoder[E]) Encode(enc *coders.Encoder, v E) {
	inner := coders.NewEncoder()
	c.Coder.Encode(inner, v)
	// Use the bytes encoding, since it's already a length prefix
	// followed by that many bytes.
	enc.Bytes(inner.Data())
}

func (c *lpCoder[E]) Decode(dec *coders.Decoder) E {
	// Use the bytes decoding, since it's already a length prefix
	// followed by that many bytes.
	inner := coders.NewDecoder(dec.Bytes())
	return c.Coder.Decode(inner)
}
