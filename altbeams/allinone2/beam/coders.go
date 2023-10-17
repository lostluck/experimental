package beam

import (
	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
)

func MakeCoder[E any]() coders.Coder[E] {
	var e E
	a := any(e)
	switch a := a.(type) {
	case structuredCoder:
		return a.makeCoder().(coders.Coder[E])
	}
	return coders.MakeCoder[E]()
}

type structuredCoder interface {
	makeCoder() any
}

var _ structuredCoder = KV[int, int]{}

func (KV[K, V]) makeCoder() any {
	return kvCoder[K, V]{
		KCoder: MakeCoder[K](),
		VCoder: MakeCoder[V](),
	}
}

type kvCoder[K Keys, V Element] struct {
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

func (Iter[V]) makeCoder() any {
	return iterCoder[V]{
		VCoder: MakeCoder[V](),
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
			cur++
			return c.VCoder.Decode(dec), cur < n
		},
	}
}
