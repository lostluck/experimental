package beam

import (
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
)

type sideInputCommon struct {
	beamMixin

	valid  bool
	global nodeIndex
}

func (si *sideInputCommon) sideInput() nodeIndex {
	return si.global
}

type sideIface interface {
	sideInput() nodeIndex
	accessPatternUrn() string
	initialize(ctx context.Context, dataCon harness.DataContext, url, sideID, transformID string)
}

type SideInputIter[E Element] struct {
	sideInputCommon

	initIterReader func(w []byte) harness.NextBuffer
}

func (*SideInputIter[E]) accessPatternUrn() string {
	return "beam:side_input:iterable:v1"
}

func (si *SideInputIter[E]) initialize(ctx context.Context, dataCon harness.DataContext, url, sideID, transformID string) {
	si.initIterReader = func(w []byte) harness.NextBuffer {
		key := &fnpb.StateKey{
			Type: &fnpb.StateKey_IterableSideInput_{
				IterableSideInput: &fnpb.StateKey_IterableSideInput{
					TransformId: transformID,
					SideInputId: sideID,
					Window:      w,
				},
			},
		}
		// 50/50 on putting this on processor directly instead
		r, err := dataCon.State.OpenReader(ctx, url, key)
		if err != nil {
			panic(err)
		}
		return r
	}
}

var _ sideIface = &SideInputIter[int]{}

func (si *SideInputIter[E]) All(ec ElmC) iter.Seq[E] {
	enc := coders.NewEncoder()
	w := ec.windows[0]
	w.Encode(enc)
	r := si.initIterReader(enc.Data())
	return iterClosure[E](r)
}

func iterClosure[E Element](r harness.NextBuffer) iter.Seq[E] {
	c := MakeCoder[E]()
	return func(perElm func(elm E) bool) {

		defer r.Close()
		for {
			buf, err := r.NextBuf()
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(err)
			}
			dec := coders.NewDecoder(buf)
			for !dec.Empty() {
				if !perElm(c.Decode(dec)) {
					return
				}
			}
		}
	}
}

func validateSideInput[E any](emt Output[E]) {
	if !emt.valid {
		panic("emitter is invalid")
	}
	var e E
	if isMetaType(e) {
		panic(fmt.Sprintf("type %T cannot be used as a side input value", e))
	}
}

// AsSideIter initializes an IterSideInput from a valid upstream Emitter.
// It allows access to the data of that Emitter's PCollection,
func AsSideIter[E Element](emt Output[E]) SideInputIter[E] {
	validateSideInput(emt)
	return SideInputIter[E]{sideInputCommon: sideInputCommon{valid: true, global: emt.globalIndex}}
}

// SideInputMap allows a side input to be accessed via multip-map key lookups.
type SideInputMap[K, V Element] struct {
	sideInputCommon

	initMapReader     func(w, k []byte) harness.NextBuffer
	initMapKeysReader func(w []byte) harness.NextBuffer
}

func (*SideInputMap[K, V]) accessPatternUrn() string {
	return "beam:side_input:multimap:v1"
}

func (si *SideInputMap[K, V]) initialize(ctx context.Context, dataCon harness.DataContext, url, sideID, transformID string) {
	si.initMapReader = func(w, k []byte) harness.NextBuffer {
		key := &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapSideInput_{
				MultimapSideInput: &fnpb.StateKey_MultimapSideInput{
					TransformId: transformID,
					SideInputId: sideID,
					Window:      w,
					Key:         k,
				},
			},
		}
		r, err := dataCon.State.OpenReader(ctx, url, key)
		if err != nil {
			panic(err)
		}
		return r
	}
	si.initMapKeysReader = func(w []byte) harness.NextBuffer {
		key := &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapKeysSideInput_{
				MultimapKeysSideInput: &fnpb.StateKey_MultimapKeysSideInput{
					TransformId: transformID,
					SideInputId: sideID,
					Window:      w,
				},
			},
		}
		// 50/50 on putting this on processor directly instead
		r, err := dataCon.State.OpenReader(ctx, url, key)
		if err != nil {
			panic(err)
		}
		return r
	}
}

var _ sideIface = &SideInputMap[int, int]{}

// Get looks up an iterator of values associated with the key.
func (si *SideInputMap[K, V]) Get(ec ElmC, k K) iter.Seq[V] {
	w := ec.windows[0]
	encW := coders.NewEncoder()
	w.Encode(encW)

	// TODO cache coders in the side inputs?
	kc := MakeCoder[K]()
	encK := coders.NewEncoder()
	kc.Encode(encK, k)
	r := si.initMapReader(encW.Data(), encK.Data())
	return iterClosure[V](r)
}

// Get looks up an iterator of values associated with the key.
func (si *SideInputMap[K, V]) Keys(ec ElmC) iter.Seq[K] {
	w := ec.windows[0]
	encW := coders.NewEncoder()
	w.Encode(encW)
	r := si.initMapKeysReader(encW.Data())
	return iterClosure[K](r)
}

// AsSideMap initializes a MapSideInput from a valid upstream Emitter.
func AsSideMap[K, V Element](emt Output[KV[K, V]]) SideInputMap[K, V] {
	validateSideInput(emt)
	return SideInputMap[K, V]{sideInputCommon: sideInputCommon{valid: true, global: emt.globalIndex}}
}
