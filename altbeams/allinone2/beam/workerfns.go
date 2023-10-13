package beam

import (
	"context"
	"math"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
)

// workerfns.go is where direct runner or SDK side transforms live.
// They provide common utility that pipeline execution needs to correctly implement the beam model.
// Note that they are largely implemented in the same manner as user DoFns.

// multiplex is a Transform inserted when a PCollection is used as an input into
// multiple downstream Transforms. The same element is emitted to each
// consumming emitter in order.
type multiplex[E Element] struct {
	Outs []Emitter[E]
}

func (fn *multiplex[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		for _, out := range fn.Outs {
			out.Emit(ec, elm)
		}
		return true
	})
	return nil
}

// discard is a Transform inserted when a PCollection is unused by a downstream Transform.
// It performs a no-op. This allows execution graphs to avoid branches and checks whether
// a consumer is valid on each element.
type discard[E Element] struct{}

func (fn *discard[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		return true
	})
	return nil
}

type flatten[E Element] struct {
	Output Emitter[E]
}

func (fn *flatten[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.Output.Emit(ec, elm)
		return true
	})
	return nil
}

type datasource[E Element] struct {
	DC  harness.DataContext
	SID harness.StreamID

	// Window Coder to produce windows
	Coder coders.Coder[E]

	Output Emitter[E]
}

func (fn *datasource[E]) ProcessBundle(ctx context.Context, dfc *DFC[[]byte]) error {
	// Connect to Data service
	elmsChan, err := fn.DC.Data.OpenElementChan(ctx, fn.SID, nil)
	if err != nil {
		return err
	}
	// TODO outputing to timers callbacks
	dfc.Process(func(ec ElmC, _ []byte) bool {
		for dataElm := range elmsChan {
			// Start reading byte blobs.
			dec := coders.NewDecoder(dataElm.Data)
			for !dec.Empty() {

				et, ws, pn := coders.DecodeWindowedValueHeader[coders.GWC](dec)
				e := fn.Coder.Decode(dec)

				fn.Output.Emit(ElmC{
					elmContext: elmContext{
						eventTime: et,
						windows:   ws,
						pane:      pn,
					},
					pcollections: ec.pcollections,
				}, e)
			}
		}
		return true
	})
	return nil
}

type datasink[E Element] struct {
	DC  harness.DataContext
	SID harness.StreamID

	// Window Coder to produce windows
	Coder coders.Coder[E]

	OnBundleFinish
}

func (fn *datasink[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	// Connect to Data service
	wc, err := fn.DC.Data.OpenWrite(ctx, fn.SID)
	if err != nil {
		return err
	}

	enc := coders.NewEncoder()
	// TODO outputing to timers callbacks
	dfc.Process(func(ec ElmC, elm E) bool {
		enc.Reset(100)
		coders.EncodeWindowedValueHeader(enc, ec.EventTime(), []coders.GWC{{}}, coders.PaneInfo{})

		fn.Coder.Encode(enc, elm)

		wc.Write(enc.Data())
		return true
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		return wc.Close()
	})
	return nil
}

// gbk groups by the key type and value type.
// TODO, remove this.
type gbk[K Keys, V Element] struct {
	Output Emitter[KV[K, Iter[V]]]

	OnBundleFinish
}

var (
	MaxET time.Time = time.UnixMilli(math.MaxInt64 / 1000)
	EOGW            = MaxET.Add(-time.Hour * 24)
)

func (fn *gbk[K, V]) ProcessBundle(ctx context.Context, dfc *DFC[KV[K, V]]) error {
	grouped := map[K][]V{}
	dfc.Process(func(ec ElmC, elm KV[K, V]) bool {
		vs := grouped[elm.Key]
		vs = append(vs, elm.Value)
		grouped[elm.Key] = vs
		return true
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		ec := dfc.ToElmC(EOGW) // TODO pull time, from the window that's been closed.
		for k, vs := range grouped {
			var i int
			out := KV[K, Iter[V]]{Key: k, Value: Iter[V]{
				source: func() (V, bool) {
					var v V
					if i < len(vs) {
						v = vs[i]
						i++
						return v, true
					}
					return v, false
				},
			}}
			fn.Output.Emit(ec, out)
		}
		return nil
	})
	return nil
}
