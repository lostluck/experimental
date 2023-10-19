package beam

import (
	"context"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
)

// This file contains the data source and datasink Transforms
// and edges. These are added in by runners for execution on
// the SDK, and never added in manually by users.

// edgeDataSource represents a data connection from the runner.
type edgeDataSource[E Element] struct {
	transform string

	port    harness.Port
	coderID string

	output nodeIndex
}

// inputs for datasink, in practice there should only be one
// but if all else fails, we can insert a flatten.
func (e *edgeDataSource[E]) inputs() map[string]nodeIndex {
	return nil
}

// outputs for DataSink is nil, since it sends back to the runner.
func (e *edgeDataSource[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeDataSource[E]) source(dc harness.DataContext) (processor, processor) {
	// This is what the Datasource emits to.
	toConsumer := newDFC[E](e.output, nil)

	// But we're lazy and just kick it off with an impulse.
	root := newDFC[[]byte](e.output, []processor{toConsumer})
	root.dofn = &datasource[E]{
		DC:     dc,
		SID:    harness.StreamID{PtransformID: e.transform, Port: e.port},
		Output: Emitter[E]{valid: true, globalIndex: e.output, localDownstreamIndex: 0},
		Coder:  MakeCoder[E](),
	}
	return root, toConsumer
}

var _ sourcer = (*edgeDataSource[int])(nil)

type sourcer interface {
	multiEdge
	source(dc harness.DataContext) (processor, processor)
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
	dfc.Process(func(ec ElmC, _ []byte) error {
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
		return nil
	})
	return nil
}

// edgeDataSink represents a data connection back to the runner.
type edgeDataSink[E Element] struct {
	transform string

	port    harness.Port
	coderID string

	input nodeIndex
}

// inputs for datasink, in practice there should only be one
// but if all else fails, we can insert a flatten.
func (e *edgeDataSink[E]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.input}
}

// outputs for DataSink is nil, since it sends back to the runner.
func (e *edgeDataSink[E]) outputs() map[string]nodeIndex {
	return nil
}

var _ sinker = (*edgeDataSink[int])(nil)

type sinker interface {
	multiEdge
	sinkDoFn(dc harness.DataContext) any
}

func (e *edgeDataSink[E]) sinkDoFn(dc harness.DataContext) any {
	return &datasink[E]{DC: dc,
		SID:   harness.StreamID{PtransformID: e.transform, Port: e.port},
		Coder: MakeCoder[E](),
	}
}

// datasink writes window value encoded elements to the runner over the configured data channel.
type datasink[E Element] struct {
	DC  harness.DataContext
	SID harness.StreamID

	// Window Coder to produce windows
	Coder coders.Coder[E]

	OnBundleFinish
}

func (fn *datasink[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	wc, err := fn.DC.Data.OpenWrite(ctx, fn.SID)
	if err != nil {
		return err
	}

	enc := coders.NewEncoder()
	// TODO outputing to timers callbacks
	dfc.Process(func(ec ElmC, elm E) error {
		enc.Reset(100)
		coders.EncodeWindowedValueHeader(enc, ec.EventTime(), []coders.GWC{{}}, coders.PaneInfo{})

		fn.Coder.Encode(enc, elm)

		wc.Write(enc.Data())
		return nil
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		return wc.Close()
	})
	return nil
}
