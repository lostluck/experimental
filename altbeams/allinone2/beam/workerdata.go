package beam

import (
	"context"
	"sync/atomic"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
)

// This file contains the data source and datasink Transforms
// and edges. These are added in by runners for execution on
// the SDK, and never added in manually by users.

// edgeDataSource represents a data connection from the runner.
type edgeDataSource[E Element] struct {
	index     edgeIndex
	transform string

	port      harness.Port
	makeCoder func() coders.Coder[E]

	output nodeIndex
}

func (e *edgeDataSource[E]) protoID() string {
	return e.transform
}

func (e *edgeDataSource[E]) edgeID() edgeIndex {
	return e.index
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

func (e *edgeDataSource[E]) source(dc harness.DataContext, mets *metricsStore) (processor, processor) {
	// This is what the Datasource emits to.
	toConsumer := newDFC[E](e.output, nil)
	toConsumer.metrics = mets

	// TODO, produce the coder via the

	// But we're lazy and just kick it off with an impulse.
	root := newDFC[[]byte](e.output, []processor{toConsumer})
	root.metrics = mets
	root.dofn = &datasource[E]{
		DC:     dc,
		SID:    harness.StreamID{PtransformID: e.transform, Port: e.port},
		Output: Emitter[E]{valid: true, globalIndex: e.output, localDownstreamIndex: 0},
		Coder:  e.makeCoder(),
	}
	return root, toConsumer
}

var _ sourcer = (*edgeDataSource[int])(nil)

type sourcer interface {
	multiEdge
	source(dc harness.DataContext, mets *metricsStore) (processor, processor)
}

type datasource[E Element] struct {
	DC  harness.DataContext
	SID harness.StreamID

	// Window Coder to produce windows
	Coder coders.Coder[E]

	Output Emitter[E]

	split atomic.Int64
}

func (fn *datasource[E]) ProcessBundle(ctx context.Context, dfc *DFC[[]byte]) error {
	// Connect to Data service
	elmsChan, err := fn.DC.Data.OpenElementChan(ctx, fn.SID, nil)
	if err != nil {
		return err
	}

	// Track the data channel index for progress and split handling.
	dc := &dataChannelIndex{transform: fn.SID.PtransformID}
	dc.index.Store(-1)
	fn.split.Store(1<<63 - 1)

	// TODO outputing to timers callbacks
	dfc.Process(func(ec ElmC, _ []byte) error {
	dataChan:
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
				if dc.IncrementAndCheckSplit(dfc, fn.split.Load()) {
					break dataChan
				}
			}
		}
		return nil
	})
	return nil
}

// edgeDataSink represents a data connection back to the runner.
type edgeDataSink[E Element] struct {
	index     edgeIndex
	transform string

	port      harness.Port
	makeCoder func() coders.Coder[E]

	input nodeIndex
}

func (e *edgeDataSink[E]) protoID() string {
	return e.transform
}

func (e *edgeDataSink[E]) edgeID() edgeIndex {
	return e.index
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
		Coder: e.makeCoder(),
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
