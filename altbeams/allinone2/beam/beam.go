// Package beam is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// This one in particular is a variant on allinone, which avoids the use of
// separate goroutines and channels to pass around elements.
package beam

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/extworker"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/runner/universal"
	"google.golang.org/protobuf/proto"
)

type Keys interface {
	comparable
}

type KV[K Keys, V Element] struct {
	Key   K
	Value V
}

type Element interface {
	any // Sadly, can't really restrict this without breaking iterators in GBK results.
}

// ElmC is the catch all context for the current element.
//
// This includes
// * Key (state and timers)
// * Windows
// * Timestamp
// * Pane
//
// Provides the downstream emission context, so it actually sends data to the next DoFn.
type ElmC struct {
	elmContext

	pcollections []processor
}

func (e *ElmC) EventTime() time.Time {
	return e.eventTime
}

// Transform is the only interface that needs to be implemented by most DoFns.
type Transform[E Element] interface {
	ProcessBundle(ctx context.Context, dfc *DFC[E]) error
}

type Iter[V Element] struct {
	source func() (V, bool) // source returns true if the element is valid.
}

func (Iter[V]) unencodable() {}

var _ unencodable = Iter[int]{}

type unencodable interface {
	unencodable()
}

func isUnencodable(v any) bool {
	_, ok := v.(unencodable)
	return ok
}

// All allows a single iteration of its stream of values.
func (it *Iter[V]) All() func(perElm func(elm V) bool) {
	return func(perElm func(elm V) bool) {
		for {
			v, ok := it.source()
			if !ok {
				return
			}
			if !perElm(v) {
				return
			}
		}
	}
}

func start(dfc *DFC[[]byte]) error {
	if err := dfc.start(context.TODO()); err != nil {
		return err
	}
	dfc.processE(elmContext{
		eventTime: time.Now(),
	}, []byte{1, 2, 3, 4, 5, 6, 7, 7})
	return nil
}

// Scope is used for building pipeline graphs.
//
// Scope is a hierarchical grouping for composite transforms. Scopes can be
// enclosed in other scopes and for a tree structure. For pipeline updates,
// the scope chain form a unique name. The scope chain can also be used for
// monitoring and visualization purposes.
type Scope struct {
	name   string
	parent *Scope

	g *graph
}

func (s *Scope) String() string {
	if s == nil {
		return ""
	}
	return s.parent.String() + "/" + s.name
}

// Run begins executes the pipeline built in the construction function.
func Run(ctx context.Context, expand func(*Scope) error, opts ...Options) (Pipeline, error) {
	opt := beamopts.Struct{
		Endpoint: "localhost:8073",
	}
	opt.Join(opts...)

	var g graph
	s := &Scope{parent: nil, g: &g}
	g.root = s

	if err := expand(s); err != nil {
		return Pipeline{}, fmt.Errorf("pipeline construction error:%w", err)
	}

	// At this point the graph is complete, and we need to turn serialize/deserialize it
	// into executing code.
	typeReg := map[string]reflect.Type{}
	pipe := g.marshal(typeReg)

	// TODO(BEAM-10610): Allow user configuration of this port, rather than kernel selected.
	srv, err := extworker.StartLoopback(ctx, 0, executeSubgraph(typeReg))

	if err != nil {
		return Pipeline{}, err
	}
	defer srv.Stop(ctx)
	serializedPayload, err := proto.Marshal(&pipepb.ExternalPayload{Endpoint: &pipepb.ApiServiceDescriptor{Url: srv.EnvironmentConfig(ctx)}})
	if err != nil {
		return Pipeline{}, err
	}

	env := &pipepb.Environment{
		Urn:          "beam:env:external:v1",
		Payload:      serializedPayload,
		Capabilities: nil, // TODO
	}
	pipe.Components.Environments["go"] = env
	handle, err := universal.Execute(ctx, pipe, opt)
	if err != nil {
		return Pipeline{}, err
	}

	r, err := handle.Metrics(ctx)
	if err != nil {
		return Pipeline{}, err
	}

	p := Pipeline{
		Counters: r.UserCounters(),
	}
	return p, nil
}

func executeSubgraph(typeReg map[string]reflect.Type) harness.ExecFunc {
	var i atomic.Uint32
	labels := map[string]*pipepb.MonitoringInfo{}
	return func(comps harness.SubGraphProto, dataCon harness.DataContext) (*fnpb.ProcessBundleResponse, map[string]*pipepb.MonitoringInfo, error) {
		newG := unmarshalToGraph(typeReg, comps)
		roots, mets := newG.build(dataCon)
		for _, root := range roots {
			if err := start(root.(*DFC[[]byte])); err != nil {
				return nil, nil, err
			}
		}
		for _, root := range roots {
			if err := root.finish(); err != nil {
				return nil, nil, err
			}
		}

		mons := mets.MonitoringInfos()

		pylds := map[string][]byte{}
		for _, mon := range mons {
			key := strconv.FormatInt(int64(i.Add(1)), 36)

			pylds[key] = mon.GetPayload()
			labels[key] = &pipepb.MonitoringInfo{
				Urn:    mon.GetUrn(),
				Type:   mon.GetType(),
				Labels: mon.GetLabels(),
			}
		}

		return &fnpb.ProcessBundleResponse{
			// ResidualRoots:        rRoots,
			MonitoringData: pylds,
			// MonitoringInfos: mons,
			// RequiresFinalization: requiresFinalization,
		}, labels, nil
	}
}

type Pipeline struct {
	Counters map[string]int64
}

func Impulse(s *Scope) Emitter[[]byte] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.edges = append(s.g.edges, &edgeImpulse{index: edgeID, output: nodeID})
	s.g.nodes = append(s.g.nodes, &typedNode[[]byte]{index: nodeID, parentEdge: edgeID})

	// This is a fictional input.
	return Emitter[[]byte]{globalIndex: nodeID}
}

// ParDo takes the users's DoFn and returns the same type for downstream piepline construction.
//
// The returned DoFn's emitter fields can then be used as inputs into other DoFns.
// What if we used Emitters as PCollections directly?
// Obviously, we'd rename the type PCollection or similar
// If only to also
func ParDo[E Element, DF Transform[E]](s *Scope, input Emitter[E], dofn DF, opts ...Options) DF {
	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	ins, outs := s.g.deferDoFn(dofn, input.globalIndex, edgeID)

	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.

	s.g.edges = append(s.g.edges, &edgeDoFn[E]{index: edgeID, dofn: dofn, ins: ins, outs: outs, parallelIn: input.globalIndex, opts: opt})

	return dofn
}

// Composite transforms allow structural re-use of sub pipelines.
type Composite[O any] interface {
	Expand(s *Scope) O
}

func Expand[I Composite[O], O any](parent *Scope, name string, comp I) O {
	s := &Scope{name: name, parent: parent, g: parent.g}
	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.
	return comp.Expand(s)
}

func Flatten[E Element](s *Scope, inputs ...Emitter[E]) Emitter[E] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}
	var ins []nodeIndex
	for _, emt := range inputs {
		in := emt.globalIndex
		ins = append(ins, in)
		s.g.consumers[in] = append(s.g.consumers[in], edgeID)
	}
	s.g.edges = append(s.g.edges, &edgeFlatten[E]{index: edgeID, ins: ins, output: nodeID})
	s.g.nodes = append(s.g.nodes, &typedNode[E]{index: nodeID, parentEdge: edgeID})

	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.
	return Emitter[E]{globalIndex: nodeID}
}

// GBK produces an output PCollection.
func GBK[K Keys, V Element](s *Scope, input Emitter[KV[K, V]], opts ...Options) Emitter[KV[K, Iter[V]]] {
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}

	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.consumers[input.globalIndex] = append(s.g.consumers[input.globalIndex], edgeID)

	s.g.edges = append(s.g.edges, &edgeGBK[K, V]{index: edgeID, input: input.globalIndex, output: nodeID, opts: opt})
	s.g.nodes = append(s.g.nodes, &typedNode[KV[K, Iter[V]]]{index: nodeID, parentEdge: edgeID})

	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.
	return Emitter[KV[K, Iter[V]]]{globalIndex: nodeID}
}
