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

func (Iter[V]) metatype() {}

var _ metatype = Iter[int]{}

type metatype interface {
	metatype()
}

// isMetaType checks if a type has characteristics that make it unsuitable
// for some usage. eg. Iter types should not be used as side input values,
// since an iterator of iterators isn't a good idea.
func isMetaType(v any) bool {
	_, ok := v.(metatype)
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
