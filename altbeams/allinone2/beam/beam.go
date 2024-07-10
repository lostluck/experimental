// Package beam is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// This one in particular is a variant on allinone, which avoids the use of
// separate goroutines and channels to pass around elements.
package beam

import (
	"context"
	"flag"
	"fmt"
	"os"
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

// Element represents any user type. Beam processses arbitary user types, but requires
// them to be encodeable.
type Element interface {
	any // Sadly, can't really restrict this without breaking iterators in GBK results.
}

// KV represents key vlaue pairs.
// These are useful to Beam pipelines, including allowing to GroupByKey, and stateful transforms.
type KV[K, V Element] struct {
	Key   K
	Value V
}

// Pair is a convenience function to build generic KVs.
func Pair[K, V Element](k K, v V) KV[K, V] {
	return KV[K, V]{Key: k, Value: v}
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

// Process is the function type for handling a single element in a bundle.
//
// Typically a closure returned from a Transform's ProcessBundle method.
//
// Errors returned from Process functions abort bundle processing, and may
// cause pipeline termination. A runner may retry a bundle that has failed.
type Process[E Element] func(ElmC, E) error

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

func start(ctx context.Context, dfc *DFC[[]byte]) error {
	if err := dfc.start(ctx); err != nil {
		return err
	}
	dfc.metrics.setState(1, dfc.edgeID)
	if err := dfc.perElm(ElmC{elmContext{
		eventTime: time.Now(),
	}, dfc.downstream}, []byte{1, 2, 3, 4, 5, 6, 7, 7}); err != nil {
		panic(fmt.Errorf("doFn id %v failed: %w", dfc.id, err))
	}
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

// Pipeline is a handle to a running or terminated pipeline for
// programmatic access to the given runner.
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

// TODO do worker initialization better.
var (
	// These flags handle the invocation by the container boot code.

	worker = flag.Bool("worker", false, "Whether binary is running in worker mode.")

	id              = flag.String("id", "", "Local identifier (required in worker mode).")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	//lint:ignore U1000 semiPersistDir flag is passed in through the boot container, will need to be removed later
	semiPersistDir = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional in worker mode).")

	useDocker  = flag.Bool("use_docker", false, "Use docker for workers instead of loopback. Temp flag for iteration.")
	useProcess = flag.Bool("use_process", false, "Spawn a boot container process for workers instead of loopback. Temp flag for iteration.")
)

// Run begins executes the pipeline built in the construction function.
func Run(ctx context.Context, expand func(*Scope) error, opts ...Options) (Pipeline, error) {
	// TODO extract job port selection
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

	fmt.Println("beam.Run: flags:", os.Args)

	if !flag.Parsed() {
		flag.Parse()
		if *worker {
			err := harness.Main(ctx, *controlEndpoint, harness.Options{
				LoggingEndpoint: *loggingEndpoint,
				// Pull from environment variables.
				// StatusEndpoint: *TODO,
				// RunnerCapabilities:  TODO,
			}, executeSubgraph(typeReg))
			return Pipeline{}, err
		}
	}

	// if err := prism.Start(ctx, prism.Options{
	// 	Location: "/home/lostluck/git/beam/sdks/go/cmd/prism/prism",
	// }); err != nil {
	// 	return Pipeline{}, err
	// }

	var env *pipepb.Environment
	if !*useProcess {
		// TODO(BEAM-10610): Allow user configuration of this port, rather than kernel selected.
		srv, err := extworker.StartLoopback(ctx, 0, executeSubgraph(typeReg))
		if err != nil {
			return Pipeline{}, err
		}
		defer srv.Stop(ctx)
		serializedPayload, err := proto.Marshal(&pipepb.ProcessPayload{
			Command: "/home/lostluck/git/beam/sdks/go/container/boot",
		})
		if err != nil {
			return Pipeline{}, err
		}
		env = &pipepb.Environment{
			Urn:          "beam:env:external:v1",
			Payload:      serializedPayload,
			Capabilities: nil, // TODO
		}
	} else if !*useDocker {
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
		env = &pipepb.Environment{
			Urn:          "beam:env:external:v1",
			Payload:      serializedPayload,
			Capabilities: nil, // TODO
		}
	} else {
		serializedPayload, err := proto.Marshal(&pipepb.DockerPayload{ContainerImage: "apache/beam_go_sdk:latest"}) // 2.57.0"})
		if err != nil {
			return Pipeline{}, err
		}
		serializedBinPayload, err := proto.Marshal(&pipepb.ArtifactFilePayload{
			Path: os.Args[0], // Recompile or similar.
		})
		if err != nil {
			return Pipeline{}, err
		}

		env = &pipepb.Environment{
			Urn:          "beam:env:docker:v1",
			Payload:      serializedPayload,
			Capabilities: nil, // TODO
			Dependencies: []*pipepb.ArtifactInformation{
				{
					TypeUrn:     "beam:artifact:type:file:v1",
					TypePayload: serializedBinPayload,
					RoleUrn:     "beam:artifact:role:go_worker_binary:v1",
				},
			},
		}
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
	var shortID atomic.Uint32
	return func(ctx context.Context, ctrl *harness.Control, dataCon harness.DataContext) (*fnpb.ProcessBundleResponse, error) {
		// 1. Provide translation function (unmarshalToGraph + types closure) to harness.
		//    * Harness then returns a graph, either getting a cached old version, or building a new one from proto.
		//    * Caches the proto in a weak map somewhere...
		g, err := ctrl.GetOrLookupPlan(dataCon, func(comps *fnpb.ProcessBundleDescriptor) any {
			return unmarshalToGraph(typeReg, comps)
		})
		if err != nil {
			return nil, err
		}
		newG := g.(*graph)

		// 2. Build a new runnable instance, get execution roots and metrics.
		roots, mets := newG.build(ctx, dataCon)

		// 3. Register the metrics handling function for this instruction with the harness.
		//    * This handles progress and tentative metrics
		ctrl.RegisterMonitor(dataCon, func() (map[string]*pipepb.MonitoringInfo, map[string][]byte) {
			mons := mets.MonitoringInfos(newG)
			pylds := map[string][]byte{}
			labels := map[string]*pipepb.MonitoringInfo{}
			for _, mon := range mons {
				key := strconv.FormatInt(int64(shortID.Add(1)), 36)

				pylds[key] = mon.GetPayload()
				labels[key] = &pipepb.MonitoringInfo{
					Urn:    mon.GetUrn(),
					Type:   mon.GetType(),
					Labels: mon.GetLabels(),
				}
			}
			return labels, pylds
		})

		// 4. Register a split handler with the harness
		//    * This handles channel splits and SDF splits
		ctrl.RegisterSplitter(dataCon, func(splits map[string]*fnpb.ProcessBundleSplitRequest_DesiredSplit) (*fnpb.ProcessBundleSplitResponse, error) {
			ret := &fnpb.ProcessBundleSplitResponse{}
			for _, root := range roots {
				split, ok := splits[root.transformID()]
				if !ok {
					continue
				}
				resp := root.split(split)
				proto.Merge(ret, resp)
			}
			return ret, nil
		})

		// TODO bundle finalization
		// The above would be cleaned up in the harness.

		// 5. Start DoFn sampling for this processing thread.
		mets.startSampling(ctx, 10*time.Millisecond, 5*time.Minute)
		defer mets.stopSampling()

		// 6. Process the bundle.
		for _, root := range roots {
			if err := start(ctx, root.(*DFC[[]byte])); err != nil {
				return nil, err
			}
		}
		// 7. Run finishes bundles.
		for _, root := range roots {
			if err := root.finish(); err != nil {
				return nil, err
			}
		}

		// 8. Respond.

		// Note, Metrics and Data would be handled outside.
		// Since we may have residuals for ProcessContinuations, or finalization
		// we return this here for those in the future.
		return &fnpb.ProcessBundleResponse{
			// ResidualRoots:        rRoots,
			// RequiresFinalization: requiresFinalization,
		}, nil
	}
}
