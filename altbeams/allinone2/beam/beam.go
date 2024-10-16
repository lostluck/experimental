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
	"log"
	"maps"
	"os"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-json-experiment/json"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/extworker"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/runner/prism"
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
	// TODO consider making these methods instead
	// and decode them on demand?
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
	ProcessBundle(dfc *DFC[E]) error
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
		return fmt.Errorf("doFn id %v failed: %w", dfc.id, err)
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
	handle   *universal.Pipeline
	cancelFn context.CancelCauseFunc
	// TODO make these methods instead & support cancellation.
	Counters      map[string]int64
	Distributions map[string]struct{ Count, Sum, Min, Max int64 }
}

func (pr *Pipeline) Wait(ctx context.Context) error {
	err := pr.handle.Wait(ctx)
	// Regardless of whether there's a pipeline error, get the metrics anyway.
	r, errMet := pr.handle.Metrics(ctx)
	if errMet != nil {
		return fmt.Errorf("couldn't extract metrics for: %w", errMet)
	}
	pr.Counters = r.UserCounters()
	pr.Distributions = r.UserDistributions()

	return err
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

type envFlags struct {
	// Endpoint directs the universal environment to an existing JobManagement instance.
	Endpoint string
	// EnvironmentType is the environment type to run the user code.
	EnvironmentType string
	// EnvironmentConfig is the environment configuration for running the user code.
	EnvironmentConfig string
}

type workerFlags struct {
	Worker          bool   // Whether we're operating as a worker or not.
	ID              string // The worker identity of this process.
	LoggingEndpoint string // The Logging service endpoint
	ControlEndpoint string // The Control service endpoint
	SemiPersist     string // The Semi persist directory. Deprecated.
}

// Configuration represents static configuration for pipelines.
//
// The same [Config] may be used for multiple pipeline launches,
// however, that requires registration of DoFns in advance.
//
// Serializable Construction time options may be registered with the [TODO]
// method, and refered to in calls to [Launch] or [Prepare].
//
// You may [Prepare] multiple pipelines in advance, in order to
// ensure their configuration is registered with an ID.
// However these pipelines will not be built until [Launch] is called with
// the provided ID.
//
// After prepare has been called,
type Configuration struct {
	worker      workerFlags
	environment envFlags

	pipelines map[string]func(*Scope) error
}

// New produces a new Beam framework configuration to statically
// configure pipelines for execution.
func New() *Configuration {
	return &Configuration{
		pipelines: map[string]func(*Scope) error{},
	}
}

// Load registers a pipeline construction function to be invoked later.
// The provided PID must be static to over multiple runs of the binary
//
// Use Load when there are multiple possible pipelines that may be invoked
// from this binary.
//
// This can be useful for a server binary that is also used as a processing pipeline, or
// when it's desirable to run tests for pipelines using a container execution mode,
// instead of loopback mode.
//
// Load panics if the expand function is nil, or if the PID is already in use.
func (cfg *Configuration) Load(pid string, expand func(*Scope) error) {
	if expand == nil {
		panic("nil pipeline expansion received")
	}
	if _, ok := cfg.pipelines[pid]; ok {
		panic("reusing existing PID: " + pid)
	}
	cfg.pipelines[pid] = expand
}

// Ready is called once the framework has been configured, and flags
// have been parsed. Ready serves as an execution split point.
//
// When Ready detects the binary is being executed as a worker, Ready will not
// return, calling [os.Exit] when execution is complete. Otherwise Ready will
// return and allow the remainder of execution to take place.
func (cfg *Configuration) Ready(ctx context.Context) Launcher {
	if !flag.Parsed() {
		cfg.FromCommandLine()
		flag.Parse()
	}

	// Workers don't escape this if block.
	wf := cfg.worker
	if wf.Worker {
		// Need to look up the PipelineOptions file from the environment.
		// Need to read in the file.
		// Need to parse the JSON in the file.

		statusEndpoint := os.Getenv("STATUS_ENDPOINT")
		runnerCapabilities := strings.Split(os.Getenv("RUNNER_CAPABILITIES"), " ")

		// Initialization logging
		//
		// We use direct output to stderr here, because it is expected that logging
		// will be captured by the framework -- which may not be functional if
		// harness.Main returns. We want to be sure any error makes it out.
		var options string
		pipelineOptionsFilename := os.Getenv("PIPELINE_OPTIONS_FILE")
		if pipelineOptionsFilename != "" {
			contents, err := os.ReadFile(pipelineOptionsFilename)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read pipeline options file '%v': %v\n", pipelineOptionsFilename, err)
				os.Exit(1)
			}
			// Overwite flag to be consistent with the legacy flag processing.
			options = string(contents)
		}

		if options != "" {
			var rawOptions map[string]any
			if err := json.Unmarshal([]byte(options), &rawOptions); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read pipeline options file '%v': %v\n", pipelineOptionsFilename, err)
				os.Exit(1)
			}
		}

		// TODO: remove this special case once Pipeline Options are functional.
		var pid string
		if len(cfg.pipelines) == 1 {
			for k := range cfg.pipelines {
				pid = k
			}
		}

		// TODO PrepPipe:
		// 1. Need to get the correct prepared pipeline PID from Pipeline Options
		// 2. Need to get the type registrations from that pipeline.
		// 3. Need to have a single function for that probably.
		expand, ok := cfg.pipelines[pid]
		if !ok || expand == nil {
			log.Fatal(fmt.Errorf("no pipeline with id %q registered", pid))
		}

		var g graph
		s := &Scope{parent: nil, g: &g}
		g.root = s

		if err := expand(s); err != nil {
			log.Fatal(fmt.Errorf("pipeline construction error:%w", err))
		}

		// At this point the graph is complete, and we need to turn serialize/deserialize it
		// into executing code.
		typeReg := map[string]reflect.Type{}
		// We don't care about the marshalled proto here.
		_ = g.marshal(typeReg)

		// TODO move this into harness directly?
		ctx = extworker.WriteWorkerID(ctx, wf.ID)
		err := harness.Main(ctx, wf.ControlEndpoint, harness.Options{
			LoggingEndpoint: wf.LoggingEndpoint,
			// Pull from environment variables.
			StatusEndpoint:     statusEndpoint,
			RunnerCapabilities: runnerCapabilities,
		}, executeSubgraph(typeReg, g.edgeMeta))
		if err == nil {
			os.Exit(0)
		}
		log.Fatal(err)
	}

	return Launcher{
		// Copy things from a config value here to avoid aliasing issues.
		ef: cfg.environment,
		wf: cfg.worker,

		pipelines: maps.Clone(cfg.pipelines),
	}
}

// Launcher is able to Run pipelines [Load]ed from a [Ready] [Configuration].
type Launcher struct {
	pipelines map[string]func(*Scope) error

	ef envFlags
	wf workerFlags
}

// Run begins execution of the pipeline represented by the given PID, and then returns a
// handle to the pipeline.
//
// Run returns an error if no pipeline has been registered with that ID.
func (l Launcher) Run(ctx context.Context, pid string, opts ...Options) (Pipeline, error) {
	if l.pipelines == nil {
		return Pipeline{}, fmt.Errorf("invalid launcher")
	}

	expand, ok := l.pipelines[pid]
	if !ok || expand == nil {
		return Pipeline{}, fmt.Errorf("no pipeline with id %q registered", pid)
	}

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

	ctx, cancelCtxFn := context.WithCancelCause(ctx)

	// If we don't have a remote endpoint, start a local prism process.
	// TODO how to better override default location for development.
	if l.ef.Endpoint == "" {
		_, err := prism.Start(ctx, prism.Options{
			//	Location: "/home/lostluck/git/beam/sdks/go/cmd/prism/prism",
			// TODO pick a port and pass it down.
		})
		if err != nil {
			cancelCtxFn(err)
			return Pipeline{}, err
		}
		l.ef.Endpoint = "localhost:8073"
		// If unset, use loopback mode when using default prism.
		if l.ef.EnvironmentType == "" {
			l.ef.EnvironmentType = "LOOPBACK"
		}
	}
	if l.ef.EnvironmentType == "" {
		l.ef.EnvironmentType = "DOCKER"
	}
	// INVARIANT: ef.EnvironmentType must be set past this point.

	opt := beamopts.Struct{
		Endpoint: l.ef.Endpoint,
	}
	opt.Join(opts...)

	env, err := extractEnv(ctx, &l.ef, typeReg, g.edgeMeta)
	if err != nil {
		cancelCtxFn(err)
		return Pipeline{}, fmt.Errorf("error configuring pipeline environment: %w", err)
	}
	// Only add deps outside of loopback/external mode.
	if env.Urn != "beam:env:external:v1" {
		serializedBinPayload, err := proto.Marshal(&pipepb.ArtifactFilePayload{
			Path: os.Args[0], // Recompile or similar.
		})
		// TODO allow worker_binary overriding
		fmt.Println("using", os.Args[0], "as the binary")
		if err != nil {
			cancelCtxFn(err)
			return Pipeline{}, err
		}
		env.Dependencies = []*pipepb.ArtifactInformation{
			{
				TypeUrn:     "beam:artifact:type:file:v1",
				TypePayload: serializedBinPayload,
				RoleUrn:     "beam:artifact:role:go_worker_binary:v1",
			},
		}
	}
	env.Capabilities = []string{"beam:protocol:monitoring_info_short_ids:v1"} // TODO
	pipe.Components.Environments["go"] = env

	handle, err := universal.Execute(ctx, pipe, opt)
	p := Pipeline{
		handle:   handle,
		cancelFn: cancelCtxFn,
	}
	if err != nil {
		return p, fmt.Errorf("job failed to start: %w", err)
	}
	return p, nil
}

// FromCommandLine registers this Config to be initialized from the command line flags of the binary.
func (cfg *Configuration) FromCommandLine() {
	cfg.Flags(flag.CommandLine)
}

// Flags initializes this Config from the flags of the provided command line set.
func (cfg *Configuration) Flags(fs *flag.FlagSet) {
	// Worker flags
	fs.BoolVar(&cfg.worker.Worker, "worker", false, "Whether binary is running in worker mode.")
	fs.StringVar(&cfg.worker.ID, "id", "", "Local identifier (required in worker mode).")
	fs.StringVar(&cfg.worker.LoggingEndpoint, "logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	fs.StringVar(&cfg.worker.ControlEndpoint, "control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	fs.StringVar(&cfg.worker.SemiPersist, "semi_persist_dir", "", "Deprecated.")

	// Environment flags
	fs.StringVar(&cfg.environment.Endpoint, "endpoint", "", "Endpoint for a JobManagement service.")
	fs.StringVar(&cfg.environment.EnvironmentType, "environment_type", "",
		"Environment Type. Possible options are DOCKER, PROCESS, and LOOPBACK.")
	fs.StringVar(&cfg.environment.EnvironmentConfig, "environment_config",
		"",
		"Set environment configuration for running the user code.\n"+
			"For DOCKER: Url for the docker image.\n"+
			"For PROCESS: json of the form {\"os\": \"<OS>\", "+
			"\"arch\": \"<ARCHITECTURE>\", \"command\": \"<process to execute>\", "+
			"\"env\":{\"<Environment variables>\": \"<ENV_VAL>\"} }. "+
			"All fields in the json are optional except command.")
}

// Launch begins to execute the pipeline built in the construction function, returning a pipeline handle.
//
// Launch is non-blocking. Call wait in the pipeline handle to wait until the job is finished, or use
// LaunchAndWait instead.
func Launch(ctx context.Context, expand func(*Scope) error, opts ...Options) (Pipeline, error) {
	cfg := New()
	pid := "beam:run:default"
	cfg.Load(pid, expand)
	launcher := cfg.Ready(ctx)
	return launcher.Run(ctx, pid, opts...)
}

// LaunchAndWait calls [Launch] and then [Pipeline.Wait], blocking until the pipeline terminates.
func LaunchAndWait(ctx context.Context, expand func(*Scope) error, opts ...Options) (Pipeline, error) {
	pr, err := Launch(ctx, expand, opts...)
	if err != nil {
		return pr, err
	}
	// Wait mutates pr.
	err = pr.Wait(ctx)
	return pr, err
}

// ExtractEnv takes the current environment configuration and pipeline pre-build
// and produces the environment proto for the runner.
func extractEnv(ctx context.Context, ef *envFlags, typeReg map[string]reflect.Type, edgeMeta map[string]any) (*pipepb.Environment, error) {
	var env *pipepb.Environment
	switch strings.ToLower(ef.EnvironmentType) {
	case "process":
		if ef.EnvironmentType == "" {
			return nil, fmt.Errorf("environment_type PROCESS requires environment_config flag to be set")
		}
		raw := &pipepb.ProcessPayload{}
		if err := json.Unmarshal([]byte(ef.EnvironmentConfig), raw); err != nil {
			return nil, fmt.Errorf("unable to json unmarshal --environment_config: %w", err)
		}
		serializedPayload, err := proto.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal ProcessPayload proto: %w", err)
		}
		env = &pipepb.Environment{
			Urn:          "beam:env:process:v1",
			Payload:      serializedPayload,
			Capabilities: nil,
		}
	case "docker":
		if ef.EnvironmentConfig == "" {
			ef.EnvironmentConfig = "apache/beam_go_sdk:latest"
		}
		serializedPayload, err := proto.Marshal(&pipepb.DockerPayload{
			// TODO set this to track some specific version.
			ContainerImage: ef.EnvironmentConfig,
		})
		if err != nil {
			return nil, err
		}
		env = &pipepb.Environment{
			Urn:          "beam:env:docker:v1",
			Payload:      serializedPayload,
			Capabilities: nil,
		}
	case "loopback":
		srv, err := extworker.StartLoopback(ctx, 0, executeSubgraph(typeReg, edgeMeta))
		if err != nil {
			return nil, err
		}
		// Kill the inprocess server once the context is canceled.
		go func() {
			<-ctx.Done()
			srv.Stop(ctx)
		}()
		serializedPayload, err := proto.Marshal(&pipepb.ExternalPayload{
			Endpoint: &pipepb.ApiServiceDescriptor{Url: srv.EnvironmentConfig(ctx)},
		})
		if err != nil {
			return nil, err
		}
		env = &pipepb.Environment{
			Urn:          "beam:env:external:v1",
			Payload:      serializedPayload,
			Capabilities: nil,
		}
	default:
		return nil, fmt.Errorf("invalid environment_type: got %v, want PROCESS, DOCKER, or LOOPBACK", ef.EnvironmentType)
	}
	return env, nil
}

func executeSubgraph(typeReg map[string]reflect.Type, edgeMeta map[string]any) harness.ExecFunc {
	var shortID atomic.Uint32
	return func(ctx context.Context, ctrl *harness.Control, dataCon harness.DataContext) (_ *fnpb.ProcessBundleResponse, err error) {
		defer func() {
			if e := recover(); e != nil && err == nil {
				err = fmt.Errorf("caught dofn panic: %s stacktrace:\n%s", e, debug.Stack())
			}
		}()
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
		newG.edgeMeta = edgeMeta

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
