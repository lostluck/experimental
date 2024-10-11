package beam

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
)

// graph.go holds the structures for the deferred processing graph.

type nodeIndex int
type edgeIndex int

func (i nodeIndex) String() string {
	return fmt.Sprintf("n%d", i)
}

func (i edgeIndex) String() string {
	return fmt.Sprintf("e%d", i)
}

// graph replicates the structure for a beam pipeline graph.
//
// graph is used for both pipeline construction for marshalling to the
// Beam Pipeline proto, and for building the bundle processor
// for ProcessBundleDescriptors
//
// During pipeline construction, a graph is built up as the user connects
// together DoFns with ParDos and other transforms. It's then serialized
// into a beam Pipeline protocol buffer message.
//
// Workers receive ProcessBundleDescriptors that contain a subgraph, which
// are unmarshaled into a new graph structure. This graph
// structure is then used to build DFCs and their resident transforms for
// processing data in a bundle.
type graph struct {
	nodes    []node         // PCollections
	edges    []multiEdge    // Transforms
	edgeMeta map[string]any // Bonus information in the graph for certain transforms.

	consumers map[nodeIndex][]edgeIndex

	root *Scope

	stateUrl string
}

type node interface {
	protoID() string
	bounded() bool
	windowingStrat()
	addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string
	newTypeMultiEdge(*edgePlaceholder, map[string]*pipepb.Coder) multiEdge
	initCoder(cid string, cs map[string]*pipepb.Coder)
	getCoder() any
}

var _ node = &typedNode[int]{}

type typedNode[E Element] struct {
	index      nodeIndex
	parentEdge edgeIndex // for debugging

	id        string
	isBounded bool
	coder     coders.Coder[E]
}

func (n *typedNode[E]) initCoder(cid string, cs map[string]*pipepb.Coder) {
	n.coder = coderFromProto[E](cs, cid)
}

func (n *typedNode[E]) getCoder() any {
	return n.coder
}

func (n *typedNode[E]) protoID() string {
	return n.id
}

func (n *typedNode[E]) bounded() bool {
	return n.isBounded
}

func (n *typedNode[E]) windowingStrat() {
	// TODO, add windowing strategies.
}

// multiEdges represent transforms.
type multiEdge interface {
	edgeID() edgeIndex
	protoID() string
	inputs() map[string]nodeIndex
	outputs() map[string]nodeIndex
}

type protoDescMultiEdge interface {
	multiEdge
	toProtoParts(translateParams) (spec *pipepb.FunctionSpec, envID, name string)
}

func (g *graph) curNodeIndex() nodeIndex {
	return nodeIndex(len(g.nodes))
}
func (g *graph) curEdgeIndex() edgeIndex {
	return edgeIndex(len(g.edges))
}

// build returns the root processors for SDK worker side execution.
func (g *graph) build(ctx context.Context, dataCon harness.DataContext) ([]processor, *metricsStore) {
	type consumer struct {
		input processor
		edge  multiEdge
	}
	var stack []consumer
	for _, edge := range g.edges {
		switch edge.(type) {
		case *edgeImpulse, sourcer:
			stack = append(stack, consumer{input: nil, edge: edge})
		default:
			// skip non-roots
		}
	}

	var roots []processor
	mets := newMetricsStore(len(g.edges))
	// We have a dummy metric to allow uninitialized metrics to know
	// they need to register themselves.
	mets.initMetric("none", "dummy", nil)

	var c consumer
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("\ncur consumer.input %#v\ncur consumer.edge %#v\nstack %#v\nroots %#v\ngraph %#v\noriginal panic: %v", c.input, c.edge, stack, roots, g, e))
		}
	}()
	addConsumers := func(proc processor, nodeID nodeIndex) {
		consumers := g.consumers[nodeID]
		switch n := len(consumers); n {
		case 0:
			proc.discard()
		case 1:
			// Easiest way to get a single value out of a map is to iterate.
			for _, v := range consumers {
				stack = append(stack, consumer{input: proc, edge: g.edges[v]})
			}
		default:
			procs := proc.multiplex(n)
			for i, v := range consumers {
				stack = append(stack, consumer{input: procs[i], edge: g.edges[v]})
			}
		}
	}
	for {
		if len(stack) == 0 {
			break
		}
		c = stack[len(stack)-1]
		stack = stack[0 : len(stack)-1]
		switch e := c.edge.(type) {
		case *edgeImpulse:
			imp := &DFC[[]byte]{id: e.output}
			roots = append(roots, imp)
			addConsumers(imp, e.output)
		case sourcer:
			root, toConsumer := e.source(dataCon, mets)
			roots = append(roots, root)
			addConsumers(toConsumer, getSingleValue(e.outputs()))
		case sinker:
			sink := e.sinkDoFn(dataCon)
			c.input.update(c.edge.edgeID(), "sink", sink, nil, mets, dataCon.LoggerForTransform(c.edge.protoID()))
		case bundleProcer: // Can't type assert generic types.
			dofn := e.actualTransform()
			uniqueName := e.options().Name

			// We split out the "raw" dofn from the
			// one the user wrote that we need to initialize
			// with the execution harness.
			userDoFn := dofn
			// If this is specifically an SDF, extract the user transform for initialization.
			if sdf, ok := dofn.(procSizedElmAndRestIface); ok {
				userDoFn = sdf.getUserTransform()
			}

			// Handle convenience wrappers for closures.
			if lwi, ok := userDoFn.(lightweightIniter); ok {
				lwi.lightweightInit(g.edgeMeta)
			}
			rv := reflect.ValueOf(userDoFn)
			if rv.Kind() == reflect.Pointer {
				rv = rv.Elem()
			}

			// Initialize side inputs.

			// Look at the inputs, and check if this is a side input field.
			for name := range e.inputs() {
				fv := rv.FieldByName(name)
				if !fv.IsValid() {
					continue
				}
				si := fv.Addr().Interface().(sideIface)

				si.initialize(ctx, dataCon, g.stateUrl, name, e.transformID())
			}

			var procs []processor // Needs to be set on the incoming DFC.
			for name, nodeID := range e.outputs() {
				splitName := strings.Split(name, "%")
				var fv reflect.Value
				switch len(splitName) {
				case 1:
					fv = rv.FieldByName(name)
				case 2:
					fv = rv.FieldByName(splitName[0])
					fid, err := strconv.Atoi(splitName[1])
					if err != nil {
						panic(err)
					}
					fv = fv.Index(fid)
				default:
					panic("unexpected name value")
				}
				emt := fv.Addr().Interface().(emitIface)
				pcolMets := emt.setPColKey(nodeID, len(procs), g.nodes[nodeID].getCoder()) // set the output index
				mets.metrics = append(mets.metrics, pcolMets)

				proc := emt.newDFC(nodeID)
				procs = append(procs, proc)

				addConsumers(proc, nodeID)
			}

			rt := rv.Type()
			for i := 0; i < rv.NumField(); i++ {
				// Only deal with exported fields.
				if !rt.Field(i).IsExported() {
					continue
				}
				fv := rv.Field(i)
				if mn, ok := fv.Addr().Interface().(metricNamer); ok {
					mn.setName("user", rt.Field(i).Name)
				}
			}
			// If this is the parallel input, the dofn needs to be set on the incoming DFC.
			c.input.update(c.edge.edgeID(), uniqueName, dofn, procs, mets, dataCon.LoggerForTransform(c.edge.protoID()))
		case flattener: // Can't type assert generic types.
			// The same flatten edge will be re-invoked multiple times, once for each input node.
			// But those nodes just need to point to the same dofn instance, and outputs
			transform, dofn, procs, first := e.flatten()
			c.input.update(e.edgeID(), transform, dofn, procs, mets, dataCon.LoggerForTransform(c.edge.protoID()))
			if first {
				// There's only one, so the loop is the best way out.
				for _, nodeID := range e.outputs() {
					addConsumers(procs[0], nodeID)
					break
				}
			}
			//	case *edgePlaceholder:
			// Ignoring for a sec.
		default:
			panic(fmt.Sprintf("unknown edge type %#v", e))
		}
	}
	return roots, mets
}
