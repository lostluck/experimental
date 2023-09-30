package beam

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// graph.go holds the structures for the deferred processing graph.

type nodeIndex int
type edgeIndex int

// graph replicates the structure for a beam pipeline graph.
type graph struct {
	seq int //  Validates that this is the same graph.

	nodes []node      // PCollections
	edges []multiEdge // Transforms

	consumers map[nodeIndex][]edgeIndex

	root *Scope
}

type node interface {
	elmType() reflect.Type
	bounded() bool
	windowingStrat()
}

type typedNode[E Element] struct {
	index      nodeIndex
	parentEdge edgeIndex

	elementType E
	isBounded   bool
}

func (n *typedNode[E]) elmType() reflect.Type {
	return reflect.TypeOf(n.elementType)
}

func (n *typedNode[E]) bounded() bool {
	return n.isBounded
}

func (n *typedNode[E]) windowingStrat() {
	// TODO, add windowing strategies.
}

// multiEdges represent transforms.
type multiEdge interface {
	inputs() map[string]nodeIndex
	outputs() map[string]nodeIndex
}

// EdgeImpulse represents an Impulse transform.
type edgeImpulse struct {
	index  edgeIndex
	output nodeIndex
}

// inputs for impulses are nil.
func (e *edgeImpulse) inputs() map[string]nodeIndex {
	return nil
}

// inputs for impulses are one.
func (e *edgeImpulse) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

// EdgeGBK represents a Group By Key transform.
type edgeGBK[E Element] struct {
	index edgeIndex

	input, output nodeIndex
}

// inputs for GBKs are one.
func (e *edgeGBK[E]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"i0": e.input}
}

// inputs for GBKs are one.
func (e *edgeGBK[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

type edgeDoFn[E Element] struct {
	index edgeIndex

	dofn      Transform[E]
	ins, outs map[string]nodeIndex
}

func (e *edgeDoFn[E]) inputs() map[string]nodeIndex {
	return e.ins
}

// inputs for GBKs are one.
func (e *edgeDoFn[E]) outputs() map[string]nodeIndex {
	return e.outs
}

type bundleProcer interface {
	multiEdge
	bundleProc() any
}

func (e *edgeDoFn[E]) bundleProc() any {
	return e.dofn
}

var _ = bundleProcer((*edgeDoFn[int])(nil))

// edgeFlatten represents a Flatten transform.
type edgeFlatten[E Element] struct {
	index edgeIndex

	ins    []nodeIndex
	output nodeIndex

	// exec build time instances.
	instance *flatten[E]
	procs    []processor
}

// inputs for flattens are plural
func (e *edgeFlatten[E]) inputs() map[string]nodeIndex {
	ins := map[string]nodeIndex{}
	for i, input := range e.ins {
		ins[fmt.Sprintf("i%d", i)] = input
	}
	return ins
}

// outputs for Flattens are one.
func (e *edgeFlatten[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeFlatten[E]) flatten() (any, []processor, bool) {
	var first bool
	if e.instance == nil {
		first = true
		e.instance = &flatten[E]{
			Output: Emitter[E]{globalIndex: e.output},
		}
		e.procs = []processor{e.instance.Output.newDFC(e.output)}
	}
	return e.instance, e.procs, first
}

type flattener interface {
	multiEdge
	// Returns the flatten instance, the downstream processors, and if this was the first call to dedup setting downstream consumers
	flatten() (any, []processor, bool)
}

var _ = flattener((*edgeFlatten[int])(nil))

func (g *graph) curNodeIndex() nodeIndex {
	return nodeIndex(len(g.nodes))
}
func (g *graph) curEdgeIndex() edgeIndex {
	return edgeIndex(len(g.edges))
}

func (g *graph) deferDoFn(dofn any, input nodeIndex, global edgeIndex) (ins, outs map[string]nodeIndex) {
	if g.consumers == nil {
		g.consumers = map[nodeIndex][]edgeIndex{}
	}
	g.consumers[input] = append(g.consumers[input], global)

	rv := reflect.ValueOf(dofn)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	ins = map[string]nodeIndex{
		"parallel": input,
		// TODO, side inputs.
	}
	outs = map[string]nodeIndex{}
	efaceRT := reflect.TypeOf((*emitIface)(nil)).Elem()
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		fv := rv.Field(i)
		sf := rt.Field(i)
		if !fv.CanAddr() || !sf.IsExported() {
			continue
		}
		switch sf.Type.Kind() {
		case reflect.Array, reflect.Slice:
			// Should we also allow for maps? Holy shit, we could also allow for maps....
			ptrEt := reflect.PointerTo(sf.Type.Elem())
			if !ptrEt.Implements(efaceRT) {
				continue
			}
			// Slice or Array
			for j := 0; j < fv.Len(); j++ {
				fvj := fv.Index(j).Addr()
				g.initEmitter(fvj.Interface().(emitIface), global, input, fmt.Sprintf("%s%%%d", sf.Name, j), outs)
			}
		case reflect.Struct:
			fv = fv.Addr()
			if emt, ok := fv.Interface().(emitIface); ok {
				g.initEmitter(emt, global, input, sf.Name, outs)
			}
			// TODO side inputs
		case reflect.Chan:
			panic("field %v is a channel")
		default:
			// Don't do anything with pointers, or other types.

		}
	}
	return ins, outs
}

func (g *graph) initEmitter(emt emitIface, global edgeIndex, input nodeIndex, name string, outs map[string]nodeIndex) {
	localIndex := len(outs)
	globalIndex := g.curNodeIndex()
	emt.setPColKey(globalIndex, localIndex)
	node := emt.newNode(globalIndex, global, g.nodes[input].bounded())
	g.nodes = append(g.nodes, node)
	outs[name] = globalIndex
}

// build returns the root processors
func (g *graph) build() []processor {
	type consumer struct {
		input processor
		edge  multiEdge
	}
	var stack []consumer
	for _, edge := range g.edges {
		switch edge.(type) {
		case *edgeImpulse:
			stack = append(stack, consumer{input: nil, edge: edge})
		default:
			// skip non-roots
		}
	}

	var roots []processor

	var c consumer
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("\ncur consumer %#v\nstack %#v\nroots %v\ngraph %#v\noriginal panic: %v", c, stack, roots, g, e))
		}
	}()
	for {
		if len(stack) == 0 {
			break
		}
		c = stack[len(stack)-1]
		stack = stack[0 : len(stack)-1]

		switch e := c.edge.(type) {
		case *edgeImpulse:
			imp := newDFC[[]byte](e.output, nil)
			roots = append(roots, imp)

			// TODO add the multiplexing logic.
			for _, v := range g.consumers[e.output] {
				stack = append(stack, consumer{input: imp, edge: g.edges[v]})
			}
		case bundleProcer: // Can't type assert generic types.
			dofn := e.bundleProc()

			rv := reflect.ValueOf(dofn)
			if rv.Kind() == reflect.Pointer {
				rv = rv.Elem()
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
				emt.setPColKey(nodeID, len(procs)) // set the output index
				proc := emt.newDFC(nodeID)
				procs = append(procs, proc)

				// Need to add to consumer stack
				for _, v := range g.consumers[nodeID] {
					stack = append(stack, consumer{input: proc, edge: g.edges[v]})
				}
			}
			// Needs to be set on the incoming DFC.
			c.input.update(dofn, procs)
		case flattener: // Can't type assert generic types.
			// The same flatten edge will be re-invoked multiple times, once for each input node.
			// But those nodes just need to point to the same dofn instance, and outputs
			dofn, procs, first := e.flatten()
			c.input.update(dofn, procs)
			if first {
				// There's only one, so the loop is the best way out.
				for _, nodeID := range e.outputs() {
					for _, v := range g.consumers[nodeID] {
						stack = append(stack, consumer{input: procs[0], edge: g.edges[v]})
					}
					break
				}
			}
		default:
			panic(fmt.Sprintf("unknown edge type %#v", e))
		}
	}
	return roots
}
