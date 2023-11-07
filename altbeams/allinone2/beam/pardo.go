package beam

import (
	"fmt"
	"reflect"

	"github.com/go-json-experiment/json"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

// ParDo takes the users's DoFn and returns the same type for downstream piepline construction.
//
// The returned DoFn's emitter fields can then be used as inputs into other DoFns.
// What if we used Emitters as PCollections directly?
// Obviously, we'd rename the type PCollection or similar
// If only to also
func ParDo[E Element, DF Transform[E]](s *Scope, input Output[E], dofn DF, opts ...Options) DF {
	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	ins, outs, sides := s.g.deferDoFn(dofn, input.globalIndex, edgeID)

	s.g.edges = append(s.g.edges, &edgeDoFn[E]{index: edgeID, dofn: dofn, ins: ins, outs: outs, sides: sides, parallelIn: input.globalIndex, opts: opt})

	return dofn
}

func (g *graph) deferDoFn(dofn any, input nodeIndex, global edgeIndex) (ins, outs map[string]nodeIndex, sides map[string]string) {
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
	}
	sides = map[string]string{}
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
			if si, ok := fv.Interface().(sideIface); ok {
				sides[sf.Name] = si.accessPatternUrn()
				// fmt.Println("initialising side intput: ", si, global, sf.Name, ins)
				g.initSideInput(si, global, sf.Name, ins)
			}
			// TODO side inputs
		case reflect.Chan:
			panic("field %v is a channel")
		default:
			// Don't do anything with pointers, or other types.

		}
	}
	return ins, outs, sides
}

func (g *graph) initEmitter(emt emitIface, global edgeIndex, input nodeIndex, name string, outs map[string]nodeIndex) {
	localIndex := len(outs)
	globalIndex := g.curNodeIndex()
	emt.setPColKey(globalIndex, localIndex, nil)
	node := emt.newNode(globalIndex.String(), globalIndex, global, g.nodes[input].bounded())
	g.nodes = append(g.nodes, node)
	outs[name] = globalIndex
}

func (g *graph) initSideInput(si sideIface, global edgeIndex, name string, ins map[string]nodeIndex) {
	globalIndex := si.sideInput()
	// Put into a special side input consumers list?
	g.consumers[globalIndex] = append(g.consumers[globalIndex], global)
	ins[name] = globalIndex
}

type edgeDoFn[E Element] struct {
	index     edgeIndex
	transform string

	dofn       Transform[E]
	ins, outs  map[string]nodeIndex // local field names to global collection ids.
	sides      map[string]string    // local id to access pattern URN
	parallelIn nodeIndex

	opts beamopts.Struct
}

func (e *edgeDoFn[E]) protoID() string {
	return e.transform
}

func (e *edgeDoFn[E]) edgeID() edgeIndex {
	return e.index
}

func (e *edgeDoFn[E]) inputs() map[string]nodeIndex {
	return e.ins
}

func (e *edgeDoFn[E]) outputs() map[string]nodeIndex {
	return e.outs
}

func (e *edgeDoFn[E]) toProtoParts(params translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	dofn := e.actualTransform()
	rv := reflect.ValueOf(dofn)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	// Register types with the lookup table.
	typeName := rv.Type().Name()
	params.TypeReg[typeName] = rv.Type()

	opts := e.options()
	if opts.Name == "" {
		name = typeName
	} else {
		name = opts.Name
	}

	wrap := dofnWrap{
		TypeName: typeName,
		DoFn:     dofn,
	}
	wrappedPayload, err := json.Marshal(&wrap, json.DefaultOptionsV2(), jsonDoFnMarshallers())
	if err != nil {
		panic(err)
	}

	var sis map[string]*pipepb.SideInput
	if len(e.sides) > 0 {
		sis = map[string]*pipepb.SideInput{}
		for local, pattern := range e.sides {
			sis[local] = &pipepb.SideInput{
				AccessPattern: &pipepb.FunctionSpec{
					Urn: pattern,
				},
				ViewFn: &pipepb.FunctionSpec{
					Urn: "dummyViewFn",
				},
				WindowMappingFn: &pipepb.FunctionSpec{
					Urn: "dummyWindowMappingFn",
				},
			}
		}
	}

	payload, _ := proto.Marshal(&pipepb.ParDoPayload{
		DoFn: &pipepb.FunctionSpec{
			Urn:     "beam:go:transform:dofn:v2",
			Payload: wrappedPayload,
		},
		SideInputs: sis,
	})

	spec = &pipepb.FunctionSpec{
		Urn:     "beam:transform:pardo:v1",
		Payload: payload,
	}
	return spec, params.DefaultEnvID, name
}

type bundleProcer interface {
	protoDescMultiEdge

	// Make this a reflect.Type and avoid instance aliasing.
	// Then we can keep the graph around, for cheaper startup vs reparsing proto
	actualTransform() any
	options() beamopts.Struct
	transformID() string
}

func (e *edgeDoFn[E]) actualTransform() any {
	return e.dofn
}

func (e *edgeDoFn[E]) options() beamopts.Struct {
	return e.opts
}

func (e *edgeDoFn[E]) transformID() string {
	return e.transform
}

var _ bundleProcer = (*edgeDoFn[int])(nil)
