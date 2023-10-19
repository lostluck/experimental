package beam

import (
	"reflect"

	"github.com/go-json-experiment/json"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

type edgeDoFn[E Element] struct {
	index     edgeIndex
	transform string

	dofn       Transform[E]
	ins, outs  map[string]nodeIndex
	parallelIn nodeIndex
	proc       processor

	opts beamopts.Struct
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

	payload, _ := proto.Marshal(&pipepb.ParDoPayload{
		DoFn: &pipepb.FunctionSpec{
			Urn:     "beam:go:transform:dofn:v2",
			Payload: wrappedPayload,
		},
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
}

func (e *edgeDoFn[E]) actualTransform() any {
	return e.dofn
}

func (e *edgeDoFn[E]) options() beamopts.Struct {
	return e.opts
}

var _ bundleProcer = (*edgeDoFn[int])(nil)
