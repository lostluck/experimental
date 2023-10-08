package beam

import (
	"fmt"
	"reflect"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/pipelinex"
	"google.golang.org/protobuf/proto"
)

type dofnWrap struct {
	TypeName string
	DoFn     any
}

func jsonDoFnMarshallers() json.Options {
	return json.WithMarshalers(
		json.NewMarshalers(
			// Turn all beam mixins into {} by default, as state should be reconstrutable from
			// the types anyway.
			json.MarshalFuncV2(func(enc *jsontext.Encoder, byp bypassInterface, opts json.Options) error {
				enc.WriteToken(jsontext.ObjectStart)
				enc.WriteToken(jsontext.ObjectEnd)
				return nil
			}),
			// No special handling for marshalling the DoFn otherwise.
		))
}

func jsonDoFnUnmarshallers(typeReg map[string]reflect.Type, name string) json.Options {
	return json.WithUnmarshalers(
		json.NewUnmarshalers(
			// Handle mixins by skipping the values.
			json.UnmarshalFuncV2(func(dec *jsontext.Decoder, val bypassInterface, opts json.Options) error {
				return dec.SkipValue()
			}),
			json.UnmarshalFuncV2(func(dec *jsontext.Decoder, val *dofnWrap, opts json.Options) error {
				for {
					tok, err := dec.ReadToken()
					if err != nil {
						return err
					}
					switch tok.Kind() {
					case '"':
						switch tok.String() {
						case "TypeName":
							tok2, err := dec.ReadToken()
							if err != nil {
								return err
							}
							val.TypeName = tok2.String()
							continue
						case "DoFn":
							dofnRT, ok := typeReg[val.TypeName]
							if !ok {
								panic(fmt.Sprintf("unknown pardo in transform %v: payload %q", name, val.TypeName))
							}
							val.DoFn = reflect.New(dofnRT).Interface()
							if err := json.UnmarshalDecode(dec, val.DoFn, opts); err != nil {
								return err
							}
							_, err = dec.ReadToken() // '}' (finish reading the value)
							return err
						}
					}
				}
			})),
	)
}

// marshal turns a pipeline graph into a normalized Beam pipeline proto.
func (g *graph) marshal(typeReg map[string]reflect.Type) *pipepb.Pipeline {
	var roots []string

	defaultEnvID := "go"

	comps := &pipepb.Components{
		Transforms:   map[string]*pipepb.PTransform{},
		Pcollections: map[string]*pipepb.PCollection{},
		WindowingStrategies: map[string]*pipepb.WindowingStrategy{
			"global": {
				WindowFn: &pipepb.FunctionSpec{
					Urn: "beam:window_fn:global_windows:v1",
				},
				MergeStatus:   pipepb.MergeStatus_NON_MERGING,
				WindowCoderId: "gwc",
				Trigger: &pipepb.Trigger{
					Trigger: &pipepb.Trigger_Default_{Default: &pipepb.Trigger_Default{}},
				},
				AccumulationMode: pipepb.AccumulationMode_DISCARDING,
				OutputTime:       pipepb.OutputTime_END_OF_WINDOW,
				ClosingBehavior:  pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
				AllowedLateness:  0,
				OnTimeBehavior:   pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY,
			},
		},
		Coders: map[string]*pipepb.Coder{
			"gwc": {
				Spec: &pipepb.FunctionSpec{Urn: "beam:coder:global_window:v1"},
			},
		},
		Environments: map[string]*pipepb.Environment{
			defaultEnvID: {
				Urn:           "go",
				Payload:       nil,
				DisplayData:   nil,
				Capabilities:  nil,
				Dependencies:  nil,
				ResourceHints: nil,
			},
		},
	}

	for i, edge := range g.edges {
		inputs := make(map[string]string)
		for name, in := range edge.inputs() {
			inputs[name] = in.String()
		}
		outputs := make(map[string]string)
		for name, out := range edge.outputs() {
			outputs[name] = out.String()
		}

		var spec *pipepb.FunctionSpec
		var uniqueName string
		envID := defaultEnvID
		switch e := edge.(type) {
		case *edgeImpulse:
			spec = &pipepb.FunctionSpec{Urn: "beam:transform:impulse:v1"}
			envID = "" // Runner transforms are left blank.
			uniqueName = "Impulse"
		case flattener:
			spec = &pipepb.FunctionSpec{Urn: "beam:transform:flatten:v1"}
			envID = "" // Runner transforms are left blank.
			uniqueName = "Flatten"
		case keygrouper:
			spec = &pipepb.FunctionSpec{Urn: "beam:transform:group_by_key:v1"}
			envID = "" // Runner transforms are left blank.
			uniqueName = "GroupByKey"
		case bundleProcer:
			dofn := e.bundleProc()
			rv := reflect.ValueOf(dofn)
			if rv.Kind() == reflect.Pointer {
				rv = rv.Elem()
			}
			// Register types with the lookup table.
			typeName := rv.Type().Name()
			typeReg[typeName] = rv.Type()

			opts := e.options()
			if opts.Name == "" {
				uniqueName = typeName
			} else {
				uniqueName = opts.Name
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
		default:
			panic(fmt.Sprintf("unknown edge type %#v", e))
		}

		comps.Transforms[edgeIndex(i).String()] = &pipepb.PTransform{
			UniqueName:    uniqueName,
			Spec:          spec,
			Inputs:        inputs,
			Outputs:       outputs,
			EnvironmentId: envID,
			Annotations:   nil,
		}
	}

	bounded := func(n node) pipepb.IsBounded_Enum {
		if n.bounded() {
			return pipepb.IsBounded_BOUNDED
		}
		return pipepb.IsBounded_UNBOUNDED
	}

	intern := map[string]string{}
	for i, node := range g.nodes {
		comps.Pcollections[nodeIndex(i).String()] = &pipepb.PCollection{
			UniqueName:          nodeIndex(i).String(), //  TODO make this "Parent.Output"
			CoderId:             node.addCoder(intern, comps.GetCoders()),
			IsBounded:           bounded(node),
			WindowingStrategyId: "global",
			DisplayData:         nil,
		}
	}

	pipe, err := pipelinex.Normalize(&pipepb.Pipeline{
		Components:       comps,
		RootTransformIds: roots,
		DisplayData:      nil,
		Requirements:     nil,
	})
	if err != nil {
		panic(err)
	}
	return pipe
}

func (n *typedNode[E]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	return addCoder[E](intern, coders)
}

// structuralCoder is a helper interface to handle structural types.
// Implementers must populate the coders map.
type structuralCoder interface {
	addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string
}

func addCoder[E any](intern map[string]string, coders map[string]*pipepb.Coder) string {
	var t E
	at := any(t)
	rt := reflect.TypeOf(t)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if id, ok := intern[rt.PkgPath()+"."+rt.Name()]; ok {
		return id
	}

	var urn string
	switch at := at.(type) {
	case []byte:
		urn = "beam:coder:bytes:v1"
	case bool:
		urn = "beam:coder:bytes:v1"
	case int, int16, int32, int64:
		urn = "beam:coder:varint:v1"
	case float64, float32:
		urn = "beam:coder:double:v1"
	case string:
		urn = "beam:coder:string_utf8:v1"
	case structuralCoder:
		return at.addCoder(intern, coders)
	default:
		panic(fmt.Sprintf("unknown coder type: generic %T, resolved %v", t, rt))
	}
	id := fmt.Sprintf("c%d", len(coders))
	coders[id] = &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     urn,
			Payload: nil,
		},
		ComponentCoderIds: nil,
	}

	return id

	// TODO
	// urnLengthPrefixCoder        = "beam:coder:length_prefix:v1"
	// urnKVCoder                  = "beam:coder:kv:v1"
	// urnIterableCoder            = "beam:coder:iterable:v1"
	// urnStateBackedIterableCoder = "beam:coder:state_backed_iterable:v1"
	// urnWindowedValueCoder       = "beam:coder:windowed_value:v1"
	// urnParamWindowedValueCoder  = "beam:coder:param_windowed_value:v1"
	// urnTimerCoder               = "beam:coder:timer:v1"
	// urnRowCoder                 = "beam:coder:row:v1"
	// urnNullableCoder            = "beam:coder:nullable:v1"
}

func (KV[K, V]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	kID, vID := addCoder[K](intern, coders), addCoder[V](intern, coders)
	id := fmt.Sprintf("c%d", len(coders))
	coders[id] = &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     "beam:coder:kv:v1",
			Payload: nil,
		},
		ComponentCoderIds: []string{kID, vID},
	}
	return id
}

func (Iter[V]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	vID := addCoder[V](intern, coders)
	id := fmt.Sprintf("c%d", len(coders))
	coders[id] = &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     "beam:coder:iterable:v1",
			Payload: nil,
		},
		ComponentCoderIds: []string{vID},
	}
	return id
}

// Figure out the necessary unmarshalling for coders.
type subGraphProto interface {
	GetCoders() map[string]*pipepb.Coder
	GetEnvironments() map[string]*pipepb.Environment
	GetPcollections() map[string]*pipepb.PCollection
	GetTransforms() map[string]*pipepb.PTransform
	GetWindowingStrategies() map[string]*pipepb.WindowingStrategy
}

func unmarshalToGraph(typeReg map[string]reflect.Type, pbd subGraphProto) *graph {
	var g graph
	g.consumers = map[nodeIndex][]edgeIndex{}

	pcolToIndex := map[string]nodeIndex{}
	for name := range pbd.GetPcollections() {
		// Get placeholder nodes in the graph, and avoid reconstructing nodes multiple times.
		// We can't create the final typedEdge here because we don't have the real element type,
		// just the coder. The coder doesn't fully specify type information.
		id := nodeIndex(len(g.nodes))
		g.nodes = append(g.nodes, nil)
		pcolToIndex[name] = id
	}

	for name, pt := range pbd.GetTransforms() {
		if len(pt.GetSubtransforms()) > 0 { // I don't think we need to worry about these though...
			panic(fmt.Sprintf("can't handle composites yet:, contained by %v", name))
		}
		spec := pt.GetSpec()

		switch spec.GetUrn() {
		case "beam:transform:impulse:v1":
			for _, global := range pt.GetOutputs() {
				id := edgeIndex(len(g.edges))
				g.edges = append(g.edges, &edgeImpulse{
					index:  id,
					output: pcolToIndex[global],
				})
			}
		case "beam:transform:flatten:v1":
			// NOTE This is similar to how DataSource needs to be added.
			edgeID := edgeIndex(len(g.edges))
			ins := map[string]nodeIndex{}
			for local, global := range pt.GetInputs() {
				id := pcolToIndex[global]
				ins[local] = id
				g.consumers[id] = append(g.consumers[id], edgeID)
			}
			outs := map[string]nodeIndex{}
			for local, global := range pt.GetOutputs() {
				id := pcolToIndex[global]
				outs[local] = id
			}
			// Add a dummy edge.
			g.edges = append(g.edges, &edgePlaceholder{
				kind: "flatten",
				id:   edgeID, ins: ins, outs: outs,
			})
		case "beam:transform:group_by_key:v1":
		case "beam:transform:pardo:v1":
			var wrap dofnWrap
			proc := decodeDoFn(spec.GetPayload(), &wrap, typeReg, name)

			if len(pt.Inputs) > 1 {
				panic(fmt.Sprintf("unimplemented: transform %v has side inputs: %v", name, pt.Inputs))
			}

			edgeID := edgeIndex(len(g.edges))
			ins := map[string]nodeIndex{}
			for local, global := range pt.GetInputs() {
				id := pcolToIndex[global]
				ins[local] = id

				g.consumers[id] = append(g.consumers[id], edgeID)
				if g.nodes[id] == nil {
					g.nodes[id] = proc.produceTypedNode(id, pbd.GetPcollections()[global].GetIsBounded() == pipepb.IsBounded_BOUNDED)
				} else {
					//	fmt.Printf("node %v already created\n", global)
				}
			}

			outs := map[string]nodeIndex{}
			for local, global := range pt.GetOutputs() {
				id := pcolToIndex[global]
				outs[local] = id
			}
			opt := beamopts.Struct{
				Name: pt.UniqueName,
			}

			g.edges = append(g.edges, proc.produceTypedEdge(edgeID, wrap.DoFn, ins, outs, opt))

			// But what we want now is to get a *DFC[E], and use that to produce a
			// typedNode if it's needed.
			// This should lead to a complete set of interior PCollections, and allow Sources to be produced.
			// The trick is any Sink/or Discarded pcollections.
			// Ideally won't re-introspect the DoFn struct, but we could probably do so in a pinch.

			// TODO: We can recover the right types! But now we need the graph again.
			// We need a method on DoFn that can be used to build up a edgeDoFn[*]
			// That needs to be given a canonical ID and a mapping in the graph.
			// Same thing with all the nodes (which are technically done first).
			// Then our existing build logic should simply work.
		}

	}
	return &g
}

func decodeDoFn(payload []byte, wrap *dofnWrap, typeReg map[string]reflect.Type, name string) processor {
	var dofnPayload pipepb.ParDoPayload
	if err := proto.Unmarshal(payload, &dofnPayload); err != nil {
		panic(err)
	}
	dofnSpec := dofnPayload.GetDoFn()

	if dofnSpec.GetUrn() != "beam:go:transform:dofn:v2" {
		panic(fmt.Sprintf("unknown pardo urn in transform %q: urn %q\n", name, dofnSpec.GetUrn()))
	}

	if err := json.Unmarshal(dofnSpec.GetPayload(), &wrap, json.DefaultOptionsV2(), jsonDoFnUnmarshallers(typeReg, name)); err != nil {
		panic(err)
	}
	dofnPtrRT := reflect.TypeOf(wrap.DoFn)
	pbm, ok := dofnPtrRT.MethodByName("ProcessBundle")
	if !ok {
		panic(fmt.Sprintf("type in transform %v doesn't have a ProcessBundle method: %v", name, dofnPtrRT))
	}
	dfcRT := pbm.Type.In(2).Elem()
	return reflect.New(dfcRT).Interface().(processor)
}