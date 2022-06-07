// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"bytes"
	"io"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	impOnce  sync.Once
	impBytes []byte
)

func impulseBytes() []byte {
	impOnce.Do(func() {
		var buf bytes.Buffer
		byt, _ := exec.EncodeElement(exec.MakeElementEncoder(coder.NewBytes()), []byte("lostluck"))

		exec.EncodeWindowedValueHeader(
			exec.MakeWindowEncoder(coder.NewGlobalWindow()),
			window.SingleGlobalWindow,
			mtime.Now(),
			typex.NoFiringPane(),
			&buf,
		)
		buf.Write(byt)
		impBytes = buf.Bytes()
	})
	return impBytes
}

func ptUrn(v pipepb.StandardPTransforms_Primitives) string {
	return proto.GetExtension(prims.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

func siUrn(v pipepb.StandardSideInputTypes_Enum) string {
	return proto.GetExtension(sids.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

var (
	prims = (pipepb.StandardPTransforms_Primitives)(0).Descriptor().Values()
	sids  = (pipepb.StandardSideInputTypes_Enum)(0).Descriptor().Values()

	// SDK transforms.
	urnTransformParDo = ptUrn(pipepb.StandardPTransforms_PAR_DO)
	// DoFn Urns
	urnGoDoFn = "beam:go:transform:dofn:v1" // Only used for Go DoFn.

	// Runner transforms.
	urnTransformImpulse = ptUrn(pipepb.StandardPTransforms_IMPULSE)
	urnTransformGBK     = ptUrn(pipepb.StandardPTransforms_GROUP_BY_KEY)
	urnTransformFlatten = ptUrn(pipepb.StandardPTransforms_FLATTEN)

	// Side Input access patterns
	urnSideInputIterable = siUrn(pipepb.StandardSideInputTypes_ITERABLE)
	urnSideInputMultiMap = siUrn(pipepb.StandardSideInputTypes_MULTIMAP)
)

func executePipeline(wk *worker, j *job) {
	pipeline := j.pipeline
	ts := pipeline.GetComponents().GetTransforms()

	// Basically need to figure out the "plan" at this point.
	// Long term: Take RootTransforms & TOPO sort them. (technically not necessary, but paranoid)
	//  - Recurse through composites from there: TOPO sort subtransforms
	//  - Eventually we have our ordered list of transforms.
	//  - Then we can figure out how to fuse transforms into bundles
	//    (producer-consumer & sibling fusion & environment awareness)
	//
	// But for now we figure out how to connect bundles together
	// First: default "impulse" bundle. "executed" by the runner.
	// Second: Derive bundle for a DoFn for the environment.

	// TODO, configure the preprocessor from pipeline options.
	// Maybe change these returns to a single struct for convenience and further
	// annotation?
	topo, succ, inputs, pcolParents := (&preprocessor{}).preProcessGraph(ts)

	// We're going to do something fun with channels for this,
	// and do a "Generator" pattern to get the bundles we need
	// to process.
	// Ultimately, we'd want some pre-processing of the pipeline
	// which will determine dependency relationshion ships, and
	// when it's safe to GC bundle results.
	// The key idea is that we have a goroutine that will block on
	// channels, and emit the next bundle to process.
	toProcess, processed := make(chan *bundle), make(chan *bundle)

	// Goroutine for executing bundles on the worker.
	go func() {
		// Send nil to start, Impulses won't require parental translation.
		processed <- nil
		for b := range toProcess {
			b.ProcessOn(wk) // Blocks until finished.
			// Metrics?
			j.metrics.contributeMetrics(<-b.Resp)
			// Send back for dependency handling afterwards.
			processed <- b
		}
		close(processed)
	}()

	// prevs is a map from current transform ID to a map from local ids to bundles instead.
	prevs := map[string]map[string]*bundle{}

	for _, tid := range topo {
		// Block until the previous bundle is done.
		<-processed
		parents := prevs[tid]
		delete(prevs, tid) // Garbage collect the data.
		V(2).Logf("making bundle for %v", tid)

		t := ts[tid]
		// Much hack.
		if t.GetSpec().GetUrn() == urnTransformFlatten {
			t.EnvironmentId = ""
		}
		switch t.GetEnvironmentId() {
		case "": // Runner Transforms
			// Runner transforms are processed immeadiately.
			b := runnerTransform(t, tid, processed, parents, pipeline)
			for _, ch := range succ[tid] {
				m, ok := prevs[ch.global]
				if !ok {
					m = make(map[string]*bundle)
				}
				m[ch.local] = b
				prevs[ch.global] = m
			}
			continue
		case wk.ID:
			// Great! this is for this environment. // Broken abstraction.
			urn := t.GetSpec().GetUrn()
			switch urn {
			case urnTransformParDo:
				// and design the consuming bundle.
				instID, bundID := wk.nextInst(), wk.nextBund()

				in := inputs[tid][0]
				dataParentID := in.global + "_" + in.local
				parentID := in.global

				V(2).Log(tid, " sources ", dataParentID, " ", parentID)
				V(2).Log(tid, " inputs ", inputs[tid], " ", len(parents))

				coders := map[string]*pipepb.Coder{}
				transforms := map[string]*pipepb.PTransform{
					tid: t, // The Transform to Execute!
				}

				pardo := &pipepb.ParDoPayload{}
				if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
					V(1).Fatalf("unable to decode ParDoPayload for transform[%v]", tid)
				}

				sis := pardo.GetSideInputs()
				iterSides := make(map[string][]byte)

				var parent *bundle
				// Get WindowedValue Coders for the transform's input and output PCollections.
				for local, global := range t.GetInputs() {
					inPID, wInCid := makeWindowedValueCoder(t, global, pipeline, coders)
					si, ok := sis[local]
					// this is the main input!
					if !ok {
						parent = parents[local]
						transforms[parentID] = sourceTransform(parentID, portFor(wInCid, wk), inPID)
						continue
					}
					// TODO, do some kind of prep here?
					switch si.GetAccessPattern().GetUrn() {
					case urnSideInputIterable:
						pb := parents[local]
						src := pcolParents[global]
						key := src.global + "_" + src.local
						V(2).Logf("urnSideInputIterable key? %v, %v", key, local)
						data, ok := pb.DataReceived[key]
						if !ok {
							ks := maps.Keys(pb.DataReceived)
							logger.Fatalf("%v not a DataReceived key, have %v", key, ks)
						}
						col := pipeline.GetComponents().GetPcollections()[global]
						cID := lpUnknownCoders(col.GetCoderId(), coders, pipeline.GetComponents().GetCoders())
						ec := coders[cID]
						ed := pullDecoder(ec, coders)

						wc := exec.MakeWindowDecoder(coder.NewGlobalWindow())
						inBuf := bytes.NewBuffer(data[0])
						var outBuf bytes.Buffer
						for {
							// TODO window projection and segmentation of side inputs.
							_, _, _, err := exec.DecodeWindowedValueHeader(wc, inBuf)
							if err == io.EOF {
								break
							}
							if err != nil {
								logger.Fatalf("can't decode windowed value header with %v: %v", wc, err)
							}
							outBuf.Write(ed(inBuf))
						}

						iterSides[local] = outBuf.Bytes()
					default:
						logger.Fatalf("local input %v (global %v) for transform %v uses accesspattern %v", local, global, tid, si.GetAccessPattern().GetUrn())

					}
				}

				// We need a new logical PCollection to represent the source
				// so we can avoid double counting PCollection metrics later.
				// But this also means replacing the ID for the input in the bundle.
				// inPIDSrc := inPID + "_src"
				for local, global := range t.GetOutputs() {
					outPID, wOutCid := makeWindowedValueCoder(t, global, pipeline, coders)
					sinkID := tid + "_" + local
					transforms[sinkID] = sinkTransform(sinkID, portFor(wOutCid, wk), outPID)
				}

				reconcileCoders(coders, pipeline.GetComponents().GetCoders())

				desc := &fnpb.ProcessBundleDescriptor{
					Id:                  bundID,
					Transforms:          transforms,
					WindowingStrategies: pipeline.GetComponents().GetWindowingStrategies(),
					Pcollections:        pipeline.GetComponents().GetPcollections(),
					Coders:              coders,
					StateApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
						Url: wk.Endpoint(),
					},
				}

				V(2).Logf("registering %v with %v:", desc.GetId(), instID) // , prototext.Format(desc))

				wk.InstReqs <- &fnpb.InstructionRequest{
					InstructionId: instID,
					Request: &fnpb.InstructionRequest_Register{
						Register: &fnpb.RegisterRequest{
							ProcessBundleDescriptor: []*fnpb.ProcessBundleDescriptor{
								desc,
							},
						},
					},
				}

				b := &bundle{
					BundID: bundID,

					InputTransformID: parentID,
					InputData:        parent.DataReceived[dataParentID],
					IterableSideInputData: map[string]map[string][]byte{
						tid: iterSides,
					},
					Resp: make(chan *fnpb.ProcessBundleResponse, 1),

					DataReceived: make(map[string][][]byte),
				}
				V(3).Logf("XXX Going to wait on data for %v: count %v - %v", instID, len(t.GetOutputs()), t.GetOutputs())
				b.DataWait.Add(len(t.GetOutputs()))
				for _, ch := range succ[tid] {
					m, ok := prevs[ch.global]
					if !ok {
						m = make(map[string]*bundle)
					}
					m[ch.local] = b
					prevs[ch.global] = m
				}
				toProcess <- b
				continue
			default:
				logger.Fatalf("unimplemented worker transform[%v]: %v", urn, prototext.Format(t))
			}
		default:
			logger.Fatalf("unknown environment[%v]", t.GetEnvironmentId())
		}
	}
	// We're done with the pipeline!
	close(toProcess)
	b := <-processed // Drain the final bundle.
	V(1).Logf("pipeline done! Final Bundle: %v", b.InstID)
}

func sourceIDs(parent *bundle) (string, string) {
	var dataParentID, parentID string
	for k := range parent.DataReceived {
		dataParentID = k
		parentID = trimRightTo(dataParentID, '_')
	}
	return dataParentID, parentID
}

func sourceTransform(parentID string, sourcePortBytes []byte, outPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: parentID,
		Spec: &pipepb.FunctionSpec{
			Urn:     "beam:runner:source:v1",
			Payload: sourcePortBytes,
		},
		Outputs: map[string]string{
			"i0": outPID,
		},
	}
	return source
}

func sinkTransform(sinkID string, sinkPortBytes []byte, inPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: sinkID,
		Spec: &pipepb.FunctionSpec{
			Urn:     "beam:runner:sink:v1",
			Payload: sinkPortBytes,
		},
		Inputs: map[string]string{
			"i0": inPID,
		},
	}
	return source
}

func portFor(wInCid string, wk *worker) []byte {
	sourcePort := &fnpb.RemoteGrpcPort{
		CoderId: wInCid,
		ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}
	sourcePortBytes, err := proto.Marshal(sourcePort)
	if err != nil {
		logger.Fatalf("bad port: %v", err)
	}
	return sourcePortBytes
}

func runnerTransform(t *pipepb.PTransform, tid string, processed chan *bundle, parents map[string]*bundle, pipeline *pipepb.Pipeline) *bundle {
	urn := t.GetSpec().GetUrn()
	switch urn {
	case urnTransformImpulse:
		// To avoid conflicts with these single transform
		// bundles, we suffix the transform IDs.
		// These will be subbed out by the pardo stage.

		// TODO Needs to be the "real" output id for the transform.
		fakeTid := tid + "_i0"
		b := &bundle{
			InputTransformID: fakeTid,
			DataReceived: map[string][][]byte{
				fakeTid: {impulseBytes()},
			},
		}
		// Get back to the start of the bundle generation loop since the worker
		// doesn't need to do anything for this.
		go func() {
			processed <- b
		}()
		return b
	case urnTransformGBK:
		var parent *bundle
		// Extract the one parent.
		for _, pb := range parents {
			parent = pb
		}
		dataParentID, _ := sourceIDs(parent)
		// TODO Needs to be real output name from the proto.
		fakeTid := tid + "_i0" // The ID from which the consumer will read from.

		ws := windowingStrategy(pipeline, tid)
		kvc := kvcoder(pipeline, tid)

		// TODO, support other window coders.
		wc := exec.MakeWindowDecoder(coder.NewGlobalWindow())
		// TODO assert this is a KV. It's probably fine, but we should fail anyway.
		coders := map[string]*pipepb.Coder{}
		kcID := lpUnknownCoders(kvc.GetComponentCoderIds()[0], coders, pipeline.GetComponents().GetCoders())
		ecID := lpUnknownCoders(kvc.GetComponentCoderIds()[1], coders, pipeline.GetComponents().GetCoders())
		reconcileCoders(coders, pipeline.GetComponents().GetCoders())

		kc := coders[kcID]
		ec := coders[ecID]

		b := &bundle{
			InputTransformID: fakeTid,
			DataReceived: map[string][][]byte{
				fakeTid: {gbkBytes(ws, wc, kc, ec, parent.DataReceived[dataParentID], coders)},
			},
		}
		go func() {
			processed <- b
		}()
		return b
	case urnTransformFlatten:
		// TODO Needs to be real output name from the proto.
		fakeTid := tid + "_i0" // The ID from which the consumer will read from.
		// Extract the data from the parents.
		// TODO extract from the correct output.
		var data [][]byte
		for _, pb := range parents {
			for _, ds := range pb.DataReceived {
				for _, d := range ds {
					data = append(data, d)
				}
			}
		}
		b := &bundle{
			InputTransformID: fakeTid,
			DataReceived: map[string][][]byte{
				fakeTid: data,
			},
		}
		go func() {
			processed <- b
		}()
		return b
	default:
		logger.Fatalf("unimplemented runner transform[%v]", urn)
	}
	return nil
}

// Gets the windowing strategy for the given transform ID.
func windowingStrategy(pipeline *pipepb.Pipeline, tid string) *pipepb.WindowingStrategy {
	comp := pipeline.GetComponents()
	t := comp.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comp.GetPcollections()[inputPColID]
	return comp.GetWindowingStrategies()[pcol.GetWindowingStrategyId()]
}

func gbkBytes(ws *pipepb.WindowingStrategy, wc exec.WindowDecoder, kc, vc *pipepb.Coder, toAggregate [][]byte, coders map[string]*pipepb.Coder) []byte {
	var outputTime func(typex.Window, mtime.Time) mtime.Time
	switch ws.GetOutputTime() {
	case pipepb.OutputTime_END_OF_WINDOW:
		outputTime = func(w typex.Window, et mtime.Time) mtime.Time {
			return w.MaxTimestamp()
		}
	default:
		logger.Fatalf("unsupported OutputTime behavior: %v", ws.GetOutputTime())
	}

	type keyTime struct {
		key    []byte
		w      typex.Window
		time   mtime.Time
		values [][]byte
	}
	// Map windows to a map of keys to a map of keys to time.
	// We ultimately emit the window, the key, the time, and the iterable of elements,
	// all contained in the final value.
	windows := map[typex.Window]map[string]keyTime{}

	kd := pullDecoder(kc, coders)
	vd := pullDecoder(vc, coders)

	// Right, need to get the key coder, and the element coder.
	// Cus I'll need to pull out anything the runner knows how to deal with.
	// And repeat.
	for _, data := range toAggregate {
		// Parse out each element's data, and repeat.
		buf := bytes.NewBuffer(data)
		for {
			ws, tm, _, err := exec.DecodeWindowedValueHeader(wc, buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Fatalf("can't decode windowed value header with %v: %v", wc, err)
			}

			keyByt := kd(buf)
			key := string(keyByt)
			value := vd(buf)
			for _, w := range ws {
				ft := outputTime(w, tm)
				wk, ok := windows[w]
				if !ok {
					wk = make(map[string]keyTime)
					windows[w] = wk
				}
				kt := wk[key]
				kt.time = ft
				kt.key = keyByt
				kt.w = w
				kt.values = append(kt.values, value)
				wk[key] = kt
			}
		}
	}

	// Everything's aggregated!
	// Time to turn things into a windowed KV<K, Iterable<V>>

	var buf bytes.Buffer
	for _, w := range windows {
		for _, kt := range w {
			exec.EncodeWindowedValueHeader(
				exec.MakeWindowEncoder(coder.NewGlobalWindow()),
				[]typex.Window{kt.w},
				kt.time,
				typex.NoFiringPane(),
				&buf,
			)
			buf.Write(kt.key)
			coder.EncodeInt32(int32(len(kt.values)), &buf)
			for _, value := range kt.values {
				buf.Write(value)
			}
		}
	}
	return buf.Bytes()
}

// bundle represents an extant ProcessBundle instruction sent to an SDK worker.
type bundle struct {
	InstID string // ID for the instruction processing this bundle.
	BundID string // ID for the ProcessBundleDescriptor

	// InputTransformID is data being sent to the SDK.
	InputTransformID string
	InputData        [][]byte

	// IterableSideInputData is a map from transformID, to inputID to data.
	IterableSideInputData map[string]map[string][]byte

	// DataReceived is where all the data received from the SDK is put
	// separated by their TransformIDs.
	// TODO( Consider turning these directly to io.Readers to avoid extra copying and allocating & GC.)
	DataReceived map[string][][]byte
	DataWait     sync.WaitGroup
	Resp         chan *fnpb.ProcessBundleResponse

	// TODO: Metrics for this bundle, can be handled after the fact.
}

// ProcessOn executes the given bundle on the given worker.
//
// Assumes the bundle is initialized (all maps are non-nil, and data waitgroup is set.)
// Assumes the bundle descriptor is already registered.
func (b *bundle) ProcessOn(wk *worker) {
	wk.mu.Lock()
	b.InstID = wk.nextInst()
	wk.bundles[b.InstID] = b
	wk.mu.Unlock()

	V(2).Logf("processing %v %v on %v", b.InstID, b.BundID, wk)

	// Tell the SDK to start processing the bundle.
	wk.InstReqs <- &fnpb.InstructionRequest{
		InstructionId: b.InstID,
		Request: &fnpb.InstructionRequest_ProcessBundle{
			ProcessBundle: &fnpb.ProcessBundleRequest{
				ProcessBundleDescriptorId: b.BundID,
			},
		},
	}

	// Send the data one at a time, rather than batching.
	// TODO: Batch Data.
	for i, d := range b.InputData {
		V(3).Logf("XXX adding data to channel for %v", b.InstID)
		wk.DataReqs <- &fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				{
					InstructionId: b.InstID,
					TransformId:   b.InputTransformID,
					Data:          d,
					IsLast:        i+1 == len(b.InputData),
				},
			},
		}
	}

	V(3).Logf("XXX waiting on data from %v", b.InstID)
	b.DataWait.Wait() // Wait until data is done.

	// logger.Printf("ProcessBundle %v done! Resp: %v", b.InstID, prototext.Format(<-b.Resp))
	// logger.Printf("ProcessBundle %v output: %v", b.InstID, b.DataReceived)

	// TODO figure out how to kick off the next Bundle with the data from here.
}
