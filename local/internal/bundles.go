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
	"fmt"
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

func ptUrn(v pipepb.StandardPTransforms_Primitives) string {
	return proto.GetExtension(prims.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

func ctUrn(v pipepb.StandardPTransforms_Composites) string {
	return proto.GetExtension(cmps.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

func cmbtUrn(v pipepb.StandardPTransforms_CombineComponents) string {
	return proto.GetExtension(cmbcomps.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

func sdfUrn(v pipepb.StandardPTransforms_SplittableParDoComponents) string {
	return proto.GetExtension(sdfcomps.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

func siUrn(v pipepb.StandardSideInputTypes_Enum) string {
	return proto.GetExtension(sids.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
}

var (
	prims    = (pipepb.StandardPTransforms_Primitives)(0).Descriptor().Values()
	cmps     = (pipepb.StandardPTransforms_Composites)(0).Descriptor().Values()
	cmbcomps = (pipepb.StandardPTransforms_CombineComponents)(0).Descriptor().Values()
	sdfcomps = (pipepb.StandardPTransforms_SplittableParDoComponents)(0).Descriptor().Values()

	sids = (pipepb.StandardSideInputTypes_Enum)(0).Descriptor().Values()

	// SDK transforms.
	urnTransformParDo                = ptUrn(pipepb.StandardPTransforms_PAR_DO)
	urnTransformCombinePerKey        = ctUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY)
	urnTransformPreCombine           = cmbtUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY_PRECOMBINE)
	urnTransformMerge                = cmbtUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY_MERGE_ACCUMULATORS)
	urnTransformExtract              = cmbtUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY_EXTRACT_OUTPUTS)
	urnTransformPairWithRestriction  = sdfUrn(pipepb.StandardPTransforms_PAIR_WITH_RESTRICTION)
	urnTransformSplitAndSize         = sdfUrn(pipepb.StandardPTransforms_SPLIT_AND_SIZE_RESTRICTIONS)
	urnTransformProcessSizedElements = sdfUrn(pipepb.StandardPTransforms_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS)
	urnTransformTruncate             = sdfUrn(pipepb.StandardPTransforms_TRUNCATE_SIZED_RESTRICTION)

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
	comps := proto.Clone(pipeline.GetComponents()).(*pipepb.Components)

	// Basically need to figure out the "plan" at this point.
	// Long term: Take RootTransforms & TOPO sort them. (technically not necessary, but paranoid) (DONE)
	//  - Recurse through composites from there: TOPO sort subtransforms (DONE)
	//  - Eventually we have our ordered list of transforms. (DONE)
	//  - Then we can figure out how to fuse transforms into bundles
	//    (producer-consumer & sibling fusion & environment awareness)
	//
	// But for now we figure out how to connect bundles together
	// First: default "impulse" bundle. "executed" by the runner.
	// Second: Derive bundle for a DoFn for the environment.

	// TODO, configure the preprocessor from pipeline options.
	// Maybe change these returns to a single struct for convenience and further
	// annotation?

	handlers := []any{
		Combine(CombineCharacteristic{EnableLifting: true}),
		ParDo(ParDoCharacteristic{DisableSDF: true}),
		Runner(RunnerCharacteristic{}),
	}

	prepro := &preprocessor{
		transformPreparers: map[string]transformPreparer{},
	}

	proc := processor{
		transformExecuters: map[string]transformExecuter{},
	}

	for _, h := range handlers {
		if th, ok := h.(transformPreparer); ok {
			for _, urn := range th.PrepareUrns() {
				prepro.transformPreparers[urn] = th
			}
		}
		if th, ok := h.(transformExecuter); ok {
			for _, urn := range th.ExecuteUrns() {
				proc.transformExecuters[urn] = th
			}
		}
	}

	topo, succ, inputs, pcolParents := prepro.preProcessGraph(comps)
	ts := comps.GetTransforms()

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
		urn := t.GetSpec().GetUrn()
		V(1).Logf("urn %v", urn)
		exe := proc.transformExecuters[urn]

		// Stopgap until everythinng's moved to handlers.
		envID := t.GetEnvironmentId()
		if exe != nil {
			envID = exe.ExecuteWith(t)
		}

		var sendBundle func()
		var b *bundle
		switch envID {
		case "": // Runner Transforms
			b = exe.ExecuteTransform(tid, t, comps, parents)
			// Runner transforms are processed immeadiately.
			sendBundle = func() {
				go func() {
					processed <- b
				}()
			}
		case wk.ID:
			// Great! this is for this environment. // Broken abstraction.
			b = buildProcessBundle(wk, tid, t, comps, inputs, parents, pcolParents)
			// FnAPI instructions need to be sent to the SDK.
			sendBundle = func() {
				toProcess <- b
			}
		default:
			logger.Fatalf("unknown environment[%v]", t.GetEnvironmentId())
		}
		for _, ch := range succ[tid] {
			m, ok := prevs[ch.global]
			if !ok {
				m = make(map[string]*bundle)
			}
			m[ch.local] = b
			prevs[ch.global] = m
		}
		sendBundle()
	}
	// We're done with the pipeline!
	close(toProcess)
	b := <-processed // Drain the final bundle.
	V(1).Logf("pipeline done! Final Bundle: %v", b.InstID)
}

func buildProcessBundle(wk *worker, tid string, t *pipepb.PTransform, comps *pipepb.Components, inputs map[string][]linkID, parents map[string]*bundle, pcolParents map[string]linkID) *bundle {
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

	var sis map[string]*pipepb.SideInput

	if t.GetSpec().GetUrn() == urnTransformParDo {
		pardo := &pipepb.ParDoPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
			V(1).Fatalf("unable to decode ParDoPayload for transform[%v]", tid)
		}

		sis = pardo.GetSideInputs()
	}
	iterSides := make(map[string][]byte)
	multiMapSides := make(map[string]map[string][][]byte)

	var parent *bundle
	// Get WindowedValue Coders for the transform's input and output PCollections.
	for local, global := range t.GetInputs() {
		inPID, wInCid := makeWindowedValueCoder(t, global, comps, coders)
		si, ok := sis[local]

		if !ok {
			// this is the main input
			parent = parents[local]
			transforms[parentID] = sourceTransform(parentID, portFor(wInCid, wk), inPID)
			continue
		}

		// this is a side input
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
			col := comps.GetPcollections()[global]
			cID := lpUnknownCoders(col.GetCoderId(), coders, comps.GetCoders())
			ec := coders[cID]
			ed := pullDecoder(ec, coders)

			wc := exec.MakeWindowDecoder(coder.NewGlobalWindow())
			// TODO fix data iteration (there could be more than one data blob)
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
		case urnSideInputMultiMap:

			pb := parents[local]
			src := pcolParents[global]
			key := src.global + "_" + src.local
			V(2).Logf("urnSideInputMultiMap key? %v, %v", key, local)
			data, ok := pb.DataReceived[key]
			if !ok {
				ks := maps.Keys(pb.DataReceived)
				logger.Fatalf("%v not a DataReceived key, have %v", key, ks)
			}
			col := comps.GetPcollections()[global]

			kvc := comps.GetCoders()[col.GetCoderId()]
			if kvc.GetSpec().GetUrn() != "beam:coder:kv:v1" {
				logger.Fatalf("multimap side inputs needs KV coder, got %v", kvc.GetSpec().GetUrn())
			}
			kcID := lpUnknownCoders(kvc.GetComponentCoderIds()[0], coders, comps.GetCoders())
			vcID := lpUnknownCoders(kvc.GetComponentCoderIds()[1], coders, comps.GetCoders())

			reconcileCoders(coders, comps.GetCoders())

			kc := coders[kcID]
			vc := coders[vcID]

			kd := pullDecoder(kc, coders)
			vd := pullDecoder(vc, coders)

			// TODO, support other window coders.
			wc := exec.MakeWindowDecoder(coder.NewGlobalWindow())
			if len(data) != 1 {
				logger.Fatalf("multimap side input had more than a single data blob, got %v", len(data))
			}
			inBuf := bytes.NewBuffer(data[0])

			// We basically need to do the GBK thing here, but without making everything into
			// a single byte slice at the end.
			keyedData := map[string][][]byte{}

			// This only does the grouping by keys, but not any sorting or recoding.
			for {
				// TODO window projection and segmentation of side inputs.
				_, _, _, err := exec.DecodeWindowedValueHeader(wc, inBuf)
				if err == io.EOF {
					break
				}
				if err != nil {
					logger.Fatalf("can't decode windowed value header with %v: %v", wc, err)
				}

				dkey := string(kd(inBuf))

				vs := keyedData[dkey]
				keyedData[dkey] = append(vs, vd(inBuf))
			}
			multiMapSides[local] = keyedData
		default:
			logger.Fatalf("local input %v (global %v) for transform %v uses accesspattern %v", local, global, tid, si.GetAccessPattern().GetUrn())

		}
	}

	// We need a new logical PCollection to represent the source
	// so we can avoid double counting PCollection metrics later.
	// But this also means replacing the ID for the input in the bundle.
	// inPIDSrc := inPID + "_src"
	for local, global := range t.GetOutputs() {
		outPID, wOutCid := makeWindowedValueCoder(t, global, comps, coders)
		sinkID := tid + "_" + local
		transforms[sinkID] = sinkTransform(sinkID, portFor(wOutCid, wk), outPID)
	}

	reconcileCoders(coders, comps.GetCoders())

	desc := &fnpb.ProcessBundleDescriptor{
		Id:                  bundID,
		Transforms:          transforms,
		WindowingStrategies: comps.GetWindowingStrategies(),
		Pcollections:        comps.GetPcollections(),
		Coders:              coders,
		StateApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}

	V(2).Logf("registering %v with %v:", desc.GetId(), instID)

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

	if parent == nil || parent.DataReceived[dataParentID] == nil {
		panic(fmt.Sprintf("corruption detected: t: %v bundID %q parentID %q, parent %v, dataParentID %q", prototext.Format(t), bundID, parentID, parent, dataParentID))
	}

	b := &bundle{
		BundID: bundID,

		InputTransformID: parentID,
		InputData:        parent.DataReceived[dataParentID],
		IterableSideInputData: map[string]map[string][]byte{
			tid: iterSides,
		},
		MultiMapSideInputData: map[string]map[string]map[string][][]byte{
			tid: multiMapSides,
		},
		Resp: make(chan *fnpb.ProcessBundleResponse, 1),

		DataReceived: make(map[string][][]byte),
	}
	V(3).Logf("XXX Going to wait on data for %v: count %v - %v", instID, len(t.GetOutputs()), t.GetOutputs())
	b.DataWait.Add(len(t.GetOutputs()))
	return b
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

type transformExecuter interface {
	ExecuteUrns() []string
	ExecuteWith(t *pipepb.PTransform) string
	ExecuteTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components, parents map[string]*bundle) *bundle
}

type processor struct {
	transformExecuters map[string]transformExecuter
}

// bundle represents an extant ProcessBundle instruction sent to an SDK worker.
type bundle struct {
	InstID string // ID for the instruction processing this bundle.
	BundID string // ID for the ProcessBundleDescriptor

	// InputTransformID is data being sent to the SDK.
	InputTransformID string
	InputData        [][]byte

	// TODO change to a single map[tid] -> map[input] -> map[window] -> struct { Iter data, MultiMap data } instead of all maps.
	// IterableSideInputData is a map from transformID, to inputID to data.
	IterableSideInputData map[string]map[string][]byte
	// MultiMapSideInputData is a map from transformID, to inputID, to data key, to data values.
	MultiMapSideInputData map[string]map[string]map[string][][]byte

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
}
