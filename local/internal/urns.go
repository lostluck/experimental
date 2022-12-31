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

// urns.go handles exttracting all the urns from the protos.

import (
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type protoEnum interface {
	~int32
	Descriptor() protoreflect.EnumDescriptor
}

// toUrn returns a function that can get the urn string from the proto.
func toUrn[Enum protoEnum]() func(Enum) string {
	evd := (Enum)(0).Descriptor().Values()
	return func(v Enum) string {
		return proto.GetExtension(evd.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
	}
}

// quickUrn handles one off urns instead of retaining a helper function.
// Notably useful for the windowFns due to their older design.
func quickUrn[Enum protoEnum](v Enum) string {
	return toUrn[Enum]()(v)
}

var (
	ptUrn   = toUrn[pipepb.StandardPTransforms_Primitives]()
	ctUrn   = toUrn[pipepb.StandardPTransforms_Composites]()
	cmbtUrn = toUrn[pipepb.StandardPTransforms_CombineComponents]()
	sdfUrn  = toUrn[pipepb.StandardPTransforms_SplittableParDoComponents]()
	siUrn   = toUrn[pipepb.StandardSideInputTypes_Enum]()
	cdrUrn  = toUrn[pipepb.StandardCoders_Enum]()
	reqUrn  = toUrn[pipepb.StandardRequirements_Enum]()
	envUrn  = toUrn[pipepb.StandardEnvironments_Environments]()
)

var (
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

	// Undocumented Urns
	urnGoDoFn          = "beam:go:transform:dofn:v1" // Only used for Go DoFn.
	urnTransformSource = "beam:runner:source:v1"     // The data source reading transform.
	urnTransformSink   = "beam:runner:sink:v1"       // The data sink writing transform.

	// Runner transforms.
	urnTransformImpulse = ptUrn(pipepb.StandardPTransforms_IMPULSE)
	urnTransformGBK     = ptUrn(pipepb.StandardPTransforms_GROUP_BY_KEY)
	urnTransformFlatten = ptUrn(pipepb.StandardPTransforms_FLATTEN)

	// Side Input access patterns
	urnSideInputIterable = siUrn(pipepb.StandardSideInputTypes_ITERABLE)
	urnSideInputMultiMap = siUrn(pipepb.StandardSideInputTypes_MULTIMAP)

	// WindowsFns
	urnWindowFnGlobal  = quickUrn(pipepb.GlobalWindowsPayload_PROPERTIES)
	urnWindowFnFixed   = quickUrn(pipepb.FixedWindowsPayload_PROPERTIES)
	urnWindowFnSliding = quickUrn(pipepb.SlidingWindowsPayload_PROPERTIES)
	urnWindowFnSession = quickUrn(pipepb.SessionWindowsPayload_PROPERTIES)

	// Coders
	urnCoderBytes      = cdrUrn(pipepb.StandardCoders_BYTES)
	urnCoderBool       = cdrUrn(pipepb.StandardCoders_BOOL)
	urnCoderDouble     = cdrUrn(pipepb.StandardCoders_DOUBLE)
	urnCoderStringUTF8 = cdrUrn(pipepb.StandardCoders_STRING_UTF8)
	urnCoderRow        = cdrUrn(pipepb.StandardCoders_ROW)
	urnCoderVarInt     = cdrUrn(pipepb.StandardCoders_VARINT)

	urnCoderGlobalWindow   = cdrUrn(pipepb.StandardCoders_GLOBAL_WINDOW)
	urnCoderIntervalWindow = cdrUrn(pipepb.StandardCoders_INTERVAL_WINDOW)
	urnCoderCustomWindow   = cdrUrn(pipepb.StandardCoders_CUSTOM_WINDOW)

	urnCoderParamWindowedValue = cdrUrn(pipepb.StandardCoders_PARAM_WINDOWED_VALUE)
	urnCoderWindowedValue      = cdrUrn(pipepb.StandardCoders_WINDOWED_VALUE)
	urnCoderTimer              = cdrUrn(pipepb.StandardCoders_TIMER)

	urnCoderKV                  = cdrUrn(pipepb.StandardCoders_KV)
	urnCoderLengthPrefix        = cdrUrn(pipepb.StandardCoders_LENGTH_PREFIX)
	urnCoderNullable            = cdrUrn(pipepb.StandardCoders_NULLABLE)
	urnCoderIterable            = cdrUrn(pipepb.StandardCoders_ITERABLE)
	urnCoderStateBackedIterable = cdrUrn(pipepb.StandardCoders_STATE_BACKED_ITERABLE)
	urnCoderShardedKey          = cdrUrn(pipepb.StandardCoders_SHARDED_KEY)

	// Requirements
	urnRequirementSplittableDoFn     = reqUrn(pipepb.StandardRequirements_REQUIRES_SPLITTABLE_DOFN)
	urnRequirementBundleFinalization = reqUrn(pipepb.StandardRequirements_REQUIRES_BUNDLE_FINALIZATION)
	urnRequirementOnWindowExpiration = reqUrn(pipepb.StandardRequirements_REQUIRES_ON_WINDOW_EXPIRATION)
	urnRequirementStableInput        = reqUrn(pipepb.StandardRequirements_REQUIRES_STABLE_INPUT)
	urnRequirementStatefulProcessing = reqUrn(pipepb.StandardRequirements_REQUIRES_STATEFUL_PROCESSING)
	urnRequirementTimeSortedInput    = reqUrn(pipepb.StandardRequirements_REQUIRES_TIME_SORTED_INPUT)

	// Environment types
	urnEnvDocker   = envUrn(pipepb.StandardEnvironments_DOCKER)
	urnEnvProcess  = envUrn(pipepb.StandardEnvironments_PROCESS)
	urnEnvExternal = envUrn(pipepb.StandardEnvironments_EXTERNAL)
	urnEnvDefault  = envUrn(pipepb.StandardEnvironments_DEFAULT)
)
