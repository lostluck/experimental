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
	"constraints"
	"hash/maphash"
	"log"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type labelsToKeyFunc func(string, map[string]string) metricKey

type urnOps struct {
	// keyFn produces the key for this metric from the labels.
	// based on the required label set for the metric from it's spec.
	keyFn labelsToKeyFunc
	// newAccum produces an accumulator assuming we don't have an accumulator for it already.
	// based on the type urn of the metric from it's spec.
	newAccum accumFactory
}

var (
	mUrn2Ops = map[string]urnOps{}
)

func init() {
	mUrn2Spec := map[string]*pipepb.MonitoringInfoSpec{}
	specs := (pipepb.MonitoringInfoSpecs_Enum)(0).Descriptor().Values()
	for i := 0; i < specs.Len(); i++ {
		enum := specs.ByNumber(protoreflect.EnumNumber(i))
		spec := proto.GetExtension(enum.Options(), pipepb.E_MonitoringInfoSpec).(*pipepb.MonitoringInfoSpec)
		mUrn2Spec[spec.GetUrn()] = spec
	}
	mUrn2Ops = buildUrnToOpsMap(mUrn2Spec)
}

func buildUrnToOpsMap(mUrn2Spec map[string]*pipepb.MonitoringInfoSpec) map[string]urnOps {
	var hasher maphash.Hash

	props := (pipepb.MonitoringInfo_MonitoringInfoLabels)(0).Descriptor().Values()
	getProp := func(l pipepb.MonitoringInfo_MonitoringInfoLabels) string {
		return proto.GetExtension(props.ByNumber(protoreflect.EnumNumber(l)).Options(), pipepb.E_LabelProps).(*pipepb.MonitoringInfoLabelProps).GetName()
	}

	l2func := make(map[uint64]labelsToKeyFunc)
	labelsToKey := func(required []pipepb.MonitoringInfo_MonitoringInfoLabels, fn labelsToKeyFunc) {
		hasher.Reset()
		// We need the string versions of things to sort against
		// for consistent hashing.
		var req []string
		for _, l := range required {
			v := getProp(l)
			req = append(req, v)
		}
		sort.Strings(req)
		for _, v := range req {
			hasher.WriteString(v)
		}
		key := hasher.Sum64()
		l2func[key] = fn
	}
	ls := func(ls ...pipepb.MonitoringInfo_MonitoringInfoLabels) []pipepb.MonitoringInfo_MonitoringInfoLabels {
		return ls
	}

	ptransformLabel := getProp(pipepb.MonitoringInfo_TRANSFORM)
	namespaceLabel := getProp(pipepb.MonitoringInfo_NAMESPACE)
	nameLabel := getProp(pipepb.MonitoringInfo_NAME)
	pcollectionLabel := getProp(pipepb.MonitoringInfo_PCOLLECTION)
	statusLabel := getProp(pipepb.MonitoringInfo_STATUS)
	serviceLabel := getProp(pipepb.MonitoringInfo_SERVICE)
	resourceLabel := getProp(pipepb.MonitoringInfo_RESOURCE)
	methodLabel := getProp(pipepb.MonitoringInfo_METHOD)

	// Here's where we build the raw map from kinds of labels to the actual functions.
	labelsToKey(ls(pipepb.MonitoringInfo_TRANSFORM,
		pipepb.MonitoringInfo_NAMESPACE,
		pipepb.MonitoringInfo_NAME),
		func(urn string, labels map[string]string) metricKey {
			return userMetricKey{
				urn:        urn,
				ptransform: labels[ptransformLabel],
				namespace:  labels[namespaceLabel],
				name:       labels[nameLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_TRANSFORM),
		func(urn string, labels map[string]string) metricKey {
			return ptransformKey{
				urn:        urn,
				ptransform: labels[ptransformLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_PCOLLECTION),
		func(urn string, labels map[string]string) metricKey {
			return pcollectionKey{
				urn:         urn,
				pcollection: labels[pcollectionLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_SERVICE,
		pipepb.MonitoringInfo_METHOD,
		pipepb.MonitoringInfo_RESOURCE,
		pipepb.MonitoringInfo_TRANSFORM,
		pipepb.MonitoringInfo_STATUS),
		func(urn string, labels map[string]string) metricKey {
			return apiRequestKey{
				urn:        urn,
				service:    labels[serviceLabel],
				method:     labels[methodLabel],
				resource:   labels[resourceLabel],
				ptransform: labels[ptransformLabel],
				status:     labels[statusLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_SERVICE,
		pipepb.MonitoringInfo_METHOD,
		pipepb.MonitoringInfo_RESOURCE,
		pipepb.MonitoringInfo_TRANSFORM),
		func(urn string, labels map[string]string) metricKey {
			return apiRequestLatenciesKey{
				urn:        urn,
				service:    labels[serviceLabel],
				method:     labels[methodLabel],
				resource:   labels[resourceLabel],
				ptransform: labels[ptransformLabel],
			}
		})

	// Specify accumulator decoders for all the metric types.
	// These are a combination of the decoder (accepting the payload bytes)
	// and represent how we hold onto them. Ultimately, these will also be
	// able to extract back out to the protos.

	typs := (pipepb.MonitoringInfoTypeUrns_Enum)(0).Descriptor().Values()
	getTyp := func(t pipepb.MonitoringInfoTypeUrns_Enum) string {
		return proto.GetExtension(typs.ByNumber(protoreflect.EnumNumber(t)).Options(), pipepb.E_BeamUrn).(string)
	}
	_ = getTyp
	getTyp(pipepb.MonitoringInfoTypeUrns_SUM_INT64_TYPE)

	typ2accumFac := map[string]accumFactory{
		getTyp(pipepb.MonitoringInfoTypeUrns_SUM_INT64_TYPE):          func() metricAccumulator { return &sumInt64{} },
		getTyp(pipepb.MonitoringInfoTypeUrns_SUM_DOUBLE_TYPE):         func() metricAccumulator { return &sumFloat64{} },
		getTyp(pipepb.MonitoringInfoTypeUrns_DISTRIBUTION_INT64_TYPE): func() metricAccumulator { return &distributionInt64{} },
		getTyp(pipepb.MonitoringInfoTypeUrns_PROGRESS_TYPE):           func() metricAccumulator { return &progress{} },
	}

	ret := make(map[string]urnOps)
	for urn, spec := range mUrn2Spec {
		hasher.Reset()
		sorted := spec.GetRequiredLabels()
		sort.Strings(sorted)
		for _, l := range sorted {
			hasher.WriteString(l)
		}
		key := hasher.Sum64()
		fn, ok := l2func[key]
		if !ok {
			// Dev printout! Otherwise we should probably always ignore things we don't know.
			log.Printf("unknown MonitoringSpec required Labels key[%v] for urn %v: %v", key, urn, sorted)
			continue
		}
		fac, ok := typ2accumFac[spec.GetType()]
		if !ok {
			// Dev printout! Otherwise we should probably always ignore things we don't know.
			log.Printf("unknown MonitoringSpec type for urn %v: %v", urn, spec.GetType())
			continue
		}
		ret[urn] = urnOps{
			keyFn:    fn,
			newAccum: fac,
		}
	}
	return ret
}

type sumInt64 struct {
	sum [2]int64
}

func (m *sumInt64) accumulate(d durability, pyld []byte) error {
	v, err := coder.DecodeVarInt(bytes.NewBuffer(pyld))
	if err != nil {
		return err
	}
	m.sum[d] += v
	return nil
}

type sumFloat64 struct {
	sum [2]float64
}

func (m *sumFloat64) accumulate(d durability, pyld []byte) error {
	v, err := coder.DecodeDouble(bytes.NewBuffer(pyld))
	if err != nil {
		return err
	}
	m.sum[d] += v
	return nil
}

type progress struct {
	snap []float64
}

func (m *progress) accumulate(_ durability, pyld []byte) error {
	buf := bytes.NewBuffer(pyld)
	// Assuming known length iterable
	n, err := coder.DecodeInt32(buf)
	if err != nil {
		return err
	}
	progs := make([]float64, 0, n)
	for i := int32(0); i < n; i++ {
		v, err := coder.DecodeDouble(buf)
		if err != nil {
			return err
		}
		progs = append(progs, v)
	}
	m.snap = progs
	return nil
}

func ordMin[T constraints.Ordered](a T, b T) T {
	if a < b {
		return a
	}
	return b
}

func ordMax[T constraints.Ordered](a T, b T) T {
	if a > b {
		return a
	}
	return b
}

type distributionInt64 struct {
	dist [2]metrics.DistributionValue
}

func (m *distributionInt64) accumulate(d durability, pyld []byte) error {
	buf := bytes.NewBuffer(pyld)
	var dist metrics.DistributionValue
	var err error
	if dist.Count, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	if dist.Sum, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	if dist.Min, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	if dist.Max, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	cur := m.dist[d]

	m.dist[d] = metrics.DistributionValue{
		Count: cur.Count + dist.Count,
		Sum:   cur.Sum + dist.Sum,
		Min:   ordMin(cur.Min, dist.Min),
		Max:   ordMax(cur.Max, dist.Max),
	}
	return nil
}

type durability int

const (
	tentative = durability(iota)
	committed
)

type metricAccumulator interface {
	accumulate(durability, []byte) error
}

type accumFactory func() metricAccumulator

type metricKey interface {
	metricKey() // marker method.
}

type userMetricKey struct {
	urn, ptransform, namespace, name string
}

func (userMetricKey) metricKey() {}

type pcollectionKey struct {
	urn, pcollection string
}

func (pcollectionKey) metricKey() {}

type ptransformKey struct {
	urn, ptransform string
}

func (ptransformKey) metricKey() {}

type apiRequestKey struct {
	urn, service, method, resource, ptransform, status string
}

func (apiRequestKey) metricKey() {}

type apiRequestLatenciesKey struct {
	urn, service, method, resource, ptransform string
}

func (apiRequestLatenciesKey) metricKey() {}

type metricsStore struct {
	accums map[metricKey]metricAccumulator
}

func (m *metricsStore) contributeMetrics(payloads *fnpb.ProcessBundleResponse) {
	// Old and busted.
	mons := payloads.GetMonitoringInfos()
	for _, mon := range mons {
		urn := mon.GetUrn()
		ops, ok := mUrn2Ops[urn]
		if !ok {
			logger.Printf("unknown metrics urn: %v", urn)
			continue
		}
		key := ops.keyFn(urn, mon.GetLabels())
		logger.Printf("metrics key for urn %v: %+v", urn, key)
	}
	// New hotness.
	mdata := payloads.GetMonitoringData()
	_ = mdata
}
