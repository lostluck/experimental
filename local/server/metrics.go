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

package server

import (
	"bytes"
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

type labelsToKeyFunc func(map[string]string) metricKey

var (
	mUrn2Spec  = map[string]*pipepb.MonitoringInfoSpec{}
	mUrn2KeyFn = map[string]labelsToKeyFunc{}
)

func init() {
	specs := (pipepb.MonitoringInfoSpecs_Enum)(0).Descriptor().Values()
	for i := 0; i < specs.Len(); i++ {
		enum := specs.ByNumber(protoreflect.EnumNumber(i))
		spec := proto.GetExtension(enum.Options(), pipepb.E_MonitoringInfoSpec).(*pipepb.MonitoringInfoSpec)
		mUrn2Spec[spec.GetUrn()] = spec
	}
	mUrn2KeyFn = buildUrnToKeysMap()
}

func buildUrnToKeysMap() map[string]labelsToKeyFunc {
	var hasher maphash.Hash

	props := (pipepb.MonitoringInfo_MonitoringInfoLabels)(0).Descriptor().Values()
	getProp := func(l pipepb.MonitoringInfo_MonitoringInfoLabels) string {
		return proto.GetExtension(props.ByNumber(protoreflect.EnumNumber(l)).Options(), pipepb.E_LabelProps).(*pipepb.MonitoringInfoLabelProps).GetName()
	}

	l2func := make(map[uint64]labelsToKeyFunc)
	labelsToKey := func(required []pipepb.MonitoringInfo_MonitoringInfoLabels, fn labelsToKeyFunc) {
		hasher.Reset()
		sort.Slice(required, func(i, j int) bool {
			return required[i] < required[j]
		})
		var req []string
		for _, l := range required {
			v := getProp(l)
			hasher.WriteString(v)
			req = append(req, v)
		}
		key := hasher.Sum64()
		l2func[key] = fn
		// Dev printout.
		logger.Printf("adding keyfn for key[%v] labels: %v", key, req)
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
		func(labels map[string]string) metricKey {
			return userMetricKey{
				ptransform: labels[ptransformLabel],
				namespace:  labels[namespaceLabel],
				name:       labels[nameLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_TRANSFORM),
		func(labels map[string]string) metricKey {
			return ptransformKey{
				ptransform: labels[ptransformLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_PCOLLECTION),
		func(labels map[string]string) metricKey {
			return pcollectionKey{
				pcollection: labels[pcollectionLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_SERVICE,
		pipepb.MonitoringInfo_METHOD,
		pipepb.MonitoringInfo_RESOURCE,
		pipepb.MonitoringInfo_TRANSFORM,
		pipepb.MonitoringInfo_STATUS),
		func(labels map[string]string) metricKey {
			return apiRequestKey{
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
		func(labels map[string]string) metricKey {
			return apiRequestLatenciesKey{
				service:    labels[serviceLabel],
				method:     labels[methodLabel],
				resource:   labels[resourceLabel],
				ptransform: labels[ptransformLabel],
			}
		})

	ret := make(map[string]labelsToKeyFunc)
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
			log.Printf("unknown MonitoringSpec required Labels key[%v] for urn %v: %v", key, urn, spec.GetRequiredLabels())
			continue
		}
		ret[urn] = fn
	}
	return ret
}

// No we don't want this approach, we want the metric *storage* to
// take in the bytes and only return errors.
func mStringToDecoder(typ string) func([]byte) (interface{}, error) {
	switch typ {
	case "beam:metrics:sum_int64:v1":
		return func(pyld []byte) (interface{}, error) {
			return coder.DecodeVarInt(bytes.NewBuffer(pyld))
		}
	case "beam:metrics:sum_double:v1":
		return func(pyld []byte) (interface{}, error) {
			return coder.DecodeDouble(bytes.NewBuffer(pyld))
		}
	case "beam:metrics:distribution_int64:v1":
		return func(pyld []byte) (interface{}, error) {
			buf := bytes.NewBuffer(pyld)
			var d metrics.DistributionValue
			var err error
			if d.Count, err = coder.DecodeVarInt(buf); err != nil {
				return nil, err
			}
			if d.Sum, err = coder.DecodeVarInt(buf); err != nil {
				return nil, err
			}
			if d.Min, err = coder.DecodeVarInt(buf); err != nil {
				return nil, err
			}
			if d.Max, err = coder.DecodeVarInt(buf); err != nil {
				return nil, err
			}

			return d, nil
		}
	default:
		logger.Fatalf("unimplemented monitoring info type: %v", typ)
		return nil
	}
}

type durability bool

const (
	tentative durability = false
	committed durability = true
)

type metricAccumulator interface {
	accumulate(durability, []byte) error
}

type metricsStore struct {
}

func (m *metricsStore) contributeMetrics(payloads *fnpb.ProcessBundleResponse) {
	// Old and busted.
	mons := payloads.GetMonitoringInfos()
	for _, mon := range mons {
		urn := mon.GetUrn()
		keyFn, ok := mUrn2KeyFn[urn]
		if !ok {
			logger.Printf("unknown metrics urn: %v", urn)
			continue
		}
		key := keyFn(mon.GetLabels())
		logger.Printf("metrics key for urn %v: %+v", urn, key)
	}
	// New hotness.
	mdata := payloads.GetMonitoringData()
	_ = mdata
}

type metricKey interface {
	metricKey() // marker method.
}

type userMetricKey struct {
	ptransform, namespace, name string
}

func (userMetricKey) metricKey() {}

type pcollectionKey struct {
	pcollection string
}

func (pcollectionKey) metricKey() {}

type ptransformKey struct {
	ptransform string
}

func (ptransformKey) metricKey() {}

type apiRequestKey struct {
	service, method, resource, ptransform, status string
}

func (apiRequestKey) metricKey() {}

type apiRequestLatenciesKey struct {
	service, method, resource, ptransform string
}

func (apiRequestLatenciesKey) metricKey() {}
