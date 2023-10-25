package beam

import (
	"sync"
	"sync/atomic"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"pgregory.net/rand"
)

type metricsStore struct {
	metrics []any

	metricNames map[int]metricLabels
}

type metricLabels struct {
	Ptransform, Namespace, Name string
}

func (ms *metricsStore) initMetric(transform, name string, v any) int {
	id := len(ms.metrics)
	ms.metrics = append(ms.metrics, v)
	ms.metricNames[id] = metricLabels{Ptransform: transform, Name: name}
	return id
}

func (ms *metricsStore) MonitoringInfos(g *graph) []*pipepb.MonitoringInfo {
	var mons []*pipepb.MonitoringInfo
	for i, v := range ms.metrics {
		enc := coders.NewEncoder()

		labels := ms.metricNames[i]
		mon := &pipepb.MonitoringInfo{
			Labels: map[string]string{
				"PTRANSFORM": labels.Ptransform,
				"NAMESPACE":  "user",
				"NAME":       labels.Name,
			},
		}
		switch m := v.(type) {
		case *int64Sum:
			mon.Urn = "beam:metric:user:sum_int64:v1"
			mon.Type = "beam:metrics:sum_int64:v1"
			enc.Varint(uint64(m.sum.Load()))
			mon.Payload = enc.Data()
		case *dataChannelIndex:
			mon.Urn = "beam:metric:data_channel:read_index:v1"
			mon.Type = "beam:metrics:sum_int64:v1"
			enc.Varint(uint64(m.index.Load()))
			mon.Payload = enc.Data()
		case *pcollectionMetrics:
			enc.Varint(uint64(m.elementCount.Load()))
			labels := map[string]string{
				"PCOLLECTION": g.nodes[m.nodeIdx].protoID(),
			}
			elmMon := &pipepb.MonitoringInfo{
				Urn:     "beam:metric:element_count:v1",
				Type:    "beam:metrics:sum_int64:v1",
				Payload: enc.Data(),
				Labels:  labels,
			}
			mons = append(mons, elmMon)

			enc := coders.NewEncoder()
			m.sampleMu.Lock()
			enc.Varint(uint64(m.sampleCount))
			enc.Varint(uint64(m.sampleSum))
			enc.Varint(uint64(m.sampleMin))
			enc.Varint(uint64(m.sampleMax))
			m.sampleMu.Unlock()
			sampleMon := &pipepb.MonitoringInfo{
				Urn:     "beam:metric:sampled_byte_size:v1",
				Type:    "beam:metrics:distribution_int64:v1",
				Payload: enc.Data(),
				Labels:  labels,
			}
			mons = append(mons, sampleMon)
			continue
		default:
			// panic(fmt.Sprintf("unknown metric type: %T", m))
		}
		mons = append(mons, mon)
	}
	return mons
}

type int64Sum struct {
	sum atomic.Int64
}

func (m *int64Sum) add(d int64) {
	m.sum.Add(d)
}

type dataChannelIndex struct {
	transform   string
	metricIndex int
	index       atomic.Int64
}

// incrementIndexAndCheckSplit increments DataSource.index by one and checks if
// the caller should abort further element processing, and finish the bundle.
// Returns true if the new value of index is greater than or equal to the split
// index, and false otherwise.
func (c *dataChannelIndex) IncrementAndCheckSplit(dfc metricSource, split int64) bool {
	if c.metricIndex == 0 {
		ms := dfc.metricsStore()
		c.metricIndex = len(ms.metrics)
		ms.metrics = append(ms.metrics, c)
		ms.metricNames[c.metricIndex] = metricLabels{Ptransform: c.transform}
	}
	newIndex := c.index.Add(1)
	return newIndex > split
}

type metricscommon struct {
	index           int
	namespace, name string
}

type Counter struct {
	beamMixin

	metricscommon
}

func (mc *metricscommon) setName(name string) {
	mc.name = name
}

type metricNamer interface {
	setName(name string)
}

type metricSource interface {
	transformID() string
	metricsStore() *metricsStore
}

func (c *Counter) Inc(dfc metricSource, diff int64) {
	ms := dfc.metricsStore()
	if c.index == 0 {
		c.index = ms.initMetric(dfc.transformID(), c.name, &int64Sum{})
	}
	ms.metrics[c.index].(*int64Sum).add(diff)
}

type pcollectionMetrics struct {
	nodeIdx nodeIndex

	elementCount  atomic.Int64
	nextSampleIdx int64

	sampleMu                                     sync.Mutex
	sampleCount, sampleSum, sampleMin, sampleMax int64
}

// Count increments the current counter, and with exponential backoff executes the sampling func.
func (c *pcollectionMetrics) Count() bool {
	cur := c.elementCount.Add(1)
	if cur == c.nextSampleIdx {
		if c.nextSampleIdx < 4 {
			c.nextSampleIdx++
		} else {
			c.nextSampleIdx = cur + rand.Int63n(cur/10+2) + 1
		}
		return true
	}
	return false
}

func (c *pcollectionMetrics) Sample(size int64) {
	c.sampleMu.Lock()
	c.sampleCount++
	c.sampleSum += size
	c.sampleMax = max(c.sampleMax, size)
	c.sampleMin = min(c.sampleMin, size)
	if c.sampleCount == 1 {
		c.sampleMax = size
		c.sampleMin = size
	}
	c.sampleMu.Unlock()
}
