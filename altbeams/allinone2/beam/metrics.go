package beam

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"pgregory.net/rand"
)

type metricsStore struct {
	metrics []any

	transitions  uint32 // Only accessed by process bundle thread.
	sample       atomic.Uint32
	samplingDone chan struct{}

	sampleMu sync.Mutex // Guards samples.
	samples  [][3]time.Duration

	metricNames map[int]metricLabels
}

func newMetricsStore(numEdges int) *metricsStore {
	return &metricsStore{
		metricNames:  map[int]metricLabels{},
		samples:      make([][3]time.Duration, numEdges),
		samplingDone: make(chan struct{}),
	}
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

	encVarInt := func(v int64) []byte {
		enc := coders.NewEncoder()
		enc.Varint(uint64(v))
		return enc.Data()
	}
	for i, v := range ms.metrics {
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
			mon.Payload = encVarInt(m.sum.Load())
		case *dataChannelIndex:
			mon.Urn = "beam:metric:data_channel:read_index:v1"
			mon.Type = "beam:metrics:sum_int64:v1"
			mon.Payload = encVarInt(m.index.Load())
		case *pcollectionMetrics:
			labels := map[string]string{
				"PCOLLECTION": g.nodes[m.nodeIdx].protoID(),
			}
			elmMon := &pipepb.MonitoringInfo{
				Urn:     "beam:metric:element_count:v1",
				Type:    "beam:metrics:sum_int64:v1",
				Payload: encVarInt(m.elementCount.Load()),
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
	ms.sampleMu.Lock()
	for edgeID, sample := range ms.samples {
		labels := map[string]string{
			"PTRANSFORM": g.edges[edgeID].protoID(),
		}
		start := &pipepb.MonitoringInfo{
			Urn:     "beam:metric:pardo_execution_time:start_bundle_msecs:v1",
			Type:    "beam:metrics:sum_int64:v1",
			Payload: encVarInt(sample[0].Milliseconds()),
			Labels:  labels,
		}
		process := &pipepb.MonitoringInfo{
			Urn:     "beam:metric:pardo_execution_time:process_bundle_msecs:v1",
			Type:    "beam:metrics:sum_int64:v1",
			Payload: encVarInt(sample[1].Milliseconds()),
			Labels:  labels,
		}
		finish := &pipepb.MonitoringInfo{
			Urn:     "beam:metric:pardo_execution_time:finish_bundle_msecs:v1",
			Type:    "beam:metrics:sum_int64:v1",
			Payload: encVarInt(sample[2].Milliseconds()),
			Labels:  labels,
		}
		total := &pipepb.MonitoringInfo{
			Urn:     "beam:metric:ptransform_execution_time:total_msecs:v1",
			Type:    "beam:metrics:sum_int64:v1",
			Payload: encVarInt((sample[0] + sample[1] + sample[2]).Milliseconds()),
			Labels:  labels,
		}
		mons = append(mons, start, process, finish, total)
	}
	ms.sampleMu.Unlock()
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

type currentSampleState struct {
	phase, transition uint32
	edge              edgeIndex
}

// setState must only be called by the bundle processing goroutine.
func (ms *metricsStore) setState(phase uint8, edgeID edgeIndex) {
	ms.transitions++
	ms.transitions = ms.transitions % 0x3FFF
	ms.storeState(uint32(phase), ms.transitions, uint32(edgeID))
}

func (ms *metricsStore) storeState(phase, transition, edgeID uint32) {
	ms.sample.Store(((phase & 0x3) << 30) ^ ((transition & 0x3FFF) << 16) ^ (edgeID & 0xFFFF))
}

func (ms *metricsStore) curState() currentSampleState {
	packed := ms.sample.Load()

	unphase := packed >> 30
	untransition := (packed >> 16) & 0x3FFF
	unedgeID := packed & 0xFFFF
	return currentSampleState{
		phase:      unphase,
		transition: untransition,
		edge:       edgeIndex(unedgeID),
	}
}

func (ms *metricsStore) startSampling(ctx context.Context, sampleInterval, logInterval time.Duration) {
	go func() {
		tick := time.NewTicker(sampleInterval)
		defer tick.Stop()
		var prev currentSampleState
		var stuck time.Duration
		nextLogTime := logInterval
		for {
			select {
			case <-ms.samplingDone:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				state := ms.curState()
				ms.sampleMu.Lock()
				ms.samples[state.edge][state.phase] += sampleInterval
				ms.sampleMu.Unlock()
				if state == prev {
					stuck += sampleInterval
				}
				if stuck >= nextLogTime {
					fmt.Printf("Operation ongoing in transform %v for at least %v without outputting or completing in state %v\n", state.edge, stuck, state.phase)
					nextLogTime += logInterval
				}
				prev = state
			}
		}
	}()
}

func (mets *metricsStore) stopSampling() {
	close(mets.samplingDone)
}
