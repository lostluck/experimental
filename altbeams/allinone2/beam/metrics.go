package beam

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
)

type metricsStore struct {
	metMu   sync.Mutex // Guards metrics list.
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
	ms.metMu.Lock()
	defer ms.metMu.Unlock()
	id := len(ms.metrics)
	ms.metrics = append(ms.metrics, v)
	ms.metricNames[id] = metricLabels{Ptransform: transform, Namespace: "user", Name: name}
	return id
}

func (ms *metricsStore) MonitoringInfos(logger *slog.Logger, g *graph) []*pipepb.MonitoringInfo {
	var mons []*pipepb.MonitoringInfo

	encVarInt := func(v int64) []byte {
		enc := coders.NewEncoder()
		enc.Varint(uint64(v))
		return enc.Data()
	}
	ms.metMu.Lock()
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
		case *int64Dist:
			mon.Urn = "beam:metric:user:distribution_int64:v1"
			mon.Type = "beam:metrics:distribution_int64:v1"

			enc := coders.NewEncoder()
			m.mu.Lock()
			enc.Varint(uint64(m.count))
			enc.Varint(uint64(m.sum))
			enc.Varint(uint64(m.min))
			enc.Varint(uint64(m.max))
			m.mu.Unlock()
			mon.Payload = enc.Data()
		case *dataChannelIndex:
			mon.Urn = "beam:metric:data_channel:read_index:v1"
			mon.Type = "beam:metrics:sum_int64:v1"
			m.mu.Lock()
			index := m.index
			// Should this be one less? We're starting at 0 right now.
			m.mu.Unlock()
			mon.Payload = encVarInt(index)
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
			mon = &pipepb.MonitoringInfo{
				Urn:     "beam:metric:sampled_byte_size:v1",
				Type:    "beam:metrics:distribution_int64:v1",
				Payload: enc.Data(),
				Labels:  labels,
			}
		default:
			if i == 0 {
				// 0 is the uninitialized value so we skip it
				continue
			}
			typ := "<nil>"
			if m != nil {
				typ = reflect.TypeOf(m).String()
			}
			logger.Warn("unknown metric type in SDK", slog.Int("index", i), slog.String("type", typ), slog.Any("labels", labels))
		}
		mons = append(mons, mon)
	}
	ms.metMu.Unlock()
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

type int64Dist struct {
	mu                   sync.Mutex
	sum, count, min, max int64
}

func (m *int64Dist) update(val int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
	m.sum += val
	m.min = min(m.min, val)
	m.max = max(m.max, val)
}

type dataChannelIndex struct {
	transform   string
	metricIndex int

	mu           sync.Mutex
	index, split int64
}

// incrementIndexAndCheckSplit increments DataSource.index by one and checks if
// the caller should abort further element processing, and finish the bundle.
// Returns true if the new value of index is greater than or equal to the split
// index, and false otherwise.
func (c *dataChannelIndex) IncrementAndCheckSplit(dfc Metrics) bool {
	if c.metricIndex == 0 {
		ms := dfc.metricsStore()
		ms.metMu.Lock()
		c.metricIndex = len(ms.metrics)
		ms.metrics = append(ms.metrics, c)
		ms.metricNames[c.metricIndex] = metricLabels{Ptransform: c.transform}
		ms.metMu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	return c.index >= c.split
}

type metricscommon struct {
	index           int
	namespace, name string
}

func (mc *metricscommon) setName(namepsace, name string) {
	mc.namespace = namepsace
	mc.name = name
}

type metricNamer interface {
	setName(namespace, name string)
}

// Metrics implementations enable users to update beam metrics for a job,
// to be aggregated by the runner.
//
// Metrics are recorded as a tuple of the transform and the metric in question,
// so the same metric name used in different transforms are considered distinct.
//
// The 'DFC' type implements Metrics and can be used for standard DoFns.
// Other contexts where user metrics are appropriate may also have a paremeter that
// implement Metrics.
type Metrics interface {
	transformID() string
	metricsStore() *metricsStore
}

// CounterInt64 represents a int64 counter metric.
type CounterInt64 struct {
	beamMixin

	metricscommon
}

func (c *CounterInt64) Inc(dfc Metrics, diff int64) {
	// TODO determine if there's a way to get this to be inlined.
	ms := dfc.metricsStore()
	if c.index == 0 {
		c.index = ms.initMetric(dfc.transformID(), c.name, &int64Sum{})
	}
	ms.metrics[c.index].(*int64Sum).add(diff)
}

type DistributionInt64 struct {
	beamMixin

	metricscommon
}

func (c *DistributionInt64) Update(dfc Metrics, val int64) {
	ms := dfc.metricsStore()
	if c.index == 0 {
		c.index = ms.initMetric(dfc.transformID(), c.name, &int64Dist{min: math.MaxInt64, max: math.MinInt64})
	}
	ms.metrics[c.index].(*int64Dist).update(val)
}

type pcollectionMetrics struct {
	nodeIdx nodeIndex

	elementCount  atomic.Int64
	nextSampleIdx int64

	sampleMu                                     sync.Mutex
	sampleCount, sampleSum, sampleMin, sampleMax int64
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
	// If a method is inlinable with an internal nil check, it's
	// worth inlining the nil check. Otherwise it's better to
	// "outline" the nil check to avoid unnecessary function call
	// overhead.
	if ms == nil {
		return
	}
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
