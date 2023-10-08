package beam

import (
	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
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

func (ms *metricsStore) MonitoringInfos() []*pipepb.MonitoringInfo {
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
			enc.Varint(uint64(m.sum))
			mon.Payload = enc.Data()
		default:
			// panic(fmt.Sprintf("unknown metric type: %T", m))
		}
		mons = append(mons, mon)
	}
	return mons
}

type int64Sum struct {
	sum int64
}

func (m *int64Sum) add(d int64) {
	m.sum += d
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
