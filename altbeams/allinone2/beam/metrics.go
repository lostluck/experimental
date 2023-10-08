package beam

type metricsStore struct {
	metrics []any

	metricNames map[int]string
}

func (ms *metricsStore) initMetric(name string, v any) int {
	id := len(ms.metrics)
	ms.metrics = append(ms.metrics, v)
	ms.metricNames[id] = name
	return id
}

func (ms *metricsStore) counters() map[string]int64 {
	counters := map[string]int64{}
	for i, m := range ms.metrics {
		switch m := m.(type) {
		case *int64Sum:
			counters[ms.metricNames[i]] = m.sum
		}
	}
	return counters
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
	metricsStore() *metricsStore
}

func (c *Counter) Inc(dfc metricSource, diff int64) {
	ms := dfc.metricsStore()
	if c.index == 0 {
		c.index = ms.initMetric(c.name, &int64Sum{})
	}
	ms.metrics[c.index].(*int64Sum).add(diff)
}
