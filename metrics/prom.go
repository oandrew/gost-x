package metrics

import (
	"maps"
	"os"
	"sync"

	"github.com/go-gost/core/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type promMetrics struct {
	host       string
	gauges     map[metrics.MetricName]*prometheus.GaugeVec
	counters   map[metrics.MetricName]*prometheus.CounterVec
	histograms map[metrics.MetricName]*prometheus.HistogramVec
}

// NewMetrics returns a Prometheus-based Metrics implementation. All metrics are
// registered with the given registry. Use nil to register with the default
// Prometheus registry.
func NewMetrics(reg *prometheus.Registry) metrics.Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer.(*prometheus.Registry)
	}

	host, _ := os.Hostname()
	m := &promMetrics{
		host: host,
		gauges: map[metrics.MetricName]*prometheus.GaugeVec{
			MetricServicesGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: string(MetricServicesGauge),
					Help: "Current number of services",
				},
				[]string{"host"}),
			MetricServiceRequestsInFlightGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: string(MetricServiceRequestsInFlightGauge),
					Help: "Current in-flight requests",
				},
				[]string{"host", "service", "client"}),
			MetricPrewarmAvailableGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: string(MetricPrewarmAvailableGauge),
					Help: "Current number of idle prewarmed node connections",
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveAvailableGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: string(MetricPredictiveAvailableGauge),
					Help: "Current number of idle predictive CONNECT tunnels",
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveHotEndpointsGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: string(MetricPredictiveHotEndpointsGauge),
					Help: "Current number of hot predictive CONNECT endpoints",
				},
				[]string{"host", "hop", "node"}),
		},
		counters: map[metrics.MetricName]*prometheus.CounterVec{
			MetricServiceRequestsCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricServiceRequestsCounter),
					Help: "Total number of requests",
				},
				[]string{"host", "service", "client"}),
			MetricServiceTransferInputBytesCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricServiceTransferInputBytesCounter),
					Help: "Total service input data transfer size in bytes",
				},
				[]string{"host", "service", "client"}),
			MetricServiceTransferOutputBytesCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricServiceTransferOutputBytesCounter),
					Help: "Total service output data transfer size in bytes",
				},
				[]string{"host", "service", "client"}),
			MetricServiceHandlerErrorsCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricServiceHandlerErrorsCounter),
					Help: "Total service handler errors",
				},
				[]string{"host", "service", "client"}),
			MetricChainErrorsCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricChainErrorsCounter),
					Help: "Total chain errors",
				},
				[]string{"host", "chain", "node"}),
			MetricRecorderRecordsCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricRecorderRecordsCounter),
					Help: "Total records written by recorder",
				},
				[]string{"host", "recorder"}),
			MetricPrewarmHitsCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPrewarmHitsCounter),
					Help: "Total prewarmed node connection pool hits",
				},
				[]string{"host", "hop", "node"}),
			MetricPrewarmMissesCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPrewarmMissesCounter),
					Help: "Total prewarmed node connection pool misses",
				},
				[]string{"host", "hop", "node"}),
			MetricPrewarmStaleCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPrewarmStaleCounter),
					Help: "Total stale prewarmed node connections",
				},
				[]string{"host", "hop", "node"}),
			MetricPrewarmFailuresCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPrewarmFailuresCounter),
					Help: "Total failed prewarmed node connection establishments",
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveHitsCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPredictiveHitsCounter),
					Help: "Total predictive CONNECT tunnel pool hits",
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveMissesCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPredictiveMissesCounter),
					Help: "Total predictive CONNECT tunnel pool misses",
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveStaleCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPredictiveStaleCounter),
					Help: "Total stale predictive CONNECT tunnels",
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveFailuresCounter: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: string(MetricPredictiveFailuresCounter),
					Help: "Total failed predictive CONNECT tunnel establishments",
				},
				[]string{"host", "hop", "node"}),
		},
		histograms: map[metrics.MetricName]*prometheus.HistogramVec{
			MetricServiceRequestsDurationObserver: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name: string(MetricServiceRequestsDurationObserver),
					Help: "Distribution of request latencies",
					Buckets: []float64{
						.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 30, 60,
					},
				},
				[]string{"host", "service"}),
			MetricNodeConnectDurationObserver: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name: string(MetricNodeConnectDurationObserver),
					Help: "Distribution of chain node connect latencies",
					Buckets: []float64{
						.01, .05, .1, .25, .5, 1, 1.5, 2, 5, 10, 15, 30, 60,
					},
				},
				[]string{"host", "chain", "node"}),
			MetricPrewarmEstablishDurationObserver: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name: string(MetricPrewarmEstablishDurationObserver),
					Help: "Distribution of background node connection establishment latencies",
					Buckets: []float64{
						.01, .05, .1, .25, .5, 1, 1.5, 2, 5, 10, 15, 30, 60,
					},
				},
				[]string{"host", "hop", "node"}),
			MetricPredictiveEstablishDurationObserver: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name: string(MetricPredictiveEstablishDurationObserver),
					Help: "Distribution of background TCP, TLS, and CONNECT establishment latencies",
					Buckets: []float64{
						.01, .05, .1, .25, .5, 1, 1.5, 2, 5, 10, 15, 30, 60,
					},
				},
				[]string{"host", "hop", "node"}),
		},
	}
	for k := range m.gauges {
		reg.MustRegister(m.gauges[k])
	}
	for k := range m.counters {
		reg.MustRegister(m.counters[k])
	}
	for k := range m.histograms {
		reg.MustRegister(m.histograms[k])
	}

	return m
}

func (m *promMetrics) Gauge(name metrics.MetricName, labels metrics.Labels) metrics.Gauge {
	v, ok := m.gauges[name]
	if !ok {
		return nopGauge
	}
	plabels := prometheus.Labels{}
	maps.Copy(plabels, labels)
	plabels["host"] = m.host
	return v.With(plabels)
}

func (m *promMetrics) Counter(name metrics.MetricName, labels metrics.Labels) metrics.Counter {
	v, ok := m.counters[name]
	if !ok {
		return nopCounter
	}
	plabels := prometheus.Labels{}
	maps.Copy(plabels, labels)
	plabels["host"] = m.host
	return v.With(plabels)
}

func (m *promMetrics) Observer(name metrics.MetricName, labels metrics.Labels) metrics.Observer {
	v, ok := m.histograms[name]
	if !ok {
		return nopObserver
	}
	plabels := prometheus.Labels{}
	maps.Copy(plabels, labels)
	plabels["host"] = m.host
	return v.With(plabels)
}

// ServiceStatusFunc returns the number of services in each state, keyed by
// state name (e.g. "running", "ready", "failed", "closed").
type ServiceStatusFunc func() map[string]float64

var (
	serviceStatusCollector     *serviceStatusGaugeCollector
	serviceStatusCollectorOnce sync.Once
)

// RegisterServiceStatusFunc registers a callback invoked at each scrape to
// collect the aggregate service status counts, exposed as MetricServiceStatusGauge.
func RegisterServiceStatusFunc(fn ServiceStatusFunc) {
	serviceStatusCollectorOnce.Do(func() {
		host, _ := os.Hostname()
		serviceStatusCollector = &serviceStatusGaugeCollector{
			desc: prometheus.NewDesc(
				string(MetricServiceStatusGauge),
				"Number of services in each state",
				[]string{"host", "state"},
				nil,
			),
			host: host,
			fn:   fn,
		}
		defaultRegistry.MustRegister(serviceStatusCollector)
	})
}

// serviceStatusGaugeCollector implements prometheus.Collector, emitting the
// MetricServiceStatusGauge counts from fn at each scrape.
type serviceStatusGaugeCollector struct {
	desc *prometheus.Desc
	host string
	fn   ServiceStatusFunc
}

func (c *serviceStatusGaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *serviceStatusGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	for state, count := range c.fn() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, count, c.host, state)
	}
}
