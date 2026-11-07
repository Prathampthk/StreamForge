package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type QueryMetrics struct {
	Registry                   *prometheus.Registry
	Requests                   *prometheus.CounterVec
	Duration, DBDuration       *prometheus.HistogramVec
	RefreshAge, RefreshHealthy *prometheus.GaugeVec
	Inflight                   prometheus.Gauge
}

func NewQueryMetrics() *QueryMetrics {
	m := &QueryMetrics{Registry: prometheus.NewRegistry(),
		Requests:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_query_requests_total", Help: "Query requests by fixed endpoint/outcome."}, []string{"kind", "outcome"}),
		Duration:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "streamforge_query_duration_seconds", Help: "End-to-end server query duration.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5}}, []string{"kind", "outcome"}),
		DBDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "streamforge_clickhouse_query_duration_seconds", Help: "Bounded ClickHouse HTTP request duration.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2}}, []string{"operation", "outcome"}),
		RefreshAge:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamforge_snapshot_age_seconds", Help: "Age of last observed successful refresh; not event freshness or a Kafka watermark."}, []string{"kind"}),
		RefreshHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamforge_snapshot_healthy", Help: "Last observed refresh health; updated on queries/readiness."}, []string{"kind"}),
		Inflight:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "streamforge_query_inflight_requests", Help: "Admitted query/readiness requests."})}
	m.Registry.MustRegister(m.Requests, m.Duration, m.DBDuration, m.RefreshAge, m.RefreshHealthy, m.Inflight)
	return m
}
func (m *QueryMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2})
}
