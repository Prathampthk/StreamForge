package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ConsumerMetrics struct {
	DLQPublished, DLQFailures              prometheus.Counter
	Registry                               *prometheus.Registry
	Assigned, Committed, Lag               *prometheus.GaugeVec
	Batches, Retries, Rebalances, Failures prometheus.Counter
	FetchRetries                           prometheus.Counter
	BatchRows, InsertDuration              prometheus.Histogram
}

func NewConsumerMetrics() *ConsumerMetrics {
	m := &ConsumerMetrics{FetchRetries: prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_consumer_fetch_retries_total", Help: "Bounded retries of transient empty polls; no collected records discarded."}), Registry: prometheus.NewRegistry(),
		DLQPublished:   prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_dlq_records_total", Help: "Durably audited quarantine completions, including skipped acknowledged retries."}),
		DLQFailures:    prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_dlq_failures_total", Help: "DLQ publication/audit failures; source not skipped."}),
		Assigned:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamforge_consumer_partition_assigned", Help: "Current ownership; partition labels bounded by 12-topic contract."}, []string{"partition"}),
		Committed:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamforge_consumer_committed_next_offset", Help: "Last confirmed next-offset commit by this instance."}, []string{"partition"}),
		Lag:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamforge_consumer_observed_lag", Help: "High watermark minus confirmed next offset sampled on fetch/commit; may be stale while idle."}, []string{"partition"}),
		Batches:        prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_consumer_batches_total", Help: "Confirmed partition inserts."}),
		Retries:        prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_consumer_insert_failures_total", Help: "Failed insert attempts, including ambiguous results."}),
		Rebalances:     prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_consumer_rebalances_total", Help: "Assignment/revocation/lost callbacks."}),
		Failures:       prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_consumer_epoch_failures_total", Help: "Epochs exiting without committing all polled records."}),
		BatchRows:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "streamforge_consumer_batch_rows", Help: "Rows per partition insert attempt.", Buckets: []float64{1, 8, 32, 64, 128, 256, 512, 1000}}),
		InsertDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "streamforge_consumer_insert_duration_seconds", Help: "Sink processing including bounded retries.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10}})}
	m.Registry.MustRegister(m.FetchRetries, m.DLQPublished, m.DLQFailures, m.Assigned, m.Committed, m.Lag, m.Batches, m.Retries, m.Rebalances, m.Failures, m.BatchRows, m.InsertDuration)
	return m
}
func (m *ConsumerMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2})
}
