// Package telemetry owns process-local metric registries and trace resources.
package telemetry

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Metrics struct {
	PublishErrors    *prometheus.CounterVec
	Registry         *prometheus.Registry
	Requests         *prometheus.CounterVec
	HTTPLatency      *prometheus.HistogramVec
	Events           *prometheus.CounterVec
	PublishLatency   *prometheus.HistogramVec
	ProducerInflight prometheus.Gauge
	HTTPInflight     prometheus.Gauge
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(),
		PublishErrors:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_producer_errors_total", Help: "Finite producer failure classes."}, []string{"class"}),
		Requests:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_ingest_requests_total", Help: "Ingestion requests by finite outcome."}, []string{"outcome"}),
		HTTPLatency:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "streamforge_ingest_request_duration_seconds", Help: "Arrival to final response, including broker wait.", Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 5, 10}}, []string{"outcome"}),
		Events:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_kafka_events_total", Help: "Producer records with resolved broker outcome; includes outcomes after HTTP cancellation."}, []string{"outcome"}),
		PublishLatency:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "streamforge_kafka_publish_duration_seconds", Help: "Producer enqueue through resolved promise.", Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 5, 10}}, []string{"outcome"}),
		ProducerInflight: prometheus.NewGauge(prometheus.GaugeOpts{Name: "streamforge_producer_inflight_records", Help: "Unsettled producer promises."}),
		HTTPInflight:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "streamforge_ingest_inflight_requests", Help: "Admitted ingestion requests."})}
	m.Registry.MustRegister(m.PublishErrors, m.Requests, m.HTTPLatency, m.Events, m.PublishLatency, m.ProducerInflight, m.HTTPInflight)
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2})
}

func NewTracing(ctx context.Context, endpoint string, sample float64) (*sdktrace.TracerProvider, error) {
	return NewServiceTracing(ctx, endpoint, sample, "streamforge-ingest")
}

func NewServiceTracing(ctx context.Context, endpoint string, sample float64, service string) (*sdktrace.TracerProvider, error) {
	options := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sample))), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service)))}
	if endpoint != "" {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(1024)))
	}
	return sdktrace.NewTracerProvider(options...), nil
}
func Tracer(provider trace.TracerProvider) trace.Tracer { return provider.Tracer("streamforge") }
func Propagator() propagation.TextMapPropagator         { return propagation.TraceContext{} }
