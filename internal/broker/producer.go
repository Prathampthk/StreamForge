// Package broker adapts the pinned Kafka client without exposing it to handlers.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/telemetry"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Producer struct {
	client  *kgo.Client
	topic   string
	metrics *telemetry.Metrics
	tracer  trace.Tracer
}

func New(c config.Config, m *telemetry.Metrics, tracer trace.Tracer) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.Brokers...), kgo.ClientID("streamforge-ingest"),
		kgo.RequiredAcks(kgo.AllISRAcks()),                   // Idempotence remains enabled; incompatible settings fail construction.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // Kafka-compatible murmur2 for nonempty keys.
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerBatchMaxBytes(1<<20), kgo.ProducerLinger(2*time.Millisecond),
		kgo.MaxBufferedRecords(c.ProducerRecords), kgo.MaxBufferedBytes(c.ProducerBytes),
		kgo.RecordDeliveryTimeout(c.PublishTimeout), kgo.RecordRetries(10),
		kgo.ProduceRequestTimeout(2*time.Second), kgo.RequestTimeoutOverhead(time.Second),
		kgo.DialTimeout(time.Second), kgo.RetryTimeout(c.PublishTimeout),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			ceiling := 25 * time.Millisecond * time.Duration(1<<min(max(attempt, 0), 5))
			return ceiling/2 + time.Duration(rand.Int64N(int64(ceiling/2)))
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Producer{client: client, topic: c.Topic, metrics: m, tracer: tracer}, nil
}

func (p *Producer) Publish(ctx context.Context, events []domain.Event, correlation string) error {
	ctx, span := p.tracer.Start(ctx, "kafka.publish", trace.WithSpanKind(trace.SpanKindProducer))
	// The producer promises own this span even if the HTTP waiter leaves early.
	records := make([]*kgo.Record, len(events))
	headers := propagation.MapCarrier{}
	telemetry.Propagator().Inject(ctx, headers)
	for i, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			span.End()
			return fmt.Errorf("encode event: %w", err)
		}
		record := &kgo.Record{Topic: p.topic, Key: []byte(event.TenantID + ":" + event.UserID), Value: value,
			Headers: []kgo.RecordHeader{{Key: "schema_version", Value: []byte("1")}, {Key: "correlation_id", Value: []byte(correlation)}, {Key: "produced_at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))}}}
		if parent := headers["traceparent"]; parent != "" {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(parent)})
		}
		records[i] = record
	}
	results := make(chan error, len(records))
	// Atomic completion also handles immediate buffer-rejection callbacks.
	var remaining atomic.Int64
	remaining.Store(int64(len(records)))
	if len(records) == 0 {
		span.End()
		return nil
	}
	for index, record := range records {
		logicalID := events[index].EventID
		started := time.Now()
		p.metrics.ProducerInflight.Inc()
		p.client.TryProduce(ctx, record, func(_ *kgo.Record, err error) {
			outcome := "acknowledged"
			if err == nil {
				telemetry.ObserveAck(ctx, logicalID)
			}
			if err != nil {
				outcome = "failed"
				class := "broker_error"
				if errors.Is(err, kgo.ErrMaxBuffered) {
					class = "buffer_full"
				} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					class = "timeout_or_cancel"
				}
				p.metrics.PublishErrors.WithLabelValues(class).Inc()
				span.SetStatus(codes.Error, "publish failed")
			}
			p.metrics.Events.WithLabelValues(outcome).Inc()
			p.metrics.PublishLatency.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
			p.metrics.ProducerInflight.Dec()
			if remaining.Add(-1) == 0 {
				span.End()
			}
			results <- err
		})
	}
	var first error
	for range records {
		select {
		case err := <-results:
			if err != nil && first == nil {
				first = err
			}
		case <-ctx.Done():
			return fmt.Errorf("Kafka outcome unresolved: %w", ctx.Err())
		}
	}
	if first != nil {
		return fmt.Errorf("Kafka publish: %w", first)
	}
	return nil
}

func (p *Producer) Ready(ctx context.Context) error {
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &p.topic}}
	response, err := p.client.Request(ctx, request)
	if err != nil {
		return fmt.Errorf("Kafka metadata: %w", err)
	}
	metadata, ok := response.(*kmsg.MetadataResponse)
	if !ok {
		return errors.New("unexpected Kafka metadata response")
	}
	if len(metadata.Topics) != 1 || metadata.Topics[0].ErrorCode != 0 || len(metadata.Topics[0].Partitions) != 12 {
		return errors.New("events topic must exist with 12 partitions")
	}
	for _, partition := range metadata.Topics[0].Partitions {
		if partition.ErrorCode != 0 || partition.Leader < 0 {
			return errors.New("Kafka partition unavailable")
		}
	}
	return nil
}
func (p *Producer) Close() { p.client.Close() }
