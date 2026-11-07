package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/pipeline"
	"streamforge/internal/telemetry"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Quarantiner interface {
	Handle(context.Context, *kgo.Record) error
}

type Consumer struct {
	quarantine Quarantiner
	client     *kgo.Client
	config     config.Consumer
	processor  pipeline.Processor
	metrics    *telemetry.ConsumerMetrics
	tracer     trace.Tracer
	logger     *slog.Logger
	hook       pipeline.Hook
	assigned   atomic.Int64
	running    atomic.Bool
}

func NewConsumer(c config.Consumer, sink pipeline.Sink, m *telemetry.ConsumerMetrics, t trace.Tracer, logger *slog.Logger, hook pipeline.Hook) (*Consumer, error) {
	consumer := &Consumer{config: c, metrics: m, tracer: t, logger: logger, hook: hook}
	consumer.processor = pipeline.Processor{Sink: sink, Attempts: c.InsertAttempts, AttemptTimeout: c.InsertTimeout, Hook: hook, OnRetry: func() { m.Retries.Inc() }}
	var owned sync.Map
	assignment := func(action string, partitions map[string][]int32) {
		m.Rebalances.Inc()
		for topic, list := range partitions {
			for _, partition := range list {
				label := strconv.Itoa(int(partition))
				if action == "assigned" {
					if _, loaded := owned.LoadOrStore(partition, true); !loaded {
						consumer.assigned.Add(1)
					}
					m.Assigned.WithLabelValues(label).Set(1)
				} else {
					if _, loaded := owned.LoadAndDelete(partition); loaded {
						consumer.assigned.Add(-1)
					}
					m.Assigned.WithLabelValues(label).Set(0)
					m.Lag.DeleteLabelValues(label)
					m.Committed.DeleteLabelValues(label)
				}
			}
			logger.Info("consumer assignment", "service", "consumer", "group", c.Group, "action", action, "topic", topic, "partitions", list)
		}
	}
	client, err := NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ClientID("streamforge-consumer"), kgo.ConsumerGroup(c.Group), kgo.ConsumeTopics(c.Topic),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.RebalanceTimeout(c.RebalanceTimeout), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(2*time.Second),
		kgo.FetchMaxWait(c.FlushInterval), kgo.FetchMaxBytes(2<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.MaxConcurrentFetches(1),
		kgo.DialTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.RetryTimeout(c.CommitTimeout),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { assignment("assigned", p) }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { assignment("revoked", p) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { assignment("lost", p) }),
	)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}
	consumer.client = client
	return consumer, nil
}
func (c *Consumer) Assigned() int64 { return c.assigned.Load() }
func (c *Consumer) Running() bool   { return c.running.Load() }
func (c *Consumer) Close()          { c.client.CloseAllowingRebalance() }

func (c *Consumer) Run(ctx context.Context) error {
	c.running.Store(true)
	defer c.running.Store(false)
	for ctx.Err() == nil {
		if err := c.epoch(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.metrics.Failures.Inc()
			return err
		}
	}
	return nil
}
func (c *Consumer) epoch(ctx context.Context) error {
	// No worker survives this epoch. Rebalances are allowed only after all
	// workers/commits stop, including every error and cancellation return.
	defer c.client.AllowRebalance()
	collect, cancel := context.WithTimeout(ctx, c.config.FlushInterval)
	defer cancel()
	partitions := map[int32][]*kgo.Record{}
	highwater := map[int32]int64{}
	count, bufferedBytes := 0, 0
	maxPoll := c.config.BatchRecords * c.config.Workers
	for count < maxPoll && bufferedBytes < c.config.BatchBytes && collect.Err() == nil {
		fetches := c.client.PollRecords(collect, maxPoll-count)
		for _, problem := range fetches.Errors() {
			if errors.Is(problem.Err, context.DeadlineExceeded) || errors.Is(problem.Err, context.Canceled) {
				continue
			}
			return fmt.Errorf("Kafka fetch %s/%d: %w", problem.Topic, problem.Partition, problem.Err)
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) { highwater[p.Partition] = p.HighWatermark })
		fetches.EachRecord(func(record *kgo.Record) {
			partitions[record.Partition] = append(partitions[record.Partition], record)
			count++
			bufferedBytes += len(record.Value) + len(record.Key) + 256
			for _, header := range record.Headers {
				bufferedBytes += len(header.Key) + len(header.Value)
			}
		})
	}
	cancel()
	if ctx.Err() != nil || count == 0 {
		return nil
	}
	work, cancelWork := context.WithTimeout(ctx, c.config.ProcessTimeout)
	defer cancelWork()
	type result struct {
		last *kgo.Record
		err  error
	}
	completed := make(chan result, len(partitions))
	slots := make(chan struct{}, c.config.Workers)
	var workers sync.WaitGroup
	for partition := range partitions {
		if partition < 0 || partition >= 12 {
			return errors.New("unexpected partition outside 12-partition contract")
		}
	}
	for partition, records := range partitions {
		c.metrics.Lag.WithLabelValues(strconv.Itoa(int(partition))).Set(float64(max(0, highwater[partition]-records[0].Offset)))
		workers.Add(1)
		go func(records []*kgo.Record) {
			defer workers.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-work.Done():
				completed <- result{err: work.Err()}
				return
			}
			last, err := c.processPartition(work, records)
			completed <- result{last: last, err: err}
		}(records)
	}
	workers.Wait()
	close(completed)
	var first error
	var safe []*kgo.Record
	for result := range completed {
		if result.err != nil && first == nil {
			first = result.err
		}
		if result.last != nil {
			safe = append(safe, result.last)
		}
	}
	sort.Slice(safe, func(i, j int) bool { return safe[i].Partition < safe[j].Partition })
	if len(safe) > 0 && ctx.Err() == nil {
		commit, cancel := context.WithTimeout(ctx, c.config.CommitTimeout)
		defer cancel()
		if c.hook != nil {
			if err := c.hook(commit, "before_commit", pipeline.Batch{}); err != nil {
				return err
			}
		}
		if err := c.client.CommitRecords(commit, safe...); err != nil {
			return fmt.Errorf("commit persisted partition offsets: %w", err)
		}
		for _, last := range safe {
			label := strconv.Itoa(int(last.Partition))
			c.metrics.Committed.WithLabelValues(label).Set(float64(last.Offset + 1))
			c.metrics.Lag.WithLabelValues(label).Set(float64(max(0, highwater[last.Partition]-last.Offset-1)))
			c.logger.Info("partition persisted and committed", "service", "consumer", "group", c.config.Group, "topic", last.Topic, "partition", last.Partition, "next_offset", last.Offset+1)
		}
	}
	return first
}

// SetQuarantiner must be called before Run; tests may omit it to verify fail-closed behavior.
func (c *Consumer) SetQuarantiner(q Quarantiner) { c.quarantine = q }
func (c *Consumer) processPartition(ctx context.Context, records []*kgo.Record) (*kgo.Record, error) {
	var last *kgo.Record
	var run []*kgo.Record
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		values := make([]pipeline.Record, len(run))
		for i, r := range run {
			values[i] = pipeline.Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Epoch: r.LeaderEpoch, Timestamp: r.Timestamp, Value: r.Value}
			for _, h := range r.Headers {
				if h.Key == "traceparent" {
					values[i].TraceParent = string(h.Value)
				}
			}
		}
		batch, err := pipeline.NewBatch(values, c.config.BatchBytes)
		if err != nil {
			return err
		}
		traceContext := telemetry.Propagator().Extract(ctx, propagation.MapCarrier{"traceparent": values[0].TraceParent})
		traceContext, span := c.tracer.Start(traceContext, "consumer.partition_insert", trace.WithSpanKind(trace.SpanKindConsumer))
		defer span.End()
		start := time.Now()
		c.metrics.BatchRows.Observe(float64(len(run)))
		err = c.processor.Process(traceContext, batch)
		c.metrics.InsertDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			return err
		}
		c.metrics.Batches.Inc()
		last = run[len(run)-1]
		run = nil
		return nil
	}
	for _, r := range records {
		if r.Topic != c.config.Topic {
			return last, errors.New("unexpected Kafka topic")
		}
		if _, err := domain.DecodeStoredEvent(r.Value); err != nil {
			if err := flush(); err != nil {
				return last, err
			}
			if c.quarantine == nil {
				return last, fmt.Errorf("poison at %s/%d/%d; no DLQ handler; source uncommitted", r.Topic, r.Partition, r.Offset)
			}
			if err := c.quarantine.Handle(ctx, r); err != nil {
				c.metrics.DLQFailures.Inc()
				return last, err
			}
			c.metrics.DLQPublished.Inc()
			last = r
		} else {
			run = append(run, r)
			if len(run) >= c.config.BatchRecords {
				if err := flush(); err != nil {
					return last, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return last, err
	}
	return last, nil
}

// Pause/Resume bound controlled backlog experiments without resetting group offsets.
func (c *Consumer) Pause()  { c.client.PauseFetchTopics(c.config.Topic) }
func (c *Consumer) Resume() { c.client.ResumeFetchTopics(c.config.Topic) }
