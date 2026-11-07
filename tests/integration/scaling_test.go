//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.opentelemetry.io/otel/trace/noop"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"streamforge/internal/analytics"
	"streamforge/internal/broker"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/pipeline"
	"streamforge/internal/query"
	"streamforge/internal/telemetry"

	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMeasuredConsumerScaling(t *testing.T) {
	if os.Getenv("STREAMFORGE_RUN_SCALING") != "1" {
		t.Skip("explicit performance experiment only")
	}
	c := sinkConfig(t)
	producer, topic := source(t, c)
	c.Topic = topic
	c.Workers = 1
	c.BatchRecords = 1000
	c.BatchBytes = 8 << 20
	tenant := "scaling_" + uuid.NewString()[:8]
	const count = 60000
	records := make([]*kgo.Record, count)
	now := time.Now().UTC().Truncate(time.Minute).Add(time.Second)
	for i := range records {
		event := domain.Event{SchemaVersion: 1, EventID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(tenant+strconv.Itoa(i))).String(), TenantID: tenant, UserID: fmt.Sprintf("u%d", i), EventType: "page_view", OccurredAt: now.Format(time.RFC3339)}
		switch n := i % 100; {
		case n < 60:
		case n < 80:
			event.EventType = "add_to_cart"
		case n < 90:
			event.EventType = "purchase"
			order, currency, amount := fmt.Sprintf("o%d", i), "USD", int64(i%10000+1)
			if i%2 == 1 {
				currency = "EUR"
			}
			event.OrderID = &order
			event.Currency = &currency
			event.AmountCents = &amount
		default:
			event.EventType = "api_request"
			status := uint16(200)
			if i%10 == 0 {
				status = 503
			}
			event.StatusCode = &status
		}
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		records[i] = &kgo.Record{Topic: topic, Partition: int32(i % 12), Key: []byte(tenant + ":" + event.UserID), Value: raw, Timestamp: now}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	sink := analytics.NewSink(c)
	defer sink.Close()
	// Both timed runs see identical already-canonical data: no first-run cache/identity advantage.
	for partition := int32(0); partition < 12; partition++ {
		var values []pipeline.Record
		for _, r := range records {
			if r.Partition == partition {
				values = append(values, pipeline.Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Value: r.Value, Timestamp: r.Timestamp})
			}
		}
		for i := 0; i < len(values); i += 1000 {
			batch, err := pipeline.NewBatch(values[i:min(i+1000, len(values))], 8<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.Insert(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
	}
	q, err := config.QueryFromEnv(func(k string) string {
		if k == "QUERY_API_KEY" {
			return "scaling-local-query-key-at-least-32"
		}
		if k == "QUERY_TENANT" {
			return tenant
		}
		return os.Getenv(k)
	})
	if err != nil {
		t.Fatal(err)
	}
	qm := telemetry.NewQueryMetrics()
	repo := analytics.NewReadRepository(q, qm, noop.NewTracerProvider().Tracer("scale"))
	defer repo.Close()
	server := httptest.NewServer(query.New(q, repo, qm, noop.NewTracerProvider().Tracer("scale"), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer server.Close()
	auditor, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.MaxVersions(kversion.V2_8_0()))
	if err != nil {
		t.Fatal(err)
	}
	defer auditor.Close()
	committed := func(group string) int64 {
		request := kmsg.NewPtrOffsetFetchRequest()
		request.Group = group
		parts := []int32{}
		for i := int32(0); i < 12; i++ {
			parts = append(parts, i)
		}
		request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: parts}}
		requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		response, err := request.RequestWith(requestCtx, auditor)
		if err != nil {
			t.Fatal(err)
		}
		var sum int64
		for _, result := range response.Topics {
			for _, part := range result.Partitions {
				if part.ErrorCode == 0 && part.Offset > 0 {
					sum += part.Offset
				}
			}
		}
		return sum
	}
	cases := []map[string]any{}
	for _, replicas := range []int{1, 2, 3} {
		settings := c
		settings.Group = "scale_" + uuid.NewString()
		var began atomic.Int64
		hook := func(_ context.Context, stage string, _ pipeline.Batch) error {
			if stage == "before_insert" {
				began.CompareAndSwap(0, time.Now().UnixNano())
			}
			return nil
		}
		group := []*instance{}
		for i := 0; i < replicas; i++ {
			consumer, err := broker.NewConsumer(settings, sink, telemetry.NewConsumerMetrics(), noop.NewTracerProvider().Tracer("scale"), slog.New(slog.NewTextHandler(io.Discard, nil)), hook)
			if err != nil {
				t.Fatal(err)
			}
			consumer.Pause()
			runCtx, cancelRun := context.WithCancel(ctx)
			member := &instance{consumer: consumer, cancel: cancelRun, done: make(chan struct{})}
			go func() { member.err = consumer.Run(runCtx); close(member.done) }()
			group = append(group, member)
			t.Cleanup(member.stop)
		}
		eventually(t, 20*time.Second, "all scaling members assigned before processing", func() bool {
			var n int64
			for _, member := range group {
				if member.consumer.Assigned() == 0 {
					return false
				}
				n += member.consumer.Assigned()
			}
			return n == 12
		})
		queryCtx, stopQueries := context.WithCancel(ctx)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var latencies []float64
		queries, failures := 0, 0
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				client := &http.Client{Timeout: 4 * time.Second}
				for queryCtx.Err() == nil {
					end := now.Truncate(time.Minute).Add(time.Minute)
					window := 15 * time.Minute
					if index == 1 {
						window = 24 * time.Hour
					}
					values := url.Values{"tenant_id": {tenant}, "from": {end.Add(-window).Format(time.RFC3339)}, "to": {end.Format(time.RFC3339)}, "interval": {"1m"}}
					request, _ := http.NewRequestWithContext(queryCtx, "GET", server.URL+"/v1/metrics/traffic?"+values.Encode(), nil)
					request.Header.Set("Authorization", "Bearer "+q.APIKey)
					start := time.Now()
					response, err := client.Do(request)
					status := 0
					if err == nil {
						status = response.StatusCode
						_, err = io.Copy(io.Discard, response.Body)
						closeErr := response.Body.Close()
						if closeErr != nil {
							err = closeErr
						}
					}
					mu.Lock()
					queries++
					if err != nil || status != 200 {
						failures++
					} else {
						latencies = append(latencies, time.Since(start).Seconds())
					}
					mu.Unlock()
					select {
					case <-queryCtx.Done():
					case <-time.After(100 * time.Millisecond):
					}
				}
			}(i)
		}
		for _, member := range group {
			member.consumer.Resume()
		}
		eventually(t, 120*time.Second, "scaling backlog fully committed", func() bool { return committed(settings.Group) == count })
		elapsed := float64(time.Now().UnixNano()-began.Load()) / float64(time.Second)
		if began.Load() == 0 {
			t.Fatal("measurement never began")
		}
		stopQueries()
		wg.Wait()
		for _, member := range group {
			member.stop()
		}
		sort.Float64s(latencies)
		var p95 any
		if len(latencies) > 0 {
			p95 = latencies[(len(latencies)-1)*95/100]
		}
		cases = append(cases, map[string]any{"replicas": replicas, "records": count, "seconds": elapsed, "persisted_and_committed_events_per_second": float64(count) / elapsed, "query_count": queries, "query_errors": failures, "query_p95_seconds": p95, "workers_per_instance": 1, "batch_records": 1000, "poll_resolution_seconds": .05})
	}
	speedup := cases[2]["persisted_and_committed_events_per_second"].(float64) / cases[0]["persisted_and_committed_events_per_second"].(float64)
	result := map[string]any{"executed_at": time.Now().UTC(), "topic": topic, "tenant": tenant, "balanced_partitions": 12, "same_prefilled_records": count, "canonical_preseeded_for_both": true, "cases": cases, "speedup_3_over_1": speedup, "target_at_least_2x": speedup >= 2}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "load", "results", "scaling.json")
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
	for i := 0; i < len(records); i += 1000 {
		assertAcceptedIDs(t, c, records[i:min(i+1000, len(records))])
	}
}
