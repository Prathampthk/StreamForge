//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/trace/noop"
	"streamforge/internal/broker"
	"streamforge/internal/domain"
	"streamforge/internal/ingest"
	"streamforge/internal/telemetry"
)

func TestReplicatedKafkaAcknowledgments(t *testing.T) {
	if os.Getenv("STREAMFORGE_RUN_RF3") != "1" {
		t.Skip("explicit owned three-broker drill only")
	}
	directory := os.Getenv("RF3_RESULTS")
	if directory == "" {
		t.Fatal("RF3_RESULTS required")
	}
	write := func(name string, value any) {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path+".tmp", raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(name string) {
		eventually(t, 90*time.Second, "host barrier "+name, func() bool { _, err := os.Stat(filepath.Join(directory, name)); return err == nil })
	}
	c := configuration(t)
	c.MaxBatchEvents = 200 // Valid configured batch limit; this drill sends 120.
	admin, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.RetryTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	metadata := func(isr int) map[int32]int32 {
		var leaders map[int32]int32
		eventually(t, 45*time.Second, fmt.Sprintf("all twelve partitions RF3 ISR%d", isr), func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r := kmsg.NewPtrMetadataRequest()
			r.AllowAutoTopicCreation = false
			r.Topics = []kmsg.MetadataRequestTopic{{Topic: &c.Topic}}
			m, err := r.RequestWith(ctx, admin)
			if err != nil || len(m.Topics) != 1 || m.Topics[0].ErrorCode != 0 || len(m.Topics[0].Partitions) != 12 {
				return false
			}
			leaders = map[int32]int32{}
			for _, p := range m.Topics[0].Partitions {
				if p.ErrorCode != 0 || p.Leader < 0 || len(p.Replicas) != 3 || len(p.ISR) != isr {
					return false
				}
				leaders[p.Partition] = p.Leader
			}
			return true
		})
		return leaders
	}
	metadata(3)
	producer, err := broker.New(c, telemetry.NewMetrics(), noop.NewTracerProvider().Tracer("rf3"))
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			producer.Close()
		}
	}()
	server := httptest.NewServer(ingest.New(c, producer, telemetry.NewMetrics(), noop.NewTracerProvider().Tracer("rf3"), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer server.Close()
	accepted := map[string]string{}
	unknown := map[string]bool{}
	journal, err := os.OpenFile(filepath.Join(directory, "accepted.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	publish := func(stage string, count, want int) {
		events := make([]domain.Event, count)
		for i := range events {
			events[i] = domain.Event{SchemaVersion: 1, EventID: uuid.NewString(), TenantID: c.Tenant, UserID: fmt.Sprintf("rf3_user_%d", i), EventType: "page_view", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
		}
		raw, err := json.Marshal(events)
		if err != nil {
			t.Fatal(err)
		}
		r, err := http.NewRequest("POST", server.URL+"/v1/events/batch", strings.NewReader(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+c.APIKey)
		r.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != want {
			t.Fatalf("%s status=%d want=%d body=%s read=%v close=%v", stage, response.StatusCode, want, body, readErr, closeErr)
		}
		if want != 202 {
			for _, e := range events {
				unknown[e.EventID] = true
			}
			return
		}
		var receipt struct {
			Accepted int `json:"accepted"`
			Acks     []struct {
				ID string    `json:"event_id"`
				At time.Time `json:"acknowledged_at"`
			} `json:"acknowledgments"`
		}
		if err := json.Unmarshal(body, &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.Accepted != count || len(receipt.Acks) != count {
			t.Fatal("incomplete acknowledgment receipt")
		}
		acks := map[string]time.Time{}
		for _, ack := range receipt.Acks {
			if ack.At.IsZero() || !acks[ack.ID].IsZero() {
				t.Fatal("invalid ack timestamp or duplicate receipt")
			}
			acks[ack.ID] = ack.At
		}
		for _, e := range events {
			if acks[e.EventID].IsZero() {
				t.Fatal("acknowledgment ID mismatch")
			}
			payload, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			accepted[e.EventID] = string(payload)
			if err := json.NewEncoder(journal).Encode(map[string]any{"stage": stage, "event": e, "acknowledged_at": acks[e.EventID]}); err != nil {
				t.Fatal(err)
			}
		}
		if err := journal.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	publish("baseline", 120, 202)
	write("baseline.json", map[string]any{"accepted": len(accepted), "leaders": metadata(3)})
	wait("reject.go")
	metadata(2)
	publish("min_isr_3_one_down", 12, 503)
	write("rejected.json", map[string]any{"status": 503, "delivery_unknown": len(unknown)})
	wait("one-down.go")
	metadata(2)
	publish("min_isr_2_one_down", 120, 202)
	write("one-down.json", map[string]any{"accepted": len(accepted)})
	wait("restored.go")
	metadata(3)
	publish("restored", 120, 202)
	producer.Close()
	closed = true
	write("restored.json", map[string]any{"accepted": len(accepted), "leaders": metadata(3)})
	wait("audit.go")
	metadata(2)
	assignments := map[int32]kgo.Offset{}
	for i := int32(0); i < 12; i++ {
		assignments[i] = kgo.NewOffset().AtStart()
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{c.Topic: assignments}), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	observed := map[string]int{}
	unknownObserved := map[string]bool{}
	partitions := map[int32]bool{}
	// Every successful acknowledgment must be readable; failed HTTP attempts
	// remain unknown and may exist. Continue through each partition's high water.
	drained := map[int32]bool{}
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) && (len(drained) < 12 || len(observed) < len(accepted)) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := reader.PollFetches(ctx)
		cancel()
		for _, problem := range fetches.Errors() {
			if problem.Err != context.DeadlineExceeded && problem.Err != context.Canceled {
				t.Fatal(problem.Err)
			}
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			// Canceled polls include a synthetic error partition (-1); the
			// error loop above handles it, and it is not a topic high water.
			if p.Err != nil {
				return
			}
			if p.Topic != c.Topic || p.Partition < 0 || p.Partition >= 12 {
				t.Fatalf("unexpected fetched topic/partition: %s/%d", p.Topic, p.Partition)
			}
			if p.HighWatermark == 0 {
				drained[p.Partition] = true
			}
			for _, record := range p.Records {
				var e domain.Event
				if err := json.Unmarshal(record.Value, &e); err != nil {
					t.Fatal(err)
				}
				if payload, ok := accepted[e.EventID]; ok {
					if string(record.Value) != payload {
						t.Fatalf("payload mismatch %s", e.EventID)
					}
					observed[e.EventID]++
					partitions[record.Partition] = true
				} else if unknown[e.EventID] {
					unknownObserved[e.EventID] = true
				} else {
					t.Fatal("unexpected event")
				}
				if record.Offset+1 >= p.HighWatermark {
					drained[p.Partition] = true
				}
			}
		})
	}
	if len(drained) != 12 || len(observed) != len(accepted) || len(partitions) != 12 {
		t.Fatalf("drained=%d verified=%d accepted=%d partitions=%d", len(drained), len(observed), len(accepted), len(partitions))
	}
	for id, count := range observed {
		if count != 1 {
			t.Fatalf("duplicate acknowledged ID %s count=%d", id, count)
		}
	}
	write("result.json", map[string]any{"success": true, "accepted": len(accepted), "verified_exact_payloads": len(observed), "missing": 0, "duplicate_acknowledged_ids": 0, "partitions_observed": len(partitions), "unknown_http_events": len(unknown), "unknown_events_observed": len(unknownObserved), "scope": "single Docker VM; broker process SIGKILL and retained-disk restart; no ClickHouse analytics or host/disk-loss guarantee"})
}
