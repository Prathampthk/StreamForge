package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func record(offset int64, timestamp string) Record {
	return Record{Topic: "events", Partition: 2, Offset: offset, Timestamp: time.Date(2026, 11, 6, 17, 0, 0, 0, time.UTC), Value: []byte(`{"schema_version":1,"event_id":"018f0f5b-c1c4-7b8f-a7a9-2d11e8fa37c2","tenant_id":"demo","user_id":"u","event_type":"purchase","order_id":"o","amount_cents":999,"currency":"USD","occurred_at":"` + timestamp + `"}`)}
}
func TestBatchCanonicalIdentityAndOrder(t *testing.T) {
	first, err := NewBatch([]Record{record(1, "2026-11-06T11:00:00-06:00")}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	again, err := NewBatch([]Record{record(1, "2026-11-06T11:00:00-06:00")}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	utc, err := NewBatch([]Record{record(4, "2026-11-06T17:00:00Z")}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != again.ID || string(first.Body) != string(again.Body) {
		t.Fatal("non-deterministic retry batch")
	}
	if first.ID == utc.ID || first.Rows[0].Payload != utc.Rows[0].Payload {
		t.Fatal("logical identity depends on Kafka coordinates or timestamp spelling")
	}
	var logical map[string]any
	if err := json.Unmarshal([]byte(first.Rows[0].Payload), &logical); err != nil {
		t.Fatal(err)
	}
	if logical["occurred_at"] != "2026-11-06T17:00:00Z" {
		t.Fatal(logical)
	}
	// Kafka offset gaps can be legal; only delivered-record order is required.
	if _, err := NewBatch([]Record{record(1, "2026-11-06T17:00:00Z"), record(4, "2026-11-06T17:00:00Z")}, 16384); err != nil {
		t.Fatal(err)
	}
	for _, records := range [][]Record{nil, {record(2, "2026-11-06T17:00:00Z"), record(1, "2026-11-06T17:00:00Z")}, {record(1, "2026-11-06T17:00:00Z"), record(1, "2026-11-06T17:00:00Z")}} {
		if _, err := NewBatch(records, 16384); err == nil {
			t.Fatal("accepted empty or regressing offsets")
		}
	}
	poison := record(1, "2026-11-06T17:00:00Z")
	poison.Value = []byte(`{"bad":true}`)
	if _, err := NewBatch([]Record{poison}, 8192); err == nil || !strings.Contains(err.Error(), "offset will not commit") {
		t.Fatal(err)
	}
	// Stored backlog age must not be revalidated against the current wall clock.
	if _, err := NewBatch([]Record{record(1, "2020-01-01T00:00:00Z")}, 8192); err != nil {
		t.Fatal(err)
	}
}

type sinkFunc func(context.Context, Batch) error

func (f sinkFunc) Insert(ctx context.Context, b Batch) error { return f(ctx, b) }
func TestAmbiguousInsertRetriesIdenticalBatch(t *testing.T) {
	b, err := NewBatch([]Record{record(3, "2026-11-06T17:00:00Z")}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	original := string(b.Body)
	p := Processor{Attempts: 3, AttemptTimeout: time.Second, Sink: sinkFunc(func(ctx context.Context, received Batch) error {
		attempts++
		if received.ID != b.ID || string(received.Body) != original {
			t.Fatal("retry mutated batch")
		}
		if attempts == 1 {
			return errors.New("response lost after actual write")
		}
		return nil
	})}
	if err := p.Process(context.Background(), b); err != nil || attempts != 2 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	attempts = 0
	p.Sink = sinkFunc(func(context.Context, Batch) error { attempts++; return errors.New("outage") })
	if err := p.Process(context.Background(), b); err == nil || attempts != 3 {
		t.Fatalf("retry budget: %d %v", attempts, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Process(canceled, b); err == nil {
		t.Fatal("cancellation ignored")
	}
}
