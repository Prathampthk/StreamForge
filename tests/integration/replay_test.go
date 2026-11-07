//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"streamforge/internal/analytics"

	"streamforge/internal/domain"
	"streamforge/internal/metadata"
	"streamforge/internal/replay"
	"testing"
	"time"
)

func TestDurableDLQAndBoundedReplay(t *testing.T) {
	c := sinkConfig(t)
	producer, topic := source(t, c)
	_, dlq := source(t, c)
	c.Topic = topic
	store, err := metadata.FromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	quarantine, err := replay.NewQuarantine(c.Brokers, store)
	if err != nil {
		t.Fatal(err)
	}
	defer quarantine.Close()
	quarantine.Topic = dlq
	tenant := "replay_" + uuid.NewString()[:8]
	first := fixture(t, producer, topic, tenant, 1, false)
	id := uuid.NewString()
	bad := domain.Event{SchemaVersion: 99, EventID: id, TenantID: tenant, UserID: "poison_user", EventType: "purchase", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
	order, currency, amount := "repaired_order", "USD", int64(321)
	bad.OrderID = &order
	bad.Currency = &currency
	bad.AmountCents = &amount
	raw, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	poison := &kgo.Record{Topic: topic, Partition: 0, Key: []byte(tenant + ":poison_user"), Value: raw, Timestamp: time.Now()}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}
	last := fixture(t, producer, topic, tenant, 1, false)
	sink := analytics.NewSink(c)
	defer sink.Close()
	worker := start(t, c, sink, nil, quarantine)
	// Attach before the group has its initial assignment; production attaches before Run.

	eventually(t, 15*time.Second, "poison quarantined and healthy tail committed", func() bool { return offset(t, c, topic, 0) >= last[0].Offset+1 })
	var dlqPartition int32
	var dlqOffset int64
	var published bool
	if err := store.Pool.QueryRow(ctx, `SELECT published_at IS NOT NULL,dlq_partition,dlq_offset FROM dlq_audit WHERE source_topic=$1 AND source_partition=0 AND source_offset=$2`, topic, poison.Offset).Scan(&published, &dlqPartition, &dlqOffset); err != nil || !published {
		t.Fatalf("audit %v", err)
	}
	if err := quarantine.Handle(ctx, poison); err != nil {
		t.Fatal(err)
	} // audited acknowledgment skips normal repeat.
	bad.SchemaVersion = 1
	repair, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	options := replay.Options{JobID: uuid.NewString(), Tenant: tenant, Actor: "integration_operator", SourceTopic: dlq, TargetTopic: topic, Partition: dlqPartition, From: dlqOffset, To: dlqOffset + 1, ByteLimit: 8 << 20}
	runner := replay.Runner{Brokers: c.Brokers, Store: store}
	if _, err := runner.Run(ctx, options); err == nil {
		t.Fatal("invalid raw payload replayed without repair")
	}
	options.JobID = uuid.NewString()
	options.Repairs = map[string]json.RawMessage{replay.EnvelopeID(topic, 0, poison.Offset): repair}
	dry, err := runner.Run(ctx, options)
	if err != nil || dry.Validated != 1 || dry.Republished != 0 {
		t.Fatalf("dry run %+v %v", dry, err)
	}
	if got := queryCH(t, c, "SELECT count() FROM events_canonical WHERE event_id='"+id+"'"); got != "0" {
		t.Fatal("dry-run published")
	}
	options.JobID = uuid.NewString()
	options.Execute = true
	applied, err := runner.Run(ctx, options)
	if err != nil || applied.Republished != 1 {
		t.Fatalf("execute %+v %v", applied, err)
	}
	resumed, err := runner.Run(ctx, options)
	if err != nil || resumed.Republished != 0 {
		t.Fatalf("resume %+v %v", resumed, err)
	}
	// Another job can intentionally replay again, but must not inflate canonical analytics.
	options.JobID = uuid.NewString()
	if _, err := runner.Run(ctx, options); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "replay canonical identity", func() bool {
		return queryCH(t, c, "SELECT count() FROM events_canonical WHERE event_id='"+id+"'") == "1"
	})
	eventually(t, 15*time.Second, "replay exact revenue", func() bool {
		return queryCH(t, c, "SELECT sum(revenue_cents) FROM revenue_1m_canonical WHERE tenant_id='"+tenant+"' AND currency='USD'") == "521"
	})
	worker.stop()
	if len(first) != 1 {
		t.Fatal("fixture")
	}
	t.Log("durable DLQ -> healthy tail; malformed refused; dry-run nonpublishing; repair preserves identity; resume skips audited ack; repeated jobs do not inflate canonical revenue")
}
func TestDLQFailureCannotAdvancePoison(t *testing.T) {
	c := sinkConfig(t)
	producer, topic := source(t, c)
	c.Topic = topic
	store, err := metadata.FromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	quarantine, err := replay.NewQuarantine(c.Brokers, store)
	if err != nil {
		t.Fatal(err)
	}
	defer quarantine.Close()
	quarantine.Topic = "missing.dlq." + uuid.NewString()
	tenant := "dlq_failure_" + uuid.NewString()[:8]
	fixture(t, producer, topic, tenant, 1, false)
	poison := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(`{"schema_version":99}`)}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fixture(t, producer, topic, tenant, 1, false)
	sink := analytics.NewSink(c)
	defer sink.Close()
	worker := start(t, c, sink, nil, quarantine)

	select {
	case <-worker.done:
		if worker.err == nil {
			t.Fatal("DLQ failure ignored")
		}
	case <-ctx.Done():
		t.Fatal("DLQ wait unbounded")
	}
	if got := offset(t, c, topic, 0); got != poison.Offset {
		t.Fatalf("commit crossed poison: %d", got)
	}
	t.Log("DLQ unavailable: valid prefix committed, poison and following record retained")
}
