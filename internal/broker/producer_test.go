package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/telemetry"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestProducerSettingsAndSaturation(t *testing.T) {
	c, err := config.FromEnv(func(key string) string {
		if key == "INGEST_API_KEY" {
			return "development-test-key-32-characters"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reserve a TCP port that deliberately never accepts Kafka traffic.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	c.Brokers = []string{listener.Addr().String()}
	c.ProducerRecords = 1
	metrics := telemetry.NewMetrics()
	producer, err := New(c, metrics, noop.NewTracerProvider().Tracer("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if producer.client.OptValue(kgo.DisableIdempotentWrite) != false {
		t.Fatal("idempotence disabled")
	}
	if producer.client.OptValue(kgo.RequiredAcks) != kgo.AllISRAcks() {
		t.Fatal("acks must be all")
	}
	if fmt.Sprint(producer.client.OptValue(kgo.MaxBufferedRecords)) != "1" {
		t.Fatal("record bound not applied")
	}
	ctx, cancel := context.WithCancel(context.Background())
	event := domain.Event{SchemaVersion: 1, EventID: "018f0f5b-c1c4-7b8f-a7a9-2d11e8fa37c2", TenantID: "test", UserID: "u", EventType: "page_view", OccurredAt: time.Now().UTC().Format(time.RFC3339)}
	first := make(chan error, 1)
	go func() { first <- producer.Publish(ctx, []domain.Event{event}, "test") }()
	deadline := time.Now().Add(2 * time.Second)
	for producer.client.BufferedProduceRecords() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if producer.client.BufferedProduceRecords() != 1 {
		t.Fatal("first record not buffered")
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	if err := producer.Publish(secondCtx, []domain.Event{event}, "test"); !errors.Is(err, kgo.ErrMaxBuffered) {
		t.Fatalf("expected saturation, got %v", err)
	}
	cancel()
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("unacknowledged record succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation blocked")
	}
	producer.Close()
	if got := testutil.ToFloat64(metrics.ProducerInflight); got != 0 {
		t.Fatalf("unsettled promises after close: %v", got)
	}
}
func TestKeyDistribution(t *testing.T) {
	partitioner := kgo.StickyKeyPartitioner(nil).ForTopic("streamforge.events.v1")
	counts := make([]int, 12)
	for i := 0; i < 12000; i++ {
		// A single large tenant still distributes distinct users across partitions.
		key := []byte(fmt.Sprintf("large_tenant:user_%d", i))
		record := &kgo.Record{Key: key}
		partition := partitioner.Partition(record, 12)
		if partition != partitioner.Partition(record, 12) {
			t.Fatal("unstable key affinity")
		}
		counts[partition]++
	}
	for _, count := range counts {
		if count < 800 || count > 1200 {
			t.Fatalf("skewed distribution: %v", counts)
		}
	}
	hot := &kgo.Record{Key: []byte("hot_tenant:hot_user")}
	expected := partitioner.Partition(hot, 12)
	for i := 0; i < 100; i++ {
		if partitioner.Partition(hot, 12) != expected {
			t.Fatal("hot user moved")
		}
	}
}
