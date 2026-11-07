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
	"strings"
	"testing"
	"time"

	"streamforge/internal/broker"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/ingest"
	"streamforge/internal/telemetry"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func configuration(t *testing.T) config.Config {
	t.Helper()
	c, err := config.FromEnv(func(key string) string {
		if key == "INGEST_API_KEY" {
			return "integration-demo-key-at-least-32-bytes"
		}
		if key == "KAFKA_BROKERS" {
			return os.Getenv(key)
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestKafkaAcknowledgedHTTP(t *testing.T) {
	c := configuration(t)
	_, isolatedTopic := source(t, config.Consumer{Brokers: c.Brokers})
	c.Topic = isolatedTopic
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	metrics := telemetry.NewMetrics()
	producer, err := broker.New(c, metrics, telemetry.Tracer(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := producer.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	app := ingest.New(c, producer, metrics, telemetry.Tracer(provider), slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	id := uuid.NewString()
	event := domain.Event{SchemaVersion: 1, EventID: id, TenantID: c.Tenant, UserID: "integration_user", EventType: "page_view", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Attributes: map[string]string{"source": "integration-test"}}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	// Logical HTTP retries are deliberately separate Kafka records in this phase.
	for i := 0; i < 2; i++ {
		request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/events", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+c.APIKey)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Correlation-ID", "integration_correlation")
		request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		result, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != 202 {
			t.Fatalf("response %d %s read=%v close=%v", response.StatusCode, result, readErr, closeErr)
		}
	}
	assignments := map[int32]kgo.Offset{}
	for i := int32(0); i < 12; i++ {
		assignments[i] = kgo.NewOffset().AtStart()
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{c.Topic: assignments}), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	found := 0
	partition := int32(-1)
	var previousOffset int64 = -1
	for found < 2 && ctx.Err() == nil {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var actual domain.Event
			if err := json.Unmarshal(record.Value, &actual); err != nil {
				t.Error(err)
				return
			}
			if actual.EventID != id {
				return
			}
			found++
			if string(record.Key) != c.Tenant+":integration_user" {
				t.Errorf("key %q", record.Key)
			}
			if actual.OccurredAt != event.OccurredAt || actual.EventType != event.EventType || actual.Attributes["source"] != "integration-test" {
				t.Errorf("payload changed: %+v", actual)
			}
			if partition >= 0 && (partition != record.Partition || previousOffset >= record.Offset) {
				t.Error("key affinity or order violated")
			}
			partition = record.Partition
			previousOffset = record.Offset
			headers := map[string]string{}
			for _, header := range record.Headers {
				headers[header.Key] = string(header.Value)
			}
			if headers["schema_version"] != "1" || headers["correlation_id"] != "integration_correlation" {
				t.Errorf("headers: %v", headers)
			}
			if !strings.HasPrefix(headers["traceparent"], "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
				t.Errorf("trace propagation: %v", headers)
			}
			if instant, err := time.Parse(time.RFC3339Nano, headers["produced_at"]); err != nil || instant.Location() != time.UTC {
				t.Errorf("produced_at: %v", headers)
			}
		})
	}
	if found != 2 {
		t.Fatalf("found %d duplicate-submission records", found)
	}
	if got := testutil.ToFloat64(metrics.Requests.WithLabelValues("accepted")); got != 2 {
		t.Fatalf("accepted requests %v", got)
	}
	if got := testutil.ToFloat64(metrics.Events.WithLabelValues("acknowledged")); got != 2 {
		t.Fatalf("ack events %v", got)
	}
	spans := exporter.GetSpans()
	if len(spans) < 4 {
		t.Fatalf("missing HTTP/publish spans: %d", len(spans))
	}
	t.Logf("verified 2 broker-acknowledged responses; duplicate ID appears twice in partition %d; key/headers/trace/payload verified", partition)
}
func TestUnavailableBrokerDoesNotAcknowledge(t *testing.T) {
	c := configuration(t)
	c.Brokers = []string{"127.0.0.1:1"}
	c.PublishTimeout = time.Second
	metrics := telemetry.NewMetrics()
	provider := sdktrace.NewTracerProvider()
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	producer, err := broker.New(c, metrics, telemetry.Tracer(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	app := ingest.New(c, producer, metrics, telemetry.Tracer(provider), slog.New(slog.NewTextHandler(io.Discard, nil)))
	raw := fmt.Sprintf(`{"schema_version":1,"event_id":%q,"tenant_id":%q,"user_id":"u","event_type":"page_view","occurred_at":%q}`, uuid.NewString(), c.Tenant, time.Now().UTC().Format(time.RFC3339Nano))
	request := httptest.NewRequest("POST", "/v1/events", strings.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+c.APIKey)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatalf("unavailable broker status %d: %s", response.Code, response.Body.String())
	}
	if got := testutil.ToFloat64(metrics.Requests.WithLabelValues("accepted")); got != 0 {
		t.Fatalf("false acceptance %v", got)
	}
	t.Log("unavailable broker -> 503; zero accepted requests")
}
