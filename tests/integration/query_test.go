//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"streamforge/internal/analytics"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/pipeline"
	"streamforge/internal/query"
	"streamforge/internal/telemetry"
)

func TestQueryGoldenLateEventsAndIsolation(t *testing.T) {
	sinkSettings := sinkConfig(t)
	sink := analytics.NewSink(sinkSettings)
	defer sink.Close()
	raw, err := os.ReadFile("../fixtures/reconciliation-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Accepted []struct {
			Event domain.Event `json:"event"`
		} `json:"accepted"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	tenant := "query_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	base := time.Now().UTC().Truncate(time.Minute).Add(-48 * time.Hour)
	original := time.Date(2026, 11, 6, 17, 22, 0, 0, time.UTC)
	ids := map[string]string{}
	records := make([]pipeline.Record, len(fixture.Accepted))
	for i, row := range fixture.Accepted {
		event := row.Event
		if ids[event.EventID] == "" {
			ids[event.EventID] = uuid.NewString()
		}
		event.EventID = ids[event.EventID]
		event.TenantID = tenant
		instant, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
		if err != nil {
			t.Fatal(err)
		}
		shifted := base.Add(instant.Sub(original))
		event.OccurredAt = shifted.Format(time.RFC3339Nano)
		if i == 1 {
			event.OccurredAt = shifted.In(time.FixedZone("test", -6*3600)).Format(time.RFC3339Nano)
		}
		value, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		records[i] = pipeline.Record{Topic: "query-golden", Partition: 0, Offset: int64(i), Timestamp: time.Now().UTC(), Value: value}
	}
	batch, err := pipeline.NewBatch(records, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	// A second tenant in the same event minute must never leak into this key's metrics.
	outside := fixture.Accepted[0].Event
	outside.EventID = uuid.NewString()
	outside.TenantID = tenant + "_other"
	outside.OccurredAt = base.Format(time.RFC3339Nano)
	value, err := json.Marshal(outside)
	if err != nil {
		t.Fatal(err)
	}
	extra, err := pipeline.NewBatch([]pipeline.Record{{Topic: "query-other", Partition: 0, Offset: 0, Timestamp: time.Now(), Value: value}}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	c, err := config.QueryFromEnv(func(key string) string {
		if key == "QUERY_API_KEY" {
			return "integration-query-key-at-least-32bytes"
		}
		if key == "QUERY_TENANT" {
			return tenant
		}
		return os.Getenv(key)
	})
	if err != nil {
		t.Fatal(err)
	}
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	metrics := telemetry.NewQueryMetrics()
	repository := analytics.NewReadRepository(c, metrics, telemetry.Tracer(provider))
	defer repository.Close()
	server := httptest.NewServer(query.New(c, repository, metrics, telemetry.Tracer(provider), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer server.Close()
	get := func(kind, currency, requestedTenant string) (int, map[string]json.RawMessage) {
		values := url.Values{"tenant_id": {requestedTenant}, "from": {base.In(time.FixedZone("test", -6*3600)).Format(time.RFC3339)}, "to": {base.Add(4 * time.Minute).Format(time.RFC3339)}, "interval": {"1m"}}
		if kind == "revenue" {
			values.Set("currency", currency)
		}
		request, err := http.NewRequest("GET", server.URL+"/v1/metrics/"+kind+"?"+values.Encode(), nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+c.APIKey)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("read=%v close=%v", err, closeErr)
		}
		var decoded map[string]json.RawMessage
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, decoded
	}
	eventually(t, 15*time.Second, "query golden snapshots", func() bool {
		status, body := get("revenue", "USD", tenant)
		if status != 200 {
			return false
		}
		var rows []analytics.Revenue
		if err := json.Unmarshal(body["data"], &rows); err != nil {
			t.Fatal(err)
		}
		return len(rows) == 4 && rows[0].Cents == "137" && rows[0].Purchases == 1 && rows[1].Cents == "0"
	})
	status, body := get("revenue", "EUR", tenant)
	var revenue []analytics.Revenue
	// Separate identity shards converge independently. USD visibility does not
	// promise that the EUR shard has included the same newly inserted batch.
	eventually(t, 15*time.Second, "independent EUR shard snapshot catches up", func() bool {
		status, body = get("revenue", "EUR", tenant)
		if status != 200 {
			return false
		}
		if err := json.Unmarshal(body["data"], &revenue); err != nil {
			t.Fatal(err)
		}
		return len(revenue) == 4 && revenue[0].Cents == "0" && revenue[1].Cents == "250" && revenue[2].Cents == "0"
	})

	eventually(t, 20*time.Second, "independent traffic snapshot catches up", func() bool {
		status, body := get("traffic", "", tenant)
		if status != 200 {
			return false
		}
		var traffic []struct {
			analytics.Traffic
			Rate float64 `json:"error_rate"`
		}
		if err := json.Unmarshal(body["data"], &traffic); err != nil {
			t.Fatal(err)
		}
		return len(traffic) == 4 && traffic[0].Events == 4 && traffic[0].RPM == 2 && traffic[0].Errors == 1 && traffic[0].Rate == .5 && traffic[1].Events == 2 && traffic[1].RPM == 1 && traffic[1].Errors == 0 && traffic[2].Events == 0 && traffic[2].Rate == 0
	})
	if status, _ := get("revenue", "USD", tenant+"_other"); status != 403 {
		t.Fatal("tenant leak")
	}
	// Late arrival updates its original event-time minute, not processing time.
	late := outside
	late.TenantID = tenant
	late.EventID = uuid.NewString()
	amount := int64(321)
	late.AmountCents = &amount
	value, err = json.Marshal(late)
	if err != nil {
		t.Fatal(err)
	}
	lateBatch, err := pipeline.NewBatch([]pipeline.Record{{Topic: "query-late", Partition: 0, Offset: 0, Timestamp: time.Now(), Value: value}}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), lateBatch); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "late event in original minute", func() bool {
		status, body := get("revenue", "USD", tenant)
		if status != 200 {
			return false
		}
		var rows []analytics.Revenue
		if err := json.Unmarshal(body["data"], &rows); err != nil {
			t.Fatal(err)
		}
		return rows[0].Cents == "458" && rows[0].Purchases == 2
	})
	// Sum two maximum valid Int64 purchases into exact Int128 without float loss.
	var largeRecords []pipeline.Record
	for i := 0; i < 2; i++ {
		large := late
		large.EventID = uuid.NewString()
		large.OccurredAt = base.Add(3 * time.Minute).Format(time.RFC3339)
		maxAmount := int64(9223372036854775807)
		large.AmountCents = &maxAmount
		value, err := json.Marshal(large)
		if err != nil {
			t.Fatal(err)
		}
		largeRecords = append(largeRecords, pipeline.Record{Topic: "query-large", Partition: 0, Offset: int64(i), Timestamp: time.Now(), Value: value})
	}
	large, err := pipeline.NewBatch(largeRecords, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), large); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "exact Int128 revenue", func() bool {
		status, body := get("revenue", "USD", tenant)
		if status != 200 {
			return false
		}
		var rows []analytics.Revenue
		if err := json.Unmarshal(body["data"], &rows); err != nil {
			t.Fatal(err)
		}
		return rows[3].Cents == "18446744073709551614"
	})
	names := map[string]bool{}
	for _, span := range exporter.GetSpans() {
		names[span.Name] = true
	}
	for _, name := range []string{"http.query.revenue", "http.query.traffic", "clickhouse.refresh_health", "clickhouse.revenue", "clickhouse.traffic"} {
		if !names[name] {
			t.Errorf("missing span %s", name)
		}
	}
	t.Log("golden currencies/minute edges/offsets, zero buckets, conflicts/retries, late events, tenant isolation and exact Int128 verified against ClickHouse")
}

func TestQueryRejectsStoppedRefreshAndDatabaseOutage(t *testing.T) {
	sinkSettings := sinkConfig(t)
	c, err := config.QueryFromEnv(func(key string) string {
		if key == "QUERY_API_KEY" {
			return "integration-query-key-at-least-32bytes"
		}
		return os.Getenv(key)
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := sdktrace.NewTracerProvider()
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	metrics := telemetry.NewQueryMetrics()
	repository := analytics.NewReadRepository(c, metrics, telemetry.Tracer(provider))
	defer repository.Close()
	handler := query.New(c, repository, metrics, telemetry.Tracer(provider), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
	now := time.Now().UTC().Truncate(time.Minute)
	values := url.Values{"tenant_id": {c.Tenant}, "from": {now.Add(-time.Minute).Format(time.RFC3339)}, "to": {now.Format(time.RFC3339)}, "interval": {"1m"}, "currency": {"USD"}}
	run := func(h http.Handler) *httptest.ResponseRecorder {
		request := httptest.NewRequest("GET", "/v1/metrics/revenue?"+values.Encode(), nil)
		request.Header.Set("Authorization", "Bearer "+c.APIKey)
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		return response
	}
	queryCH(t, sinkSettings, "SYSTEM STOP VIEW revenue_1m_refresh")
	defer queryCH(t, sinkSettings, "SYSTEM START VIEW revenue_1m_refresh")
	eventually(t, 20*time.Second, "inflight refresh finishes before Disabled metadata", func() bool {
		refresh, err := repository.Refresh(context.Background(), "revenue")
		return err == nil && refresh.Status == "Disabled"
	})
	if response := run(handler); response.Code != 503 || !strings.Contains(response.Body.String(), "snapshot_unavailable") {
		t.Fatalf("stopped refresh returned %d %s", response.Code, response.Body.String())
	}
	queryCH(t, sinkSettings, "SYSTEM START VIEW revenue_1m_refresh")
	eventually(t, 20*time.Second, "partitioned refresh healthy before negative shard check", func() bool {
		refresh, err := repository.Refresh(context.Background(), "revenue")
		return err == nil && refresh.Failed == 0 && refresh.Status != "Disabled"
	})
	queryCH(t, sinkSettings, "SYSTEM STOP VIEW identity_metrics_00_refresh")
	defer queryCH(t, sinkSettings, "SYSTEM START VIEW identity_metrics_00_refresh")
	eventually(t, 20*time.Second, "stopped source shard is rejected independently of wrapper age", func() bool {
		refresh, err := repository.Refresh(context.Background(), "revenue")
		return err == nil && refresh.Failed != 0
	})
	if response := run(handler); response.Code != 503 || !strings.Contains(response.Body.String(), "snapshot_unavailable") {
		t.Fatalf("stopped source shard returned %d %s", response.Code, response.Body.String())
	}
	queryCH(t, sinkSettings, "SYSTEM START VIEW identity_metrics_00_refresh")
	c.ClickHouseURL = "http://127.0.0.1:1"
	failedRepository := analytics.NewReadRepository(c, telemetry.NewQueryMetrics(), telemetry.Tracer(provider))
	defer failedRepository.Close()
	failed := query.New(c, failedRepository, telemetry.NewQueryMetrics(), telemetry.Tracer(provider), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
	if response := run(failed); response.Code != 503 {
		t.Fatal("unavailable database returned success")
	}
	t.Log("real stopped wrapper/source shard and unreachable database reject queries with 503; refreshes restarted")
}
