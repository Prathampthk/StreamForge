package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"streamforge/internal/domain"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeterministicMixedWorkloadAndUUIDv7(t *testing.T) {
	r := &run{seed: 42, tenant: "benchmark", prefix: [8]byte{1, 2, 3, 4}}
	at := time.Date(2026, 11, 6, 17, 0, 0, 0, time.UTC)
	counts := map[string]int{}
	for i := uint64(0); i < 10000; i++ {
		event := r.event(i, at)
		if err := event.Validate(at, domain.Policy{}); err != nil {
			t.Fatal(err)
		}
		id, err := uuid.Parse(event.EventID)
		if err != nil || id.Version() != 7 {
			t.Fatal("not UUIDv7")
		}
		counts[event.EventType]++
		if i < 10 {
			first, _ := json.Marshal(event)
			again, _ := json.Marshal(r.event(i, at))
			if string(first) != string(again) {
				t.Fatal("nondeterministic seed")
			}
		}
	}
	for kind, want := range map[string]int{"page_view": 6000, "add_to_cart": 2000, "purchase": 1000, "api_request": 1000} {
		if counts[kind] < want-180 || counts[kind] > want+180 {
			t.Fatalf("unexpected distribution %v", counts)
		}
	}
}

// Headers can arrive before the body; client latency must include that wait.
func TestClientAcknowledgmentLatencyIncludesResponseBody(t *testing.T) {
	event := (&run{seed: 42, tenant: "benchmark"}).event(1, time.Now().UTC())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.(http.Flusher).Flush()
		time.Sleep(100 * time.Millisecond)
		if err := json.NewEncoder(w).Encode(map[string]any{"accepted": 1, "acknowledgments": []map[string]any{{"event_id": event.EventID, "acknowledged_at": time.Now().UTC()}}}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	var accepted, attempts bytes.Buffer
	r := &run{client: server.Client(), ingest: server.URL, writer: bufio.NewWriter(&accepted), attemptWriter: bufio.NewWriter(&attempts), seconds: map[int64]int64{}}
	if _, err := r.publish(context.Background(), []domain.Event{event}, "steady", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(r.latency) != 1 || r.latency[0] < .09 {
		t.Fatalf("body wait omitted: %v", r.latency)
	}
}

func TestQueryWorkloadsCoverBothEndpointsWindowsAndFailures(t *testing.T) {
	seen := map[string]int{}
	var seenMu sync.Mutex
	var failRevenue atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		values := request.URL.Query()
		from, err := time.Parse(time.RFC3339, values.Get("from"))
		if err != nil {
			t.Error(err)
		}
		to, err := time.Parse(time.RFC3339, values.Get("to"))
		if err != nil {
			t.Error(err)
		}
		if values.Get("tenant_id") != "query_fixture" || request.Header.Get("Authorization") != "Bearer query_key" {
			t.Error("missing query tenant/authentication")
		}
		seenMu.Lock()
		seen[request.URL.Path+":"+values.Get("currency")+":"+to.Sub(from).String()]++
		seenMu.Unlock()
		if failRevenue.Load() && strings.HasSuffix(request.URL.Path, "/revenue") {
			http.Error(w, "unavailable", 503)
			return
		}
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	r := &run{client: server.Client(), query: server.URL, queryKey: "query_key", tenant: "query_fixture"}
	for caller := range 2 {
		for sequence := range 4 {
			r.sampleQuery(context.Background(), caller, sequence)
		}
	}
	seenMu.Lock()
	observed := make(map[string]int, len(seen))
	for key, value := range seen {
		observed[key] = value
	}
	seenMu.Unlock()

	if len(observed) != 6 || len(r.queryWorkloads) != 6 || r.queryCount != 8 || r.queryErrors != 0 {
		t.Fatalf("incomplete workload coverage: seen=%v stats=%v", observed, r.queryWorkloads)
	}
	for _, window := range []string{"15m0s", "24h0m0s"} {
		for _, currency := range []string{"USD", "EUR"} {
			if observed["/v1/metrics/revenue:"+currency+":"+window] != 1 {
				t.Fatalf("missing revenue currency/window %s/%s", currency, window)
			}
		}
		if observed["/v1/metrics/traffic::"+window] != 2 {
			t.Fatalf("missing traffic window %s", window)
		}
	}
	failRevenue.Store(true)
	r.sampleQuery(context.Background(), 0, 1)
	stats := r.queryWorkloads["revenue_USD_15m"]
	if stats.Requests != 2 || stats.Errors != 1 || len(stats.Latencies) != 1 || r.queryErrors != 1 || len(r.queryLatency) != 8 {
		t.Fatalf("failed request contaminated successful latency: %+v", stats)
	}
}
