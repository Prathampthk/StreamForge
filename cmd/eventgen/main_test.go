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
