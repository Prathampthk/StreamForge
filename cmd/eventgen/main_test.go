package main

import (
	"encoding/json"
	"github.com/google/uuid"
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
