package domain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestContractFixtures(t *testing.T) {
	now := time.Date(2026, 11, 6, 17, 22, 14, 211000000, time.UTC)
	files, err := filepath.Glob("../../tests/fixtures/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("fixtures missing")
	}
	for _, file := range files {
		if !strings.HasPrefix(filepath.Base(file), "valid-") && !strings.HasPrefix(filepath.Base(file), "invalid-") {
			continue
		}
		t.Run(filepath.Base(file), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeEvent(raw, now, Policy{MaxLateness: 7 * 24 * time.Hour, FutureSkew: 5 * time.Minute})
			if (err != nil) != strings.HasPrefix(filepath.Base(file), "invalid-") {
				t.Fatalf("unexpected validation: %v", err)
			}
		})
	}
}
func TestValidationBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 6, 17, 22, 14, 0, time.UTC)
	base := map[string]any{"schema_version": 1, "event_id": "018f0f5b-c1c4-7b8f-a7a9-2d11e8fa37c2", "tenant_id": "demo", "user_id": "u", "event_type": "page_view", "occurred_at": now.Format(time.RFC3339)}
	cases := []struct {
		name  string
		patch map[string]any
		valid bool
	}{
		{"exact past boundary", map[string]any{"occurred_at": now.Add(-24 * time.Hour).Format(time.RFC3339)}, true},
		{"too old", map[string]any{"occurred_at": now.Add(-24*time.Hour - time.Second).Format(time.RFC3339)}, false},
		{"future boundary", map[string]any{"occurred_at": now.Add(time.Minute).Format(time.RFC3339)}, true},
		{"future rejected", map[string]any{"occurred_at": now.Add(time.Minute + time.Second).Format(time.RFC3339)}, false},
		{"offset accepted", map[string]any{"occurred_at": "2026-11-06T11:22:14-06:00"}, true},
		{"bad offset", map[string]any{"occurred_at": "2026-11-06T17:22:14+24:00"}, false},
		{"comma time", map[string]any{"occurred_at": "2026-11-06T17:22:14,0Z"}, false},
		{"null attribute", map[string]any{"attributes": map[string]any{"source": nil}}, false},
		{"null attributes", map[string]any{"attributes": nil}, false},
		{"unknown case", map[string]any{"SCHEMA_VERSION": 1}, false},
		{"tenant colon", map[string]any{"tenant_id": "a:b"}, false},
		{"purchase zero", map[string]any{"event_type": "purchase", "order_id": "o", "amount_cents": 0, "currency": "USD"}, true},
		{"joined currencies", map[string]any{"event_type": "purchase", "order_id": "o", "amount_cents": 1, "currency": "USD\nUSN"}, false},
		{"noncurrency", map[string]any{"event_type": "purchase", "order_id": "o", "amount_cents": 1, "currency": "ZZZ"}, false},
		{"negative", map[string]any{"event_type": "purchase", "order_id": "o", "amount_cents": -1, "currency": "USD"}, false},
		{"float money", map[string]any{"event_type": "purchase", "order_id": "o", "amount_cents": 1.5, "currency": "USD"}, false},
		{"status lower", map[string]any{"event_type": "api_request", "status_code": 100}, true},
		{"status upper", map[string]any{"event_type": "api_request", "status_code": 599}, true},
		{"status high", map[string]any{"event_type": "api_request", "status_code": 600}, false},
		{"pii key", map[string]any{"attributes": map[string]string{"email": "secret"}}, false},
		{"large attr", map[string]any{"attributes": map[string]string{"source": strings.Repeat("a", 65)}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := map[string]any{}
			for k, v := range base {
				row[k] = v
			}
			for k, v := range tc.patch {
				row[k] = v
			}
			raw, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			event, err := DecodeEvent(raw, now, Policy{MaxLateness: 24 * time.Hour, FutureSkew: time.Minute})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if tc.name == "offset accepted" && event.OccurredAt != "2026-11-06T11:22:14-06:00" {
				t.Fatal("original timestamp changed")
			}
		})
	}
	for _, raw := range []string{`{"schema_version":1,"schema_version":2}`, `{} {}`, `{"attributes":{"source":"a","source":"b"}}`, strings.Repeat("[", 18) + strings.Repeat("]", 18), "null", strings.Repeat(" ", 8193)} {
		if _, err := DecodeEvent([]byte(raw), now, Policy{}); err == nil {
			t.Fatalf("accepted ambiguous or oversized JSON: %q", raw)
		}
	}
}
