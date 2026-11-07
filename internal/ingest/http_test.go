package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/telemetry"

	"go.opentelemetry.io/otel/trace/noop"
)

type fakePublisher struct {
	publish  func(context.Context, []domain.Event, string) error
	readyErr error
	calls    atomic.Int64
}

func (p *fakePublisher) Publish(ctx context.Context, events []domain.Event, id string) error {
	p.calls.Add(1)
	if p.publish != nil {
		return p.publish(ctx, events, id)
	}
	return nil
}
func (p *fakePublisher) Ready(context.Context) error { return p.readyErr }
func setup(t *testing.T, p *fakePublisher) (*Server, http.Handler) {
	t.Helper()
	c, err := config.FromEnv(func(key string) string {
		if key == "INGEST_API_KEY" {
			return "development-test-key-32-characters"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	c.MaxRequests = 1
	server := New(c, p, telemetry.NewMetrics(), noop.NewTracerProvider().Tracer("test"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	return server, server.Handler()
}
func validEvent() string {
	raw, _ := json.Marshal(map[string]any{"schema_version": 1, "event_id": "018f0f5b-c1c4-7b8f-a7a9-2d11e8fa37c2", "tenant_id": "merchant_demo", "user_id": "u1", "event_type": "page_view", "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)})
	return string(raw)
}
func request(handler http.Handler, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer development-test-key-32-characters")
	r.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	return response
}
func TestHTTPValidationAndAuth(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		status           int
	}{
		{"single", "/v1/events", validEvent(), 202},
		{"batch", "/v1/events/batch", "[" + validEvent() + "," + validEvent() + "]", 202},
		{"invalid batch atomic validation", "/v1/events/batch", "[" + validEvent() + ",{}]", 400},
		{"empty batch", "/v1/events/batch", "[]", 400},
		{"large", "/v1/events", strings.Repeat("x", 8193), 413},
		{"malformed", "/v1/events", "{", 400},
		{"unknown", "/v1/events", strings.Replace(validEvent(), `"schema_version"`, `"unknown"`, 1), 400},
		{"tenant mismatch", "/v1/events", strings.Replace(validEvent(), "merchant_demo", "other", 1), 403},
		{"batch too many", "/v1/events/batch", "[" + strings.Repeat(validEvent()+",", 100) + validEvent() + "]", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakePublisher{}
			_, h := setup(t, p)
			response := request(h, tc.path, tc.body)
			if response.Code != tc.status {
				t.Fatalf("got %d: %s", response.Code, response.Body.String())
			}
			if tc.status != 202 && p.calls.Load() != 0 {
				t.Fatal("invalid event published")
			}
			if response.Header().Get("X-Correlation-ID") == "" {
				t.Fatal("missing correlation ID")
			}
		})
	}
	p := &fakePublisher{}
	_, h := setup(t, p)
	for _, auth := range []string{"", "Bearer wrong", "Basic development-test-key-32-characters"} {
		r := httptest.NewRequest("POST", "/v1/events", strings.NewReader(validEvent()))
		r.Header.Set("Authorization", auth)
		response := httptest.NewRecorder()
		h.ServeHTTP(response, r)
		if response.Code != 401 {
			t.Fatal(response.Code)
		}
	}
	if p.calls.Load() != 0 {
		t.Fatal("unauthenticated publication")
	}
}
func TestAcknowledgmentAndSaturation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := &fakePublisher{publish: func(ctx context.Context, _ []domain.Event, _ string) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	server, h := setup(t, p)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- request(h, "/v1/events", validEvent()) }()
	<-started
	select {
	case <-done:
		t.Fatal("responded before broker acknowledged")
	default:
	}
	response := request(h, "/v1/events", validEvent())
	if response.Code != 503 || response.Header().Get("Retry-After") == "" {
		t.Fatalf("saturation: %d", response.Code)
	}
	server.Drain()
	response = request(h, "/v1/events", validEvent())
	if response.Code != 503 {
		t.Fatal("accepted during drain")
	}
	close(release)
	if response = <-done; response.Code != 202 {
		t.Fatalf("inflight drain response %d", response.Code)
	}
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
	if response.Code != 503 {
		t.Fatal("ready during drain")
	}
}
func TestFailureCancellationAndRateLimit(t *testing.T) {
	p := &fakePublisher{publish: func(context.Context, []domain.Event, string) error { return errors.New("secret error must not leak") }}
	_, h := setup(t, p)
	response := request(h, "/v1/events", validEvent())
	if response.Code != 503 || strings.Contains(response.Body.String(), "secret") {
		t.Fatal(response.Body.String())
	}
	p = &fakePublisher{publish: func(ctx context.Context, _ []domain.Event, _ string) error { <-ctx.Done(); return ctx.Err() }}
	server, h := setup(t, p)
	server.config.PublishTimeout = 10 * time.Millisecond
	if response = request(h, "/v1/events", validEvent()); response.Code != 503 {
		t.Fatal("timeout acknowledged")
	}
	p = &fakePublisher{}
	server, h = setup(t, p)
	server.limiter.SetLimit(0)
	server.limiter.SetBurst(1)
	if response = request(h, "/v1/events", validEvent()); response.Code != 202 {
		t.Fatal(response.Code)
	}
	if response = request(h, "/v1/events", validEvent()); response.Code != 429 {
		t.Fatal(response.Code)
	}
}
func TestHealthMetricsAndMediaType(t *testing.T) {
	p := &fakePublisher{readyErr: errors.New("down")}
	_, h := setup(t, p)
	for path, status := range map[string]int{"/livez": 200, "/readyz": 503, "/metrics": 200} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != status {
			t.Fatalf("%s: %d", path, response.Code)
		}
	}
	r := httptest.NewRequest("POST", "/v1/events", strings.NewReader(validEvent()))
	r.Header.Set("Authorization", "Bearer development-test-key-32-characters")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, r)
	if response.Code != 415 {
		t.Fatal(response.Code)
	}
	if p.calls.Load() != 0 {
		t.Fatal("published unsupported content")
	}
}
