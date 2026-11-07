package query

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"
	"streamforge/internal/analytics"
	"streamforge/internal/config"
	"streamforge/internal/telemetry"
)

type fakeRead struct {
	refresh analytics.Refresh
	err     error
	wait    chan struct{}
	entered chan struct{}
	once    sync.Once
	revenue []analytics.Revenue
	traffic []analytics.Traffic
}

func (f *fakeRead) Refresh(ctx context.Context, kind string) (analytics.Refresh, error) {
	return f.refresh, f.err
}
func (f *fakeRead) Revenue(ctx context.Context, w analytics.Window) ([]analytics.Revenue, error) {
	if f.wait != nil {
		f.once.Do(func() { close(f.entered) })
		select {
		case <-f.wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.revenue, f.err
}
func (f *fakeRead) Traffic(ctx context.Context, w analytics.Window) ([]analytics.Traffic, error) {
	return f.traffic, f.err
}
func querySetup(t *testing.T, f *fakeRead) (*Server, http.Handler) {
	t.Helper()
	c, err := config.QueryFromEnv(func(k string) string {
		switch k {
		case "QUERY_API_KEY":
			return "local-query-test-key-at-least-32bytes"
		case "CLICKHOUSE_PASSWORD":
			return "local-test"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	c.MaxRequests = 1
	s := New(c, f, telemetry.NewQueryMetrics(), noop.NewTracerProvider().Tracer("test"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	return s, s.Handler()
}
func params() url.Values {
	return url.Values{"tenant_id": {"merchant_demo"}, "from": {"2026-11-06T17:22:00Z"}, "to": {"2026-11-06T17:24:00Z"}, "interval": {"1m"}, "currency": {"USD"}}
}
func callQuery(h http.Handler, values url.Values) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/v1/metrics/revenue?"+values.Encode(), nil)
	r.Header.Set("Authorization", "Bearer local-query-test-key-at-least-32bytes")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestWindowValidation(t *testing.T) {
	for _, tc := range []struct {
		key, value string
		ok         bool
	}{
		{"", "", true}, {"from", "2026-11-06T11:22:00-06:00", true}, {"from", "2026-11-06T17:22:01Z", false}, {"from", "2026-11-06T17:22:00.1Z", false}, {"from", "2026-11-06T17:22:00,0Z", false}, {"from", "2026-11-06T17:22:00+24:00", false},
		{"to", "2026-11-06T17:22:00Z", false}, {"to", "2026-11-08T17:22:00Z", false}, {"to", "2200-01-01T00:00:00Z", false}, {"tenant_id", "a' OR 1", false}, {"interval", "1h", false}, {"currency", "", false}, {"currency", "ZZZ", false}, {"extra", "x", false},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			v := params()
			if tc.key != "" {
				v.Set(tc.key, tc.value)
			}
			window, err := ParseWindow(v, "revenue", 24*time.Hour)
			if (err == nil) != tc.ok {
				t.Fatalf("ok=%v err=%v", tc.ok, err)
			}
			if tc.key == "from" && tc.ok && window.From.Format(time.RFC3339) != "2026-11-06T17:22:00Z" {
				t.Fatal(window)
			}
		})
	}
	v := params()
	v.Add("tenant_id", "other")
	if _, err := ParseWindow(v, "revenue", 24*time.Hour); err == nil {
		t.Fatal("duplicate parameter accepted")
	}
	v = params()
	v.Del("currency")
	if _, err := ParseWindow(v, "traffic", 24*time.Hour); err != nil {
		t.Fatal(err)
	}
}
func TestDenseBucketsAndExactMoney(t *testing.T) {
	window, err := ParseWindow(params(), "revenue", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := denseRevenue(window, []analytics.Revenue{{Minute: "2026-11-06T17:22:00Z", Purchases: 2, Cents: "18446744073709551614"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Cents != "18446744073709551614" || rows[1].Cents != "0" {
		t.Fatal(rows)
	}
	traffic, err := denseTraffic(window, []analytics.Traffic{{Minute: "2026-11-06T17:22:00Z", Events: 4, RPM: 2, Errors: 1}})
	if err != nil || traffic[0].ErrorRate != .5 || traffic[1].ErrorRate != 0 {
		t.Fatalf("%+v %v", traffic, err)
	}
	if _, err := denseRevenue(window, []analytics.Revenue{{Minute: "2026-11-06T17:24:00Z", Cents: "1"}}); err == nil {
		t.Fatal("exclusive upper bound ignored")
	}
	if _, err := denseTraffic(window, []analytics.Traffic{{Minute: "2026-11-06T17:22:00Z", RPM: 1, Errors: 2}}); err == nil {
		t.Fatal("bad error count accepted")
	}
}
func TestStaleFailedMissingAndQueryFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		r      analytics.Refresh
		err    error
		status int
	}{
		{"fresh", analytics.Refresh{Status: "Scheduled", LastSuccess: time.Now().Unix()}, nil, 200},
		{"running", analytics.Refresh{Status: "Running", LastSuccess: time.Now().Unix()}, nil, 200},
		{"stale", analytics.Refresh{Status: "Scheduled", LastSuccess: time.Now().Add(-time.Minute).Unix()}, nil, 503},
		{"failed", analytics.Refresh{Status: "Scheduled", LastSuccess: time.Now().Unix(), Failed: 1}, nil, 503},
		{"missing", analytics.Refresh{}, nil, 503}, {"stopped", analytics.Refresh{Status: "Disabled", LastSuccess: time.Now().Unix()}, nil, 503},
		{"database", analytics.Refresh{}, errors.New("sensitive internal database error"), 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, h := querySetup(t, &fakeRead{refresh: tc.r, err: tc.err})
			w := callQuery(h, params())
			if w.Code != tc.status || strings.Contains(w.Body.String(), "sensitive") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	f := &fakeRead{refresh: analytics.Refresh{Status: "Scheduled", LastSuccess: time.Now().Unix()}, wait: make(chan struct{}), entered: make(chan struct{})}
	s, h := querySetup(t, f)
	s.config.RequestTimeout = 10 * time.Millisecond
	if w := callQuery(h, params()); w.Code != 503 {
		t.Fatal("query deadline ignored")
	}
}
func TestTenantIsolationSaturationAndConcurrency(t *testing.T) {
	f := &fakeRead{refresh: analytics.Refresh{Status: "Scheduled", LastSuccess: time.Now().Unix()}, wait: make(chan struct{}), entered: make(chan struct{})}
	s, h := querySetup(t, f)
	values := params()
	values.Set("tenant_id", "other")
	if w := callQuery(h, values); w.Code != 403 {
		t.Fatal(w.Code)
	}
	unauth := httptest.NewRecorder()
	h.ServeHTTP(unauth, httptest.NewRequest("GET", "/v1/metrics/revenue?"+params().Encode(), nil))
	if unauth.Code != 401 {
		t.Fatal(unauth.Code)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- callQuery(h, params()) }()
	<-f.entered
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w := callQuery(h, params()); w.Code != 503 {
				t.Errorf("saturation %d", w.Code)
			}
		}()
	}
	wg.Wait()
	close(f.wait)
	if w := <-done; w.Code != 200 {
		t.Fatal(w.Code)
	}
	s.Drain()
	if w := callQuery(h, params()); w.Code != 503 {
		t.Fatal("drain ignored")
	}
	families, err := s.metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"kind": true, "outcome": true, "operation": true}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if !allowed[label.GetName()] {
					t.Fatalf("unexpected label %s", label.GetName())
				}
				if strings.Contains(label.GetValue(), "merchant") {
					t.Fatal("tenant metric label")
				}
			}
		}
	}
}
func TestSuccessfulResponseContract(t *testing.T) {
	_, h := querySetup(t, &fakeRead{refresh: analytics.Refresh{Status: "Scheduled", LastSuccess: time.Now().Unix()}})
	w := callQuery(h, params())
	var body struct {
		Data     []analytics.Revenue `json:"data"`
		Snapshot string              `json:"snapshot_last_success"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 2 || body.Data[1].Cents != "0" || body.Snapshot == "" {
		t.Fatal(w.Body.String())
	}
}
