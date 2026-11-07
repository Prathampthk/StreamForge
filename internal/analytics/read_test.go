package analytics

import (
	"context"
	"go.opentelemetry.io/otel/trace/noop"
	"io"
	"net/http"
	"net/http/httptest"
	"streamforge/internal/config"
	"streamforge/internal/telemetry"
	"strings"
	"testing"
	"time"
)

func TestReadUsesTypedParametersAndBounds(t *testing.T) {
	tenant := "x' OR 1=1 --"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(string(raw), tenant) || r.URL.Query().Get("param_tenant") != tenant {
			t.Error("SQL interpolation or missing parameter")
		}
		for _, key := range []string{"readonly", "max_execution_time", "max_memory_usage", "max_threads", "wait_end_of_query"} {
			if r.URL.Query().Get(key) == "" {
				t.Errorf("missing %s", key)
			}
		}
		if _, err := io.WriteString(w, `{"minute":"2026-11-06T17:22:00Z","purchase_count":2,"revenue_cents":"18446744073709551614"}`); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	c := config.Query{ClickHouseURL: server.URL, ClickHouseUser: "local", ClickHousePassword: "local", MaxRequests: 1, DBTimeout: time.Second}
	repo := NewReadRepository(c, telemetry.NewQueryMetrics(), noop.NewTracerProvider().Tracer("test"))
	defer repo.Close()
	rows, err := repo.Revenue(context.Background(), Window{Tenant: tenant, Currency: "USD", From: time.Now(), To: time.Now().Add(time.Minute)})
	if err != nil || len(rows) != 1 || rows[0].Cents != "18446744073709551614" {
		t.Fatalf("%v %v", rows, err)
	}
}
