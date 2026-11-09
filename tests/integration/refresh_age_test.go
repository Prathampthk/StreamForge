//go:build integration

package integration

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace/noop"
	"streamforge/internal/analytics"
	"streamforge/internal/config"
	"streamforge/internal/query"
	"streamforge/internal/telemetry"
)

// Execute the production health SQL against native ClickHouse with controlled
// metadata. Only the test-owned table is substituted; no real refresh is altered.
func TestSnapshotAgeUsesSuccessfulCompletion(t *testing.T) {
	c := sinkConfig(t)
	table := "streamforge.refresh_age_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	queryCH(t, c, "CREATE TABLE "+table+" (database String,view String,status String,last_success_time Nullable(DateTime),last_success_duration_ms Nullable(UInt64),exception String) ENGINE=Memory")
	defer queryCH(t, c, "DROP TABLE "+table)
	target, err := url.Parse(c.ClickHouseURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = c.ClickHouseTLS
	proxy.Transport = transport
	defer transport.CloseIdleConnections()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 65537))
		if err != nil || len(raw) > 65536 || !strings.Contains(string(raw), "FROM system.view_refreshes") {
			http.Error(w, "unexpected fixture request", 500)
			return
		}
		body := strings.ReplaceAll(string(raw), "FROM system.view_refreshes", "FROM "+table)
		r.Header.Del("Content-Length")
		r.Body = io.NopCloser(strings.NewReader(body))
		r.ContentLength = int64(len(body))
		proxy.ServeHTTP(w, r)
	}))
	defer upstream.Close()
	settings, err := config.QueryFromEnv(func(key string) string {
		switch key {
		case "QUERY_API_KEY":
			return "integration-completion-key-at-least-32"
		case "QUERY_TENANT":
			return "completion_fixture"
		case "CLICKHOUSE_URL":
			return upstream.URL
		default:
			return os.Getenv(key)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	settings.MaxSnapshotAge = 10 * time.Second
	metrics := telemetry.NewQueryMetrics()
	repo := analytics.NewReadRepository(settings, metrics, noop.NewTracerProvider().Tracer("completion-test"))
	defer repo.Close()
	api := httptest.NewServer(query.New(settings, repo, metrics, noop.NewTracerProvider().Tracer("completion-test"), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer api.Close()
	migration, err := os.ReadFile("../../migrations/clickhouse/009_completed_snapshot_age.sql")
	if err != nil {
		t.Fatal(err)
	}
	var guard string
	for _, line := range strings.Split(string(migration), "\n") {
		if strings.Contains(line, "SELECT throwIf") {
			guard = strings.SplitN(line, "AND (", 2)[1]
			guard = strings.TrimSuffix(guard, ")=0")
			guard = strings.ReplaceAll(guard, "FROM system.view_refreshes", "FROM "+table)
			break
		}
	}
	if guard == "" {
		t.Fatal("migration guard not found")
	}
	cases := []struct {
		name, view, status, duration, exception string
		startAge                                int
		missing, nullStart                      bool
		want                                    int
	}{
		{name: "old_start_recent_completion", want: 200},
		{name: "stale_source_completion", view: "identity_metrics_03_refresh", startAge: 25, want: 503},
		{name: "missing_source_duration", view: "identity_metrics_03_refresh", duration: "NULL", want: 503},
		{name: "missing_source_start", view: "identity_metrics_03_refresh", nullStart: true, want: 503},
		{name: "failed_source", view: "identity_metrics_03_refresh", exception: "failed", want: 503},
		{name: "disabled_source", view: "identity_metrics_03_refresh", status: "Disabled", want: 503},
		{name: "missing_source", view: "identity_metrics_03_refresh", missing: true, want: 503},
		{name: "failed_wrapper", view: "revenue_1m_refresh", exception: "failed", want: 503},
		{name: "stale_wrapper", view: "revenue_1m_refresh", startAge: 25, want: 503},
		{name: "missing_wrapper", view: "revenue_1m_refresh", missing: true, want: 503},
		{name: "zero_duration_valid", view: "identity_metrics_03_refresh", duration: "0", startAge: 7, want: 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queryCH(t, c, "TRUNCATE TABLE "+table)
			var rows []string
			for i := 0; i < 18; i++ {
				view := fmt.Sprintf("identity_metrics_%02d_refresh", i)
				if i == 16 {
					view = "revenue_1m_refresh"
				} else if i == 17 {
					view = "traffic_1m_refresh"
				}
				status, duration, exception, age := "Scheduled", "5000", "", 12
				if tc.view == view {
					if tc.missing {
						continue
					}
					if tc.status != "" {
						status = tc.status
					}
					if tc.duration != "" {
						duration = tc.duration
					}
					if tc.startAge != 0 {
						age = tc.startAge
					}
					exception = tc.exception
				}
				start := fmt.Sprintf("now()-INTERVAL %d SECOND", age)
				if tc.view == view && tc.nullStart {
					start = "NULL"
				}
				rows = append(rows, fmt.Sprintf("SELECT 'streamforge','%s','%s',%s,%s,'%s'", view, status, start, duration, exception))
			}
			queryCH(t, c, "INSERT INTO "+table+" "+strings.Join(rows, " UNION ALL "))
			response, err := http.Get(api.URL + "/readyz")
			if err != nil {
				t.Fatal(err)
			}
			_, readErr := io.Copy(io.Discard, response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("readiness response: read=%v close=%v", readErr, closeErr)
			}
			if response.StatusCode != tc.want {
				t.Fatalf("readiness=%d want %d", response.StatusCode, tc.want)
			}
			request, err := http.NewRequest("POST", c.ClickHouseURL+"/?database=streamforge&wait_end_of_query=1", strings.NewReader(guard))
			if err != nil {
				t.Fatal(err)
			}
			request.SetBasicAuth(c.ClickHouseUser, c.ClickHousePassword)
			guardResponse, err := (&http.Client{Timeout: 3 * time.Second, Transport: transport}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(io.LimitReader(guardResponse.Body, 8192))
			closeErr = guardResponse.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("guard response: read=%v close=%v", readErr, closeErr)
			}
			wantReject := tc.want == 503 && strings.HasPrefix(tc.view, "identity_metrics_")
			if wantReject {
				if guardResponse.StatusCode == 200 || !strings.Contains(string(body), "Code: 395.") {
					t.Fatalf("source guard failed for wrong reason: status=%d body=%s", guardResponse.StatusCode, body)
				}
			} else if guardResponse.StatusCode != 200 {
				t.Fatalf("unexpected source guard rejection: status=%d body=%s", guardResponse.StatusCode, body)
			}
		})
	}
}
