package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/telemetry"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Window struct {
	Tenant, Currency string
	From, To         time.Time
}
type Revenue struct {
	Minute    string `json:"minute"`
	Purchases uint64 `json:"purchase_count"`
	Cents     string `json:"revenue_cents"`
}
type Traffic struct {
	Minute string `json:"minute"`
	Events uint64 `json:"events_per_minute"`
	RPM    uint64 `json:"rpm"`
	Errors uint64 `json:"api_errors"`
}
type Refresh struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	LastSuccess int64  `json:"last_success_unix"`
	Failed      uint8  `json:"failed"`
}

// ReadRepository owns a bounded HTTP pool; fixed SQL uses typed value parameters.
type ReadRepository struct {
	endpoint, user, password string
	client                   *http.Client
	transport                *http.Transport
	tracer                   trace.Tracer
	metrics                  *telemetry.QueryMetrics
}

func NewReadRepository(c config.Query, m *telemetry.QueryMetrics, t trace.Tracer) *ReadRepository {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxConnsPerHost: c.MaxRequests, MaxIdleConnsPerHost: c.MaxRequests, MaxIdleConns: c.MaxRequests, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: c.DBTimeout}
	return &ReadRepository{endpoint: strings.TrimRight(c.ClickHouseURL, "/"), user: c.ClickHouseUser, password: c.ClickHousePassword, transport: transport, client: &http.Client{Transport: transport, Timeout: c.DBTimeout}, tracer: t, metrics: m}
}
func (r *ReadRepository) Close() { r.transport.CloseIdleConnections() }
func (r *ReadRepository) Refresh(ctx context.Context, kind string) (Refresh, error) {
	view := "revenue_1m_refresh"
	if kind == "traffic" {
		view = "traffic_1m_refresh"
	} else if kind != "revenue" {
		return Refresh{}, errors.New("unknown metric kind")
	}
	raw, err := r.execute(ctx, "refresh_health", `SELECT view AS name,status,ifNull(toUnixTimestamp(last_success_time),0) AS last_success_unix,toUInt8(exception!='') AS failed FROM system.view_refreshes WHERE database={db:String} AND view={view:String} FORMAT JSONEachRow`, url.Values{"param_db": {"streamforge"}, "param_view": {view}})
	if err != nil {
		return Refresh{}, err
	}
	var value Refresh
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &value); err != nil || value.Name != view {
		return value, errors.New("refresh metadata missing or invalid")
	}
	return value, nil
}
func parameters(w Window) url.Values {
	return url.Values{"param_tenant": {w.Tenant}, "param_from": {w.From.UTC().Format("2006-01-02 15:04:05")}, "param_to": {w.To.UTC().Format("2006-01-02 15:04:05")}, "param_currency": {w.Currency}}
}
func (r *ReadRepository) Revenue(ctx context.Context, w Window) ([]Revenue, error) {
	raw, err := r.execute(ctx, "revenue", `SELECT formatDateTime(minute,'%Y-%m-%dT%H:%i:%SZ','UTC') AS minute,purchase_count,toString(revenue_cents) AS revenue_cents FROM revenue_1m_canonical AS snapshot WHERE snapshot.tenant_id={tenant:String} AND currency={currency:String} AND snapshot.minute>={from:DateTime('UTC')} AND snapshot.minute<{to:DateTime('UTC')} ORDER BY snapshot.minute LIMIT 1441 FORMAT JSONEachRow`, parameters(w))
	if err != nil {
		return nil, err
	}
	return decodeLines[Revenue](raw)
}
func (r *ReadRepository) Traffic(ctx context.Context, w Window) ([]Traffic, error) {
	raw, err := r.execute(ctx, "traffic", `SELECT formatDateTime(minute,'%Y-%m-%dT%H:%i:%SZ','UTC') AS minute,events_per_minute,rpm,api_errors FROM traffic_1m_canonical AS snapshot WHERE snapshot.tenant_id={tenant:String} AND snapshot.minute>={from:DateTime('UTC')} AND snapshot.minute<{to:DateTime('UTC')} ORDER BY snapshot.minute LIMIT 1441 FORMAT JSONEachRow`, parameters(w))
	if err != nil {
		return nil, err
	}
	return decodeLines[Traffic](raw)
}
func decodeLines[T any](raw string) ([]T, error) {
	rows := make([]T, 0)
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var row T
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, errors.New("invalid ClickHouse row")
		}
		rows = append(rows, row)
		if len(rows) > 1440 {
			return nil, errors.New("query row limit exceeded")
		}
	}
	return rows, nil
}
func (r *ReadRepository) execute(ctx context.Context, operation, sql string, params url.Values) (raw string, err error) {
	start := time.Now()
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		r.metrics.DBDuration.WithLabelValues(operation, outcome).Observe(time.Since(start).Seconds())
	}()
	ctx, span := r.tracer.Start(ctx, "clickhouse."+operation, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	params.Set("database", "streamforge")
	params.Set("readonly", "1")
	params.Set("wait_end_of_query", "1")
	params.Set("max_execution_time", "1")
	params.Set("max_memory_usage", "134217728")
	params.Set("max_threads", "2")
	params.Set("output_format_json_quote_64bit_integers", "0")
	request, err := http.NewRequestWithContext(ctx, "POST", r.endpoint+"/?"+params.Encode(), strings.NewReader(sql))
	if err != nil {
		return "", fmt.Errorf("query request: %w", err)
	}
	request.SetBasicAuth(r.user, r.password)
	response, err := r.client.Do(request)
	if err != nil {
		span.SetStatus(codes.Error, "database request failed")
		return "", fmt.Errorf("ClickHouse read: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		span.SetStatus(codes.Error, "database response failed")
		return "", fmt.Errorf("ClickHouse read body: %w", readErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("ClickHouse response close: %w", closeErr)
	}
	if len(body) > 1<<20 || response.StatusCode != 200 || strings.HasPrefix(strings.TrimSpace(string(body)), "Code:") {
		span.SetStatus(codes.Error, "database execution failed")
		return "", errors.New("ClickHouse query failed or response exceeds limit")
	}
	return string(body), nil
}
