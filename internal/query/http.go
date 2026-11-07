// Package query exposes bounded tenant-isolated minute metrics.
package query

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"streamforge/internal/analytics"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/telemetry"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Repository interface {
	Refresh(context.Context, string) (analytics.Refresh, error)
	Revenue(context.Context, analytics.Window) ([]analytics.Revenue, error)
	Traffic(context.Context, analytics.Window) ([]analytics.Traffic, error)
}
type Server struct {
	config     config.Query
	repository Repository
	metrics    *telemetry.QueryMetrics
	tracer     trace.Tracer
	logger     *slog.Logger
	now        func() time.Time
	slots      chan struct{}
	hash       [32]byte
	draining   atomic.Bool
}

func New(c config.Query, r Repository, m *telemetry.QueryMetrics, t trace.Tracer, logger *slog.Logger) *Server {
	return &Server{config: c, repository: r, metrics: m, tracer: t, logger: logger, now: time.Now, slots: make(chan struct{}, c.MaxRequests), hash: sha256.Sum256([]byte(c.APIKey))}
}
func (s *Server) Drain() { s.draining.Store(true) }
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/metrics/revenue", func(w http.ResponseWriter, r *http.Request) { s.metric(w, r, "revenue") })
	mux.HandleFunc("GET /v1/metrics/traffic", func(w http.ResponseWriter, r *http.Request) { s.metric(w, r, "traffic") })
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { send(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() || !s.admit() {
			send(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		defer s.release()
		ctx, cancel := context.WithTimeout(r.Context(), s.config.RequestTimeout)
		defer cancel()
		for _, kind := range []string{"revenue", "traffic"} {
			if _, err := s.refresh(ctx, kind); err != nil {
				send(w, 503, map[string]string{"status": "snapshot_unavailable"})
				return
			}
		}
		send(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		code := 404
		if r.URL.Path == "/v1/metrics/revenue" || r.URL.Path == "/v1/metrics/traffic" {
			code = 405
			w.Header().Set("Allow", "GET")
		}
		fail(w, code, "not_found", "unknown path or method", uuid.NewString())
	})
	return mux
}
func (s *Server) admit() bool {
	select {
	case s.slots <- struct{}{}:
		s.metrics.Inflight.Inc()
		return true
	default:
		return false
	}
}
func (s *Server) release() { <-s.slots; s.metrics.Inflight.Dec() }
func ParseWindow(values url.Values, kind string, maxWindow time.Duration) (analytics.Window, error) {
	w := analytics.Window{}
	for key, value := range values {
		if len(value) != 1 || (key != "tenant_id" && key != "from" && key != "to" && key != "interval" && !(kind == "revenue" && key == "currency")) {
			return w, errors.New("unknown or repeated query parameter")
		}
	}
	w.Tenant = values.Get("tenant_id")
	if !domain.ValidIdentifier(w.Tenant) || values.Get("interval") != "1m" {
		return w, errors.New("valid tenant_id and interval=1m are required")
	}
	from, err := domain.ParseTimestamp(values.Get("from"))
	if err != nil {
		return w, errors.New("from must be RFC3339")
	}
	to, err := domain.ParseTimestamp(values.Get("to"))
	if err != nil {
		return w, errors.New("to must be RFC3339")
	}
	w.From = from.UTC()
	w.To = to.UTC()
	// DateTime('UTC') snapshot keys use the UInt32 epoch range. Limit endpoints
	// to full UTC calendar years that fit rather than allowing silent wraparound.
	if w.From.Year() < 1970 || w.To.Year() > 2105 || !w.From.Equal(w.From.Truncate(time.Minute)) || !w.To.Equal(w.To.Truncate(time.Minute)) || !w.To.After(w.From) || w.To.Sub(w.From) > maxWindow {
		return w, errors.New("require minute-aligned [from,to) within 1970..2105 and configured window cap")
	}
	if kind == "revenue" {
		w.Currency = values.Get("currency")
		if !domain.ValidCurrency(w.Currency) {
			return w, errors.New("valid explicit currency is required")
		}
	}
	return w, nil
}
func (s *Server) refresh(ctx context.Context, kind string) (analytics.Refresh, error) {
	refresh, err := s.repository.Refresh(ctx, kind)
	if err != nil {
		s.metrics.RefreshHealthy.WithLabelValues(kind).Set(0)
		return refresh, err
	}
	age := s.now().Sub(time.Unix(refresh.LastSuccess, 0))
	s.metrics.RefreshAge.WithLabelValues(kind).Set(max(0, age.Seconds()))
	if !s.fresh(refresh) {
		s.metrics.RefreshHealthy.WithLabelValues(kind).Set(0)
		return refresh, errors.New("snapshot unavailable")
	}
	s.metrics.RefreshHealthy.WithLabelValues(kind).Set(1)
	return refresh, nil
}
func (s *Server) fresh(r analytics.Refresh) bool {
	age := s.now().Sub(time.Unix(r.LastSuccess, 0))
	return r.LastSuccess > 0 && r.Failed == 0 && (r.Status == "Scheduled" || r.Status == "Running") && age >= -time.Second && age <= s.config.MaxSnapshotAge
}

type trafficBucket struct {
	analytics.Traffic
	ErrorRate float64 `json:"error_rate"`
}

func denseRevenue(w analytics.Window, rows []analytics.Revenue) ([]analytics.Revenue, error) {
	slots := map[string]analytics.Revenue{}
	for _, row := range rows {
		instant, err := time.Parse(time.RFC3339, row.Minute)
		n, ok := new(big.Int).SetString(row.Cents, 10)
		if err != nil || row.Minute != instant.UTC().Format(time.RFC3339) || instant.Before(w.From) || !instant.Before(w.To) || !instant.Equal(instant.Truncate(time.Minute)) || !ok || n.Sign() < 0 || n.BitLen() > 127 || row.Cents != n.String() {
			return nil, errors.New("invalid revenue snapshot")
		}
		if _, exists := slots[row.Minute]; exists {
			return nil, errors.New("duplicate revenue bucket")
		}
		slots[row.Minute] = row
	}
	out := make([]analytics.Revenue, 0, int(w.To.Sub(w.From)/time.Minute))
	for at := w.From; at.Before(w.To); at = at.Add(time.Minute) {
		minute := at.Format(time.RFC3339)
		row, ok := slots[minute]
		if !ok {
			row = analytics.Revenue{Minute: minute, Cents: "0"}
		}
		out = append(out, row)
	}
	return out, nil
}
func denseTraffic(w analytics.Window, rows []analytics.Traffic) ([]trafficBucket, error) {
	slots := map[string]analytics.Traffic{}
	for _, row := range rows {
		instant, err := time.Parse(time.RFC3339, row.Minute)
		if err != nil || row.Minute != instant.UTC().Format(time.RFC3339) || instant.Before(w.From) || !instant.Before(w.To) || !instant.Equal(instant.Truncate(time.Minute)) || row.Errors > row.RPM || row.RPM > row.Events {
			return nil, errors.New("invalid traffic snapshot")
		}
		if _, exists := slots[row.Minute]; exists {
			return nil, errors.New("duplicate traffic bucket")
		}
		slots[row.Minute] = row
	}
	out := make([]trafficBucket, 0, int(w.To.Sub(w.From)/time.Minute))
	for at := w.From; at.Before(w.To); at = at.Add(time.Minute) {
		minute := at.Format(time.RFC3339)
		row, ok := slots[minute]
		if !ok {
			row = analytics.Traffic{Minute: minute}
		}
		rate := float64(0)
		if row.RPM > 0 {
			rate = float64(row.Errors) / float64(row.RPM)
		}
		out = append(out, trafficBucket{Traffic: row, ErrorRate: rate})
	}
	return out, nil
}
func (s *Server) metric(w http.ResponseWriter, r *http.Request, kind string) {
	start := time.Now()
	correlation := r.Header.Get("X-Correlation-ID")
	if len(correlation) > 64 || !domain.ValidIdentifier(correlation) {
		correlation = uuid.NewString()
	}
	w.Header().Set("X-Correlation-ID", correlation)
	ctx := telemetry.Propagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := s.tracer.Start(ctx, "http.query."+kind, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
	defer cancel()
	outcome := "internal_error"
	defer func() {
		s.metrics.Requests.WithLabelValues(kind, outcome).Inc()
		s.metrics.Duration.WithLabelValues(kind, outcome).Observe(time.Since(start).Seconds())
		s.logger.Info("query request", "service", "query-api", "correlation_id", correlation, "trace_id", span.SpanContext().TraceID().String(), "kind", kind, "outcome", outcome)
	}()
	reject := func(status int, code, message string) {
		outcome = code
		span.SetStatus(codes.Error, code)
		if status == 503 {
			w.Header().Set("Retry-After", "1")
		}
		fail(w, status, code, message, correlation)
	}
	supplied := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(supplied[:], s.hash[:]) != 1 {
		reject(401, "unauthorized", "valid bearer key required")
		return
	}
	if s.draining.Load() || !s.admit() {
		reject(503, "saturated", "query service unavailable")
		return
	}
	defer s.release()
	if len(r.URL.RawQuery) > 8192 {
		reject(400, "invalid_query", "query string exceeds limit")
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		reject(400, "invalid_query", "malformed query string")
		return
	}
	window, err := ParseWindow(values, kind, s.config.MaxWindow)
	if err != nil {
		reject(400, "invalid_query", err.Error())
		return
	}
	if window.Tenant != s.config.Tenant {
		reject(403, "tenant_forbidden", "API key does not grant tenant")
		return
	}
	refresh, err := s.refresh(ctx, kind)
	if err != nil {
		reject(503, "snapshot_unavailable", "canonical snapshot is missing, failed, stale or unavailable")
		return
	}
	var data any
	if kind == "revenue" {
		var rows []analytics.Revenue
		rows, err = s.repository.Revenue(ctx, window)
		if err == nil {
			data, err = denseRevenue(window, rows)
		}
	} else {
		var rows []analytics.Traffic
		rows, err = s.repository.Traffic(ctx, window)
		if err == nil {
			data, err = denseTraffic(window, rows)
		}
	}
	if err != nil || ctx.Err() != nil {
		reject(503, "query_unavailable", "database query failed or timed out")
		return
	}
	if !s.fresh(refresh) {
		s.metrics.RefreshHealthy.WithLabelValues(kind).Set(0)
		reject(503, "snapshot_unavailable", "snapshot aged beyond limit during query")
		return
	}
	outcome = "ok"
	send(w, 200, map[string]any{"tenant_id": window.Tenant, "currency": window.Currency, "from": window.From.Format(time.RFC3339), "to": window.To.Format(time.RFC3339), "interval": "1m", "snapshot_last_success": time.Unix(refresh.LastSuccess, 0).UTC().Format(time.RFC3339), "snapshot_age_seconds": max(0, s.now().Sub(time.Unix(refresh.LastSuccess, 0)).Seconds()), "correlation_id": correlation, "data": data})
}
func fail(w http.ResponseWriter, status int, code, message, id string) {
	body := map[string]any{"code": code, "message": message, "correlation_id": id}
	if status == 503 {
		body["retry_after_seconds"] = 1
	}
	send(w, status, body)
}
func send(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Debug("query response write failed", "error", err)
	}
}
