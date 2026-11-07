// Package ingest implements the authenticated ingestion HTTP boundary.
package ingest

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/telemetry"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

// Publisher returns nil only after every event has a successful broker promise.
type Publisher interface {
	Publish(context.Context, []domain.Event, string) error
	Ready(context.Context) error
}

type Server struct {
	config    config.Config
	publisher Publisher
	metrics   *telemetry.Metrics
	tracer    trace.Tracer
	logger    *slog.Logger
	now       func() time.Time
	slots     chan struct{}
	readiness chan struct{}
	limiter   *rate.Limiter
	keyHash   [32]byte
	draining  atomic.Bool
}

func New(c config.Config, p Publisher, m *telemetry.Metrics, t trace.Tracer, logger *slog.Logger) *Server {
	return &Server{config: c, publisher: p, metrics: m, tracer: t, logger: logger, now: time.Now,
		slots: make(chan struct{}, c.MaxRequests), readiness: make(chan struct{}, 1), limiter: rate.NewLimiter(rate.Limit(c.RequestsPerSecond), c.Burst), keyHash: sha256.Sum256([]byte(c.APIKey))}
}
func (s *Server) Drain() { s.draining.Store(true) }
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) { s.events(w, r, false) })
	mux.HandleFunc("POST /v1/events/batch", func(w http.ResponseWriter, r *http.Request) { s.events(w, r, true) })
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		status := http.StatusNotFound
		code := "not_found"
		if r.URL.Path == "/v1/events" || r.URL.Path == "/v1/events/batch" {
			status = http.StatusMethodNotAllowed
			code = "method_not_allowed"
			w.Header().Set("Allow", "POST")
		}
		correlation := uuid.NewString()
		w.Header().Set("X-Correlation-ID", correlation)
		failure(w, status, code, "unknown path or unsupported method", correlation, 0)
	})
	return mux
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		respond(w, 503, map[string]string{"status": "draining"})
		return
	}
	select {
	case s.readiness <- struct{}{}:
		defer func() { <-s.readiness }()
	default:
		respond(w, 503, map[string]string{"status": "busy"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.publisher.Ready(ctx); err != nil {
		respond(w, 503, map[string]string{"status": "dependency_unavailable"})
		return
	}
	if s.draining.Load() {
		respond(w, 503, map[string]string{"status": "draining"})
		return
	}
	respond(w, 200, map[string]string{"status": "ready"})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request, batch bool) {
	started := s.now()
	correlation := r.Header.Get("X-Correlation-ID")
	if len(correlation) > 64 || !domain.ValidIdentifier(correlation) {
		correlation = uuid.NewString()
	}
	w.Header().Set("X-Correlation-ID", correlation)
	ctx := telemetry.Propagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := s.tracer.Start(ctx, "http.ingest", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
	defer cancel()
	outcome := "internal_error"
	defer func() {
		s.metrics.Requests.WithLabelValues(outcome).Inc()
		s.metrics.HTTPLatency.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
		s.logger.Info("ingest request", "service", "ingest-api", "correlation_id", correlation, "trace_id", span.SpanContext().TraceID().String(), "outcome", outcome, "duration_ms", time.Since(started).Milliseconds())
	}()
	reject := func(status int, code, message string, retry int) {
		outcome = code
		span.SetStatus(codes.Error, code)
		failure(w, status, code, message, correlation, retry)
	}
	supplied := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(supplied[:], s.keyHash[:]) != 1 {
		reject(401, "unauthorized", "valid bearer API key required", 0)
		return
	}
	if s.draining.Load() {
		reject(503, "draining", "server is shutting down", 1)
		return
	}
	if !s.limiter.Allow() {
		reject(429, "rate_limited", "request rate limit reached", 1)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		reject(503, "saturated", "inflight request limit reached", 1)
		return
	}
	s.metrics.HTTPInflight.Inc()
	defer s.metrics.HTTPInflight.Dec()
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		reject(415, "unsupported_media_type", "Content-Type must be application/json", 0)
		return
	}
	if r.Header.Get("Content-Encoding") != "" && r.Header.Get("Content-Encoding") != "identity" {
		reject(415, "unsupported_encoding", "compressed request bodies are unsupported", 0)
		return
	}
	limit := int64(domain.MaxEventBytes)
	if batch {
		limit = int64(s.config.MaxBodyBytes)
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reject(413, "body_too_large", "request body exceeds configured limit", 0)
		} else {
			reject(400, "invalid_body", "could not read request body", 0)
		}
		return
	}
	if ctx.Err() != nil {
		reject(503, "request_timeout", "request deadline exceeded", 1)
		return
	}
	var rawEvents []json.RawMessage
	if batch {
		if err := domain.CheckJSON(body); err != nil {
			reject(400, "invalid_event", err.Error(), 0)
			return
		}
		if err := json.Unmarshal(body, &rawEvents); err != nil || len(rawEvents) == 0 || len(rawEvents) > s.config.MaxBatchEvents {
			reject(400, "invalid_batch", "batch must be a nonempty JSON array within the event-count limit", 0)
			return
		}
	} else {
		rawEvents = []json.RawMessage{body}
	}
	events := make([]domain.Event, len(rawEvents))
	for i, raw := range rawEvents {
		if len(raw) > domain.MaxEventBytes {
			reject(413, "event_too_large", "event exceeds 8192 bytes", 0)
			return
		}
		event, err := domain.DecodeEvent(raw, s.now(), domain.Policy{MaxLateness: s.config.MaxLateness, FutureSkew: s.config.FutureSkew})
		if err != nil {
			reject(400, "invalid_event", err.Error(), 0)
			return
		}
		if event.TenantID != s.config.Tenant {
			reject(403, "tenant_forbidden", "API key does not grant this tenant", 0)
			return
		}
		events[i] = event
	}
	publishCtx, publishCancel := context.WithTimeout(ctx, s.config.PublishTimeout)
	defer publishCancel()
	if err := s.publisher.Publish(publishCtx, events, correlation); err != nil {
		reject(503, "publish_unconfirmed", "broker acknowledgment not confirmed; delivery may have occurred", 1)
		return
	}
	// A canceled caller cannot turn a late broker completion into HTTP acceptance.
	if ctx.Err() != nil || publishCtx.Err() != nil {
		reject(503, "publish_unconfirmed", "deadline elapsed; delivery may have occurred", 1)
		return
	}
	outcome = "accepted"
	respond(w, http.StatusAccepted, map[string]any{"accepted": len(events), "correlation_id": correlation})
}

type errorEnvelope struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	CorrelationID     string `json:"correlation_id"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
}

func failure(w http.ResponseWriter, status int, code, message, correlation string, retry int) {
	if retry > 0 {
		w.Header().Set("Retry-After", "1")
	}
	respond(w, status, errorEnvelope{Code: code, Message: message, CorrelationID: correlation, RetryAfterSeconds: retry})
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	// net/http owns write failures (usually disconnected clients). Payloads here
	// are only JSON-safe primitives; acceptance metrics describe server outcome.
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Debug("HTTP response write failed", "error", err)
	}
}
