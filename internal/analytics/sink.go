// Package analytics implements the synchronous ClickHouse HTTP insert boundary.
package analytics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/pipeline"
)

const insertSQL = "INSERT INTO events_raw (event_id, tenant_id, user_id, session_id, event_type, occurred_at, received_at, order_id, amount_cents, currency, status_code, kafka_topic, kafka_partition, kafka_offset, sink_batch_id, schema_version, payload_json) FORMAT JSONEachRow\n"

type Sink struct {
	endpoint, user, password string
	client                   *http.Client
	transport                *http.Transport
}

func NewSink(c config.Consumer) *Sink {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxConnsPerHost: c.Workers, MaxIdleConns: c.Workers, MaxIdleConnsPerHost: c.Workers, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: c.InsertTimeout}
	return &Sink{endpoint: strings.TrimRight(c.ClickHouseURL, "/"), user: c.ClickHouseUser, password: c.ClickHousePassword, client: &http.Client{Transport: transport, Timeout: c.InsertTimeout}, transport: transport}
}
func (s *Sink) Insert(ctx context.Context, b pipeline.Batch) error {
	params := url.Values{"database": {"streamforge"}, "async_insert": {"0"}, "wait_end_of_query": {"1"}, "insert_deduplicate": {"0"}, "materialized_views_ignore_errors": {"0"}, "max_execution_time": {"10"}}
	// The complete immutable body is resent after ambiguous/partial outcomes.
	return s.execute(ctx, params, io.MultiReader(strings.NewReader(insertSQL), bytes.NewReader(b.Body)), "")
}
func (s *Sink) Ready(ctx context.Context) error {
	return s.execute(ctx, url.Values{"database": {"streamforge"}, "wait_end_of_query": {"1"}}, strings.NewReader("SELECT count() FROM event_identity_states WHERE 0"), "0")
}
func (s *Sink) execute(ctx context.Context, params url.Values, body io.Reader, expected string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint+"/?"+params.Encode(), body)
	if err != nil {
		return fmt.Errorf("ClickHouse request: %w", err)
	}
	request.SetBasicAuth(s.user, s.password)
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("ClickHouse insert/request outcome unknown: %w", err)
	}

	// Consume to EOF within a bounded body. With wait_end_of_query the HTTP
	// status reflects execution, including dependent incremental MV failures.
	payload, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	closeErr := response.Body.Close()
	if err != nil {
		return fmt.Errorf("ClickHouse response outcome unknown: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("ClickHouse close outcome unknown: %w", closeErr)
	}
	if len(payload) > 64<<10 {
		return errors.New("ClickHouse response exceeds limit; outcome unknown")
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("ClickHouse status %d (details omitted; inspect server logs)", response.StatusCode)
	}
	if strings.TrimSpace(string(payload)) != expected {
		return errors.New("ClickHouse returned unexpected response; execution outcome unconfirmed")
	}
	return nil
}
func (s *Sink) Close() { s.transport.CloseIdleConnections() }
