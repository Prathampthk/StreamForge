// eventgen produces deterministic bounded HTTP workloads and records actual acknowledgments.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"streamforge/internal/domain"
	"streamforge/internal/telemetry"
)

type receipt struct {
	Event domain.Event `json:"event"`
	Hash  string       `json:"payload_sha256"`
	At    time.Time    `json:"acknowledged_at"`
	Stage string       `json:"stage"`
}
type task struct {
	Start uint64
	Count int
}
type run struct {
	mu                                                             sync.Mutex
	client                                                         *http.Client
	ingest, query, key, queryKey, tenant, out                      string
	seed                                                           uint64
	prefix                                                         [8]byte
	started                                                        time.Time
	rate, batch                                                    int
	accepted, steady, attempted, rejected, unknown, dropped        int64
	latency, queryLatency, freshness                               []float64
	queryErrors, queryCount, probeErrors, probeCount, probeDropped int
	seconds                                                        map[int64]int64
	acceptedFile, attemptFile                                      *os.File
	writer, attemptWriter                                          *bufio.Writer
	writeErr                                                       error
	probeMinutes                                                   []time.Time
}

func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
func (r *run) event(index uint64, at time.Time) domain.Event {
	var id uuid.UUID
	ms := uint64(at.UnixMilli())
	for i := 5; i >= 0; i-- {
		id[i] = byte(ms)
		ms >>= 8
	}
	id[6] = 0x70 | byte(index>>8)&15
	id[7] = byte(index)
	copy(id[8:12], r.prefix[:4])
	id[8] = (id[8] & 63) | 128
	binary.BigEndian.PutUint32(id[12:], uint32(index))
	h := mix(index ^ r.seed)
	event := domain.Event{SchemaVersion: 1, EventID: id.String(), TenantID: r.tenant, UserID: fmt.Sprintf("user_%d", h%100000), EventType: "page_view", OccurredAt: at.UTC().Format(time.RFC3339Nano), Attributes: map[string]string{"source": "eventgen", "campaign": hex.EncodeToString(r.prefix[:])}}
	switch n := h % 100; {
	case n < 60:
	case n < 80:
		event.EventType = "add_to_cart"
	case n < 90:
		event.EventType = "purchase"
		order := fmt.Sprintf("order_%d", index)
		amount := int64(mix(h)%100000 + 1)
		currency := "USD"
		if h&1 == 1 {
			currency = "EUR"
		}
		event.OrderID = &order
		event.AmountCents = &amount
		event.Currency = &currency
	default:
		event.EventType = "api_request"
		status := uint16(200)
		if mix(h)%100 < 10 {
			status = 503
		}
		event.StatusCode = &status
	}
	return event
}
func (r *run) publish(ctx context.Context, events []domain.Event, stage string, deadline time.Time) ([]telemetry.Ack, error) {
	body, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, "POST", r.ingest+"/v1/events/batch", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+r.key)
	start := time.Now()
	response, err := r.client.Do(request)
	status := 0
	var decoded struct {
		Accepted int             `json:"accepted"`
		Acks     []telemetry.Ack `json:"acknowledgments"`
		Code     string          `json:"code"`
	}
	if err == nil {
		status = response.StatusCode
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		closeErr := response.Body.Close()
		if readErr != nil {
			err = readErr
		} else if closeErr != nil {
			err = closeErr
		} else {
			err = json.Unmarshal(raw, &decoded)
		}
	}
	elapsed := time.Since(start).Seconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	outcome := "unknown"
	valid := err == nil && status == 202 && decoded.Accepted == len(events) && len(decoded.Acks) == len(events)
	ackMap := map[string]time.Time{}
	for _, ack := range decoded.Acks {
		ackMap[ack.EventID] = ack.At
	}
	if valid {
		for _, event := range events {
			if ackMap[event.EventID].IsZero() {
				valid = false
			}
		}
	}
	if valid {
		outcome = "accepted"
		r.accepted += int64(len(events))
		if stage == "steady" {
			r.latency = append(r.latency, elapsed)
		}
		for _, event := range events {
			raw, _ := json.Marshal(event)
			sum := sha256.Sum256(raw)
			if encodeErr := json.NewEncoder(r.writer).Encode(receipt{Event: event, Hash: hex.EncodeToString(sum[:]), At: ackMap[event.EventID], Stage: stage}); encodeErr != nil {
				r.writeErr = encodeErr
			}
			if stage == "steady" && !ackMap[event.EventID].Before(r.started) && ackMap[event.EventID].Before(deadline) {
				r.steady++
				r.seconds[int64(ackMap[event.EventID].Sub(r.started)/time.Second)]++
			}
		}
	} else if status == 400 || status == 401 || status == 403 || status == 413 || status == 415 || status == 429 || decoded.Code == "saturated" || decoded.Code == "draining" {
		outcome = "rejected"
		r.rejected += int64(len(events))
	} else {
		r.unknown += int64(len(events))
	}
	if stage == "steady" {
		r.attempted += int64(len(events))
	}
	if encodeErr := json.NewEncoder(r.attemptWriter).Encode(map[string]any{"stage": stage, "outcome": outcome, "http_status": status, "events": events, "completed_at": time.Now().UTC()}); encodeErr != nil {
		r.writeErr = encodeErr
	}
	if !valid {
		return nil, fmt.Errorf("HTTP publication unconfirmed/rejected (status %d)", status)
	}
	return decoded.Acks, nil
}
func (r *run) get(ctx context.Context, kind, currency string, from, to time.Time) ([]byte, error) {
	values := url.Values{"tenant_id": {r.tenant}, "from": {from.UTC().Format(time.RFC3339)}, "to": {to.UTC().Format(time.RFC3339)}, "interval": {"1m"}}
	if currency != "" {
		values.Set("currency", currency)
	}
	request, err := http.NewRequestWithContext(ctx, "GET", r.query+"/v1/metrics/"+kind+"?"+values.Encode(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+r.queryKey)
	response, err := r.client.Do(request)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("query status %d", response.StatusCode)
	}
	return raw, nil
}
func quantile(values []float64, q float64) any {
	if len(values) == 0 {
		return nil
	}
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	return copyValues[int(math.Ceil(q*float64(len(copyValues))))-1]
}
func (r *run) probe(ctx context.Context, index int, deadline time.Time) {
	at := r.probeMinutes[index%len(r.probeMinutes)]
	event := r.event(uint64(0xf0000000+index), at)
	order, currency, amount := "probe_"+strconv.Itoa(index), "JPY", int64(1)
	event.EventType = "purchase"
	event.OrderID = &order
	event.Currency = &currency
	event.AmountCents = &amount
	event.StatusCode = nil
	acks, err := r.publish(ctx, []domain.Event{event}, "probe", deadline)
	if err != nil {
		r.mu.Lock()
		r.probeErrors++
		r.mu.Unlock()
		return
	}
	acknowledged := acks[0].At
	probeDeadline := acknowledged.Add(30 * time.Second)
	success := false
	for ctx.Err() == nil && time.Now().Before(probeDeadline) {
		raw, err := r.get(ctx, "revenue", "JPY", at, at.Add(time.Minute))
		if err == nil {
			var body struct {
				Data []struct {
					Cents string `json:"revenue_cents"`
					Count uint64 `json:"purchase_count"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &body) == nil && len(body.Data) == 1 && body.Data[0].Cents == "1" && body.Data[0].Count == 1 {
				success = true
				break
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probeCount++
	if success {
		r.freshness = append(r.freshness, time.Since(acknowledged).Seconds())
	} else {
		r.probeErrors++
	}
}
func metrics(client *http.Client, address string) map[float64]float64 {
	result := map[float64]float64{}
	response, err := client.Get(address + "/metrics")
	if err != nil {
		return result
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "streamforge_ingest_request_duration_seconds_bucket{") || !strings.Contains(line, `outcome="accepted"`) {
			continue
		}
		at := strings.Index(line, `le="`)
		if at < 0 {
			continue
		}
		label := strings.SplitN(line[at+4:], `"`, 2)[0]
		upper, _ := strconv.ParseFloat(label, 64)
		parts := strings.Fields(line)
		if len(parts) == 2 {
			value, _ := strconv.ParseFloat(parts[1], 64)
			result[upper] = value
		}
	}
	return result
}
func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, "eventgen:", err)
		os.Exit(1)
	}
}
func execute() error {
	rate := flag.Int("rate", 1000, "offered events/second")
	duration := flag.Duration("duration", 15*time.Minute, "steady duration <=30m")
	batch := flag.Int("batch", 100, "events/request 1..100")
	workers := flag.Int("workers", 32, "bounded HTTP workers 1..128")
	seed := flag.Uint64("seed", 42, "deterministic workload seed")
	out := flag.String("out", "/results", "private raw result directory")
	tenant := flag.String("tenant", "benchmark", "isolated tenant")
	ingest := flag.String("ingest", "http://bench-ingest:8080", "ingestion origin")
	queryURL := flag.String("query", "http://bench-query:8082", "query origin")
	history := flag.Bool("seed-history", true, "seed one event per minute for previous 24h")
	isolated := flag.Bool("isolated-tenant", false, "caller guarantees a new run-owned tenant with no prior JPY data")
	flag.Parse()
	if *rate < 1 || *rate > 100000 || *duration < time.Second || *duration > 30*time.Minute || *batch < 1 || *batch > 100 || *workers < 1 || *workers > 128 || *duration > time.Duration(11000000/int64(*rate))*time.Second || !domain.ValidIdentifier(*tenant) {
		return errors.New("invalid bounds (max 11M offered events)")
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		return err
	}
	accepted, err := os.OpenFile(filepath.Join(*out, "accepted.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer accepted.Close()
	attempts, err := os.OpenFile(filepath.Join(*out, "attempts.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer attempts.Close()
	r := &run{client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxConnsPerHost: *workers + 8, MaxIdleConnsPerHost: *workers + 8, MaxIdleConns: 2 * (*workers + 8)}}, ingest: *ingest, query: *queryURL, key: os.Getenv("INGEST_API_KEY"), queryKey: os.Getenv("QUERY_API_KEY"), tenant: *tenant, out: *out, seed: *seed, rate: *rate, batch: *batch, seconds: map[int64]int64{}, acceptedFile: accepted, attemptFile: attempts, writer: bufio.NewWriterSize(accepted, 1<<20), attemptWriter: bufio.NewWriterSize(attempts, 1<<20)}
	if len(r.key) < 24 || len(r.queryKey) < 24 {
		return errors.New("INGEST_API_KEY and QUERY_API_KEY required")
	}
	runID := uuid.New()
	copy(r.prefix[:], runID[:8])
	setup, cancelSetup := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelSetup()
	now := time.Now().UTC().Truncate(time.Minute)
	if *history {
		for i := 0; i < 1440; i += 100 {
			events := []domain.Event{}
			for j := i; j < min(i+100, 1440); j++ {
				events = append(events, r.event(uint64(0xe0000000+j), now.Add(-24*time.Hour+time.Duration(j)*time.Minute)))
			}
			if _, err := r.publish(setup, events, "seed", time.Time{}); err != nil {
				return fmt.Errorf("seed: %w", err)
			}
		}
	}
	// A wrapper-owned fresh tenant has no prior JPY contributions. This lets
	// measurement count unhealthy query responses instead of aborting setup.
	if *isolated {
		if !strings.HasPrefix(*tenant, "bench_") {
			return errors.New("isolated mode requires wrapper-owned bench tenant")
		}
		for i := 0; i < int(duration.Seconds())+1; i++ {
			r.probeMinutes = append(r.probeMinutes, now.Add(-6*24*time.Hour+time.Duration(i)*time.Minute))
		}
	}
	// Otherwise reserve actually empty JPY minutes for reusable pilot tenants.
	for days := 6; days >= 2 && len(r.probeMinutes) < int(duration.Seconds())+1; days-- {
		base := now.Add(-time.Duration(days) * 24 * time.Hour)
		raw, err := r.get(setup, "revenue", "JPY", base, base.Add(24*time.Hour))
		for err != nil && setup.Err() == nil {
			select {
			case <-setup.Done():
			case <-time.After(time.Second):
			}
			raw, err = r.get(setup, "revenue", "JPY", base, base.Add(24*time.Hour))
		}
		if err != nil {
			return fmt.Errorf("probe planning: %w", err)
		}
		var body struct {
			Data []struct {
				Minute string `json:"minute"`
				Count  uint64 `json:"purchase_count"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return err
		}
		for _, row := range body.Data {
			if row.Count == 0 {
				at, err := time.Parse(time.RFC3339, row.Minute)
				if err != nil {
					return err
				}
				r.probeMinutes = append(r.probeMinutes, at)
			}
		}
	}
	if len(r.probeMinutes) < int(duration.Seconds())+1 {
		return errors.New("insufficient empty probe minutes")
	}
	before := metrics(r.client, r.ingest)
	r.started = time.Now().UTC()
	progressDone := make(chan struct{})
	progressStopped := make(chan struct{})
	defer func() { close(progressDone); <-progressStopped }()
	go func() {
		defer close(progressStopped)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				r.mu.Lock()
				raw, err := json.Marshal(map[string]any{"elapsed_seconds": time.Since(r.started).Seconds(), "accepted_in_steady": r.steady, "attempted": r.attempted, "driver_dropped": r.dropped, "query_errors": r.queryErrors, "freshness_failures": r.probeErrors})
				r.mu.Unlock()
				if err == nil {
					if err := os.WriteFile(filepath.Join(r.out, "progress.tmp"), raw, 0600); err == nil {
						if err := os.Rename(filepath.Join(r.out, "progress.tmp"), filepath.Join(r.out, "progress.json")); err != nil {
							fmt.Fprintln(os.Stderr, "progress rename failed")
						}
					}
				}
			}
		}
	}()

	deadline := r.started.Add(*duration)
	ctx, cancel := context.WithDeadline(context.Background(), deadline.Add(35*time.Second))
	defer cancel()
	queue := make(chan task, *workers)
	var workerGroup sync.WaitGroup
	for i := 0; i < *workers; i++ {
		workerGroup.Add(1)
		go func() {
			defer workerGroup.Done()
			for task := range queue {
				events := make([]domain.Event, task.Count)
				for i := range events {
					index := task.Start + uint64(i)
					at := r.started.Add(time.Duration(float64(index) / float64(r.rate) * float64(time.Second)))
					events[i] = r.event(index, at)
				}
				r.publish(ctx, events, "steady", deadline)
			}
		}()
	}
	queryCtx, stopQueries := context.WithDeadline(ctx, deadline)
	defer stopQueries()
	var auxiliaries sync.WaitGroup
	for i := 0; i < 2; i++ {
		auxiliaries.Add(1)
		go func(index int) {
			defer auxiliaries.Done()
			for queryCtx.Err() == nil {
				end := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
				window := 15 * time.Minute
				if index == 1 {
					window = 24 * time.Hour
				}
				start := time.Now()
				_, err := r.get(queryCtx, "traffic", "", end.Add(-window), end)
				elapsed := time.Since(start).Seconds()
				r.mu.Lock()
				r.queryCount++
				if err != nil {
					r.queryErrors++
				} else {
					r.queryLatency = append(r.queryLatency, elapsed)
				}
				r.mu.Unlock()
				select {
				case <-queryCtx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}(i)
	}
	probeQueue := make(chan int, 4)
	var probes sync.WaitGroup
	for i := 0; i < 4; i++ {
		probes.Add(1)
		go func() {
			defer probes.Done()
			for index := range probeQueue {
				r.probe(ctx, index, deadline)
			}
		}()
	}
	auxiliaries.Add(1)
	go func() {
		defer auxiliaries.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		index := 0
		for {
			select {
			case <-queryCtx.Done():
				return
			case <-ticker.C:
				select {
				case probeQueue <- index:
				default:
					r.mu.Lock()
					r.probeDropped++
					r.mu.Unlock()
				}
				index++
			}
		}
	}()
	total := int64(duration.Seconds() * float64(*rate))
	for index := int64(0); index < total; index += int64(*batch) {
		due := r.started.Add(time.Duration(float64(index) / float64(*rate) * float64(time.Second)))
		if delay := time.Until(due); delay > 0 {
			time.Sleep(delay)
		}
		count := min(*batch, int(total-index))
		if time.Now().After(deadline) {
			r.mu.Lock()
			r.dropped += total - index
			r.mu.Unlock()
			break
		}
		select {
		case queue <- task{Start: uint64(index), Count: count}:
		default:
			r.mu.Lock()
			r.dropped += int64(count)
			r.mu.Unlock()
		}
	}
	close(queue)
	workerGroup.Wait()
	stopQueries()
	auxiliaries.Wait()
	close(probeQueue)
	probes.Wait()
	after := metrics(r.client, r.ingest)
	var upper, serverEstimate any
	bounds := []float64{}
	var count float64
	for b, v := range after {
		delta := v - before[b]
		if math.IsInf(b, 1) {
			count = delta
		} else {
			bounds = append(bounds, b)
		}
	}
	sort.Float64s(bounds)
	lower, lowerCount := float64(0), float64(0)
	for _, b := range bounds {
		current := after[b] - before[b]
		if current >= count*.95 && count > 0 {
			upper = b
			if current > lowerCount {
				serverEstimate = lower + (b-lower)*(count*.95-lowerCount)/(current-lowerCount)
			} else {
				serverEstimate = b
			}
			break
		}
		lower = b
		lowerCount = current
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writer.Flush(); err != nil {
		return err
	}
	if err := r.attemptWriter.Flush(); err != nil {
		return err
	}
	if err := accepted.Sync(); err != nil {
		return err
	}
	if err := attempts.Sync(); err != nil {
		return err
	}
	if r.writeErr != nil {
		return r.writeErr
	}
	samples := []map[string]any{}
	for i := int64(0); i < int64(duration.Seconds()); i++ {
		samples = append(samples, map[string]any{"second": i, "acknowledged_events": r.seconds[i]})
	}
	summary := map[string]any{"run_id": runID.String(), "tenant": r.tenant, "seed": *seed, "started_at": r.started, "duration_seconds": duration.Seconds(), "offered_events": total, "attempted_steady_events": r.attempted, "accepted_all_stages": r.accepted, "accepted_in_steady_interval": r.steady, "accepted_events_per_second": float64(r.steady) / duration.Seconds(), "rejected_events_all_stages": r.rejected, "unknown_events_all_stages": r.unknown, "driver_dropped_events": r.dropped, "client_ack_p50_seconds": quantile(r.latency, .5), "client_ack_p95_seconds": quantile(r.latency, .95), "client_ack_p99_seconds": quantile(r.latency, .99), "server_ack_p95_bucket_upper_seconds": upper, "server_ack_p95_histogram_estimate_seconds": serverEstimate, "query_concurrency": 2, "query_count": r.queryCount, "query_errors": r.queryErrors, "query_p95_seconds": quantile(r.queryLatency, .95), "freshness_poll_seconds": .1, "freshness_timeout_seconds": 30, "freshness_samples": r.probeCount, "freshness_censored_or_failed": r.probeErrors, "freshness_driver_dropped": r.probeDropped, "freshness_success_p50_seconds": quantile(r.freshness, .5), "freshness_success_p95_seconds": quantile(r.freshness, .95), "freshness_success_p99_seconds": quantile(r.freshness, .99), "freshness_success_max_seconds": quantile(r.freshness, 1), "per_second": samples}
	raw, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "summary.json"), raw, 0600); err != nil {
		return err
	}
	fmt.Println(string(raw[:min(len(raw), 3000)]))
	return nil
}
