//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"streamforge/internal/analytics"
	"streamforge/internal/domain"
	"streamforge/internal/pipeline"
)

type drillProcess struct {
	command *exec.Cmd
	output  processOutput
	done    chan error
	exited  bool
}

func shutdownDirectory(t *testing.T) string {
	t.Helper()
	if os.Getenv("STREAMFORGE_RUN_PROCESS_SHUTDOWN") != "1" {
		t.Skip("explicit owned process-shutdown drill only")
	}
	path := os.Getenv("RF3_RESULTS")
	if path == "" {
		t.Fatal("RF3_RESULTS required")
	}
	t.Cleanup(func() {
		if t.Failed() {
			shutdownReport(t, "failed.json", map[string]any{"test": t.Name(), "success": false})
		}
	})
	return path
}
func shutdownReport(t *testing.T, name string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(os.Getenv("RF3_RESULTS"), name)
	if err := os.WriteFile(path+".tmp", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}
func startDrillProcess(t *testing.T, binary string, settings map[string]string) *drillProcess {
	t.Helper()
	p := &drillProcess{done: make(chan error, 1)}
	p.command = exec.Command(filepath.Join(os.Getenv("RF3_RESULTS"), binary))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, ok := settings[key]; !ok {
			p.command.Env = append(p.command.Env, entry)
		}
	}
	for key, value := range settings {
		p.command.Env = append(p.command.Env, key+"="+value)
	}
	p.command.Stdout = &p.output
	p.command.Stderr = &p.output
	if err := p.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.command.Wait() }()
	t.Cleanup(func() {
		if !p.exited {
			_ = p.command.Process.Kill()
			select {
			case <-p.done:
			case <-time.After(3 * time.Second):
				t.Error("child did not terminate")
			}
		}
	})
	t.Cleanup(func() {
		path := filepath.Join(os.Getenv("RF3_RESULTS"), fmt.Sprintf("%s-%d.log", binary, p.command.Process.Pid))
		if err := os.WriteFile(path, []byte(p.output.String()), 0600); err != nil {
			t.Error(err)
		}
		if t.Failed() {
			t.Logf("%s child output: %s", binary, p.output.String())
		}
	})
	return p
}
func (p *drillProcess) stopped(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-p.done:
		p.exited = true
		if err != nil {
			t.Fatalf("process failed: %v output=%s", err, p.output.String())
		}
	case <-time.After(timeout):
		t.Fatalf("process shutdown exceeded %s: %s", timeout, p.output.String())
	}
}
func (p *drillProcess) signal(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
}
func waitDrillReady(t *testing.T, origin string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	eventually(t, 30*time.Second, "real process readiness", func() bool {
		r, err := client.Get(origin + "/readyz")
		if err != nil {
			return false
		}
		_, readErr := io.Copy(io.Discard, r.Body)
		closeErr := r.Body.Close()
		return readErr == nil && closeErr == nil && r.StatusCode == 200
	})
}
func waitListenerClosed(t *testing.T, address string) {
	t.Helper()
	eventually(t, 2*time.Second, "listener closes during drain", func() bool {
		c, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return true
		}
		_ = c.Close()
		return false
	})
}
func shutdownAdmin(t *testing.T) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BROKERS"), ",")...), kgo.RetryTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func shutdownMetadata(t *testing.T, c *kgo.Client, isr int) map[int32]bool {
	t.Helper()
	var leaders map[int32]bool
	eventually(t, 45*time.Second, fmt.Sprintf("shutdown fixture ISR%d", isr), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r := kmsg.NewPtrMetadataRequest()
		topic := "streamforge.events.v1"
		r.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
		m, err := r.RequestWith(ctx, c)
		if err != nil || len(m.Topics) != 1 || m.Topics[0].ErrorCode != 0 || len(m.Topics[0].Partitions) != 12 {
			return false
		}
		leaders = map[int32]bool{}
		for _, p := range m.Topics[0].Partitions {
			if p.ErrorCode != 0 || p.Leader < 0 || len(p.ISR) != isr || len(p.Replicas) != 3 {
				return false
			}
			leaders[p.Leader] = true
		}
		return true
	})
	return leaders
}
func shutdownMinimum(c *kgo.Client, value string, leaders map[int32]bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := kmsg.NewPtrIncrementalAlterConfigsRequest()
	r.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{ResourceType: 2, ResourceName: "streamforge.events.v1", Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{Name: "min.insync.replicas", Op: 0, Value: &value}}}}
	// Administrative requests may otherwise pick the intentionally dead seed.
	// Partition metadata above identifies brokers currently leading live ISRs.
	live := int32(-1)
	for id := range leaders {
		if live < 0 || id < live {
			live = id
		}
	}
	if live < 0 {
		return fmt.Errorf("no confirmed live broker for ISR override")
	}
	response, err := r.RequestWith(ctx, c.Broker(int(live)))
	if err != nil {
		return err
	}
	if len(response.Resources) != 1 || response.Resources[0].ErrorCode != 0 {
		return fmt.Errorf("ISR override rejected")
	}
	return nil
}
func verifyShutdownMinimum(t *testing.T, c *kgo.Client, leaders map[int32]bool, value string) {
	t.Helper()
	eventually(t, 5*time.Second, "ISR override installed on live leaders", func() bool {
		for id := range leaders {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r := kmsg.NewPtrDescribeConfigsRequest()
			r.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: 2, ResourceName: "streamforge.events.v1", ConfigNames: []string{"min.insync.replicas"}}}
			response, err := r.RequestWith(ctx, c.Broker(int(id)))
			cancel()
			if err != nil || len(response.Resources) != 1 || response.Resources[0].ErrorCode != 0 {
				return false
			}
			found := false
			for _, config := range response.Resources[0].Configs {
				if config.Name == "min.insync.replicas" && config.Value != nil && *config.Value == value {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
}

type shutdownHTTP struct {
	status int
	body   []byte
	err    error
}

func shutdownPost(origin, key string, events []domain.Event) shutdownHTTP {
	raw, err := json.Marshal(events)
	if err != nil {
		return shutdownHTTP{err: err}
	}
	r, err := http.NewRequest("POST", origin+"/v1/events/batch", strings.NewReader(string(raw)))
	if err != nil {
		return shutdownHTTP{err: err}
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+key)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(r)
	if err != nil {
		return shutdownHTTP{err: err}
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return shutdownHTTP{err: readErr}
	}
	if closeErr != nil {
		return shutdownHTTP{err: closeErr}
	}
	return shutdownHTTP{status: response.StatusCode, body: body}
}
func shutdownEvents(tenant string) []domain.Event {
	events := make([]domain.Event, 12)
	for i := range events {
		events[i] = domain.Event{SchemaVersion: 1, EventID: uuid.NewString(), TenantID: tenant, UserID: fmt.Sprintf("shutdown_user_%d", i), EventType: "page_view", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	return events
}
func TestIngestSIGTERM(t *testing.T) {
	shutdownDirectory(t)
	admin := shutdownAdmin(t)
	leaders := shutdownMetadata(t, admin, 2)
	defer func() {
		if err := shutdownMinimum(admin, "2", leaders); err != nil {
			t.Error(err)
		}
	}()
	accepted := map[string]string{}
	unknown := map[string]bool{}
	var outcomes []map[string]any
	acknowledged := func(response shutdownHTTP, events []domain.Event) {
		if response.err != nil || response.status != 202 {
			t.Fatalf("expected acknowledgment: status=%d err=%v body=%s", response.status, response.err, response.body)
		}
		var receipt struct {
			Accepted int `json:"accepted"`
			Acks     []struct {
				ID string    `json:"event_id"`
				At time.Time `json:"acknowledged_at"`
			} `json:"acknowledgments"`
		}
		if err := json.Unmarshal(response.body, &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.Accepted != len(events) || len(receipt.Acks) != len(events) {
			t.Fatal("incomplete receipt")
		}
		ids := map[string]bool{}
		for _, a := range receipt.Acks {
			if a.At.IsZero() || ids[a.ID] {
				t.Fatal("invalid receipt")
			}
			ids[a.ID] = true
		}
		for _, event := range events {
			if !ids[event.EventID] {
				t.Fatal("receipt ID missing")
			}
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			accepted[event.EventID] = string(raw)
		}
	}
	for _, release := range []bool{true, false} {
		t.Run(fmt.Sprintf("release_dependency_%v", release), func(t *testing.T) {
			if err := shutdownMinimum(admin, "2", leaders); err != nil {
				t.Fatal(err)
			}
			verifyShutdownMinimum(t, admin, leaders, "2")
			key := "shutdown-ingest-key-at-least-32-bytes"
			tenant := "shutdown_fixture"
			p := startDrillProcess(t, "ingest-api", map[string]string{"HTTP_ADDR": "127.0.0.1:18080", "INGEST_API_KEY": key, "INGEST_TENANT": tenant, "TRACE_SAMPLE_RATIO": "0"})
			origin := "http://127.0.0.1:18080"
			waitDrillReady(t, origin)
			if release {
				events := shutdownEvents(tenant)
				acknowledged(shutdownPost(origin, key, events), events)
			}
			if err := shutdownMinimum(admin, "3", leaders); err != nil {
				t.Fatal(err)
			}
			verifyShutdownMinimum(t, admin, leaders, "3")
			events := shutdownEvents(tenant)
			pending := make(chan shutdownHTTP, 1)
			go func() { pending <- shutdownPost(origin, key, events) }()
			eventually(t, 3*time.Second, "real producer has unresolved promises", func() bool {
				r, err := (&http.Client{Timeout: time.Second}).Get(origin + "/metrics")
				if err != nil {
					return false
				}
				raw, readErr := io.ReadAll(io.LimitReader(r.Body, 65536))
				closeErr := r.Body.Close()
				if readErr != nil || closeErr != nil {
					return false
				}
				for _, line := range strings.Split(string(raw), "\n") {
					if strings.HasPrefix(line, "streamforge_producer_inflight_records ") {
						value, err := strconv.ParseFloat(strings.TrimPrefix(line, "streamforge_producer_inflight_records "), 64)
						return err == nil && value > 0
					}
				}
				return false
			})
			began := time.Now()
			p.signal(t)
			waitListenerClosed(t, "127.0.0.1:18080")
			select {
			case err := <-p.done:
				p.exited = true
				t.Fatalf("exited with unresolved publication: %v %s", err, p.output.String())
			default:
			}
			if release {
				if err := shutdownMinimum(admin, "2", leaders); err != nil {
					t.Fatal(err)
				}
			}
			var response shutdownHTTP
			select {
			case response = <-pending:
			case <-time.After(10 * time.Second):
				t.Fatal("HTTP waiter did not finish")
			}
			if release {
				acknowledged(response, events)
			} else {
				var failure struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(response.body, &failure); err != nil {
					t.Fatal(err)
				}
				if response.err != nil || response.status != 503 || failure.Code != "publish_unconfirmed" {
					t.Fatalf("false acceptance or wrong failure: %+v code=%s", response, failure.Code)
				}
				for _, e := range events {
					unknown[e.EventID] = true
				}
			}
			p.stopped(t, 15*time.Second)
			elapsed := time.Since(began).Seconds()
			if elapsed > 15 {
				t.Fatalf("default shutdown deadline exceeded: %f", elapsed)
			}
			outcomes = append(outcomes, map[string]any{"dependency_released": release, "http_status": response.status, "exit_code": 0, "shutdown_seconds": elapsed})
		})
	}
	if err := shutdownMinimum(admin, "2", leaders); err != nil {
		t.Fatal(err)
	}
	assignments := map[int32]kgo.Offset{}
	for i := int32(0); i < 12; i++ {
		assignments[i] = kgo.NewOffset().AtStart()
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BROKERS"), ",")...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"streamforge.events.v1": assignments}), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	observed := map[string]bool{}
	unknownObserved := map[string]bool{}
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline) && len(observed) < len(accepted); {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := reader.PollFetches(ctx)
		cancel()
		fetches.EachRecord(func(r *kgo.Record) {
			var e domain.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Fatal(err)
			}
			if payload, ok := accepted[e.EventID]; ok {
				if string(r.Value) != payload {
					t.Fatal("accepted payload mismatch")
				}
				observed[e.EventID] = true
			} else if unknown[e.EventID] {
				unknownObserved[e.EventID] = true
			}
		})
	}
	if len(accepted) != 24 || len(observed) != 24 {
		t.Fatalf("accepted=%d verified=%d", len(accepted), len(observed))
	}
	shutdownReport(t, "ingest-shutdown.json", map[string]any{"success": true, "accepted": len(accepted), "verified_exact_payloads": len(observed), "unknown_http_events": len(unknown), "unknown_events_observed": len(unknownObserved), "outcomes": outcomes, "scope": "real ingestion binary; SIGTERM with unresolved broker promises; default 5s publish/8s request/15s shutdown limits"})
}

func TestConsumerSIGTERM(t *testing.T) {
	shutdownDirectory(t)
	eventually(t, 90*time.Second, "host restores third broker", func() bool {
		_, err := os.Stat(filepath.Join(os.Getenv("RF3_RESULTS"), "consumer-ready.go"))
		return err == nil
	})
	admin := shutdownAdmin(t)
	shutdownMetadata(t, admin, 3)
	c := sinkConfig(t)
	c.Group = "shutdown_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	producer, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	tenant := "consumer_shutdown_fixture"
	records := fixture(t, producer, c.Topic, tenant, 10, true)
	settings := map[string]string{"CONSUMER_GROUP": c.Group, "CONSUMER_HTTP_ADDR": "127.0.0.1:18081", "CONSUMER_FAILPOINT": "after_insert", "ALLOW_TEST_FAILPOINTS": "true", "TRACE_SAMPLE_RATIO": "0"}
	first := startDrillProcess(t, "consumer", settings)
	eventually(t, 25*time.Second, "real consumer confirmed insert before commit", func() bool {
		return strings.Contains(first.output.String(), "test failpoint reached") && strings.Contains(first.output.String(), "after_insert")
	})
	first.signal(t)
	first.stopped(t, 10*time.Second)
	offsetClient, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.MaxVersions(kversion.V2_8_0()), kgo.RetryTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer offsetClient.Close()
	var beforeOffsets map[int32]int64
	eventually(t, 5*time.Second, "successful native offset fetch after SIGTERM", func() bool {
		r := kmsg.NewPtrOffsetFetchRequest()
		r.Group = c.Group
		partitions := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		r.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: c.Topic, Partitions: partitions}}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		response, err := r.RequestWith(ctx, offsetClient)
		if err != nil || response.ErrorCode != 0 || len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 12 {
			return false
		}
		beforeOffsets = map[int32]int64{}
		for _, partition := range response.Topics[0].Partitions {
			if partition.ErrorCode != 0 || partition.Partition < 0 || partition.Partition >= 12 {
				return false
			}
			if partition.Offset > 0 {
				t.Fatalf("premature commit partition=%d offset=%d", partition.Partition, partition.Offset)
			}
			beforeOffsets[partition.Partition] = partition.Offset
		}
		return len(beforeOffsets) == 12
	})
	settings["CONSUMER_FAILPOINT"] = ""
	second := startDrillProcess(t, "consumer", settings)
	waitDrillReady(t, "http://127.0.0.1:18081")
	ids := make([]string, len(records))
	for i, record := range records {
		var event domain.Event
		if err := json.Unmarshal(record.Value, &event); err != nil {
			t.Fatal(err)
		}
		ids[i] = "'" + event.EventID + "'"
	}
	eventually(t, 25*time.Second, "replayed accepted IDs visible", func() bool {
		return queryCH(t, c, "SELECT count() FROM events_canonical WHERE event_id IN ("+strings.Join(ids, ",")+")") == fmt.Sprint(len(records))
	})
	assertAcceptedIDs(t, c, records)
	eventually(t, 25*time.Second, "recovered exact consumer metrics", func() bool {
		revenue := queryCH(t, c, "SELECT currency,sum(purchase_count),sum(revenue_cents) FROM revenue_1m_canonical WHERE tenant_id='"+tenant+"' GROUP BY currency ORDER BY currency")
		traffic := queryCH(t, c, "SELECT sum(events_per_minute),sum(rpm),sum(api_errors) FROM traffic_1m_canonical WHERE tenant_id='"+tenant+"'")
		return revenue == "EUR\t2\t500\nUSD\t2\t200" && traffic == "10\t4\t2"
	})
	for _, record := range records {
		eventually(t, 10*time.Second, "recovered commit", func() bool { return offset(t, c, c.Topic, record.Partition) >= record.Offset+1 })
	}
	raw := queryCH(t, c, "SELECT count()-uniqExact(event_id) FROM events_raw")
	duplicates, err := strconv.Atoi(raw)
	if err != nil || duplicates <= 0 {
		t.Fatalf("expected raw replay evidence: %s %v", raw, err)
	}
	second.signal(t)
	second.stopped(t, 10*time.Second)
	shutdownReport(t, "consumer-shutdown.json", map[string]any{"success": true, "accepted_fixture_ids": 10, "verified_fixture_ids": 10, "stop_point": "after_insert_before_commit", "premature_commits": 0, "offset_fetch_success": true, "committed_before_restart": beforeOffsets, "raw_replay_duplicate_rows": duplicates, "revenue_cents": map[string]int{"USD": 200, "EUR": 500}, "traffic": map[string]int{"events": 10, "rpm": 4, "errors": 2}, "first_exit_code": 0, "recovered_exit_code": 0})
}

func TestQuerySIGTERM(t *testing.T) {
	shutdownDirectory(t)
	c := sinkConfig(t)
	at := time.Now().UTC()
	rows := make([]pipeline.Record, 2)
	for i := range rows {
		order, currency, amount := fmt.Sprintf("query_shutdown_order_%d", i), "USD", int64(100)
		event := domain.Event{SchemaVersion: 1, EventID: uuid.NewString(), TenantID: "query_shutdown_fixture", UserID: "query_user", EventType: "purchase", OccurredAt: at.Format(time.RFC3339Nano), OrderID: &order, Currency: &currency, AmountCents: &amount}
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		rows[i] = pipeline.Record{Topic: "query-shutdown-fixture", Partition: 0, Offset: int64(i), Timestamp: at, Value: payload}
	}
	batch, err := pipeline.NewBatch(rows, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sink := analytics.NewSink(c)
	defer sink.Close()
	if err := sink.Insert(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "independent query shutdown fixture", func() bool {
		return queryCH(t, c, "SELECT sum(revenue_cents) FROM revenue_1m_canonical WHERE tenant_id='query_shutdown_fixture' AND currency='USD'") == "200"
	})
	target, err := url.Parse(c.ClickHouseURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 65536))
		if err != nil {
			http.Error(w, "fixture read", 500)
			return
		}
		if strings.Contains(string(raw), "FROM revenue_1m_canonical") {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		r.Body = io.NopCloser(strings.NewReader(string(raw)))
		r.ContentLength = int64(len(raw))
		r.Header.Del("Content-Length")
		proxy.ServeHTTP(w, r)
	}))
	defer upstream.Close()
	key := "shutdown-query-key-at-least-32-bytes"
	p := startDrillProcess(t, "query-api", map[string]string{"QUERY_HTTP_ADDR": "127.0.0.1:18082", "QUERY_API_KEY": key, "QUERY_TENANT": "query_shutdown_fixture", "CLICKHOUSE_URL": upstream.URL, "TRACE_SAMPLE_RATIO": "0"})
	origin := "http://127.0.0.1:18082"
	waitDrillReady(t, origin)
	from := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	to := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	values := url.Values{"tenant_id": {"query_shutdown_fixture"}, "from": {from.Format(time.RFC3339)}, "to": {to.Format(time.RFC3339)}, "currency": {"USD"}, "interval": {"1m"}}
	pending := make(chan shutdownHTTP, 1)
	go func() {
		r, err := http.NewRequest("GET", origin+"/v1/metrics/revenue?"+values.Encode(), nil)
		if err != nil {
			pending <- shutdownHTTP{err: err}
			return
		}
		r.Header.Set("Authorization", "Bearer "+key)
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(r)
		if err != nil {
			pending <- shutdownHTTP{err: err}
			return
		}
		raw, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			err = readErr
		} else {
			err = closeErr
		}
		pending <- shutdownHTTP{status: response.StatusCode, body: raw, err: err}
	}()
	select {
	case <-started:
	case response := <-pending:
		t.Fatalf("query did not reach native read: %+v", response)
	case <-time.After(3 * time.Second):
		t.Fatal("native read not started")
	}
	began := time.Now()
	p.signal(t)
	waitListenerClosed(t, "127.0.0.1:18082")
	select {
	case err := <-p.done:
		p.exited = true
		t.Fatalf("query process exited before in-flight response: %v", err)
	default:
	}
	close(release)
	var response shutdownHTTP
	select {
	case response = <-pending:
	case <-time.After(5 * time.Second):
		t.Fatal("query response did not finish")
	}
	if response.err != nil || response.status != 200 {
		t.Fatalf("drained query failed: %+v", response)
	}
	var result struct {
		Data []struct {
			Cents string `json:"revenue_cents"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.body, &result); err != nil {
		t.Fatal(err)
	}
	var cents int
	for _, row := range result.Data {
		value, err := strconv.Atoi(row.Cents)
		if err != nil {
			t.Fatal(err)
		}
		cents += value
	}
	if cents != 200 {
		t.Fatalf("wrong drained query value: %d", cents)
	}
	p.stopped(t, 10*time.Second)
	shutdownReport(t, "query-shutdown.json", map[string]any{"success": true, "http_status": 200, "revenue_cents": cents, "exit_code": 0, "shutdown_seconds": time.Since(began).Seconds(), "scope": "real query binary; controlled proxy delays a real ClickHouse read during SIGTERM; default deadlines"})
}
