//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"streamforge/internal/analytics"
	"streamforge/internal/broker"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/pipeline"
	"streamforge/internal/telemetry"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.opentelemetry.io/otel/trace/noop"
)

func sinkConfig(t *testing.T) config.Consumer {
	t.Helper()
	c, err := config.ConsumerFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	c.Group = "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	return c
}
func queryCH(t *testing.T, c config.Consumer, sql string) string {
	t.Helper()
	request, err := http.NewRequest("POST", c.ClickHouseURL+"/?database=streamforge&wait_end_of_query=1", strings.NewReader(sql))
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(c.ClickHouseUser, c.ClickHousePassword)
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || response.StatusCode != 200 {
		t.Fatalf("CH %d read=%v close=%v: %s", response.StatusCode, err, closeErr, payload)
	}
	return strings.TrimSpace(string(payload))
}
func eventually(t *testing.T, timeout time.Duration, description string, check func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out: " + description)
}
func source(t *testing.T, c config.Consumer) (*kgo.Client, string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	topic := "streamforge.test." + strings.ReplaceAll(uuid.NewString(), "-", "")
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 5000
	retention := "3600000"
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 12, ReplicationFactor: 1, Configs: []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: &retention}}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.Topics {
		if topic.ErrorCode != 0 {
			t.Fatalf("create test topic: %+v", topic)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TopicNames = []string{topic}
		request.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		request.TimeoutMillis = 5000
		if _, err := request.RequestWith(ctx, client); err != nil {
			t.Log("test-topic cleanup:", err)
		}
	})
	return client, topic
}
func fixture(t *testing.T, client *kgo.Client, topic, tenant string, count int, spread bool) []*kgo.Record {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Minute).Add(time.Second)
	records := make([]*kgo.Record, count)
	for i := range records {
		event := domain.Event{SchemaVersion: 1, EventID: uuid.NewString(), TenantID: tenant, UserID: fmt.Sprintf("u%d", i), EventType: "page_view", OccurredAt: now.Format(time.RFC3339Nano)}
		switch i % 5 {
		case 0:
			order, currency, amount := "order_1", "USD", int64(100)
			event.EventType = "purchase"
			event.OrderID = &order
			event.Currency = &currency
			event.AmountCents = &amount
		case 1:
			order, currency, amount := "order_2", "EUR", int64(250)
			event.EventType = "purchase"
			event.OrderID = &order
			event.Currency = &currency
			event.AmountCents = &amount
		case 3:
			status := uint16(200)
			event.EventType = "api_request"
			event.StatusCode = &status
		case 4:
			status := uint16(503)
			event.EventType = "api_request"
			event.StatusCode = &status
		}
		value, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		partition := int32(0)
		if spread {
			partition = int32(i % 12)
		}
		records[i] = &kgo.Record{Topic: topic, Partition: partition, Key: []byte(tenant + ":" + event.UserID), Value: value, Timestamp: now}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return records
}

type instance struct {
	consumer *broker.Consumer
	cancel   context.CancelFunc
	done     chan struct{}
	err      error
	once     sync.Once
}

func start(t *testing.T, c config.Consumer, sink pipeline.Sink, hook pipeline.Hook) *instance {
	t.Helper()
	consumer, err := broker.NewConsumer(c, sink, telemetry.NewConsumerMetrics(), noop.NewTracerProvider().Tracer("test"), slog.New(slog.NewTextHandler(io.Discard, nil)), hook)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	running := &instance{consumer: consumer, cancel: cancel, done: make(chan struct{})}
	go func() { running.err = consumer.Run(ctx); close(running.done) }()
	t.Cleanup(running.stop)
	return running
}
func (i *instance) stop() { i.once.Do(func() { i.cancel(); <-i.done; i.consumer.Close() }) }
func offset(t *testing.T, c config.Consumer, topic string, partition int32) int64 {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.MaxVersions(kversion.V2_8_0()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = c.Group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{partition}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if response.ErrorCode != 0 {
		return -1
	}
	for _, result := range response.Topics {
		for _, p := range result.Partitions {
			if p.Partition == partition {
				if p.ErrorCode != 0 {
					return -1
				}
				return p.Offset
			}
		}
	}
	return -1
}
func assertCanonical(t *testing.T, c config.Consumer, tenant string, count int) {
	t.Helper()
	eventually(t, 15*time.Second, "all canonical IDs visible", func() bool {
		return queryCH(t, c, fmt.Sprintf("SELECT count() FROM events_canonical WHERE tenant_id='%s'", tenant)) == fmt.Sprint(count)
	})
	if count == 5 {
		eventually(t, 15*time.Second, "scheduled revenue refresh", func() bool {
			return queryCH(t, c, fmt.Sprintf("SELECT currency,sum(purchase_count),sum(revenue_cents) FROM revenue_1m_canonical WHERE tenant_id='%s' GROUP BY currency ORDER BY currency", tenant)) == "EUR\t1\t250\nUSD\t1\t100"
		})
		eventually(t, 15*time.Second, "scheduled traffic refresh", func() bool {
			return queryCH(t, c, fmt.Sprintf("SELECT sum(events_per_minute),sum(rpm),sum(api_errors) FROM traffic_1m_canonical WHERE tenant_id='%s'", tenant)) == "5\t2\t1"
		})
	}
}
func TestCanonicalReplayAndConflict(t *testing.T) {
	c := sinkConfig(t)
	producer, topic := source(t, c)
	tenant := "canonical_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	records := fixture(t, producer, topic, tenant, 5, false)
	sink := analytics.NewSink(c)
	defer sink.Close()
	convert := func(records []*kgo.Record) []pipeline.Record {
		out := make([]pipeline.Record, len(records))
		for i, r := range records {
			out[i] = pipeline.Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Value: r.Value, Timestamp: r.Timestamp}
		}
		return out
	}
	original, err := pipeline.NewBatch(convert(records), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		one, err := pipeline.NewBatch(convert([]*kgo.Record{record}), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Insert(context.Background(), one); err != nil {
			t.Fatal(err)
		}
	}
	assertCanonical(t, c, tenant, 5)
	if got := queryCH(t, c, fmt.Sprintf("SELECT count() FROM events_raw WHERE tenant_id='%s'", tenant)); got != "15" {
		t.Fatalf("expected forensic duplicates, got %s", got)
	}
	var event domain.Event
	if err := json.Unmarshal(records[0].Value, &event); err != nil {
		t.Fatal(err)
	}
	changed := int64(999)
	event.AmountCents = &changed
	event.TenantID = "conflicting_tenant"
	event.OccurredAt = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	conflicting, err := pipeline.NewBatch([]pipeline.Record{{Topic: topic, Partition: 1, Offset: 0, Timestamp: time.Now(), Value: value}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), conflicting); err != nil {
		t.Fatal(err)
	}
	if got := queryCH(t, c, fmt.Sprintf("SELECT count() FROM event_identity_conflicts WHERE event_id='%s'", event.EventID)); got != "1" {
		t.Fatalf("conflict invisible: %s", got)
	}
	assertCanonical(t, c, tenant, 4)
	if got := queryCH(t, c, fmt.Sprintf("SELECT count() FROM events_canonical WHERE event_id='%s'", event.EventID)); got != "0" {
		t.Fatal("conflicting identity chosen silently")
	}
	eventually(t, 10*time.Second, "prior conflicting contribution withdrawn", func() bool {
		return queryCH(t, c, fmt.Sprintf("SELECT sum(revenue_cents) FROM revenue_1m_canonical WHERE tenant_id='%s' AND currency='USD'", tenant)) == "0"
	})
	t.Log("15 raw deliveries -> 5 canonical events; cross-tenant/time/amount conflict visible and contribution withdrawn")
}

// The helper is the real consumer engine in a separately killable process.
func TestConsumerProcess(t *testing.T) {
	if os.Getenv("STREAMFORGE_CONSUMER_HELPER") != "1" {
		return
	}
	c := sinkConfig(t)
	c.Topic = os.Getenv("STREAMFORGE_TEST_TOPIC")
	c.Group = os.Getenv("STREAMFORGE_TEST_GROUP")
	if endpoint := os.Getenv("STREAMFORGE_TEST_SINK"); endpoint != "" {
		c.ClickHouseURL = endpoint
	}
	sink := analytics.NewSink(c)
	defer sink.Close()
	point := os.Getenv("STREAMFORGE_TEST_POINT")
	hook := func(ctx context.Context, stage string, b pipeline.Batch) error {
		if stage == point {
			fmt.Println("FAILPOINT:" + stage)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	consumer, err := broker.NewConsumer(c, sink, telemetry.NewConsumerMetrics(), noop.NewTracerProvider().Tracer("helper"), slog.New(slog.NewTextHandler(io.Discard, nil)), hook)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestConsumerSIGKILLRecovery(t *testing.T) {
	for _, point := range []string{"before_insert", "after_insert", "before_commit", "during_retry", "during_rebalance"} {
		t.Run(point, func(t *testing.T) {
			c := sinkConfig(t)
			producer, topic := source(t, c)
			c.Topic = topic
			tenant := "crash_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			records := fixture(t, producer, topic, tenant, 5, false)
			var accepted atomic.Int64
			endpoint := c.ClickHouseURL
			if point == "during_retry" {
				target, err := url.Parse(c.ClickHouseURL)
				if err != nil {
					t.Fatal(err)
				}
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					forward := r.Clone(r.Context())
					forward.URL = target.ResolveReference(r.URL)
					forward.RequestURI = ""
					response, err := http.DefaultClient.Do(forward)
					if err != nil {
						http.Error(w, "forward failed", 503)
						return
					}
					_, readErr := io.Copy(io.Discard, response.Body)
					closeErr := response.Body.Close()
					if readErr != nil || closeErr != nil || response.StatusCode != 200 {
						http.Error(w, "sink failed", 503)
						return
					}
					accepted.Add(1)
					// Close after the real server accepted the insert: client sees no response.
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					if err := connection.Close(); err != nil {
						t.Error(err)
					}
				}))
				defer proxy.Close()
				endpoint = proxy.URL
			}
			childPoint := point
			if point == "during_rebalance" {
				childPoint = "after_insert"
			}
			command := exec.Command(os.Args[0], "-test.run=^TestConsumerProcess$")
			command.Env = append(os.Environ(), "STREAMFORGE_CONSUMER_HELPER=1", "STREAMFORGE_TEST_TOPIC="+topic, "STREAMFORGE_TEST_GROUP="+c.Group, "STREAMFORGE_TEST_POINT="+childPoint, "STREAMFORGE_TEST_SINK="+endpoint)
			output, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			killed := false
			defer func() {
				if !killed {
					if err := command.Process.Kill(); err != nil {
						t.Log(err)
					}
					if err := command.Wait(); err != nil {
						t.Log(err)
					}
				}
			}()
			reached := make(chan struct{})
			scanDone := make(chan struct{})
			go func() {
				defer close(scanDone)
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					if strings.Contains(scanner.Text(), "FAILPOINT:"+childPoint) {
						close(reached)
						return
					}
				}
			}()
			select {
			case <-reached:
			case <-time.After(20 * time.Second):
				t.Fatalf("failpoint not reached: %s", stderr.String())
			}
			if got := offset(t, c, topic, 0); got > records[0].Offset {
				t.Fatalf("premature commit at %s: %d", point, got)
			}
			if point == "during_retry" && accepted.Load() == 0 {
				t.Fatal("ambiguous case did not perform actual insert")
			}
			sink := analytics.NewSink(c)
			defer sink.Close()
			var replacement *instance
			if point == "during_rebalance" {
				replacement = start(t, c, sink, nil)
				eventually(t, 10*time.Second, "broker begins rebalance while old owner has uncommitted insert", func() bool { return groupState(t, c) == "PreparingRebalance" })
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := command.Wait(); err == nil {
				t.Fatal("worker survived SIGKILL")
			}
			killed = true
			<-scanDone
			if replacement == nil {
				replacement = start(t, c, sink, nil)
			}
			eventually(t, 20*time.Second, "replacement commits all persisted records", func() bool { return offset(t, c, topic, 0) >= records[len(records)-1].Offset+1 })
			assertCanonical(t, c, tenant, 5)
			assertAcceptedIDs(t, c, records)
			replacement.stop()
			if point == "after_insert" || point == "before_commit" || point == "during_retry" || point == "during_rebalance" {
				if got := queryCH(t, c, fmt.Sprintf("SELECT count() FROM events_raw WHERE tenant_id='%s'", tenant)); got == "5" {
					t.Fatal("expected replay evidence in raw table")
				}
			}
			t.Logf("SIGKILL at %s: no premature offset, replacement drained, canonical metrics exact", point)
		})
	}
}
func TestConsumerRebalanceOneToThree(t *testing.T) {
	c := sinkConfig(t)
	producer, topic := source(t, c)
	c.Topic = topic
	sink := analytics.NewSink(c)
	defer sink.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func(ctx context.Context, stage string, b pipeline.Batch) error {
		if stage == "after_insert" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	first := start(t, c, sink, hook)
	eventually(t, 10*time.Second, "one consumer owns all partitions", func() bool { return first.consumer.Assigned() == 12 })
	tenant := "rebalance_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	records := fixture(t, producer, topic, tenant, 60, true)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("insert hook not reached")
	}
	second := start(t, c, sink, nil)
	third := start(t, c, sink, nil)
	close(release)
	eventually(t, 20*time.Second, "three consumers own 12 partitions", func() bool {
		return first.consumer.Assigned() > 0 && second.consumer.Assigned() > 0 && third.consumer.Assigned() > 0 && first.consumer.Assigned()+second.consumer.Assigned()+third.consumer.Assigned() == 12
	})
	last := map[int32]int64{}
	for _, record := range records {
		last[record.Partition] = record.Offset + 1
	}
	eventually(t, 20*time.Second, "rebalance drain", func() bool {
		for p, next := range last {
			if offset(t, c, topic, p) < next {
				return false
			}
		}
		return true
	})
	assertCanonical(t, c, tenant, 60)
	assertAcceptedIDs(t, c, records)
	eventually(t, 10*time.Second, "rebalance aggregate by currency", func() bool {
		return queryCH(t, c, fmt.Sprintf("SELECT currency,sum(revenue_cents) FROM revenue_1m_canonical WHERE tenant_id='%s' GROUP BY currency ORDER BY currency", tenant)) == "EUR\t3000\nUSD\t1200"
	})
	t.Logf("1 -> 3 assignment: %d/%d/%d; 60 unique events, EUR=3000/USD=1200 cents; no performance claim", first.consumer.Assigned(), second.consumer.Assigned(), third.consumer.Assigned())
}
func TestPoisonCannotCommitPastGap(t *testing.T) {
	c := sinkConfig(t)
	producer, topic := source(t, c)
	c.Topic = topic
	c.BatchRecords = 1
	tenant := "poison_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	first := fixture(t, producer, topic, tenant, 1, false)
	poison := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(`{"schema_version":99}`)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fixture(t, producer, topic, tenant, 1, false)
	sink := analytics.NewSink(c)
	defer sink.Close()
	worker := start(t, c, sink, nil)
	select {
	case <-worker.done:
		if worker.err == nil {
			t.Fatal("poison ignored")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("poison not detected")
	}
	if got := offset(t, c, topic, 0); got != first[0].Offset+1 {
		t.Fatalf("committed past poison or lost completed prefix: %d", got)
	}
	if got := queryCH(t, c, fmt.Sprintf("SELECT count() FROM events_canonical WHERE tenant_id='%s'", tenant)); got != "1" {
		t.Fatal("poison gap skipped")
	}
	t.Log("valid prefix committed; poison and later valid record remain uncommitted")
}

func TestSinkOutagePreservesBacklog(t *testing.T) {
	c := sinkConfig(t)
	producer, topic := source(t, c)
	c.Topic = topic
	tenant := "outage_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	records := fixture(t, producer, topic, tenant, 5, false)
	var attempts atomic.Int64
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "injected sink unavailable", 503)
	}))
	defer unavailable.Close()
	failedConfig := c
	failedConfig.ClickHouseURL = unavailable.URL
	badSink := analytics.NewSink(failedConfig)
	defer badSink.Close()
	failed := start(t, failedConfig, badSink, nil)
	select {
	case <-failed.done:
		if failed.err == nil {
			t.Fatal("outage returned success")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("retry budget was not bounded")
	}
	failed.stop()
	if attempts.Load() != int64(c.InsertAttempts) {
		t.Fatalf("insert attempt budget: %d", attempts.Load())
	}
	if got := offset(t, c, topic, 0); got >= 0 {
		t.Fatalf("offset advanced during sink outage: %d", got)
	}
	if got := queryCH(t, c, fmt.Sprintf("SELECT count() FROM events_raw WHERE tenant_id='%s'", tenant)); got != "0" {
		t.Fatal("outage created false persistence")
	}
	recoveredSink := analytics.NewSink(c)
	defer recoveredSink.Close()
	recovered := start(t, c, recoveredSink, nil)
	eventually(t, 15*time.Second, "recovered sink drains retained Kafka backlog", func() bool { return offset(t, c, topic, 0) >= records[len(records)-1].Offset+1 })
	assertCanonical(t, c, tenant, 5)
	assertAcceptedIDs(t, c, records)
	recovered.stop()
	t.Log("sink unavailable: 3 bounded attempts, no offset commit; restored sink drained all 5 events with exact canonical metrics")
}

func groupState(t *testing.T, c config.Consumer) string {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.MaxVersions(kversion.V2_8_0()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{c.Group}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range response.Groups {
		if group.Group == c.Group {
			return group.State
		}
	}
	return ""
}

func assertAcceptedIDs(t *testing.T, c config.Consumer, records []*kgo.Record) {
	t.Helper()
	ids := make([]string, len(records))
	for i, record := range records {
		var event domain.Event
		if err := json.Unmarshal(record.Value, &event); err != nil {
			t.Fatal(err)
		}
		if _, err := uuid.Parse(event.EventID); err != nil {
			t.Fatal(err)
		}
		ids[i] = "'" + event.EventID + "'"
	}
	if got := queryCH(t, c, "SELECT count() FROM events_canonical WHERE event_id IN ("+strings.Join(ids, ",")+")"); got != fmt.Sprint(len(records)) {
		t.Fatalf("accepted-ID reconciliation failed: %s of %d", got, len(records))
	}
}
