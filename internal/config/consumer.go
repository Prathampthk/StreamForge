package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"streamforge/internal/platform"
	"strings"
	"time"
)

type Consumer struct {
	ClickHouseTLS      *tls.Config
	Brokers            []string
	Group              string
	Topic              string
	Address            string
	ClickHouseURL      string
	ClickHouseUser     string
	ClickHousePassword string
	BatchRecords       int
	BatchBytes         int
	Workers            int
	InsertAttempts     int
	FlushInterval      time.Duration
	InsertTimeout      time.Duration
	ProcessTimeout     time.Duration
	CommitTimeout      time.Duration
	RebalanceTimeout   time.Duration
	ShutdownTimeout    time.Duration
	TraceSample        float64
	OTLPEndpoint       string
	Failpoint          string
}

func LoadConsumer() (Consumer, error) { return ConsumerFromEnv(os.Getenv) }
func ConsumerFromEnv(get func(string) string) (Consumer, error) {
	str := func(key, def string) string {
		if v := get(key); v != "" {
			return v
		}
		return def
	}
	var parseErr error
	number := func(key string, def int) int {
		n, err := strconv.Atoi(str(key, strconv.Itoa(def)))
		if err != nil {
			parseErr = fmt.Errorf("%s must be an integer", key)
		}
		return n
	}
	duration := func(key, def string) time.Duration {
		n, err := time.ParseDuration(str(key, def))
		if err != nil {
			parseErr = fmt.Errorf("%s must be a duration", key)
		}
		return n
	}
	c := Consumer{Brokers: strings.Split(str("KAFKA_BROKERS", "localhost:9092"), ","), Group: str("CONSUMER_GROUP", "streamforge-sink-v1"), Topic: str("KAFKA_TOPIC", "streamforge.events.v1"), Address: str("CONSUMER_HTTP_ADDR", "127.0.0.1:8081"), ClickHouseURL: str("CLICKHOUSE_URL", "http://localhost:8123"), ClickHouseUser: str("CLICKHOUSE_USER", "streamforge"), ClickHousePassword: get("CLICKHOUSE_PASSWORD"),
		BatchRecords: number("CONSUMER_BATCH_RECORDS", 128), BatchBytes: number("CONSUMER_BATCH_BYTES", 1<<20), Workers: number("CONSUMER_WORKERS", 4), InsertAttempts: number("CONSUMER_INSERT_ATTEMPTS", 3),
		FlushInterval: duration("CONSUMER_FLUSH_INTERVAL", "250ms"), InsertTimeout: duration("CONSUMER_INSERT_TIMEOUT", "2s"), ProcessTimeout: duration("CONSUMER_PROCESS_TIMEOUT", "10s"), CommitTimeout: duration("CONSUMER_COMMIT_TIMEOUT", "3s"), RebalanceTimeout: duration("CONSUMER_REBALANCE_TIMEOUT", "30s"), ShutdownTimeout: duration("CONSUMER_SHUTDOWN_TIMEOUT", "8s"), Failpoint: get("CONSUMER_FAILPOINT")}
	var err error
	c.TraceSample, err = strconv.ParseFloat(str("TRACE_SAMPLE_RATIO", "0.1"), 64)
	if err != nil || !(c.TraceSample >= 0 && c.TraceSample <= 1) {
		return c, errors.New("TRACE_SAMPLE_RATIO must be 0..1")
	}
	c.OTLPEndpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT")
	if c.OTLPEndpoint != "" {
		endpoint, err := url.Parse(c.OTLPEndpoint)
		if err != nil || endpoint.Host == "" || endpoint.User != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return c, errors.New("invalid OTLP endpoint")
		}
	}
	if ca := get("CLICKHOUSE_CA_PEM"); ca != "" {
		if !strings.HasPrefix(c.ClickHouseURL, "https://") {
			return c, errors.New("ClickHouse CA requires HTTPS")
		}
		var tlsErr error
		c.ClickHouseTLS, tlsErr = platform.TLSConfig(ca)
		if tlsErr != nil {
			return c, tlsErr
		}
	}

	if parseErr != nil {
		return c, parseErr
	}
	if c.Topic != "streamforge.events.v1" || c.Group == "" || len(c.Group) > 128 {
		return c, errors.New("consumer requires v1 topic and bounded group name")
	}
	for _, b := range c.Brokers {
		if _, _, err := net.SplitHostPort(b); err != nil {
			return c, fmt.Errorf("consumer broker: %w", err)
		}
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return c, fmt.Errorf("consumer HTTP address: %w", err)
	}
	endpoint, err := url.Parse(c.ClickHouseURL)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return c, errors.New("ClickHouse URL must be an http(s) origin without credentials/query/path")
	}
	if c.ClickHouseUser == "" || c.ClickHousePassword == "" {
		return c, errors.New("ClickHouse username and password are required")
	}
	if c.BatchRecords < 1 || c.BatchRecords > 1000 || c.BatchBytes < c.BatchRecords*8192 || c.BatchBytes > 8<<20 || c.Workers < 1 || c.Workers > 12 || c.InsertAttempts < 1 || c.InsertAttempts > 10 {
		return c, errors.New("invalid consumer batch, concurrency or retry bounds")
	}
	if c.FlushInterval < 10*time.Millisecond || c.FlushInterval > time.Second || c.InsertTimeout < 100*time.Millisecond || c.InsertTimeout > 10*time.Second || c.ProcessTimeout < c.InsertTimeout || c.ProcessTimeout > 30*time.Second || c.CommitTimeout < time.Second || c.CommitTimeout > 10*time.Second || c.RebalanceTimeout < c.ProcessTimeout+c.CommitTimeout+c.FlushInterval+5*time.Second || c.RebalanceTimeout > time.Minute || c.ShutdownTimeout < time.Second || c.ShutdownTimeout > time.Minute {
		return c, errors.New("consumer timing budgets must fit within rebalance timeout with >=5s margin")
	}
	switch c.Failpoint {
	case "", "before_insert", "after_insert", "before_commit", "during_retry":
	default:
		return c, errors.New("unknown consumer failpoint")
	}
	if c.Failpoint != "" && get("ALLOW_TEST_FAILPOINTS") != "true" {
		return c, errors.New("failpoints require ALLOW_TEST_FAILPOINTS=true")
	}
	return c, nil
}
