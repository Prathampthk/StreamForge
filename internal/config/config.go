// Package config loads the ingestion process's typed, validated settings.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"streamforge/internal/domain"
)

type Config struct {
	Address, Topic, APIKey, Tenant, OTLPEndpoint                               string
	Brokers                                                                    []string
	MaxConnections, MaxRequests, MaxBatchEvents, MaxBodyBytes, ProducerRecords int
	ProducerBytes                                                              int
	RequestsPerSecond                                                          float64
	Burst                                                                      int
	PublishTimeout, RequestTimeout, ShutdownTimeout, MaxLateness, FutureSkew   time.Duration
	TraceSample                                                                float64
}

func Load() (Config, error) { return FromEnv(os.Getenv) }
func FromEnv(get func(string) string) (Config, error) {
	c := Config{}
	str := func(key, fallback string) string {
		if s := get(key); s != "" {
			return s
		}
		return fallback
	}
	var parseErr error
	integer := func(key string, fallback int) int {
		value, err := strconv.Atoi(str(key, strconv.Itoa(fallback)))
		if err != nil {
			parseErr = fmt.Errorf("%s must be an integer", key)
		}
		return value
	}
	duration := func(key, fallback string) time.Duration {
		value, err := time.ParseDuration(str(key, fallback))
		if err != nil {
			parseErr = fmt.Errorf("%s must be a duration", key)
		}
		return value
	}
	number := func(key, fallback string) float64 {
		value, err := strconv.ParseFloat(str(key, fallback), 64)
		if err != nil {
			parseErr = fmt.Errorf("%s must be a number", key)
		}
		return value
	}
	c.Address = str("HTTP_ADDR", "127.0.0.1:8080")
	c.Topic = str("KAFKA_TOPIC", "streamforge.events.v1")
	c.Brokers = strings.Split(str("KAFKA_BROKERS", "localhost:9092"), ",")
	c.APIKey = get("INGEST_API_KEY")
	c.Tenant = str("INGEST_TENANT", "merchant_demo")
	c.OTLPEndpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT")
	c.MaxConnections = integer("MAX_CONNECTIONS", 256)
	c.MaxRequests = integer("MAX_INFLIGHT_REQUESTS", 64)
	c.MaxBatchEvents = integer("MAX_BATCH_EVENTS", 100)
	c.MaxBodyBytes = integer("MAX_BODY_BYTES", 1<<20)
	c.ProducerRecords = integer("PRODUCER_MAX_RECORDS", 1000)
	c.ProducerBytes = integer("PRODUCER_MAX_BYTES", 8<<20)
	c.RequestsPerSecond = number("REQUESTS_PER_SECOND", "2000")
	c.Burst = integer("REQUEST_BURST", 2000)
	c.PublishTimeout = duration("PUBLISH_TIMEOUT", "5s")
	c.RequestTimeout = duration("REQUEST_TIMEOUT", "8s")
	c.ShutdownTimeout = duration("SHUTDOWN_TIMEOUT", "15s")
	c.MaxLateness = duration("MAX_LATENESS", "168h")
	c.FutureSkew = duration("FUTURE_SKEW", "5m")
	c.TraceSample = number("TRACE_SAMPLE_RATIO", "0.1")
	if parseErr != nil {
		return c, parseErr
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.OTLPEndpoint != "" {
		endpoint, err := url.Parse(c.OTLPEndpoint)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil {
			return errors.New("OTLP endpoint must be an http(s) URL without credentials")
		}
	}

	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return fmt.Errorf("HTTP_ADDR: %w", err)
	}
	if c.Topic != "streamforge.events.v1" {
		return errors.New("KAFKA_TOPIC must be streamforge.events.v1")
	}
	for _, broker := range c.Brokers {
		if _, _, err := net.SplitHostPort(broker); err != nil {
			return fmt.Errorf("KAFKA_BROKERS: %w", err)
		}
	}
	if len(c.Brokers) == 0 || len(c.APIKey) < 24 || !domain.ValidIdentifier(c.Tenant) {
		return errors.New("configure brokers, a >=24 byte API key, and valid tenant")
	}
	if c.MaxConnections < 1 || c.MaxConnections > 10000 || c.MaxRequests < 1 || c.MaxRequests > c.MaxConnections || c.MaxBatchEvents < 1 || c.MaxBatchEvents > 1000 || c.MaxBodyBytes < domain.MaxEventBytes || c.MaxBodyBytes > 8<<20 {
		return errors.New("invalid connection, request or body bounds")
	}
	if c.ProducerRecords < c.MaxBatchEvents || c.ProducerRecords > 100000 || c.ProducerBytes < domain.MaxEventBytes || c.ProducerBytes > 256<<20 {
		return errors.New("invalid producer bounds")
	}
	if !(c.RequestsPerSecond > 0 && c.RequestsPerSecond <= 1e6) || c.Burst < 1 || c.Burst > 1000000 {
		return errors.New("invalid rate limits")
	}
	if c.PublishTimeout < time.Second || c.PublishTimeout > time.Minute || c.RequestTimeout <= c.PublishTimeout || c.RequestTimeout > 2*time.Minute || c.ShutdownTimeout <= c.RequestTimeout || c.ShutdownTimeout > 5*time.Minute {
		return errors.New("require 1s <= publish <=60s < request <=120s < shutdown <=300s")
	}
	if c.MaxLateness <= 0 || c.MaxLateness > 7*24*time.Hour || c.FutureSkew < 0 || c.FutureSkew > time.Hour {
		return errors.New("lateness must be >0 and <=7 days, future skew between 0 and 1 hour")
	}
	if !(c.TraceSample >= 0 && c.TraceSample <= 1) {
		return errors.New("TRACE_SAMPLE_RATIO must be 0..1")
	}
	return nil
}
