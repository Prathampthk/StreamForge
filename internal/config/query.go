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

	"streamforge/internal/domain"
)

type Query struct {
	ClickHouseTLS                                                                            *tls.Config
	Address, APIKey, Tenant, ClickHouseURL, ClickHouseUser, ClickHousePassword, OTLPEndpoint string
	MaxConnections, MaxRequests                                                              int
	DBTimeout, RequestTimeout, ShutdownTimeout, MaxWindow, MaxSnapshotAge                    time.Duration
	TraceSample                                                                              float64
}

func LoadQuery() (Query, error) { return QueryFromEnv(os.Getenv) }
func QueryFromEnv(get func(string) string) (Query, error) {
	str := func(key, def string) string {
		if v := get(key); v != "" {
			return v
		}
		return def
	}
	var parseErr error
	number := func(key string, def int) int {
		v, err := strconv.Atoi(str(key, strconv.Itoa(def)))
		if err != nil {
			parseErr = fmt.Errorf("%s must be an integer", key)
		}
		return v
	}
	duration := func(key, def string) time.Duration {
		v, err := time.ParseDuration(str(key, def))
		if err != nil {
			parseErr = fmt.Errorf("%s must be a duration", key)
		}
		return v
	}
	c := Query{Address: str("QUERY_HTTP_ADDR", "127.0.0.1:8082"), APIKey: get("QUERY_API_KEY"), Tenant: str("QUERY_TENANT", "merchant_demo"), ClickHouseURL: str("CLICKHOUSE_URL", "http://localhost:8123"), ClickHouseUser: str("CLICKHOUSE_USER", "streamforge"), ClickHousePassword: get("CLICKHOUSE_PASSWORD"), OTLPEndpoint: get("OTEL_EXPORTER_OTLP_ENDPOINT"),
		MaxConnections: number("QUERY_MAX_CONNECTIONS", 128), MaxRequests: number("QUERY_MAX_REQUESTS", 16), DBTimeout: duration("QUERY_DB_TIMEOUT", "1s"), RequestTimeout: duration("QUERY_REQUEST_TIMEOUT", "3s"), ShutdownTimeout: duration("QUERY_SHUTDOWN_TIMEOUT", "8s"), MaxWindow: duration("QUERY_MAX_WINDOW", "24h"), MaxSnapshotAge: duration("QUERY_MAX_SNAPSHOT_AGE", "10s")}
	var err error
	c.TraceSample, err = strconv.ParseFloat(str("TRACE_SAMPLE_RATIO", "0.1"), 64)
	if err != nil || !(c.TraceSample >= 0 && c.TraceSample <= 1) {
		return c, errors.New("TRACE_SAMPLE_RATIO must be 0..1")
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
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return c, fmt.Errorf("query address: %w", err)
	}
	if len(c.APIKey) < 24 || !domain.ValidIdentifier(c.Tenant) || c.ClickHouseUser == "" || c.ClickHousePassword == "" {
		return c, errors.New("query key >=24 bytes, valid tenant and database credentials required")
	}
	endpoint, err := url.Parse(c.ClickHouseURL)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || (endpoint.Path != "" && endpoint.Path != "/") || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return c, errors.New("ClickHouse URL must be an http(s) origin without credentials/query/path")
	}
	if c.OTLPEndpoint != "" {
		u, err := url.Parse(c.OTLPEndpoint)
		if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return c, errors.New("invalid OTLP endpoint")
		}
	}
	if c.MaxConnections < 1 || c.MaxConnections > 10000 || c.MaxRequests < 1 || c.MaxRequests > c.MaxConnections || c.MaxRequests > 128 {
		return c, errors.New("invalid query concurrency bounds")
	}
	if c.DBTimeout < 100*time.Millisecond || c.DBTimeout > 5*time.Second || c.RequestTimeout < 2*c.DBTimeout || c.RequestTimeout > 15*time.Second || c.ShutdownTimeout <= c.RequestTimeout || c.ShutdownTimeout > time.Minute {
		return c, errors.New("invalid query timeout budgets")
	}
	if c.MaxWindow < time.Minute || c.MaxWindow > 24*time.Hour || c.MaxWindow%time.Minute != 0 || c.MaxSnapshotAge < time.Second || c.MaxSnapshotAge > time.Minute {
		return c, errors.New("invalid query window or snapshot age bounds")
	}
	return c, nil
}
