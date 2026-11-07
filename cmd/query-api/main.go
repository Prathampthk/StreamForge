package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/netutil"
	"streamforge/internal/analytics"
	"streamforge/internal/config"
	"streamforge/internal/query"
	"streamforge/internal/telemetry"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		address := os.Getenv("QUERY_HTTP_ADDR")
		if address == "" {
			address = "127.0.0.1:8082"
		}
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			os.Exit(1)
		}
		client := &http.Client{Timeout: 4 * time.Second}
		response, err := client.Get("http://127.0.0.1:" + port + "/readyz")
		if err != nil {
			os.Exit(1)
		}
		if err := response.Body.Close(); err != nil || response.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("query API stopped", "service", "query-api", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	c, err := config.LoadQuery()
	if err != nil {
		return fmt.Errorf("query configuration: %w", err)
	}
	provider, err := telemetry.NewServiceTracing(context.Background(), c.OTLPEndpoint, c.TraceSample, "streamforge-query")
	if err != nil {
		return fmt.Errorf("query tracing: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := provider.Shutdown(ctx); err != nil {
			logger.Error("query tracing shutdown", "error", err)
		}
	}()
	metrics := telemetry.NewQueryMetrics()
	repository := analytics.NewReadRepository(c, metrics, telemetry.Tracer(provider))
	defer repository.Close()
	app := query.New(c, repository, metrics, telemetry.Tracer(provider), logger)
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	server := &http.Server{Handler: app.Handler(), ReadHeaderTimeout: time.Second, ReadTimeout: c.RequestTimeout, WriteTimeout: c.RequestTimeout + time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10, BaseContext: func(net.Listener) context.Context { return base }}
	listener, err := net.Listen("tcp", c.Address)
	if err != nil {
		return fmt.Errorf("query listen: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(netutil.LimitListener(listener, c.MaxConnections)) }()
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	logger.Info("query API listening", "service", "query-api", "address", c.Address)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("query server: %w", err)
		}
	case <-signals.Done():
		app.Drain()
		ctx, cancel := context.WithTimeout(context.Background(), c.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			cancelBase()
			if closeErr := server.Close(); closeErr != nil {
				logger.Error("query close", "error", closeErr)
			}
			return fmt.Errorf("query shutdown: %w", err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("query serve shutdown: %w", err)
		}
	}
	logger.Info("query API drained", "service", "query-api")
	return nil
}
