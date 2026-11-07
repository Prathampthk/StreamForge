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

	"streamforge/internal/broker"
	"streamforge/internal/config"
	"streamforge/internal/ingest"
	"streamforge/internal/telemetry"

	"golang.org/x/net/netutil"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		address := os.Getenv("HTTP_ADDR")
		if address == "" {
			address = "127.0.0.1:8080"
		}
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			os.Exit(1)
		}
		client := &http.Client{Timeout: 2 * time.Second}
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
		logger.Error("ingestion stopped", "service", "ingest-api", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	c, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	tracing, err := telemetry.NewTracing(context.Background(), c.OTLPEndpoint, c.TraceSample)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := tracing.Shutdown(ctx); err != nil {
			logger.Error("tracing shutdown", "error", err)
		}
	}()
	metrics := telemetry.NewMetrics()
	producer, err := broker.New(c, metrics, telemetry.Tracer(tracing))
	if err != nil {
		return err
	}
	defer producer.Close()
	application := ingest.New(c, producer, metrics, telemetry.Tracer(tracing), logger)
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	server := &http.Server{Addr: c.Address, Handler: application.Handler(), ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: c.RequestTimeout, WriteTimeout: c.RequestTimeout + time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
		BaseContext: func(net.Listener) context.Context { return base }}
	listener, err := net.Listen("tcp", c.Address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	listener = netutil.LimitListener(listener, c.MaxConnections)
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("ingestion listening", "service", "ingest-api", "address", c.Address, "topic", c.Topic, "max_requests", c.MaxRequests)
	select {
	case err := <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP serve: %w", err)
		}
	case <-signals.Done():
		application.Drain()
		shutdown, cancel := context.WithTimeout(context.Background(), c.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			cancelBase()
			if closeErr := server.Close(); closeErr != nil {
				logger.Error("HTTP close", "error", closeErr)
			}
			return fmt.Errorf("HTTP shutdown deadline: %w", err)
		}
		if err := <-stopped; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP shutdown: %w", err)
		}
	}
	logger.Info("HTTP requests drained", "service", "ingest-api")
	return nil
}
