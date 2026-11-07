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

	"streamforge/internal/analytics"
	"streamforge/internal/broker"
	"streamforge/internal/config"
	"streamforge/internal/pipeline"
	"streamforge/internal/telemetry"

	"golang.org/x/net/netutil"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		address := os.Getenv("CONSUMER_HTTP_ADDR")
		if address == "" {
			address = "127.0.0.1:8081"
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
		logger.Error("consumer stopped; uncommitted records will replay", "service", "consumer", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	c, err := config.LoadConsumer()
	if err != nil {
		return fmt.Errorf("consumer configuration: %w", err)
	}
	tracing, err := telemetry.NewServiceTracing(context.Background(), c.OTLPEndpoint, c.TraceSample, "streamforge-consumer")
	if err != nil {
		return fmt.Errorf("consumer tracing: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := tracing.Shutdown(ctx); err != nil {
			logger.Error("consumer tracing shutdown", "error", err)
		}
	}()
	sink := analytics.NewSink(c)
	defer sink.Close()
	metrics := telemetry.NewConsumerMetrics()
	var hook pipeline.Hook
	if c.Failpoint != "" {
		hook = func(ctx context.Context, stage string, b pipeline.Batch) error {
			if stage != c.Failpoint {
				return nil
			}
			logger.Warn("test failpoint reached", "service", "consumer", "stage", stage, "batch_id", b.ID)
			<-ctx.Done()
			return ctx.Err()
		}
	}
	consumer, err := broker.NewConsumer(c, sink, metrics, telemetry.Tracer(tracing), logger, hook)
	if err != nil {
		return err
	}
	defer consumer.Close()
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx, cancel := context.WithCancel(signals)
	defer cancel()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil || !consumer.Running() || consumer.Assigned() == 0 {
			w.WriteHeader(503)
			return
		}
		request, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := sink.Ready(request); err != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8 << 10}
	listener, err := net.Listen("tcp", c.Address)
	if err != nil {
		return fmt.Errorf("consumer health listener: %w", err)
	}
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Serve(netutil.LimitListener(listener, 32)) }()
	workerDone := make(chan error, 1)
	go func() { workerDone <- consumer.Run(ctx) }()
	logger.Info("consumer starting", "service", "consumer", "group", c.Group, "health_address", listener.Addr().String())
	var result error
	select {
	case result = <-workerDone:
		cancel()
	case err := <-httpDone:
		cancel()
		result = fmt.Errorf("consumer health server: %w", err)
		if workerErr := <-workerDone; workerErr != nil {
			logger.Error("consumer stop after health failure", "error", workerErr)
		}
	case <-signals.Done():
		cancel()
		result = <-workerDone
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), c.ShutdownTimeout)
	defer cancelShutdown()
	if err := server.Shutdown(shutdown); err != nil {
		if closeErr := server.Close(); closeErr != nil {
			logger.Error("consumer health close", "error", closeErr)
		}
		return fmt.Errorf("consumer health shutdown: %w", err)
	}
	if result == nil {
		logger.Info("consumer drained", "service", "consumer", "group", c.Group)
	}
	if errors.Is(result, context.Canceled) {
		return nil
	}
	return result
}
