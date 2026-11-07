package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestProcessHelper(t *testing.T) {
	if os.Getenv("STREAMFORGE_TEST_PROCESS") != "1" {
		return
	}
	if err := run(slog.New(slog.NewJSONHandler(os.Stdout, nil))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
func TestSIGTERMSettlesUnconfirmedRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestProcessHelper$")
	command.Env = append(os.Environ(), "STREAMFORGE_TEST_PROCESS=1", "HTTP_ADDR="+address, "KAFKA_BROKERS=127.0.0.1:1", "INGEST_API_KEY=development-test-key-32-characters", "PUBLISH_TIMEOUT=1s", "REQUEST_TIMEOUT=3s", "SHUTDOWN_TIMEOUT=5s", "OTEL_EXPORTER_OTLP_ENDPOINT=")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if command.ProcessState == nil {
			if err := command.Process.Kill(); err != nil {
				t.Log(err)
			}
			if err := command.Wait(); err != nil {
				t.Log(err)
			}
		}
	}()
	client := &http.Client{Timeout: 4 * time.Second}
	url := "http://" + address
	ready := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		response, err := client.Get(url + "/livez")
		if err == nil {
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			ready = response.StatusCode == 200
			if ready {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("process did not start")
	}
	raw, err := json.Marshal(map[string]any{"schema_version": 1, "event_id": "018f0f5b-c1c4-7b8f-a7a9-2d11e8fa37c2", "tenant_id": "merchant_demo", "user_id": "u", "event_type": "page_view", "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest("POST", url+"/v1/events", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer development-test-key-32-characters")
	request.Header.Set("Content-Type", "application/json")
	result := make(chan int, 1)
	requestError := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if err != nil {
			requestError <- err
			return
		}
		if err := response.Body.Close(); err != nil {
			requestError <- err
			return
		}
		result <- response.StatusCode
	}()
	inflight := false
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); {
		response, err := client.Get(url + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("read=%v close=%v", err, closeErr)
		}
		if strings.Contains(string(body), "streamforge_ingest_inflight_requests 1") {
			inflight = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !inflight {
		t.Fatal("request was not admitted")
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case status := <-result:
		if status != 503 {
			t.Fatalf("unconfirmed request returned %d", status)
		}
	case err := <-requestError:
		t.Fatal(err)
	case <-time.After(4 * time.Second):
		t.Fatal("shutdown request hung")
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("process shutdown: %v\n%s", err, output.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process shutdown exceeded deadline")
	}
	if !strings.Contains(output.String(), "HTTP requests drained") {
		t.Fatalf("missing drain log: %s", output.String())
	}
}
