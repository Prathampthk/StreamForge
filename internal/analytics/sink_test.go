package analytics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"streamforge/internal/config"
	"streamforge/internal/pipeline"
)

func TestSynchronousInsertSettingsAndErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "test" || password != "local-only" {
			t.Error("missing auth")
		}
		for key, value := range map[string]string{"async_insert": "0", "wait_end_of_query": "1", "insert_deduplicate": "0", "materialized_views_ignore_errors": "0"} {
			if r.URL.Query().Get(key) != value {
				t.Errorf("unsafe %s", key)
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if !strings.HasPrefix(string(body), "INSERT INTO events_raw") {
			t.Error("missing insert")
		}
		w.WriteHeader(503)
	}))
	defer server.Close()
	sink := NewSink(config.Consumer{ClickHouseURL: server.URL, ClickHouseUser: "test", ClickHousePassword: "local-only", Workers: 1, InsertTimeout: time.Second})
	defer sink.Close()
	if err := sink.Insert(context.Background(), pipeline.Batch{Body: []byte("{}\n")}); err == nil {
		t.Fatal("false insert confirmation")
	}
}

func TestHTTP200WithErrorBodyIsNotConfirmation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		if _, err := io.WriteString(w, "Code: 159. DB::Exception: query failed"); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	sink := NewSink(config.Consumer{ClickHouseURL: server.URL, ClickHouseUser: "test", ClickHousePassword: "local-only", Workers: 1, InsertTimeout: time.Second})
	defer sink.Close()
	if err := sink.Insert(context.Background(), pipeline.Batch{Body: []byte("{}\n")}); err == nil {
		t.Fatal("HTTP 200 execution error was acknowledged")
	}
}
