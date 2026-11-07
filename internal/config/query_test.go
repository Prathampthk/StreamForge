package config

import "testing"

func TestQueryConfig(t *testing.T) {
	for _, tc := range []struct {
		key, value string
		ok         bool
	}{{"", "", true}, {"QUERY_API_KEY", "short", false}, {"QUERY_MAX_REQUESTS", "0", false}, {"QUERY_DB_TIMEOUT", "3s", false}, {"QUERY_MAX_WINDOW", "25h", false}, {"QUERY_MAX_SNAPSHOT_AGE", "0s", false}, {"CLICKHOUSE_URL", "http://x:secret@host", false}, {"TRACE_SAMPLE_RATIO", "NaN", false}} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			env := map[string]string{"QUERY_API_KEY": "test-query-key-with-at-least-32bytes", "CLICKHOUSE_PASSWORD": "local"}
			if tc.key != "" {
				env[tc.key] = tc.value
			}
			_, err := QueryFromEnv(func(k string) string { return env[k] })
			if (err == nil) != tc.ok {
				t.Fatalf("%v", err)
			}
		})
	}
}
