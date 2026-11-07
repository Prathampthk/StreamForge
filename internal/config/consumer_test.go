package config

import "testing"

func TestConsumerConfig(t *testing.T) {
	for _, tc := range []struct {
		key, value string
		valid      bool
	}{
		{"", "", true}, {"CONSUMER_BATCH_RECORDS", "0", false}, {"CONSUMER_BATCH_BYTES", "8192", false}, {"CONSUMER_WORKERS", "13", false}, {"CONSUMER_INSERT_ATTEMPTS", "0", false},
		{"CONSUMER_PROCESS_TIMEOUT", "30s", false}, {"CONSUMER_REBALANCE_TIMEOUT", "11s", false}, {"CONSUMER_FLUSH_INTERVAL", "0s", false}, {"CONSUMER_FAILPOINT", "after_insert", false},
		{"CLICKHOUSE_URL", "http://name:secret@localhost:8123", false}, {"CLICKHOUSE_URL", "http://localhost:8123/custom", false}, {"CLICKHOUSE_PASSWORD", "", false}, {"TRACE_SAMPLE_RATIO", "NaN", false},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			env := map[string]string{"CLICKHOUSE_PASSWORD": "development-only"}
			if tc.key != "" {
				env[tc.key] = tc.value
			}
			_, err := ConsumerFromEnv(func(k string) string { return env[k] })
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
		})
	}
}
