package config

import "testing"

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		key, value string
		ok         bool
	}{
		{"", "", true}, {"PUBLISH_TIMEOUT", "0s", false}, {"PUBLISH_TIMEOUT", "not-time", false},
		{"MAX_INFLIGHT_REQUESTS", "-1", false}, {"MAX_BATCH_EVENTS", "0", false}, {"PRODUCER_MAX_BYTES", "1", false},
		{"PRODUCER_MAX_RECORDS", "1", false}, {"REQUEST_TIMEOUT", "2s", false}, {"SHUTDOWN_TIMEOUT", "1s", false},
		{"KAFKA_BROKERS", "", true}, {"KAFKA_BROKERS", "invalid", false}, {"KAFKA_TOPIC", "other", false},
		{"INGEST_API_KEY", "short", false}, {"INGEST_TENANT", "a:b", false}, {"TRACE_SAMPLE_RATIO", "NaN", false},
		{"REQUESTS_PER_SECOND", "NaN", false}, {"MAX_BODY_BYTES", "abc", false}, {"MAX_LATENESS", "169h", false},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			env := map[string]string{"INGEST_API_KEY": "development-test-key-32-characters"}
			if tc.key != "" {
				env[tc.key] = tc.value
			}
			_, err := FromEnv(func(key string) string { return env[key] })
			if (err == nil) != tc.ok {
				t.Fatalf("ok=%v err=%v", tc.ok, err)
			}
		})
	}
}
