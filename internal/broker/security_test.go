package broker

import "testing"

func TestSecurityFailsClosed(t *testing.T) {
	for _, env := range []map[string]string{{"KAFKA_TLS": "invalid"}, {"KAFKA_SASL_USERNAME": "u", "KAFKA_SASL_PASSWORD": "p"}, {"KAFKA_TLS": "true", "KAFKA_SASL_USERNAME": "u"}, {"KAFKA_TLS": "true", "KAFKA_CA_PEM": "bad"}} {
		if _, err := SecurityOptions(func(k string) string { return env[k] }); err == nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
	opts, err := SecurityOptions(func(k string) string {
		return map[string]string{"KAFKA_TLS": "true", "KAFKA_SASL_USERNAME": "u", "KAFKA_SASL_PASSWORD": "p"}[k]
	})
	if err != nil || len(opts) != 2 {
		t.Fatal("TLS/SCRAM not configured", err)
	}
}
