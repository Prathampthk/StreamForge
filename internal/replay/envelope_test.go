package replay

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"streamforge/internal/domain"
	"testing"
	"time"
)

func TestEnvelopeIdentityRepairAndRedaction(t *testing.T) {
	id := uuid.NewString()
	raw := []byte(`{"schema_version":99,"event_id":"` + id + `","tenant_id":"demo","user_id":"u","event_type":"page_view","occurred_at":"2026-11-06T17:00:00Z"}`)
	record := &kgo.Record{Topic: "streamforge.events.v1", Partition: 2, Offset: 3, Key: []byte("demo:u"), Value: raw, Headers: []kgo.RecordHeader{{Key: "Authorization", Value: []byte("secret")}, {Key: "correlation_id", Value: []byte("safe")}}}
	e := NewEnvelope(record, time.Now())
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.Raw) != string(raw) || len(decoded.Headers) != 1 || decoded.ID != EnvelopeID(record.Topic, 2, 3) {
		t.Fatal("forensic bytes/identity/header policy")
	}
	if _, _, err := decoded.Payload(nil, "demo"); err == nil {
		t.Fatal("malformed replay allowed")
	}
	valid := domain.Event{SchemaVersion: 1, EventID: id, TenantID: "demo", UserID: "u", EventType: "page_view", OccurredAt: "2026-11-06T17:00:00Z"}
	repair, _ := json.Marshal(valid)
	if _, _, err := decoded.Payload(repair, "demo"); err != nil {
		t.Fatal(err)
	}
	valid.EventID = uuid.NewString()
	changed, _ := json.Marshal(valid)
	if _, _, err := decoded.Payload(changed, "demo"); err == nil {
		t.Fatal("identity-changing repair accepted")
	}
	if _, _, err := decoded.Payload(repair, "other"); err == nil {
		t.Fatal("tenant escape")
	}
	if _, err := DecodeEnvelope([]byte(`{"envelope_version":1}`)); err == nil {
		t.Fatal("incomplete envelope accepted")
	}
}
func TestReplayBounds(t *testing.T) {
	o := Options{JobID: uuid.NewString(), Tenant: "demo", Actor: "operator", SourceTopic: DLQTopic, TargetTopic: "streamforge.events.v1", Partition: 0, From: 0, To: 1, ByteLimit: 8 << 20}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	o.To = 1001
	if err := o.Validate(); err == nil {
		t.Fatal("unbounded range")
	}
}
