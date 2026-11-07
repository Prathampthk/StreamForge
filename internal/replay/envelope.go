// Package replay owns poison envelopes and bounded audited replay.
package replay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"streamforge/internal/domain"
	"strings"
	"time"
)

const DLQTopic = "streamforge.events.dlq.v1"
const MaxEnvelopeBytes = 2 << 20

type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
type Envelope struct {
	Version         int       `json:"envelope_version"`
	ID              string    `json:"envelope_id"`
	EventID         *string   `json:"event_id,omitempty"`
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int32     `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	Raw             []byte    `json:"raw_base64"`
	Key             []byte    `json:"key_base64"`
	Headers         []Header  `json:"headers"`
	FailureClass    string    `json:"failure_class"`
	FailureCode     string    `json:"failure_code"`
	Error           string    `json:"error"`
	Component       string    `json:"failed_component"`
	RetryCount      int       `json:"retry_count"`
	FirstFailureAt  time.Time `json:"first_failure_at"`
	DLQAt           time.Time `json:"dlq_at"`
}

func EnvelopeID(topic string, partition int32, offset int64) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("streamforge:%s:%d:%d", topic, partition, offset))).String()
}
func safeHeader(key string) bool {
	return key == "schema_version" || key == "correlation_id" || key == "traceparent" || key == "produced_at"
}
func NewEnvelope(r *kgo.Record, first time.Time) Envelope {
	e := Envelope{Version: 1, ID: EnvelopeID(r.Topic, r.Partition, r.Offset), SourceTopic: r.Topic, SourcePartition: r.Partition, SourceOffset: r.Offset, Raw: r.Value, Key: r.Key, FailureClass: "permanent_validation", FailureCode: "invalid_event", Error: "event violates v1 contract", Component: "consumer", FirstFailureAt: first.UTC(), DLQAt: time.Now().UTC(), Headers: []Header{}}
	var parsed struct {
		ID string `json:"event_id"`
	}
	if json.Unmarshal(r.Value, &parsed) == nil && len(parsed.ID) == 36 {
		if id, err := uuid.Parse(parsed.ID); err == nil {
			value := id.String()
			e.EventID = &value
		}
	}
	for _, h := range r.Headers {
		if safeHeader(h.Key) {
			e.Headers = append(e.Headers, Header{Key: h.Key, Value: h.Value})
		}
	}
	return e
}
func DecodeEnvelope(raw []byte) (Envelope, error) {
	var e Envelope
	if len(raw) > MaxEnvelopeBytes {
		return e, errors.New("DLQ envelope exceeds bound")
	}
	if err := domain.CheckJSON(raw); err != nil {
		return e, errors.New("malformed DLQ JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&e); err != nil {
		return e, errors.New("invalid DLQ envelope")
	}
	if e.Version != 1 || e.SourceTopic == "" || len(e.SourceTopic) > 249 || e.SourcePartition < 0 || e.SourcePartition >= 12 || e.SourceOffset < 0 || e.ID != EnvelopeID(e.SourceTopic, e.SourcePartition, e.SourceOffset) || len(e.Raw) > 1<<20 || e.FirstFailureAt.IsZero() || e.DLQAt.IsZero() || e.FailureCode != "invalid_event" || e.FailureClass != "permanent_validation" || e.Component != "consumer" || e.RetryCount < 0 {
		return e, errors.New("DLQ envelope invariants failed")
	}
	for _, h := range e.Headers {
		if !safeHeader(h.Key) {
			return e, errors.New("unsafe DLQ header")
		}
	}
	if e.EventID != nil {
		id, err := uuid.Parse(*e.EventID)
		if err != nil || id.String() != *e.EventID {
			return e, errors.New("invalid original event ID")
		}
	}
	return e, nil
}
func (e Envelope) Payload(repair []byte, tenant string) (domain.Event, []byte, error) {
	raw := e.Raw
	if repair != nil {
		raw = repair
	}
	event, err := domain.DecodeStoredEvent(raw)
	if err != nil {
		return event, nil, errors.New("replay payload invalid; provide a valid explicit repair")
	}
	if e.EventID == nil || strings.ToLower(event.EventID) != *e.EventID || event.TenantID != tenant || string(e.Key) != event.TenantID+":"+event.UserID {
		return event, nil, errors.New("replay identity/tenant/key mismatch")
	}
	// Preserve the immutable logical event, not a new UUID or a new timestamp.
	at, err := domain.ParseTimestamp(event.OccurredAt)
	if err != nil {
		return event, nil, err
	}
	event.OccurredAt = at.UTC().Format(time.RFC3339Nano)
	encoded, err := json.Marshal(event)
	if err != nil {
		return event, nil, fmt.Errorf("encode replay: %w", err)
	}
	return event, encoded, nil
}
