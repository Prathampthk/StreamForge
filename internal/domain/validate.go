package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const MaxEventBytes = 8192

//go:embed currencies.txt
var currencySnapshot string

var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var timestamp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]([01]\d|2[0-3]):[0-5]\d)$`)
var attributeValue = regexp.MustCompile(`^[A-Za-z0-9_.-]{0,64}$`)

func ValidIdentifier(value string) bool { return identifier.MatchString(value) }

type Policy struct{ MaxLateness, FutureSkew time.Duration }

// DecodeEvent rejects ambiguous JSON, unknown fields, invalid UTF-8 and a
// second top-level value. Clock-relative validation uses an injected instant.
func DecodeEvent(raw []byte, now time.Time, policy Policy) (Event, error) {
	var event Event
	if len(raw) > MaxEventBytes {
		return event, errors.New("event exceeds 8192 bytes")
	}
	if !utf8.Valid(raw) {
		return event, errors.New("event must be UTF-8")
	}
	if err := CheckJSON(raw); err != nil {
		return event, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return event, errors.New("event must be a JSON object")
	}
	allowed := map[string]bool{"schema_version": true, "event_id": true, "tenant_id": true, "user_id": true, "session_id": true, "event_type": true, "occurred_at": true, "order_id": true, "amount_cents": true, "currency": true, "status_code": true, "attributes": true}
	for key := range fields {
		if !allowed[key] {
			return event, errors.New("unknown event field")
		}
	}
	if value, ok := fields["attributes"]; ok {
		var attrs map[string]json.RawMessage
		if err := json.Unmarshal(value, &attrs); err != nil || attrs == nil {
			return event, errors.New("attributes must be an object")
		}
		for _, value := range attrs {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return event, errors.New("attribute values must be strings")
			}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, errors.New("event has invalid fields or field types")
	}
	if err := event.Validate(now, policy); err != nil {
		return event, err
	}
	event.EventID = strings.ToLower(event.EventID)
	return event, nil
}

// CheckJSON bounds nesting and rejects duplicate object names at every level.
// encoding/json otherwise accepts last-value-wins objects.
func CheckJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var visit func(int) error
	visit = func(depth int) error {
		if depth > 16 {
			return errors.New("JSON nesting exceeds 16 levels")
		}
		token, err := decoder.Token()
		if err != nil {
			return errors.New("malformed JSON")
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		if delimiter != '{' && delimiter != '[' {
			return errors.New("malformed JSON")
		}
		seen := map[string]bool{}
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return errors.New("malformed JSON")
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate or invalid JSON field")
				}
				seen[name] = true
			}
			if err := visit(depth + 1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	if err := visit(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("expected one JSON value")
	}
	return nil
}

func (e Event) Validate(now time.Time, p Policy) error {
	if e.SchemaVersion != 1 {
		return errors.New("schema_version must be 1")
	}
	if len(e.EventID) != 36 {
		return errors.New("event_id must be a UUID")
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return errors.New("event_id must be a UUID")
	}
	if !ValidIdentifier(e.TenantID) || !ValidIdentifier(e.UserID) {
		return errors.New("invalid tenant_id or user_id")
	}
	if e.SessionID != nil && !ValidIdentifier(*e.SessionID) {
		return errors.New("invalid session_id")
	}
	occurred, err := ParseTimestamp(e.OccurredAt)
	if err != nil {
		return errors.New("occurred_at must be RFC3339 with at most nanosecond precision")
	}
	if occurred.After(now.Add(p.FutureSkew)) || occurred.Before(now.Add(-p.MaxLateness)) {
		return errors.New("occurred_at is outside the allowed time window")
	}
	switch e.EventType {
	case "purchase":
		if e.OrderID == nil || !ValidIdentifier(*e.OrderID) || e.AmountCents == nil || *e.AmountCents < 0 || e.Currency == nil {
			return errors.New("purchase requires order_id, nonnegative amount_cents and currency")
		}
		if !ValidCurrency(*e.Currency) {
			return errors.New("currency is not in the pinned ISO 4217 snapshot")
		}
		if e.StatusCode != nil {
			return errors.New("purchase must not include status_code")
		}
	case "api_request":
		if e.StatusCode == nil || *e.StatusCode < 100 || *e.StatusCode > 599 {
			return errors.New("api_request requires status_code 100-599")
		}
		if e.OrderID != nil || e.AmountCents != nil || e.Currency != nil {
			return errors.New("api_request must not include purchase fields")
		}
	case "page_view", "add_to_cart":
		if e.StatusCode != nil || e.OrderID != nil || e.AmountCents != nil || e.Currency != nil {
			return errors.New("unused conditional fields must be absent or null")
		}
	default:
		return errors.New("unsupported event_type")
	}
	if len(e.Attributes) > 3 {
		return errors.New("too many attributes")
	}
	for key, value := range e.Attributes {
		if key != "source" && key != "device" && key != "campaign" {
			return errors.New("unsupported attribute key")
		}
		if !attributeValue.MatchString(value) {
			return fmt.Errorf("invalid %s attribute", key)
		}
	}
	return nil
}

// DecodeStoredEvent validates Kafka payload shape and invariants without
// reapplying ingress wall-clock age limits to retained backlog or retries.
func DecodeStoredEvent(raw []byte) (Event, error) {
	if len(raw) > MaxEventBytes {
		return Event{}, errors.New("stored event exceeds 8192 bytes")
	}
	var timestampOnly struct {
		OccurredAt string `json:"occurred_at"`
	}
	if err := json.Unmarshal(raw, &timestampOnly); err != nil {
		return Event{}, errors.New("malformed stored event")
	}
	instant, err := time.Parse(time.RFC3339Nano, timestampOnly.OccurredAt)
	if err != nil {
		return Event{}, errors.New("invalid stored event timestamp")
	}
	return DecodeEvent(raw, instant, Policy{})
}

// ValidCurrency checks the pinned ISO snapshot, excluding test/no-currency codes.
func ValidCurrency(value string) bool {
	return len(value) == 3 && strings.Contains("\n"+currencySnapshot, "\n"+value+"\n")
}

func ParseTimestamp(value string) (time.Time, error) {
	if !timestamp.MatchString(value) {
		return time.Time{}, errors.New("timestamp must be RFC3339 with at most nanosecond precision")
	}
	instant, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp: %w", err)
	}
	return instant, nil
}
