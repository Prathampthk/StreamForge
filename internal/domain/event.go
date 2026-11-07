// Package domain defines the v1 wire contract, without runtime validation.
package domain

// Event preserves the caller's RFC3339 timestamp on the wire. Phase 2 will
// parse and validate it before publishing; pointers distinguish absent money
// and status fields from legitimate zero values.
type Event struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id"`
	TenantID      string            `json:"tenant_id"`
	UserID        string            `json:"user_id"`
	SessionID     *string           `json:"session_id,omitempty"`
	EventType     string            `json:"event_type"`
	OccurredAt    string            `json:"occurred_at"`
	OrderID       *string           `json:"order_id,omitempty"`
	AmountCents   *int64            `json:"amount_cents,omitempty"`
	Currency      *string           `json:"currency,omitempty"`
	StatusCode    *uint16           `json:"status_code,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}
