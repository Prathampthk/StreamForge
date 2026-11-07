// Package pipeline owns immutable partition batches and bounded sink retries.
package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"streamforge/internal/domain"
)

type Record struct {
	Topic       string
	Partition   int32
	Offset      int64
	Epoch       int32
	Timestamp   time.Time
	Value       []byte
	TraceParent string
}
type Row struct {
	EventID       string  `json:"event_id"`
	TenantID      string  `json:"tenant_id"`
	UserID        string  `json:"user_id"`
	SessionID     string  `json:"session_id"`
	EventType     string  `json:"event_type"`
	OccurredAt    string  `json:"occurred_at"`
	ReceivedAt    string  `json:"received_at"`
	OrderID       string  `json:"order_id"`
	AmountCents   int64   `json:"amount_cents"`
	Currency      string  `json:"currency"`
	StatusCode    *uint16 `json:"status_code"`
	Topic         string  `json:"kafka_topic"`
	Partition     int32   `json:"kafka_partition"`
	Offset        int64   `json:"kafka_offset"`
	BatchID       string  `json:"sink_batch_id"`
	SchemaVersion int     `json:"schema_version"`
	Payload       string  `json:"payload_json"`
}
type Batch struct {
	ID, Topic string
	Partition int32
	Records   []Record
	Rows      []Row
	Body      []byte
}

func NewBatch(records []Record, maxBytes int) (Batch, error) {
	if len(records) == 0 {
		return Batch{}, errors.New("empty partition batch")
	}
	b := Batch{Topic: records[0].Topic, Partition: records[0].Partition, Records: records}
	sum := sha256.New()
	total := 0
	previous := int64(-1)
	for _, record := range records {
		if record.Topic != b.Topic || record.Partition != b.Partition || record.Offset < 0 || record.Offset <= previous {
			return Batch{}, errors.New("batch must have one partition and increasing offsets")
		}
		total += len(record.Value)
		if total > maxBytes {
			return Batch{}, errors.New("partition batch byte limit exceeded")
		}
		previous = record.Offset
		event, err := domain.DecodeStoredEvent(record.Value)
		if err != nil {
			return Batch{}, fmt.Errorf("poison at %s/%d/%d (offset will not commit): %w", record.Topic, record.Partition, record.Offset, err)
		}
		occurred, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
		if err != nil {
			return Batch{}, fmt.Errorf("parse stored event time: %w", err)
		}
		event.OccurredAt = occurred.UTC().Format(time.RFC3339Nano)
		event.EventID = strings.ToLower(event.EventID)
		logical, err := json.Marshal(event)
		if err != nil {
			return Batch{}, fmt.Errorf("canonicalize event: %w", err)
		}
		// Length prefixes prevent ambiguous concatenations in the diagnostic batch ID.
		fmt.Fprintf(sum, "%d:%s:%d:%d:%d:", len(record.Topic), record.Topic, record.Partition, record.Offset, len(record.Value))
		if _, err := sum.Write(record.Value); err != nil {
			return Batch{}, fmt.Errorf("hash batch: %w", err)
		}
		row := Row{EventID: event.EventID, TenantID: event.TenantID, UserID: event.UserID, EventType: event.EventType,
			OccurredAt: occurred.UTC().Format("2006-01-02 15:04:05.000"), ReceivedAt: record.Timestamp.UTC().Format("2006-01-02 15:04:05.000"),
			StatusCode: event.StatusCode, Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, SchemaVersion: event.SchemaVersion, Payload: string(logical)}
		if event.SessionID != nil {
			row.SessionID = *event.SessionID
		}
		if event.OrderID != nil {
			row.OrderID = *event.OrderID
		}
		if event.AmountCents != nil {
			row.AmountCents = *event.AmountCents
		}
		if event.Currency != nil {
			row.Currency = *event.Currency
		}
		b.Rows = append(b.Rows, row)
	}
	b.ID = hex.EncodeToString(sum.Sum(nil))
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for i := range b.Rows {
		b.Rows[i].BatchID = b.ID
		if err := encoder.Encode(b.Rows[i]); err != nil {
			return Batch{}, fmt.Errorf("encode sink row: %w", err)
		}
	}
	b.Body = body.Bytes()
	return b, nil
}

type Sink interface {
	Insert(context.Context, Batch) error
}
type Hook func(context.Context, string, Batch) error

type Processor struct {
	Sink           Sink
	Attempts       int
	AttemptTimeout time.Duration
	Hook           Hook
	OnRetry        func()
}

func (p Processor) Process(ctx context.Context, b Batch) error {
	if p.Attempts < 1 || p.AttemptTimeout <= 0 {
		return errors.New("invalid processor retry policy")
	}
	if p.Hook != nil {
		if err := p.Hook(ctx, "before_insert", b); err != nil {
			return err
		}
	}
	var last error
	for attempt := 0; attempt < p.Attempts; attempt++ {
		request, cancel := context.WithTimeout(ctx, p.AttemptTimeout)
		last = p.Sink.Insert(request, b)
		cancel()
		if last == nil {
			if p.Hook != nil {
				return p.Hook(ctx, "after_insert", b)
			}
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("sink processing canceled: %w", ctx.Err())
		}
		if p.OnRetry != nil {
			p.OnRetry()
		}
		if p.Hook != nil {
			if err := p.Hook(ctx, "during_retry", b); err != nil {
				return err
			}
		}
		if attempt+1 == p.Attempts {
			break
		}
		ceiling := 50 * time.Millisecond * time.Duration(1<<min(attempt, 4))
		timer := time.NewTimer(ceiling/2 + time.Duration(rand.Int64N(int64(ceiling/2))))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("sink retry canceled: %w", ctx.Err())
		}
	}
	return fmt.Errorf("sink retry budget exhausted (source remains uncommitted): %w", last)
}
