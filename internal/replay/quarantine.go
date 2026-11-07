package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"streamforge/internal/metadata"
	"time"
)

type Quarantine struct {
	Store    *metadata.Store
	Producer *kgo.Client
	Topic    string
}

func NewQuarantine(brokers []string, store *metadata.Store) (*Quarantine, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxBufferedRecords(12), kgo.MaxBufferedBytes(24<<20), kgo.ProducerBatchMaxBytes(2<<20), kgo.RecordDeliveryTimeout(3*time.Second), kgo.RecordRetries(3), kgo.DialTimeout(time.Second), kgo.RetryTimeout(3*time.Second))
	if err != nil {
		return nil, fmt.Errorf("DLQ producer: %w", err)
	}
	return &Quarantine{Store: store, Producer: client, Topic: DLQTopic}, nil
}
func (q *Quarantine) Close() { q.Producer.Close() }
func (q *Quarantine) Handle(ctx context.Context, r *kgo.Record) error {
	provisional := NewEnvelope(r, time.Now())
	_, err := q.Store.Pool.Exec(ctx, `INSERT INTO dlq_audit(source_topic,source_partition,source_offset,envelope_id,event_id,failure_code) VALUES($1,$2,$3,$4,$5,'invalid_event') ON CONFLICT DO NOTHING`, r.Topic, r.Partition, r.Offset, provisional.ID, provisional.EventID)
	if err != nil {
		return errors.New("DLQ audit unavailable; source uncommitted")
	}
	transaction, err := q.Store.Pool.Begin(ctx)
	if err != nil {
		return errors.New("begin DLQ audit")
	}
	defer func() {
		rollback, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = transaction.Rollback(rollback)
	}()
	var first time.Time
	var published *time.Time
	if err := transaction.QueryRow(ctx, `SELECT first_failure_at,published_at FROM dlq_audit WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3 FOR UPDATE`, r.Topic, r.Partition, r.Offset).Scan(&first, &published); err != nil {
		return errors.New("lock DLQ audit")
	}
	if published != nil {
		return transaction.Commit(ctx)
	}
	envelope := NewEnvelope(r, first)
	raw, err := json.Marshal(envelope)
	if err != nil || len(raw) > MaxEnvelopeBytes {
		return errors.New("DLQ envelope exceeds bound; source uncommitted")
	}
	record := &kgo.Record{Topic: q.Topic, Key: []byte(envelope.ID), Value: raw}
	// A canceled waiter returns without pretending an ambiguous publish failed.
	result := make(chan error, 1)
	q.Producer.TryProduce(ctx, record, func(_ *kgo.Record, err error) { result <- err })
	select {
	case err := <-result:
		if err != nil {
			return fmt.Errorf("DLQ publish unconfirmed: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("DLQ publish unresolved: %w", ctx.Err())
	}
	_, err = transaction.Exec(ctx, `UPDATE dlq_audit SET published_at=now(),dlq_partition=$4,dlq_offset=$5 WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3`, r.Topic, r.Partition, r.Offset, record.Partition, record.Offset)
	if err != nil {
		return errors.New("DLQ acknowledgment audit failed; source uncommitted")
	}
	if err := transaction.Commit(ctx); err != nil {
		return errors.New("DLQ acknowledgment audit commit failed; source uncommitted")
	}
	return nil
}
