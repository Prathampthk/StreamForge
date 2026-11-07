package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"streamforge/internal/broker"
	"streamforge/internal/domain"
	"streamforge/internal/metadata"
	"time"
)

type Options struct {
	JobID, Tenant, Actor, SourceTopic, TargetTopic string
	Partition                                      int32
	From, To                                       int64
	Execute                                        bool
	ByteLimit                                      int
	Repairs                                        map[string]json.RawMessage
}
type Result struct {
	JobID                           string `json:"job_id"`
	Scanned, Republished, Validated int64
	DryRun                          bool `json:"dry_run"`
}
type Runner struct {
	Brokers []string
	Store   *metadata.Store
}

func (o Options) Validate() error {
	if parsed, err := uuid.Parse(o.JobID); err != nil || parsed.String() != o.JobID {
		return errors.New("job-id must be UUID")
	}
	if !domain.ValidIdentifier(o.Tenant) || !domain.ValidIdentifier(o.Actor) || o.SourceTopic == "" || o.TargetTopic == "" || o.Partition < 0 || o.Partition >= 12 || o.From < 0 || o.To <= o.From || o.To-o.From > 1000 || o.ByteLimit < 1 || o.ByteLimit > 16<<20 || len(o.Repairs) > 1000 {
		return errors.New("invalid bounded replay selector")
	}
	return nil
}

type planned struct {
	record    *kgo.Record
	envelope  Envelope
	payload   []byte
	event     domain.Event
	duplicate bool
}

func (r Runner) Run(ctx context.Context, o Options) (result Result, runErr error) {
	result = Result{JobID: o.JobID, DryRun: !o.Execute}
	if err := o.Validate(); err != nil {
		return result, err
	}
	repairs, err := json.Marshal(o.Repairs)
	if err != nil {
		return result, err
	}
	digest := sha256.Sum256(repairs)
	selector, _ := json.Marshal(map[string]any{"partition": o.Partition, "from": o.From, "to": o.To, "target": o.TargetTopic, "execute": o.Execute, "byte_limit": o.ByteLimit, "repair_sha256": hex.EncodeToString(digest[:])})
	job, err := r.Store.LockJob(ctx, o.JobID, o.Tenant, o.Actor, o.SourceTopic, selector)
	if err != nil {
		return result, err
	}
	defer job.Close()
	defer func() {
		status, message := "completed", ""
		if runErr != nil {
			status = "failed"
			message = "replay validation/publication/audit failed"
		}
		finish, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := job.Finish(finish, status, result.Scanned, message); err != nil {
			runErr = errors.Join(runErr, errors.New("finish replay audit failed"))
		}
	}()
	client, err := broker.NewClient(kgo.SeedBrokers(r.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{o.SourceTopic: {o.Partition: kgo.NewOffset().At(o.From)}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(2<<20), kgo.FetchMaxPartitionBytes(2<<20), kgo.FetchMaxWait(100*time.Millisecond), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxBufferedRecords(1), kgo.MaxBufferedBytes(2<<20), kgo.RecordDeliveryTimeout(3*time.Second), kgo.RecordRetries(3))
	if err != nil {
		return result, err
	}
	defer client.Close()
	// The whole range is validated before any publication, with a separate finite byte budget.
	plan := []planned{}
	seen := map[string]string{}
	total := 0
	complete := false
	for !complete {
		fetches := client.PollRecords(ctx, 1)
		if err := fetches.Err(); err != nil {
			return result, fmt.Errorf("bounded replay read: %w", err)
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if p.LogStartOffset > o.From || p.HighWatermark < o.To {
				runErr = errors.New("selector outside retained DLQ range")
			}
		})
		if runErr != nil {
			return result, runErr
		}
		for _, record := range fetches.Records() {
			if record.Offset >= o.To {
				complete = true
				break
			}
			result.Scanned++
			total += len(record.Value)
			if total > o.ByteLimit {
				return result, errors.New("replay byte budget exceeded")
			}
			envelope, err := DecodeEnvelope(record.Value)
			if err != nil {
				return result, err
			}
			if envelope.SourceTopic != o.TargetTopic {
				return result, errors.New("replay source topic does not match target contract")
			}
			event, payload, err := envelope.Payload(o.Repairs[envelope.ID], o.Tenant)
			if err != nil {
				return result, err
			}
			prior, duplicate := seen[envelope.ID]
			if duplicate && prior != string(payload) {
				return result, errors.New("conflicting duplicate DLQ envelopes")
			}
			plan = append(plan, planned{record: record, envelope: envelope, payload: payload, event: event, duplicate: duplicate})
			seen[envelope.ID] = string(payload)
			if record.Offset+1 >= o.To {
				complete = true
			}
		}
	}
	for envelopeID := range o.Repairs {
		if _, ok := seen[envelopeID]; !ok {
			return result, errors.New("repair map contains an unselected envelope")
		}
	}
	if result.Scanned == 0 {
		return result, errors.New("empty DLQ selection")
	}
	for _, item := range plan {
		if item.duplicate {
			if err := job.Audit(ctx, o.SourceTopic, o.Partition, item.record.Offset, item.event.EventID, "duplicate_envelope"); err != nil {
				return result, err
			}
			continue
		}
		done, err := job.AlreadyPublished(ctx, o.SourceTopic, o.Partition, item.record.Offset)
		if err != nil {
			return result, err
		}
		if done {
			continue
		}
		if !o.Execute {
			result.Validated++
			if err := job.Audit(ctx, o.SourceTopic, o.Partition, item.record.Offset, item.event.EventID, "validated"); err != nil {
				return result, err
			}
			continue
		}
		record := &kgo.Record{Topic: o.TargetTopic, Key: item.envelope.Key, Value: item.payload, Headers: []kgo.RecordHeader{{Key: "schema_version", Value: []byte("1")}, {Key: "correlation_id", Value: []byte(o.JobID)}, {Key: "produced_at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))}}}
		response := make(chan error, 1)
		client.TryProduce(ctx, record, func(_ *kgo.Record, err error) { response <- err })
		select {
		case err := <-response:
			if err != nil {
				return result, fmt.Errorf("replay publish unconfirmed: %w", err)
			}
		case <-ctx.Done():
			return result, ctx.Err()
		}
		if err := job.Audit(ctx, o.SourceTopic, o.Partition, item.record.Offset, item.event.EventID, "republished"); err != nil {
			return result, err
		}
		result.Republished++
	}
	return result, nil
}
