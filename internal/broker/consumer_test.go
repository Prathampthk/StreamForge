package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFetchRetryCannotDiscardRecords(t *testing.T) {
	cases := []struct {
		name                 string
		err                  error
		collected            int
		records              bool
		wantError, wantRetry bool
	}{
		{"empty_coordinator", fmt.Errorf("session: %w", &kgo.ErrGroupSession{Err: kerr.NotCoordinator}), 0, false, true, true},
		{"empty_unknown_topic", kerr.UnknownTopicOrPartition, 0, false, true, true},
		{"prior_records", kerr.NotCoordinator, 1, false, true, false},
		{"records_with_error", kerr.NotCoordinator, 0, true, true, false},
		{"corrupt_record", kerr.CorruptMessage, 0, false, true, false},
		{"permanent_auth", kerr.TopicAuthorizationFailed, 0, false, true, false},
		{"unknown_error", errors.New("unknown fetch error"), 0, false, true, false},
		{"collection_timeout", context.DeadlineExceeded, 0, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			partition := kgo.FetchPartition{Partition: 0, Err: tc.err}
			if tc.records {
				partition.Records = []*kgo.Record{{Offset: 0, Value: []byte("accepted")}}
			}
			fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "events", Partitions: []kgo.FetchPartition{partition}}}}}
			err := fetchFailure(fetches, tc.collected)
			var empty *emptyFetchFailure
			if (err != nil) != tc.wantError || errors.As(err, &empty) != tc.wantRetry {
				t.Fatalf("err=%v retry=%v", err, empty != nil)
			}
		})
	}
	mixed := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "events", Partitions: []kgo.FetchPartition{{Partition: 0, Err: kerr.NotCoordinator}, {Partition: 1, Err: kerr.TopicAuthorizationFailed}}}}}}
	var empty *emptyFetchFailure
	if err := fetchFailure(mixed, 0); err == nil || errors.As(err, &empty) || !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("mixed permanent failure must not retry: %v", err)
	}
}
