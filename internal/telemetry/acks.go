package telemetry

import (
	"context"
	"sync"
	"time"
)

type Ack struct {
	EventID string    `json:"event_id"`
	At      time.Time `json:"acknowledged_at"`
}
type AckTimes struct {
	mu     sync.Mutex
	values []Ack
}
type ackKey struct{}

func WithAckTimes(ctx context.Context) (context.Context, *AckTimes) {
	a := &AckTimes{}
	return context.WithValue(ctx, ackKey{}, a), a
}
func ObserveAck(ctx context.Context, id string) {
	if a, ok := ctx.Value(ackKey{}).(*AckTimes); ok {
		a.mu.Lock()
		a.values = append(a.values, Ack{EventID: id, At: time.Now().UTC()})
		a.mu.Unlock()
	}
}
func (a *AckTimes) Values() []Ack {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Ack(nil), a.values...)
}
