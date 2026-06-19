package outputtracker

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type OutputTracker struct {
	sent map[string]uint64
}

func New() *OutputTracker {
	return &OutputTracker{
		sent: map[string]uint64{},
	}
}

func (t *OutputTracker) RegisterBatch(routingKey string) uint64 {
	t.sent[routingKey]++
	return t.sent[routingKey]
}

func (t *OutputTracker) ForEach(fn func(routingKey string, total uint64)) {
	for rk, total := range t.sent {
		fn(rk, total)
	}
}

func (t *OutputTracker) Total() uint64 {
	var total uint64
	for _, count := range t.sent {
		total += count
	}
	return total
}

func (t *OutputTracker) Marshal(w *wire.Writer) {
	w.Uint64(uint64(len(t.sent)))
	for rk, total := range t.sent {
		w.String(rk)
		w.Uint64(total)
	}
}

func Unmarshal(r *wire.Reader) (*OutputTracker, error) {
	n := r.Uint64()
	sent := make(map[string]uint64, n)
	for range n {
		rk := r.String()
		sent[rk] = r.Uint64()
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return &OutputTracker{sent: sent}, nil
}
