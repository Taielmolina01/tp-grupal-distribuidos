package aggregate

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.tracker.Marshal(w)
	w.Uint32(uint32(len(s.acumuladores)))
	for method, p := range s.acumuladores {
		w.String(method)
		w.Float64(p.totalSum)
		w.Uint32(uint32(p.totalCount))
	}
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("aggregate: unmarshal tracker: %w", err)
	}

	n := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("aggregate: unmarshal header: %w", r.Err())
	}

	acum := make(map[string]partial, n)
	for range n {
		method := r.String()
		sum := r.Float64()
		count := r.Uint32()
		if r.Err() != nil {
			return nil, fmt.Errorf("aggregate: unmarshal acum entry: %w", r.Err())
		}
		acum[method] = partial{totalSum: sum, totalCount: int(count)}
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("aggregate: unmarshal: %w", r.Err())
	}

	return &clientState{
		tracker:      tracker,
		acumuladores: acum,
	}, nil
}
