package sum

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/transfer"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.tracker.Marshal(w)
	w.Uint32(uint32(len(s.acumuladores)))
	for method, partial := range s.acumuladores {
		w.String(method)
		w.Float64(partial.Sum)
		w.Uint32(uint32(partial.Amount))
	}
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("sum: unmarshal tracker: %w", err)
	}

	n := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("sum: unmarshal header: %w", r.Err())
	}

	acum := make(map[string]transfer.SumByMethod, n)
	for range n {
		method := r.String()
		sum := r.Float64()
		amount := r.Uint32()
		if r.Err() != nil {
			return nil, fmt.Errorf("sum: unmarshal acum entry: %w", r.Err())
		}
		acum[method] = transfer.SumByMethod{
			Method: method,
			Sum:    sum,
			Amount: int(amount),
		}
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("sum: unmarshal: %w", r.Err())
	}

	return &clientState{
		tracker:      tracker,
		acumuladores: acum,
	}, nil
}
