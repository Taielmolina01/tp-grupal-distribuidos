package averagefilter

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	marshalAvgs(w, s.avgs)
	s.transfersTracker.Marshal(w)
	s.avgsTracker.Marshal(w)
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	avgs, err := unmarshalAvgs(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal avgs: %w", err)
	}
	transfersTracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal transfers tracker: %w", err)
	}
	avgsTracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal avgs tracker: %w", err)
	}
	if r.Err() != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal state: %w", r.Err())
	}

	return &clientState{
		avgs:             avgs,
		transfersTracker: transfersTracker,
		avgsTracker:      avgsTracker,
	}, nil
}

func marshalAvgs(w *wire.Writer, avgs map[string]float64) {
	w.Uint32(uint32(len(avgs)))
	for method, avg := range avgs {
		w.String(method)
		w.Float64(avg)
	}
}

func unmarshalAvgs(r *wire.Reader) (map[string]float64, error) {
	n := r.Uint32()
	if r.Err() != nil {
		return nil, r.Err()
	}
	avgs := make(map[string]float64, n)
	for range n {
		method := r.String()
		avg := r.Float64()
		if r.Err() != nil {
			return nil, r.Err()
		}
		avgs[method] = avg
	}
	return avgs, nil
}
