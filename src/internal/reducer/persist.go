package reducer

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/transfer"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.tracker.Marshal(w)
	w.Uint32(uint32(len(s.maxByBank)))
	for key, rec := range s.maxByBank {
		w.String(key)
		records.TransferAfterCurrencyCodec.Marshal(w, &rec)
	}
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("reducer: unmarshal tracker: %w", err)
	}

	n := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("reducer: unmarshal header: %w", r.Err())
	}

	maxByBank := make(map[string]transfer.TransferAfterCurrency, n)
	for range n {
		key := r.String()
		rec := records.TransferAfterCurrencyCodec.Unmarshal(r)
		if r.Err() != nil {
			return nil, fmt.Errorf("reducer: unmarshal entry: %w", r.Err())
		}
		maxByBank[key] = rec
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("reducer: unmarshal: %w", r.Err())
	}

	return &clientState{
		tracker:   tracker,
		maxByBank: maxByBank,
	}, nil
}
