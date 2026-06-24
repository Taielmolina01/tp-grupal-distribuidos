package join

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/transfer"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.leftTracker.Marshal(w)
	s.rightTracker.Marshal(w)
	w.Bool(s.emitted)

	w.Uint32(uint32(len(s.leftBuffer)))
	for key, t := range s.leftBuffer {
		w.String(key)
		records.TransferForQ2Codec.Marshal(w, &t)
	}

	w.Uint32(uint32(len(s.rightBuffer)))
	for key, a := range s.rightBuffer {
		w.String(key)
		records.AccountCodec.Marshal(w, &a)
	}

	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	leftTracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("join: unmarshal left tracker: %w", err)
	}
	rightTracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("join: unmarshal right tracker: %w", err)
	}

	emitted := r.Bool()

	nLeft := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("join: unmarshal left header: %w", r.Err())
	}
	leftBuffer := make(map[string]transfer.TransferForQ2, nLeft)
	for range nLeft {
		key := r.String()
		t := records.TransferForQ2Codec.Unmarshal(r)
		if r.Err() != nil {
			return nil, fmt.Errorf("join: unmarshal left entry: %w", r.Err())
		}
		leftBuffer[key] = t
	}

	nRight := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("join: unmarshal right header: %w", r.Err())
	}
	rightBuffer := make(map[string]account.Account, nRight)
	for range nRight {
		key := r.String()
		a := records.AccountCodec.Unmarshal(r)
		if r.Err() != nil {
			return nil, fmt.Errorf("join: unmarshal right entry: %w", r.Err())
		}
		rightBuffer[key] = a
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("join: unmarshal: %w", r.Err())
	}

	return &clientState{
		leftBuffer:   leftBuffer,
		rightBuffer:  rightBuffer,
		leftTracker:  leftTracker,
		rightTracker: rightTracker,
		emitted:      emitted,
	}, nil
}
