package commonfilter

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.tracker.Marshal(w)
	s.outputTracker.Marshal(w)
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("commonfilter: unmarshal tracker: %w", err)
	}

	outTracker, err := outputtracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("commonfilter: unmarshal outputTracker: %w", err)
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("commonfilter: unmarshal: %w", r.Err())
	}

	return &clientState{
		tracker:       tracker,
		outputTracker: outTracker,
	}, nil
}
