package fetcher

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func MarshalClientState(state *clientState) []byte {
	w := wire.NewWriter()
	state.tracker.Marshal(w)
	state.outputTracker.Marshal(w)
	return w.Bytes()
}

func UnmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)
	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, err
	}
	outputTracker, err := outputtracker.Unmarshal(r)
	if err != nil {
		return nil, err
	}
	return &clientState{tracker: tracker, outputTracker: outputTracker}, nil
}
