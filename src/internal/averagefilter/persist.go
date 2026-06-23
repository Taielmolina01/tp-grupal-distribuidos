package averagefilter

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.transfersTracker.Marshal(w)
	s.avgsTracker.Marshal(w)
	s.outputTracker.Marshal(w)
	w.Bool(s.avgsReady)
	w.Uint32(uint32(s.expectedAvgRecords))
	marshalAvgs(w, s.avgs)
	marshalPending(w, s.pending)
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	transfersTracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal transfers tracker: %w", err)
	}
	avgsTracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal avgs tracker: %w", err)
	}
	ot, err := outputtracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal output tracker: %w", err)
	}

	state := &clientState{
		transfersTracker:   transfersTracker,
		avgsTracker:        avgsTracker,
		outputTracker:      ot,
		avgsReady:          r.Bool(),
		expectedAvgRecords: int(r.Uint32()),
		bufferFiles:        nil,
	}

	state.avgs, err = unmarshalAvgs(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal avgs: %w", err)
	}
	state.pending, err = unmarshalPending(r)
	if err != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal pending: %w", err)
	}
	if r.Err() != nil {
		return nil, fmt.Errorf("averagefilter: unmarshal state: %w", r.Err())
	}

	return state, nil
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

func marshalPending(w *wire.Writer, pending []queryresult.Query3Result) {
	w.Uint32(uint32(len(pending)))
	for i := range pending {
		w.String(pending[i].FromBank)
		w.String(pending[i].FromAccount)
		w.String(pending[i].PaymentFormat)
		w.Float64(pending[i].Amount)
	}
}

func unmarshalPending(r *wire.Reader) ([]queryresult.Query3Result, error) {
	n := r.Uint32()
	if r.Err() != nil {
		return nil, r.Err()
	}
	pending := make([]queryresult.Query3Result, 0, n)
	for range n {
		result := queryresult.Query3Result{
			FromBank:      r.String(),
			FromAccount:   r.String(),
			PaymentFormat: r.String(),
			Amount:        r.Float64(),
		}
		if r.Err() != nil {
			return nil, r.Err()
		}
		pending = append(pending, result)
	}
	return pending, nil
}
