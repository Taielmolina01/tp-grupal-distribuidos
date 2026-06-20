package joinaccounts

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalTransferState(s *transferPartialState) []byte {
	w := wire.NewWriter()
	s.transferTracker.Marshal(w)
	s.qualifiedOutputTracker.Marshal(w)
	marshalAccountMap(w, s.left)
	marshalAccountMap(w, s.right)
	return w.Bytes()
}

func unmarshalTransferState(data []byte) (*transferPartialState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal transferTracker: %w", err)
	}

	qualifiedOutTracker, err := outputtracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal qualifiedOutputTracker: %w", err)
	}

	left, err := unmarshalAccountMap(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal left: %w", err)
	}

	right, err := unmarshalAccountMap(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal right: %w", err)
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal transfer state: %w", r.Err())
	}

	return &transferPartialState{
		transferTracker:        tracker,
		qualifiedOutputTracker: qualifiedOutTracker,
		left:                   left,
		right:                  right,
	}, nil
}

func marshalQualifiedState(s *qualifiedPartialState) []byte {
	w := wire.NewWriter()
	s.qualifiedTracker.Marshal(w)
	s.chainOutputTracker.Marshal(w)
	marshalAccountSet(w, s.qualifyingLeft)
	marshalAccountSet(w, s.qualifyingRight)
	return w.Bytes()
}

func unmarshalQualifiedState(data []byte) (*qualifiedPartialState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal qualifiedTracker: %w", err)
	}

	chainOutTracker, err := outputtracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal chainOutputTracker: %w", err)
	}

	qualifyingLeft, err := unmarshalAccountSet(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal qualifyingLeft: %w", err)
	}

	qualifyingRight, err := unmarshalAccountSet(r)
	if err != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal qualifyingRight: %w", err)
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("joinaccounts: unmarshal qualified state: %w", r.Err())
	}

	return &qualifiedPartialState{
		qualifiedTracker:   tracker,
		chainOutputTracker: chainOutTracker,
		qualifyingLeft:     qualifyingLeft,
		qualifyingRight:    qualifyingRight,
	}, nil
}

func marshalAccountMap(w *wire.Writer, m map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}) {
	w.Uint32(uint32(len(m)))
	for outer, inner := range m {
		w.String(outer.BankID)
		w.String(outer.AccountNumber)
		w.Uint32(uint32(len(inner)))
		for k := range inner {
			w.String(k.BankID)
			w.String(k.AccountNumber)
		}
	}
}

func unmarshalAccountMap(r *wire.Reader) (map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}, error) {
	outerLen := r.Uint32()
	if r.Err() != nil {
		return nil, r.Err()
	}
	m := make(map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}, outerLen)
	for range outerLen {
		bankID := r.String()
		accNum := r.String()
		if r.Err() != nil {
			return nil, r.Err()
		}
		outerKey := account.AccountIdentifier{BankID: bankID, AccountNumber: accNum}
		innerLen := r.Uint32()
		if r.Err() != nil {
			return nil, r.Err()
		}
		inner := make(map[account.AccountIdentifier]struct{}, innerLen)
		for range innerLen {
			ib := r.String()
			ia := r.String()
			if r.Err() != nil {
				return nil, r.Err()
			}
			inner[account.AccountIdentifier{BankID: ib, AccountNumber: ia}] = struct{}{}
		}
		m[outerKey] = inner
	}
	return m, nil
}

func marshalAccountSet(w *wire.Writer, m map[account.AccountIdentifier]struct{}) {
	w.Uint32(uint32(len(m)))
	for k := range m {
		w.String(k.BankID)
		w.String(k.AccountNumber)
	}
}

func unmarshalAccountSet(r *wire.Reader) (map[account.AccountIdentifier]struct{}, error) {
	n := r.Uint32()
	if r.Err() != nil {
		return nil, r.Err()
	}
	m := make(map[account.AccountIdentifier]struct{}, n)
	for range n {
		bankID := r.String()
		accNum := r.String()
		if r.Err() != nil {
			return nil, r.Err()
		}
		m[account.AccountIdentifier{BankID: bankID, AccountNumber: accNum}] = struct{}{}
	}
	return m, nil
}
