package filteraccountseen

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.tracker.Marshal(w)
	w.Uint32(uint32(len(s.seenAccounts)))
	for acc := range s.seenAccounts {
		w.String(acc.BankID)
		w.String(acc.AccountNumber)
	}
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("filteraccountseen: unmarshal tracker: %w", err)
	}

	n := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("filteraccountseen: unmarshal header: %w", r.Err())
	}

	seenAccounts := make(map[account.AccountIdentifier]struct{}, n)
	for range n {
		bankID := r.String()
		accNum := r.String()
		if r.Err() != nil {
			return nil, fmt.Errorf("filteraccountseen: unmarshal seen entry: %w", r.Err())
		}
		seenAccounts[account.AccountIdentifier{BankID: bankID, AccountNumber: accNum}] = struct{}{}
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("filteraccountseen: unmarshal: %w", r.Err())
	}

	return &clientState{
		tracker:      tracker,
		seenAccounts: seenAccounts,
	}, nil
}
