package acumaccounts

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

func marshalClientState(s *clientState) []byte {
	w := wire.NewWriter()
	s.transferTracker.Marshal(w)
	w.Uint32(uint32(len(s.acum)))
	for pair, count := range s.acum {
		w.String(pair.Left.BankID)
		w.String(pair.Left.AccountNumber)
		w.String(pair.Right.BankID)
		w.String(pair.Right.AccountNumber)
		w.Uint8(uint8(count))
	}
	return w.Bytes()
}

func unmarshalClientState(data []byte) (*clientState, error) {
	r := wire.NewReader(data)

	tracker, err := sendertracker.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("acumaccounts: unmarshal transferTracker: %w", err)
	}

	n := r.Uint32()
	if r.Err() != nil {
		return nil, fmt.Errorf("acumaccounts: unmarshal header: %w", r.Err())
	}

	acum := make(map[account.AccountPair]int8, n)
	for range n {
		leftBank := r.String()
		leftAcc := r.String()
		rightBank := r.String()
		rightAcc := r.String()
		count := r.Uint8()
		if r.Err() != nil {
			return nil, fmt.Errorf("acumaccounts: unmarshal acum entry: %w", r.Err())
		}
		pair := account.AccountPair{
			Left:  account.AccountIdentifier{BankID: leftBank, AccountNumber: leftAcc},
			Right: account.AccountIdentifier{BankID: rightBank, AccountNumber: rightAcc},
		}
		acum[pair] = int8(count)
	}

	if r.Err() != nil {
		return nil, fmt.Errorf("acumaccounts: unmarshal: %w", r.Err())
	}

	return &clientState{
		transferTracker: tracker,
		acum:            acum,
	}, nil
}
