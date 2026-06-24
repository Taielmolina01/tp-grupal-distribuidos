package reducer

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateReducerMaxAmountFromBank(config ReducerConfig) (worker.Worker, error) {
	return newReducer(
		config,
		func(t1, t2 transfer.TransferAfterCurrency) transfer.TransferAfterCurrency {
			if t1.AmountPaid > t2.AmountPaid {
				return t1
			} else if t1.AmountPaid == t2.AmountPaid {
				if t1.FromBankAccount > t2.FromBankAccount {
					return t1
				} else {
					return t2
				}
			} else {
				return t2
			}
		},
		func(t transfer.TransferAfterCurrency) string { return normalizer.NormalizeBankID(t.FromBank) },
		transfer.ProjectForQ2,
		records.TransferForQ2Codec,
	)
}

func CreateReducerCount(config ReducerConfig) (worker.Worker, error) {
	return newCountReducer(
		config,
	)
}
