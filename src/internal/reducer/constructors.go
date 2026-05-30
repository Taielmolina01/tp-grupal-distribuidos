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
			} else {
				return t2
			}
		},
		func(t transfer.TransferAfterCurrency) string { return normalizer.NormalizeBankID(t.FromBank) },
		transfer.ProjectForQ2,
		records.TransferAfterCurrencyCodec,
		records.TransferForQ2Codec,
		config.QueryId,
	)
}

func CreateReducerCount(config ReducerConfig) (worker.Worker, error) {
	return newCountReducer(
		config,
	)
}
