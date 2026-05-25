package reducer

import (
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateReducerMaxAmountFromBank(config ReducerConfig) (worker.Worker, error) {
	return newReducer(
		config,
		func(t1, t2 transfer.Transfer) transfer.Transfer {
			if t1.AmountPaid > t2.AmountPaid {
				return t1
			} else {
				return t2
			}
		},
		func(t transfer.Transfer) string { return normalizer.NormalizeBankID(t.FromBank) },
		config.QueryId,
	)
}

func CreateReducerCount(config ReducerConfig) (worker.Worker, error) {
	return newCountReducer(
		config,
	)
}
