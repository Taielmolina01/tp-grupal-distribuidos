package reducer

import (
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateReducerMaxAmountFromBank(config ReducerConfig) (worker.Worker, error) {
	return newReducer[transfer.Transfer](
		config,
		func(t1, t2 transfer.Transfer) transfer.Transfer {
			if t1.AmountPaid > t2.AmountPaid {
				return t1
			} else {
				return t2
			}
		},
	)
}
