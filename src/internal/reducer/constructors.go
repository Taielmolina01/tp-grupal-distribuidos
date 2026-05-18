package reducer

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/worker"
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
