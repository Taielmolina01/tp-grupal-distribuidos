package join

import (
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateTransferAccountByBankJoin(config JoinConfig) (worker.Worker, error) {
	config.QueryID = queryresult.Query2ID
	return NewJoin(config)
}
