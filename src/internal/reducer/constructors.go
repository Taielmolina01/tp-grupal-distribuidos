package reducer

import (
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateReducerCount(config ReducerConfig) (worker.Worker, error) {
	return newCountReducer(
		config,
	)
}
