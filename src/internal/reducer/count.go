package reducer

import (
	"log/slog"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newCountReducer(
	config ReducerConfig,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(
		config.InputQueue,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	out, err := middleware.CreateQueueMiddleware(
		config.OutputQueues[0],
		connSettings,
	)

	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("while closing input queue", "err", err)
		}
		return nil, err
	}

	return &CountReducer{
		actualCount: 0,
		inputQueue:  inputQueue,
		outputQueue: out,
		queryId:     config.QueryId,
	}, nil
}

func (count *CountReducer) Run() {
	if err := count.inputQueue.StartConsuming(count.handleMessage); err != nil {
		slog.Error("while consuming from input queue", "err", err)
	}
}

func (count *CountReducer) handleMessage(msg middleware.Message, ack, nack func()) {
	// No deberían llegar repetidos asique simplemente por cada vez que me llamen sumo uno al contador
	count.actualCount++
}

func (count *CountReducer) HandleSignals() {
	if err := count.inputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from input queue", "err", err)
	}
	if err := count.inputQueue.Close(); err != nil {
		slog.Error("while closing input queue", "err", err)
	}
	if err := count.outputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
