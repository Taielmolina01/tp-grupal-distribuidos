package reducer

import (
	"log/slog"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
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
		inputQueue:    inputQueue,
		outputQueue:   out,
		queryId:       config.QueryId,
		countByClient: map[int]uint32{},
	}, nil
}

func (count *CountReducer) Run() {
	if err := count.inputQueue.StartConsuming(count.handleMessage); err != nil {
		slog.Error("while consuming from input queue", "err", err)
	}
}

func (count *CountReducer) handleMessage(msg middleware.Message, ack, nack func()) {
	// No deberían llegar repetidos asique simplemente por cada vez que me llamen sumo uno al contador
	// si me llega un EOF ahi si deberia cortar  y mandar el resultado a la siguiente etapa. Ver si necesito
	// un EOF o N EOF siendo N la cantidad de filters de la etapa anterior.
	defer ack()

	deserialized, err := inner.DeserializeData[transfer.Transfer](&msg) // no está el generic aca
	if err != nil {
		slog.Error("while deserializing message", "err", err)
		return
	}

	if !deserialized.IsEOF() {
		count.countByClient[deserialized.ClientID]++
	} else {
		result, err := inner.SerializeData(inner.DataMsg[queryresult.Query5Result]{
			Payload: queryresult.Query5Result{
				Qty: count.countByClient[deserialized.ClientID],
			},
			QueryID: count.queryId,
			EOF: &inner.EOFInfo{
				TotalMessages: 1,
			},
			ClientID: deserialized.ClientID,
		})
		if err != nil {
			slog.Error("while serializing EOF message", "err", err)
			return
		}
		if err := count.outputQueue.Send(*result); err != nil {
			slog.Error("while sending EOF message", "err", err)
		}
	}

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
