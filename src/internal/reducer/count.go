package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newCountReducer(
	config ReducerConfig,
) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := newmiddleware.NewQueueMiddleware(
		connSettings,
		config.InputQueue,
	)

	if err != nil {
		return nil, err
	}

	out, err := newmiddleware.NewQueueMiddleware(
		connSettings,
		config.OutputQueues[0],
	)

	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("while closing input queue", "err", err)
		}
		return nil, err
	}

	return &CountReducer{
		inputQueue:        inputQueue,
		outputQueue:       out,
		queryId:           config.QueryId,
		countByClient:     map[int]uint32{},
		eofsByClient:      map[int]uint32{},
		inputEofsExpected: uint32(config.InputEofsExpected),
	}, nil
}

func (count *CountReducer) Run() {
	if err := count.inputQueue.StartConsuming(count.handleMessage); err != nil {
		slog.Error("while consuming from input queue", "err", err)
	}
}

func (count *CountReducer) handleMessage(msg newmiddleware.Message, ack, nack func()) {
	defer ack()
	deserialized, err := inner.DeserializeData[transfer.Transfer](&msg) // no está el generic aca
	if err != nil {
		slog.Error("while deserializing message", "err", err)
		return
	}

	slog.Info("message received", "deserialized", deserialized)

	if !deserialized.IsEOF() {
		count.countByClient[deserialized.ClientID]++
	} else {
		if count.eofsByClient[deserialized.ClientID]++; count.eofsByClient[deserialized.ClientID] < count.inputEofsExpected {
			return
		}

		result, err := inner.SerializeData(inner.DataMsg[queryresult.Query5Result]{
			Payload: queryresult.Query5Result{
				Qty: count.countByClient[deserialized.ClientID],
			},
			QueryID:  count.queryId,
			EOF:      nil,
			ClientID: deserialized.ClientID,
		})
		if err != nil {
			slog.Error("while serializing EOF message", "err", err)
			return
		}
		if err := count.outputQueue.Send(*result); err != nil {
			slog.Error("while sending EOF message", "err", err)
		}

		result, err = inner.SerializeData(inner.DataMsg[queryresult.Query5Result]{
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

		delete(count.countByClient, deserialized.ClientID)
	}

}

func (count *CountReducer) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	count.close()
}

func (count *CountReducer) close() {
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
