package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
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
		inputQueue:        inputQueue,
		outputQueue:       out,
		queryId:           config.QueryId,
		countByClient:     map[int]uint32{},
		eofsByClient:      map[int]uint32{},
		inputEofsExpected: uint32(config.InputEofsExpected),
	}, nil
}

func (count *CountReducer) Run() {
	defer count.close()
	if err := count.inputQueue.StartConsuming(count.handleMessage); err != nil {
		slog.Error("while consuming from input queue", "err", err)
	}
}

func (count *CountReducer) handleMessage(msg middleware.Message, ack, nack func()) {
	defer ack()
	input, err := batch.Read([]byte(msg.Body), records.FinalTransferForQ5Codec)
	if err != nil {
		slog.Error("while deserializing input batch", "err", err)
		return
	}

	if !input.EOF {
		count.countByClient[input.ClientID] += uint32(len(input.Records))
		return
	}

	if count.eofsByClient[input.ClientID]++; count.eofsByClient[input.ClientID] < count.inputEofsExpected {
		return
	}

	resultBody := batch.Write(
		input.ClientID,
		count.queryId,
		0,
		0,
		[]queryresult.Query5Result{{Qty: count.countByClient[input.ClientID]}},
		records.Query5ResultCodec,
	)
	if err := count.outputQueue.Send(middleware.Message{Body: string(resultBody)}); err != nil {
		slog.Error("while sending result message", "err", err)
	}

	eofBody := batch.WriteEOF(input.ClientID, count.queryId, 0, 0, 1)
	if err := count.outputQueue.Send(middleware.Message{Body: string(eofBody)}); err != nil {
		slog.Error("while sending EOF message", "err", err)
	}

	delete(count.countByClient, input.ClientID)
}

func (count *CountReducer) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := count.inputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from input queue", "err", err)
	}
}

func (count *CountReducer) close() {
	if err := count.inputQueue.Close(); err != nil {
		slog.Error("while closing input queue", "err", err)
	}
	if err := count.outputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
