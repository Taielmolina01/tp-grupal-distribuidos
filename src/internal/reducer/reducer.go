package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/worker"
)

const eofRingQueuePrefix = "REDUCE"

func newReducer[T, O comparable](
	config ReducerConfig,
	reducerFunction func(T, T) T,
	keyFunc func(T) string,
	projectFunc func(T) O,
	inputCodec wire.Codec[T],
	outputCodec wire.Codec[O],
	queryId uint8,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.InputQueue, config.InputRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueues := make([]middleware.Middleware, 0, len(config.OutputQueues))
	for _, outputQueue := range config.OutputQueues {
		m, err := middleware.CreateQueueMiddleware(outputQueue, connSettings)
		if err != nil {
			if err := inputExchange.Close(); err != nil {
				slog.Error("While closing input exchange", "err", err)
			}
			return nil, err
		}
		outputQueues = append(outputQueues, m)
	}

	eofInputName, eofOutputName := eofring.GetInputAndOutputQueueNames(
		config.Id,
		config.ReducerAmount,
		eofRingQueuePrefix,
		eofRingQueuePrefix,
	)

	eofInput, err := middleware.CreateQueueMiddleware(
		eofInputName,
		connSettings,
	)

	if err != nil {
		for _, outputQueue := range outputQueues {
			if err := outputQueue.Close(); err != nil {
				slog.Error("While closing output queue", "err", err)
			}
		}
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input exchange", "err", err)
		}
		return nil, err
	}

	eofOutput, err := middleware.CreateQueueMiddleware(
		eofOutputName,
		connSettings,
	)

	if err != nil {
		for _, outputQueue := range outputQueues {
			if err := outputQueue.Close(); err != nil {
				slog.Error("While closing output queue", "err", err)
			}
		}
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input exchange", "err", err)
		}
		if err := eofInput.Close(); err != nil {
			slog.Error("While closing EOF input queue", "err", err)
		}
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	reducer := &Reducer[T, O]{
		id:                config.Id,
		inputExchange:     inputExchange,
		outputQueues:      outputQueues,
		reducerMonitor:    NewReducerMonitor[T](handlerMessages),
		reducerFunction:   reducerFunction,
		keyFunc:           keyFunc,
		projectFunc:       projectFunc,
		inputEofsExpected: config.InputEofsExpected,
		inputEofCount:     map[int]int{},
		totalRealAmount:   map[int]uint32{},
		inputCodec:        inputCodec,
		outputCodec:       outputCodec,
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.ReducerAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, total uint32, isCoordinator bool) error {
			values := reducer.reducerMonitor.GetValuesCopyByClientIdAndDelete(clientID)

			byShard := make(map[int][]O)
			for _, v := range values {
				idx := shard.CalculateIndexForShard(clientID, keyFunc(v), len(reducer.outputQueues))
				byShard[idx] = append(byShard[idx], projectFunc(v))
			}
			for idx, group := range byShard {
				body := batch.Write(clientID, reducer.queryId, 0, 0, group, reducer.outputCodec)
				if err := reducer.outputQueues[idx].Send(middleware.Message{Body: string(body)}); err != nil {
					return err
				}
			}

			eofBody := batch.WriteEOF(clientID, reducer.queryId, 0, 0, total)
			for _, outputQueue := range reducer.outputQueues {
				if err := outputQueue.Send(middleware.Message{Body: string(eofBody)}); err != nil {
					return err
				}
			}
			return nil
		},
		queryId,
	)

	reducer.eofHandler = eofHandler
	reducer.handlerMessages = handlerMessages
	reducer.outputQueueEof = eofOutput
	reducer.queryId = queryId

	return reducer, nil
}

func (reducer *Reducer[T, O]) Run() {
	defer func() {
		if err := reducer.close(); err != nil {
			slog.Error("While closing reducer", "err", err)
		}
	}()
	go reducer.eofHandler.Run()
	if err := reducer.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		reducer.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (reducer *Reducer[T, O]) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), reducer.inputCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		nack()
		return
	}

	if input.EOF {
		reducer.handleEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		record := input.Records[i]
		reducer.reducerMonitor.AddValue(
			input.ClientID,
			reducer.keyFunc(record),
			record,
			reducer.reducerFunction,
		)
	}
}

func (reducer *Reducer[T, O]) handleEOF(clientID int, total uint32) {
	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   reducer.handlerMessages.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(reducer.id),
		FilteredAmount: reducer.handlerMessages.GetForwardedMessagesAmountByClientId(clientID),
	}

	if err := reducer.outputQueueEof.Send(middleware.Message{Body: string(eofring.SerializeRingMessage(eofRingMessage))}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
	}
}

func (reducer *Reducer[T, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := reducer.inputExchange.StopConsuming(); err != nil {
		slog.Error("while closing reducer", "err", err)
	}
}

func (reducer *Reducer[T, O]) close() error {
	if err := reducer.inputExchange.Close(); err != nil {
		return err
	}
	for _, outputQueue := range reducer.outputQueues {
		if err := outputQueue.Close(); err != nil {
			return err
		}
	}
	return nil
}
