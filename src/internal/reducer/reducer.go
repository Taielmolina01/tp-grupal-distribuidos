package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/worker"
)

const eofRingQueuePrefix = "REDUCE"

func newReducer[T comparable](
	config ReducerConfig,
	reducerFunction func(T, T) T,
	keyFunc func(T) string,
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

	reducer := &Reducer[T]{
		id:                config.Id,
		inputExchange:     inputExchange,
		outputQueues:      outputQueues,
		reducerMonitor:    NewReducerMonitor[T](handlerMessages),
		reducerFunction:   reducerFunction,
		keyFunc:           keyFunc,
		inputEofsExpected: config.InputEofsExpected,
		inputEofCount:     map[int]int{},
		totalRealAmount:   map[int]uint32{},
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.ReducerAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			values := reducer.reducerMonitor.GetValuesCopyByClientIdAndDelete(clientID)
			for _, v := range values {
				msgOutput, err := inner.SerializeData(inner.DataMsg[T]{
					Payload:  v,
					ClientID: clientID,
					QueryID:  reducer.queryId,
				})
				if err != nil {
					return err
				}
				if err := reducer.outputQueues[shard.CalculateIndexForShard(
					clientID,
					keyFunc(v),
					len(reducer.outputQueues))].Send(*msgOutput); err != nil {
					return err
				}
			}

			for _, outputQueue := range reducer.outputQueues {
				if err := outputQueue.Send(*msg); err != nil {
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

func (reducer *Reducer[T]) Run() {
	go reducer.eofHandler.Run()
	if err := reducer.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		reducer.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (reducer *Reducer[T]) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	result, err := inner.DeserializeData[T](&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if result.IsEOF() {
		reducer.inputEofCount[result.ClientID]++
		reducer.totalRealAmount[result.ClientID] = result.EOF.TotalMessages
		// slog.Info("input EOF received", "client_id", result.ClientID, "count", reducer.inputEofCount[result.ClientID], "expected", reducer.inputEofsExpected)

		eofRingMessage := eofmessagetypes.EofRingMessage{
			RealAmount:     reducer.totalRealAmount[result.ClientID],
			ActualAmount:   reducer.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			CoordinatorId:  uint32(reducer.id),
			FilteredAmount: reducer.handlerMessages.GetForwardedMessagesAmountByClientId(result.ClientID),
		}
		delete(reducer.inputEofCount, result.ClientID)
		delete(reducer.totalRealAmount, result.ClientID)

		serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(eofRingMessage)

		if err != nil {
			slog.Error("While serializing EOF message", "err", err)
			return
		}

		if err := reducer.outputQueueEof.Send(
			*serializedEofRingMessage,
		); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
		}
		// slog.Info("EOF message sent to EOF ring", "reducer_id", reducer.id, "client_id", eofRingMessage.ClientId, "real_amount", eofRingMessage.RealAmount, "actual_amount", eofRingMessage.ActualAmount)
	} else {
		key := reducer.keyFunc(result.Payload)

		reducer.reducerMonitor.AddValue(
			result.ClientID,
			key,
			result.Payload,
			reducer.reducerFunction,
		)
	}

}

func (reducer *Reducer[T]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := reducer.Close(); err != nil {
		slog.Error("While closing reducer node", "err", err)
	}
}

func (reducer *Reducer[T]) Close() error {
	if err := reducer.inputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := reducer.inputExchange.Close(); err != nil {
		return err
	}
	for _, outputQueue := range reducer.outputQueues {
		if err := outputQueue.StopConsuming(); err != nil {
			return err
		}
		if err := outputQueue.Close(); err != nil {
			return err
		}
	}
	return nil
}
