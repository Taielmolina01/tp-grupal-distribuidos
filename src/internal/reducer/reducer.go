package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/worker"
)

type ReducerConfig struct {
	Id             int
	ReducerAmount  int
	MomHost        string
	MomPort        int
	InputExchange  string
	OutputExchange string
	QueryId        uint8
	OutputQueue    string
	InputRoutingKeys  []string
	OutputRoutingKeys []string
}

type Reducer[T comparable] struct {
	id              int
	inputExchange   middleware.Middleware
	outputExchange  middleware.Middleware
	actualValue     *T
	callback        func(T, T) T
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	outputQueueEof  middleware.Middleware
	queryId        uint8
	clientID       int
}


func newReducer[T comparable](config ReducerConfig, callback func(T, T) T, queryId uint8) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.OutputQueue, config.OutputRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, config.OutputQueue, config.OutputRoutingKeys, connSettings)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input queue", "err", err)
		}
		return nil, err
	}
	
	next := config.Id + 1
	if config.Id == config.ReducerAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		"REDUCE_"+strconv.Itoa(config.Id),
		connSettings,
	)

	eofOutput, err := middleware.CreateQueueMiddleware(
		"REDUCE_"+strconv.Itoa(next),
		connSettings,
	)

	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	
	reducer := &Reducer[T]{
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		actualValue:    nil,
		callback:       callback,
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.ReducerAmount,
		uint32(config.Id),
		outputExchange,
		handlerMessages,
		func() error {
			msgOutput, err := inner.SerializeData(inner.DataMsg[T]{
				Payload:  *reducer.actualValue,
				ClientID: reducer.clientID,
				QueryID:  reducer.queryId,
			})
			if err != nil {
				return err
			}
			return reducer.outputExchange.Send(*msgOutput)
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
		slog.Info("EOF message received, sending final result")

		if reducer.actualValue == nil {
			slog.Info("No data received, sending EOF message")
		}

		eofRingMessage := eofmessagetypes.EofRingMessage{
			RealAmount:     result.EOF.TotalMessages,
			ActualAmount:   reducer.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			Leader:         uint32(reducer.id),
			FilteredAmount: reducer.handlerMessages.GetFilteredMessagesAmountByClientId(result.ClientID),
		}
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
		slog.Info("EOF message sent to EOF ring", "filter_id", reducer.id, "client_id", eofRingMessage.ClientId, "real_amount", eofRingMessage.RealAmount, "actual_amount", eofRingMessage.ActualAmount)
		slog.Info("Total messages processed by filter", "filter_id", reducer.id, "client_id", reducer.id, "processed_messages", reducer.handlerMessages.GetProcessedMessagesAmountByClientId(int(reducer.id)))
	} else {
		reducer.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)
		reducer.clientID = result.ClientID
		if reducer.actualValue == nil {
			reducer.actualValue = &result.Payload
		} else {
			actualValue := reducer.callback(*reducer.actualValue, result.Payload)
			reducer.actualValue = &actualValue
		}
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
	if err := reducer.outputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := reducer.outputExchange.Close(); err != nil {
		return err
	}
	return nil
}
