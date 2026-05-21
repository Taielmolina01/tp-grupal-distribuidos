package filter

import (
	"log/slog"
	"strconv"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newFilter[T comparable](config FilterConfig, callback func(T) bool) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Named shared queue bound to the transfers exchange with the Q1234 routing key.
	// Multiple filter instances using this same queue compete for messages (BBB buffer).
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.InputQueue, config.InputRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, config.OutputQueue, config.OutputRoutingKeys, connSettings)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("while closing input exchange", "err", err)
		}
		return nil, err
	}

	next := config.Id + 1

	if config.Id == config.FilterAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		"FILTER_"+string(config.Type)+"_"+strconv.Itoa(config.Id),
		connSettings,
	)

	eofOutput, err := middleware.CreateQueueMiddleware(
		"FILTER_"+string(config.Type)+"_"+strconv.Itoa(next),
		connSettings,
	)

	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	return &Filter[T]{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		callback:       callback,
		eofHandler: eofring.CreateEofRingAlgorithm(
			eofInput,
			eofOutput,
			config.FilterAmount,
			uint32(config.Id),
			outputExchange,
			handlerMessages,
		),
		handlerMessages: handlerMessages,
		outputQueueEof:  eofOutput,
		filterType:      config.Type,
	}, nil
}

func (filter *Filter[T]) Run() {
	slog.Info("Starting filter consumers", "filter_id", filter.id)
	go filter.eofHandler.Run()
	if err := filter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		filter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (filter *Filter[T]) handleMessage(msg middleware.Message, ack, nack func()) {
	ack()

	result, err := inner.DeserializeData[T](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if result.IsEOF() {
		eofRingMessage := eofmessagetypes.EofRingMessage{
			RealAmount:     result.EOF.TotalMessages,
			ActualAmount:   filter.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			Leader:         uint32(filter.id),
			FilteredAmount: filter.handlerMessages.GetFilteredMessagesAmountByClientId(result.ClientID),
		}
		serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(eofRingMessage)
		if err != nil {
			slog.Error("While serializing EOF ring message", "err", err)
			return
		}
		if err := filter.outputQueueEof.Send(
			*serializedEofRingMessage,
		); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
		}
		slog.Info("EOF message sent to EOF ring", "filter_id", filter.id, "client_id", eofRingMessage.ClientId, "real_amount", eofRingMessage.RealAmount, "actual_amount", eofRingMessage.ActualAmount)
		slog.Info("Total messages processed by filter", "filter_id", filter.id, "client_id", filter.id, "processed_messages", filter.handlerMessages.GetProcessedMessagesAmountByClientId(int(filter.id)))
	}

	filter.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)

	slog.Info("Message processed by filter", "filter_id", filter.id, "client_id", result.ClientID, "processed_messages", filter.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID))

	if filter.callback(result.Payload) {
		filter.handlerMessages.AddFilteredMessagesAmountByClientId(result.ClientID, 1)
		filter.outputExchange.Send(msg)
	}
}

func (filter *Filter[T]) HandleSignals() {

}

func (filter *Filter[T]) Close() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
