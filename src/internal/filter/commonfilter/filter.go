package commonfilter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/worker"
)

const _EOF_RING_QUEUE_PREFIX = "FILTER_%s_"

func NewFilter[T any, O any](
	config filter.FilterConfig,
	filterFunction func(T) bool,
	inputToOutput func(T) O,
	inputCodec wire.Codec[T],
	outputCodec wire.Codec[O],
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(
		config.InputExchange,
		config.InputQueue,
		config.InputRoutingKeys,
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(
		config.OutputExchange,
		config.OutputQueue,
		config.OutputRoutingKeys,
		connSettings,
	)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input exchange after output exchange creation failure", "err", err)
		}
		return nil, err
	}

	eofInputQueueName, eofOutputQueueName := eofring.GetInputAndOutputQueueNames(
		config.Id,
		config.FilterAmount,
		fmt.Sprintf(_EOF_RING_QUEUE_PREFIX, config.Type),
		fmt.Sprintf(_EOF_RING_QUEUE_PREFIX, config.Type),
	)

	eofInput, err := middleware.CreateQueueMiddleware(
		eofInputQueueName,
		connSettings,
	)

	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input exchange after EOF input queue creation failure", "err", err)
		}
		if err := outputExchange.Close(); err != nil {
			slog.Error("While closing output exchange after EOF input queue creation failure", "err", err)
		}
		return nil, err
	}

	eofOutput, err := middleware.CreateQueueMiddleware(
		eofOutputQueueName,
		connSettings,
	)

	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input exchange after EOF input queue creation failure", "err", err)
		}
		if err := outputExchange.Close(); err != nil {
			slog.Error("While closing output exchange after EOF input queue creation failure", "err", err)
		}
		if err := eofInput.Close(); err != nil {
			slog.Error("While closing EOF input queue after EOF output queue creation failure", "err", err)
		}
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	return &Filter[T, O]{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		filterFunction: filterFunction,
		eofHandler: eofring.CreateEofRingAlgorithm(
			eofInput,
			eofOutput,
			config.FilterAmount,
			uint32(config.Id),
			handlerMessages,
			func(clientID int, total uint32, isCoordinator bool) error {
				if isCoordinator {
					return outputExchange.Send(middleware.Message{Body: batch.WriteEOF(clientID, config.QueryId, 0, 0, total)})
				}
				return nil
			},
			config.QueryId,
		),
		handlerMessages: handlerMessages,
		outputQueueEof:  eofOutput,
		filterType:      config.Type,
		outputTransform: inputToOutput,
		queryId:         config.QueryId,
		inputCodec:      inputCodec,
		outputCodec:     outputCodec,
	}, nil
}

func (filter *Filter[T, O]) Run() {
	defer func() {
		if err := filter.close(); err != nil {
			slog.Error("While closing filter", "err", err)
		}
	}()

	go filter.eofHandler.Run()
	if err := filter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		filter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (filter *Filter[T, O]) handleMessage(msg middleware.Message, ack, _ func()) {
	ack()

	input, err := batch.Read(msg.Body, filter.inputCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		filter.handleEOF(input.ClientID, input.Total)
		return
	}

	outputs := make([]O, 0, len(input.Records))
	filter.handlerMessages.AddProcessedMessagesAmountByClientId(input.ClientID, uint32(len(input.Records)))
	for i := range input.Records {
		if filter.filterFunction(input.Records[i]) {
			filter.handlerMessages.AddForwardedMessagesAmountByClientId(input.ClientID, 1)
			outputs = append(outputs, filter.outputTransform(input.Records[i]))
		}
	}

	if len(outputs) == 0 {
		return
	}

	body := batch.Write(input.ClientID, filter.queryId, uint8(filter.id), input.Seq, outputs, filter.outputCodec)
	if err := filter.outputExchange.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("While sending batch to output exchange", "err", err)
	}
}

func (filter *Filter[T, O]) handleEOF(clientID int, total uint32) {
	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   filter.handlerMessages.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(filter.id),
		FilteredAmount: filter.handlerMessages.GetForwardedMessagesAmountByClientId(clientID),
	}

	if err := filter.outputQueueEof.Send(middleware.Message{Body: eofring.SerializeRingMessage(eofRingMessage)}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
	}
}

func (filter *Filter[T, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := filter.inputExchange.StopConsuming(); err != nil {
		slog.Error("while stop consuming from input exchange", "err", err)
	}
}

func (filter *Filter[T, O]) close() error {

	if err := filter.inputExchange.Close(); err != nil {
		slog.Error("while closing input exchange", "err", err)
		return err
	}

	if err := filter.eofHandler.Close(); err != nil {
		slog.Error("while closing EOF handler", "err", err)
		return err
	}

	// no estoy seguro si aca deberia closear siendo que no es ni mi exchange ni mi queue
	if err := filter.outputExchange.Close(); err != nil {
		slog.Error("while closing output exchange", "err", err)
		return err
	}
	if err := filter.outputQueueEof.Close(); err != nil {
		slog.Error("while closing EOF output queue", "err", err)
		return err
	}
	return nil
}
