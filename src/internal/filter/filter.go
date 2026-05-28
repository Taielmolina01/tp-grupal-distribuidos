package filter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/worker"
)

const eofRingQueueNamePrefix = "FILTER_%s_"

func newFilter[T comparable, O comparable](
	config FilterConfig,
	filterFunction func(T) bool,
	inputToOutput func(T) O,
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
		fmt.Sprintf(eofRingQueueNamePrefix, config.Type),
		fmt.Sprintf(eofRingQueueNamePrefix, config.Type),
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
			func(clientID int, msg *middleware.Message, isCoordinator bool) error {
				if isCoordinator {
					return outputExchange.Send(*msg)
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
	}, nil
}

func (filter *Filter[T, O]) Run() {
	defer filter.close()

	go filter.eofHandler.Run()
	if err := filter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		filter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (filter *Filter[T, O]) handleMessage(msg middleware.Message, ack, _ func()) {
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
			CoordinatorId:  uint32(filter.id),
			FilteredAmount: filter.handlerMessages.GetForwardedMessagesAmountByClientId(result.ClientID),
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
	} else {
		filter.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)
		if filter.filterFunction(result.Payload) {
			filter.handlerMessages.AddForwardedMessagesAmountByClientId(result.ClientID, 1)
			payload := filter.outputTransform(result.Payload)
			msgOutput, err := inner.SerializeData(
				inner.DataMsg[O]{
					Payload:  payload,
					ClientID: result.ClientID,
					QueryID:  uint8(filter.queryId)},
			)
			if err != nil {
				slog.Error("While serializing output message", "err", err)
				return
			}
			if err := filter.outputExchange.Send(*msgOutput); err != nil {
				slog.Error("While sending message to output exchange", "err", err)
			}
		}
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
