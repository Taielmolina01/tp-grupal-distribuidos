package convertedamountfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	DATE_LAYOUT            = "2006-01-02"
	DATE_LAYOUT_WITH_HOUR  = "2006-01-02 15:04"
	FILE_LAYOUT            = "%s_%s.csv"
	_EOF_RING_QUEUE_PREFIX = "FILTER_CONVERTED_AMOUNT"
)

func CreateConvertedAmountFilter(config filter.FilterConfig) (worker.Worker, error) {
	return newConvertedAmountFilter(
		config,
	)
}

func newConvertedAmountFilter(
	config filter.FilterConfig,
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
	outputQueue, err := middleware.CreateQueueMiddleware(
		config.OutputQueue,
		connSettings,
	)

	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("while closing input queue", "err", err)
		}
		return nil, err
	}

	messagesMonitor := msgmonitor.NewMessageMonitor()

	eofInputQueueName, eofOutputQueueName := eofring.GetInputAndOutputQueueNames(
		config.Id,
		config.FilterAmount,
		_EOF_RING_QUEUE_PREFIX,
		_EOF_RING_QUEUE_PREFIX,
	)

	eofInputQueue, err := middleware.CreateQueueMiddleware(
		eofInputQueueName,
		connSettings,
	)

	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("while closing input queue after EOF input queue creation failure", "err", err)
		}
		if err := outputQueue.Close(); err != nil {
			slog.Error("while closing output queue after EOF input queue creation failure", "err", err)
		}
		return nil, err
	}

	eofOutputQueue, err := middleware.CreateQueueMiddleware(
		eofOutputQueueName,
		connSettings,
	)

	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("while closing input queue after EOF output queue creation failure", "err", err)
		}
		if err := outputQueue.Close(); err != nil {
			slog.Error("while closing output queue after EOF output queue creation failure", "err", err)
		}
		if err := eofInputQueue.Close(); err != nil {
			slog.Error("while closing EOF input queue after EOF output queue creation failure", "err", err)
		}
		return nil, err
	}

	filter := &ConvertedAmountFilter{
		InputQueue:      inputQueue,
		OutputQueue:     outputQueue,
		QueryID:         config.QueryID,
		HandlerMessages: messagesMonitor,
		Id:              uint32(config.Id),
		Quote:           config.Quote,
		AmountThreshold: config.Amount,
		Pending:         make(map[int][]transfer.FinalTransferForQ5),
		EofOutputQueue:  eofOutputQueue,
	}

	filter.EofRing = eofring.CreateEofRingAlgorithm(
		eofInputQueue,
		eofOutputQueue,
		config.FilterAmount,
		uint32(config.Id),
		messagesMonitor,
		func(clientID int, _ uint64, total uint32, isCoordinator bool) error {
			if isCoordinator {
				if err := outputQueue.Send(middleware.Message{Body: batch.WriteEOF(clientID, config.QueryID, 0, 0, total)}); err != nil {
					slog.Error("while sending EOF from coordinator", "client_id", clientID, "err", err)
					return err
				}

			}
			return nil
		},
		config.QueryID,
	)

	return filter, nil
}

func (filter *ConvertedAmountFilter) Run() {
	defer filter.close()
	go filter.EofRing.Run()

	if err := filter.InputQueue.StartConsuming(filter.consume); err != nil {
		slog.Error("while starting consuming from left input queue", "err", err)
		return
	}
}

func (filter *ConvertedAmountFilter) consume(msg middleware.Message, ack, _ func()) {
	defer ack()

	input, err := batch.Read(msg.Body, records.FetcherResponseCodec)
	if err != nil {
		slog.Error("while deserializing batch", "err", err)
		return
	}
	if input.EOF {
		filter.handleEOF(input.ClientID, input.Total)
		return
	}

	filter.HandlerMessages.AddProcessedMessagesAmountByClientId(
		input.ClientID,
		uint32(len(input.Records)),
	)
	for _, t := range input.Records {
		if t.ConvertedAmount < filter.AmountThreshold {
			filter.emit(input.ClientID, transfer.ProjectForQ5Final())
			filter.HandlerMessages.AddForwardedMessagesAmountByClientId(
				input.ClientID,
				1,
			)
		}
	}

	filter.flush(input.ClientID)
}

// Helpers

func (filter *ConvertedAmountFilter) handleEOF(clientID int, total uint32) {

	filter.flush(clientID)

	actual := filter.HandlerMessages.GetProcessedMessagesAmountByClientId(clientID)
	filtered := filter.HandlerMessages.GetForwardedMessagesAmountByClientId(clientID)

	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   actual,
		ClientId:       clientID,
		CoordinatorId:  uint32(filter.Id),
		FilteredAmount: filtered,
	}

	if err := filter.EofOutputQueue.Send(middleware.Message{Body: eofring.SerializeRingMessage(eofRingMessage)}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "filter_id", filter.Id, "client_id", clientID, "err", err)
	}
}

func (filter *ConvertedAmountFilter) emit(clientID int, response transfer.FinalTransferForQ5) {
	filter.Pending[clientID] = append(filter.Pending[clientID], response)
}

func (filter *ConvertedAmountFilter) flush(clientID int) {
	results := filter.Pending[clientID]
	delete(filter.Pending, clientID)

	if len(results) == 0 {
		return
	}

	body := batch.Write(clientID, filter.QueryID, 0, 0, results, records.FinalTransferForQ5Codec)
	if err := filter.OutputQueue.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("while sending results batch", "err", err)
	}
}

// Closure

func (filter *ConvertedAmountFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := filter.InputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from left input queue", "err", err)
	}
}

func (filter *ConvertedAmountFilter) close() {
	if err := filter.InputQueue.Close(); err != nil {
		slog.Error("while closing left input queue", "err", err)
	}
	if err := filter.OutputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
