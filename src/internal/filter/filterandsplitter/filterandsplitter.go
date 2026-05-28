package filterandsplitter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type FilterAndSplitterConfig struct {
	Id        int
	StartDate time.Time
	EndDate   time.Time

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	FilterAndSpliterAmount int

	MomHost string
	MomPort int

	InputMiddlewareName  string
	InputMiddlewareQueue string
	InputRoutingKeys     []string

	QueryID int
}

type FilterAndSplitter struct {
	id        int
	startDate time.Time
	endDate   time.Time

	hasher shard.Hasher

	inputMiddleware  middleware.Middleware
	outputMiddleware newmiddleware.Middleware
	eofInput         middleware.Middleware
	eofOutput        middleware.Middleware
	eofHandler       eofring.EofRingAlgorithm

	handlerMessages msgmonitor.MessageMonitor

	queryID int
}

func getRingNextIndex(config FilterAndSplitterConfig) int {
	if config.Id == config.FilterAndSpliterAmount-1 {
		return 0
	}
	return config.Id + 1
}

func NewFilterAndSplitter(config FilterAndSplitterConfig) (_ *FilterAndSplitter, err error) {
	const ring_prefix = "FILTER_AND_SPLIITER_EOF_"

	oldConnSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	newConnSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	handlerMessages := msgmonitor.NewMessageMonitor()

	var (
		inputMiddleware  middleware.Middleware
		outputMiddleware newmiddleware.Middleware
		eofInput         middleware.Middleware
		eofOutput        middleware.Middleware
	)

	defer func() {
		if err != nil {
			if eofOutput != nil {
				eofOutput.Close()
			}
			if eofInput != nil {
				eofInput.Close()
			}
			if outputMiddleware != nil {
				outputMiddleware.Close()
			}
			if inputMiddleware != nil {
				inputMiddleware.Close()
			}
		}
	}()

	inputMiddleware, err = middleware.CreateExchangeMiddleware(
		config.InputMiddlewareName,
		config.InputMiddlewareQueue,
		config.InputRoutingKeys,
		oldConnSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(newConnSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	eofInput, err = middleware.CreateQueueMiddleware(ring_prefix+strconv.Itoa(config.Id), oldConnSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF input queue: %w", err)
	}

	eofOutput, err = middleware.CreateQueueMiddleware(ring_prefix+strconv.Itoa(getRingNextIndex(config)), oldConnSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF output queue: %w", err)
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.FilterAndSpliterAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			handlerMessages.RemoveClient(clientID)
			if isCoordinator {
				return outputMiddleware.Send(newmiddleware.Message{
					Body:       msg.Body,
					RoutingKey: newmiddleware.BroadcastRoutingKey,
				})
			}
			return nil
		},
		uint8(config.QueryID),
	)

	return &FilterAndSplitter{
		id:               config.Id,
		startDate:        config.StartDate,
		endDate:          config.EndDate,
		hasher:           shard.New(config.OutputMiddlewareAmount),
		queryID:          config.QueryID,
		handlerMessages:  handlerMessages,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		eofInput:         eofInput,
		eofOutput:        eofOutput,
		eofHandler:       eofHandler,
	}, nil
}

func (f *FilterAndSplitter) Run() {
	defer f.close()
	go f.eofHandler.Run()

	if err := f.inputMiddleware.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (f *FilterAndSplitter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.inputMiddleware.StopConsuming()
	f.eofInput.StopConsuming()
}

func (f *FilterAndSplitter) close() {
	f.inputMiddleware.Close()
	f.eofInput.Close()
	f.eofOutput.Close()
	f.outputMiddleware.Close()
}

func (f *FilterAndSplitter) handleInput(msg middleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[transfer.TransferAfterCurrency](&msg)

	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		f.handleEOF(*m)
		return
	}

	f.handleRecord(m.ClientID, m.Payload)
}

func (f *FilterAndSplitter) handleRecord(clientID int, record transfer.TransferAfterCurrency) {
	f.handlerMessages.AddProcessedMessagesAmountByClientId(clientID, 1)

	if record.Timestamp.Before(f.startDate) || record.Timestamp.After(f.endDate) {
		return
	}

	if record.FromBankAccount == record.ToBankAccount && record.FromBank == record.ToBank {
		return
	}

	projected := transfer.ProjectForQ4(record)
	output := []transfer.SplittedTransfer{
		{Transfer: projected, IsLeftPart: true},
		{Transfer: projected, IsLeftPart: false},
	}

	for _, o := range output {
		var bank, acc string
		if o.IsLeftPart {
			bank = o.Transfer.FromBank
			acc = o.Transfer.FromBankAccount
		} else {
			bank = o.Transfer.ToBank
			acc = o.Transfer.ToBankAccount
		}

		msg, err := inner.SerializeData(inner.DataMsg[transfer.SplittedTransfer]{
			ClientID: clientID,
			QueryID:  uint8(f.queryID),
			Payload:  o,
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
			return
		}

		routingKey := fmt.Sprintf("shard-%d", f.hasher.ShardFor(clientID, bank, acc))

		f.handlerMessages.AddForwardedMessagesAmountByClientId(clientID, 1)
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: routingKey}); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (f *FilterAndSplitter) handleEOF(data inner.DataMsg[transfer.TransferAfterCurrency]) {
	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     data.EOF.TotalMessages,
		ActualAmount:   f.handlerMessages.GetProcessedMessagesAmountByClientId(data.ClientID),
		ClientId:       data.ClientID,
		CoordinatorId:  uint32(f.id),
		FilteredAmount: f.handlerMessages.GetForwardedMessagesAmountByClientId(data.ClientID),
	}
	serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(eofRingMessage)
	if err != nil {
		slog.Error("While serializing EOF ring message", "err", err)
		return
	}
	if err := f.eofOutput.Send(*serializedEofRingMessage); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
	}
}
