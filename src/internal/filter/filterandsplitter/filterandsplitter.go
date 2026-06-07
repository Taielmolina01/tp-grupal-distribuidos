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
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
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

	QueryID uint8
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

	queryID uint8
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
		func(clientID int, total uint32, isCoordinator bool) error {
			if isCoordinator {
				seq := handlerMessages.NextSeqByClientId(clientID)
				handlerMessages.RemoveClient(clientID)
				return outputMiddleware.Send(newmiddleware.Message{
					Body:       string(batch.WriteEOF(clientID, config.QueryID, uint8(config.Id), seq, total)),
					RoutingKey: newmiddleware.BroadcastRoutingKey,
				})
			}
			handlerMessages.RemoveClient(clientID)
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
	input, err := batch.Read([]byte(msg.Body), records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		f.handleEOF(input.ClientID, input.Total)
		return
	}

	if f.handlerMessages.IsDuplicate(input.ClientID, int(input.SenderID), input.Seq) {
		slog.Warn("Discarding duplicate batch", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
		return
	}

	f.handleBatch(input.ClientID, input.Records)
}

func (f *FilterAndSplitter) handleBatch(clientID int, records []transfer.TransferAfterCurrency) {
	seq := f.handlerMessages.NextSeqByClientId(clientID)
	byShard := make(map[string][]transfer.SplittedTransfer)

	for i := range records {
		record := records[i]
		f.handlerMessages.AddProcessedMessagesAmountByClientId(clientID, 1)

		if record.Timestamp.Before(f.startDate) || record.Timestamp.After(f.endDate) {
			continue
		}
		if record.FromBankAccount == record.ToBankAccount && record.FromBank == record.ToBank {
			continue
		}

		projected := transfer.ProjectForQ4(record)
		for _, o := range []transfer.SplittedTransfer{
			{Transfer: projected, IsLeftPart: true},
			{Transfer: projected, IsLeftPart: false},
		} {
			var bank, acc string
			if o.IsLeftPart {
				bank, acc = o.Transfer.FromBank, o.Transfer.FromBankAccount
			} else {
				bank, acc = o.Transfer.ToBank, o.Transfer.ToBankAccount
			}
			routingKey := fmt.Sprintf("shard-%d", f.hasher.ShardFor(clientID, bank, acc))
			byShard[routingKey] = append(byShard[routingKey], o)
			f.handlerMessages.AddForwardedMessagesAmountByClientId(clientID, 1)
		}
	}

	for routingKey, group := range byShard {
		body := splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), seq, group)
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: string(body), RoutingKey: routingKey}); err != nil {
			slog.Error("While sending output batch", "err", err)
		}
	}
}

func (f *FilterAndSplitter) handleEOF(clientID int, total uint32) {
	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   f.handlerMessages.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(f.id),
		FilteredAmount: f.handlerMessages.GetForwardedMessagesAmountByClientId(clientID),
	}
	if err := f.eofOutput.Send(middleware.Message{Body: string(eofring.SerializeRingMessage(eofRingMessage))}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
	}
}
