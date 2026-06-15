package filterandsplitter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
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
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	_EOF_RING_QUEUE_PREFIX = "FILTER_AND_SPLIITER_EOF"
)

func NewFilterAndSplitter(config FilterAndSplitterConfig) (worker.Worker, error) {
	oldConnSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	newConnSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	handlerMessages := msgmonitor.NewMessageMonitor()
	// if err := handlerMessages.LoadFromDisk(config.MonitorPersistPath); err != nil {
	// 	return nil, fmt.Errorf("loading monitor state from disk: %w", err)
	// }

	var (
		inputMiddleware  middleware.Middleware
		outputMiddleware newmiddleware.Middleware
		eofInput         middleware.Middleware
		eofOutput        middleware.Middleware
		err              error
	)

	defer func() {
		if err == nil {
			return
		}
		if eofOutput != nil {
			if err := eofOutput.Close(); err != nil {
				slog.Error("While closing EOF output", "err", err)
			}
		}
		if eofInput != nil {
			if err := eofInput.Close(); err != nil {
				slog.Error("While closing EOF input", "err", err)
			}
		}
		if outputMiddleware != nil {
			if err := outputMiddleware.Close(); err != nil {
				slog.Error("While closing output middleware", "err", err)
			}
		}
		if inputMiddleware != nil {
			if err := inputMiddleware.Close(); err != nil {
				slog.Error("While closing input middleware", "err", err)
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

	eofInputQueueName, eofOutputQueueName := eofring.GetInputAndOutputQueueNames(
		config.Id,
		config.FilterAndSpliterAmount,
		_EOF_RING_QUEUE_PREFIX,
		_EOF_RING_QUEUE_PREFIX,
	)

	eofInput, err = middleware.CreateQueueMiddleware(eofInputQueueName, oldConnSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF input queue: %w", err)
	}

	eofOutput, err = middleware.CreateQueueMiddleware(eofOutputQueueName, oldConnSettings)
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
				// seq := handlerMessages.NextSeqByClientId(clientID)
				handlerMessages.RemoveClient(clientID)
				// if err := handlerMessages.SaveToDisk(config.MonitorPersistPath); err != nil {
				// 	slog.Error("While persisting monitor state after EOF", "err", err)
				// }
				return outputMiddleware.Send(newmiddleware.Message{
					Body:       batch.WriteEOF(clientID, config.QueryID, uint8(config.Id), 0, total),
					RoutingKey: newmiddleware.BroadcastRoutingKey,
				})
			}
			handlerMessages.RemoveClient(clientID)
			// if err := handlerMessages.SaveToDisk(config.MonitorPersistPath); err != nil {
			// 	slog.Error("While persisting monitor state after EOF", "err", err)
			// }
			return nil
		},
		uint8(config.QueryID),
	)

	return &FilterAndSplitter{
		id:                 config.Id,
		startDate:          config.StartDate,
		endDate:            config.EndDate,
		hasher:             shard.New(config.OutputMiddlewareAmount),
		queryID:            config.QueryID,
		handlerMessages:    handlerMessages,
		monitorPersistPath: config.MonitorPersistPath,
		inputMiddleware:    inputMiddleware,
		outputMiddleware:   outputMiddleware,
		eofInput:           eofInput,
		eofOutput:          eofOutput,
		eofHandler:         eofHandler,
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

	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input middleware consumer", "filter_id", f.id, "err", err)
	}
	if err := f.eofInput.StopConsuming(); err != nil {
		slog.Error("While stopping EOF input consumer", "filter_id", f.id, "err", err)
	}
}

func (f *FilterAndSplitter) close() {
	if err := f.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "filter_id", f.id, "err", err)
	}
	if err := f.eofInput.Close(); err != nil {
		slog.Error("While closing EOF input", "filter_id", f.id, "err", err)
	}
	if err := f.eofOutput.Close(); err != nil {
		slog.Error("While closing EOF output", "filter_id", f.id, "err", err)
	}
	if err := f.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "filter_id", f.id, "err", err)
	}
}

func (f *FilterAndSplitter) handleInput(msg middleware.Message, ack func()) {
	defer ack()
	input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		f.handleEOF(input.ClientID, input.Total)
		return
	}

	// if f.handlerMessages.IsDuplicate(input.ClientID, int(input.SenderID), input.Seq) {
	// 	slog.Warn("Discarding duplicate batch", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
	// 	return
	// }

	f.handleBatch(input.ClientID, input.Records)
	// if err := f.handlerMessages.SaveToDisk(f.monitorPersistPath); err != nil {
	// 	slog.Error("While persisting monitor state", "err", err)
	// }
}

func (f *FilterAndSplitter) handleBatch(clientID int, records []transfer.TransferAfterCurrency) {
	// seq := f.handlerMessages.NextSeqByClientId(clientID)
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
		body := splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), 0, group) // body := splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), seq, group)
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: routingKey}); err != nil {
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
	if err := f.eofOutput.Send(middleware.Message{Body: eofring.SerializeRingMessage(eofRingMessage)}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
	}
}
