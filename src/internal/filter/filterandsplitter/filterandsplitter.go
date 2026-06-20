package filterandsplitter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	_EOF_RING_QUEUE_PREFIX = "FILTER_AND_SPLIITER_EOF"
	_EOF_OUTPUT_KEY        = "__eof"
)

func NewFilterAndSplitter(config FilterAndSplitterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
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
		}
	}()

	inputMiddleware, err = newmiddleware.NewShardedMiddleware(
		connSettings, config.InputMiddlewareName, config.InputMiddlewareQueue, config.InputShardKey,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	recovered, err := ckpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			transferTracker: sendertracker.New(10_000_000),
			outputTracker:   outputtracker.New(),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("DEBUG recovered client state",
			"clientID", clientID,
			"processed", state.transferTracker.GetMsgCount(),
			"expected", state.transferTracker.TotalInput(),
		)
	}

	node := &FilterAndSplitter{
		id:               config.Id,
		startDate:        config.StartDate,
		endDate:          config.EndDate,
		outputAmount:     config.OutputMiddlewareAmount,
		prevNodeAmt:      config.FilterCurrencyAmt,
		hasher:           shard.New(config.OutputMiddlewareAmount),
		queryID:          config.QueryID,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		checkpoint:       ckpt,
		states:           states,
	}

	return node, nil
}

func (f *FilterAndSplitter) Run() {
	defer f.close()

	if err := f.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		f.handleInput(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (f *FilterAndSplitter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.stopConsuming()
}

func (f *FilterAndSplitter) close() {
	if err := f.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "err", err)
	}

	if err := f.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "err", err)
	}
}

func (f *FilterAndSplitter) handleInput(msg newmiddleware.Message, ack func(), nack func()) {
	input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("decode failed", "err", err)
		ack()
		return
	}

	clientID := input.ClientID
	state := f.states.For(clientID)
	tracker := state.transferTracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		tracker.RegisterBatch(int(input.SenderID))
		if err := f.processBatch(input, state); err != nil {
			slog.Error("process batch failed", "err", err)
			nack()
			f.stopConsuming()
			return
		}
	}

	tracker.Claim(int(input.SenderID), input.Seq)

	if tracker.IsComplete(f.prevNodeAmt) {
		if err := f.finishTransfersStep(clientID, state); err != nil {
			slog.Error("finishing transfers step failed", "err", err)
			nack()
			f.stopConsuming()
			return
		}
		if err := f.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			f.stopConsuming()
			return
		}
		ack()

		f.states.Delete(clientID)
		f.checkpoint.DeleteClient(clientID)
		return
	}

	if err := f.checkpoint.SaveClient(clientID, state); err != nil {
		slog.Error("persist failed, stopping", "err", err)
		nack()
		f.stopConsuming()
		return
	}

	ack()
}

func (f *FilterAndSplitter) stopConsuming() {
	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "err", err)
	}
}

func (f *FilterAndSplitter) processBatch(input batch.Msg[transfer.TransferAfterCurrency], state *clientState) error {
	clientID := input.ClientID
	byShard := make(map[string][]transfer.SplittedTransfer)

	for _, record := range input.Records {
		if record.Timestamp.Before(f.startDate) || record.Timestamp.After(f.endDate) {
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
		}
	}

	for routingKey, group := range byShard {
		body := splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), input.Seq, group)
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: routingKey}); err != nil {
			return err
		}
		state.outputTracker.RegisterBatch(routingKey)
	}
	return nil
}

func (f *FilterAndSplitter) finishTransfersStep(clientID int, state *clientState) error {
	eofSeq := state.transferTracker.GetEOFSeq()
	var sendErr error
	for i := range f.outputAmount {
		if sendErr != nil {
			break
		}
		rk := fmt.Sprintf("shard-%d", i)
		total := state.outputTracker.CountFor(rk)
		eofBody := splittransfer.WriteEOF(clientID, f.queryID, uint8(f.id), eofSeq, uint32(total))
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			slog.Error("While sending EOF", "routingKey", rk, "err", err)
			sendErr = err
		}
	}
	return sendErr
}
