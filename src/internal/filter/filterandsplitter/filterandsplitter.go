package filterandsplitter

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	"tp-grupal-distribuidos/internal/common/diskstore"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/seqstoreprotocol"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	_EOF_RING_QUEUE_PREFIX = "FILTER_AND_SPLIITER_EOF"
	_EOF_OUTPUT_KEY        = "__eof"
)

func NewFilterAndSplitter(config FilterAndSplitterConfig) (worker.Worker, error) {
	oldConnSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	newConnSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	handlerMessages := msgmonitor.NewMessageMonitor()

	var (
		inputMiddleware  middleware.Middleware
		outputMiddleware newmiddleware.Middleware
		eofInput         middleware.Middleware
		eofOutput        middleware.Middleware
		seqStore         newmiddleware.RPCClient
		err              error
	)

	defer func() {
		if err != nil {
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
			if seqStore != nil {
				if err := seqStore.Close(); err != nil {
					slog.Error("While closing seqstore client", "err", err)
				}
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

	seqStore, err = newmiddleware.NewRPCClientMiddleware(newConnSettings, config.SeqStoreQueue)
	if err != nil {
		return nil, fmt.Errorf("creating seqstore client: %w", err)
	}

	if err = os.MkdirAll(filepath.Dir(config.MonitorPersistPath), 0755); err != nil {
		return nil, fmt.Errorf("creating persist directory: %w", err)
	}

	node := &FilterAndSplitter{
		id:                 config.Id,
		startDate:          config.StartDate,
		endDate:            config.EndDate,
		hasher:             shard.New(config.OutputMiddlewareAmount),
		queryID:            config.QueryID,
		handlerMessages:    handlerMessages,
		monitorPersistPath: config.MonitorPersistPath,
		tempMonitorPath:    config.MonitorPersistPath + ".tmp",
		outputPersistPath:  filepath.Join(filepath.Dir(config.MonitorPersistPath), "output.bin"),
		inputMiddleware:    inputMiddleware,
		outputMiddleware:   outputMiddleware,
		eofInput:           eofInput,
		eofOutput:          eofOutput,
		seqStore:           seqStore,
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.FilterAndSpliterAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, seq uint64, total uint32, isCoordinator bool) error {
			handlerMessages.RemoveClient(clientID)

			if isCoordinator {
				if err := node.removeFromSeqStore(clientID); err != nil {
					return fmt.Errorf("removing client %d from seqstore: %w", clientID, err)
				}

				if err := outputMiddleware.Send(newmiddleware.Message{
					Body:       batch.WriteEOF(clientID, config.QueryID, uint8(config.Id), seq, total),
					RoutingKey: newmiddleware.BroadcastRoutingKey,
				}); err != nil {
					return err
				}
			}

			return node.commitState()
		},
		uint8(config.QueryID),
	)

	node.eofHandler = eofHandler

	return node, nil
}

func (f *FilterAndSplitter) Run() {
	defer f.close()

	if err := f.startupRecovery(); err != nil {
		slog.Error("Startup recovery failed, aborting", "err", err)
		return
	}

	go f.eofHandler.Run()

	if err := f.inputMiddleware.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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

	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input middleware consumer", "err", err)
	}
	if err := f.eofInput.StopConsuming(); err != nil {
		slog.Error("While stopping EOF input consumer", "err", err)
	}
}

func (f *FilterAndSplitter) close() {
	if err := f.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "err", err)
	}
	if err := f.eofInput.Close(); err != nil {
		slog.Error("While closing EOF input", "err", err)
	}
	if err := f.eofOutput.Close(); err != nil {
		slog.Error("While closing EOF output", "err", err)
	}
	if err := f.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "err", err)
	}
	if err := f.seqStore.Close(); err != nil {
		slog.Error("While closing seqstore client", "err", err)
	}
}

func (f *FilterAndSplitter) handleBatch(clientID int, seq uint64, records []transfer.TransferAfterCurrency) map[string][]byte {
	byShard := make(map[string][]transfer.SplittedTransfer)

	for i := range records {
		record := records[i]
		f.handlerMessages.AddProcessedMessagesAmountByClientId(clientID, 1)

		if record.Timestamp.Before(f.startDate) || record.Timestamp.After(f.endDate) {
			continue
		}
		// if record.FromBankAccount == record.ToBankAccount && record.FromBank == record.ToBank {
		// 	continue
		// }

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

	output := make(map[string][]byte, len(byShard))
	for routingKey, group := range byShard {
		output[routingKey] = splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), seq, group)
	}
	return output
}

func (f *FilterAndSplitter) handleInput(msg middleware.Message, ack func(), nack func()) {
	input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		ack()
		return
	}

	byShard, err := f.processBatch(input)
	if err != nil {
		slog.Error("While processing batch", "err", err)
		f.recoverState()
		ack()
		return
	}

	if err = f.writeTemp(); err != nil {
		slog.Error("While writing temp", "err", err)
		f.recoverState()
		nack()
		return
	}

	if err = f.writeOutput(byShard); err != nil {
		slog.Error("While writing ouput", "err", err)
		nack()
		return
	}

	accept, err := f.tryCommit(input.ClientID, input.Seq)
	if err != nil {
		slog.Error("While trying to commit", "err", err)
		nack()
		return
	}

	// A partir de acá damos el ack y trabajamos con lo que tenemos en disco ante caidas
	ack()

	if !accept {
		slog.Warn("Seq rejected, rolling back", "seq", input.Seq)
		f.recoverState()
		return
	}

	if err = f.sendOutputs(byShard); err != nil {
		slog.Error("sendOutputs failed, killing process", "err", err)
		f.kill()
		return
	}

	if err = f.commitState(); err != nil {
		slog.Error("commitState failed, killing process", "err", err)
		f.kill()
	}
}

func (f *FilterAndSplitter) recoverState() {
	if err := diskstore.RemoveIfExists(f.outputPersistPath); err != nil {
		slog.Error("recoverState: cannot clean output, killing node", "err", err)
		f.kill()
	}

	if err := f.handlerMessages.LoadFromDisk(f.monitorPersistPath); err != nil {
		slog.Error("recoverState: cannot load backup, killing node", "err", err)
		f.kill()
		return
	}

	slog.Info("STATUS RECOVERED", "len", f.handlerMessages.Len())
}

func (f *FilterAndSplitter) kill() {
	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("kill: stopping input middleware", "err", err)
	}
	if err := f.eofInput.StopConsuming(); err != nil {
		slog.Error("kill: stopping EOF input", "err", err)
	}
}

func (f *FilterAndSplitter) processBatch(input batch.Msg[transfer.TransferAfterCurrency]) (map[string][]byte, error) {
	if input.EOF {
		msg := eofmessagetypes.EofRingMessage{
			RealAmount:     input.Total,
			ActualAmount:   f.handlerMessages.GetProcessedMessagesAmountByClientId(input.ClientID),
			ClientId:       input.ClientID,
			CoordinatorId:  uint32(f.id),
			FilteredAmount: f.handlerMessages.GetForwardedMessagesAmountByClientId(input.ClientID),
			Seq:            input.Seq,
		}
		return map[string][]byte{_EOF_OUTPUT_KEY: eofring.SerializeRingMessage(msg)}, nil
	}

	return f.handleBatch(input.ClientID, input.Seq, input.Records), nil
}

func (f *FilterAndSplitter) writeTemp() error {
	return f.handlerMessages.SaveToDisk(f.tempMonitorPath)
}

func (f *FilterAndSplitter) commitState() error {
	if err := os.Rename(f.tempMonitorPath, f.monitorPersistPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("commit: rename monitor: %w", err)
	}
	return diskstore.RemoveIfExists(f.outputPersistPath)
}

func (f *FilterAndSplitter) startupRecovery() error {
	slog.Info("Start recovery")
	byShard, err := diskstore.Read(f.outputPersistPath)
	if errors.Is(err, os.ErrNotExist) {
		slog.Info(".output not found")
		return f.handlerMessages.LoadFromDisk(f.monitorPersistPath)
	}
	if err != nil {
		slog.Info(".output file err", "err", err)
		return fmt.Errorf("startup recovery: read output: %w", err)
	}

	slog.Info("Pending output found, re-sending", "filter_id", f.id)
	if err := f.sendOutputs(byShard); err != nil {
		return fmt.Errorf("startup recovery: send outputs: %w", err)
	}

	if err := f.loadState(); err != nil {
		return fmt.Errorf("startup recovery: load state: %w", err)
	}

	return f.commitState()
}

func (f *FilterAndSplitter) loadState() error {
	if _, err := os.Stat(f.tempMonitorPath); err == nil {
		if err := f.handlerMessages.LoadFromDisk(f.tempMonitorPath); err == nil {
			return nil
		}
		slog.Warn("Temp monitor state unreadable, falling back to backup", "filter_id", f.id)
	}
	return f.handlerMessages.LoadFromDisk(f.monitorPersistPath)
}

func (f *FilterAndSplitter) writeOutput(byShard map[string][]byte) error {
	return diskstore.WriteAtomic(f.outputPersistPath, byShard)
}

const seqStoreTimeout = 5 * time.Second

func (f *FilterAndSplitter) tryCommit(clientID int, seq uint64) (bool, error) {
	resp, err := f.seqStore.Call(seqstoreprotocol.EncodeClaimRequest(clientID, seq), seqStoreTimeout)
	if err != nil {
		return false, fmt.Errorf("tryCommit rpc call: %w", err)
	}
	if len(resp) < 1 {
		return false, fmt.Errorf("tryCommit: empty response")
	}
	return resp[0] != 0, nil
}

func (f *FilterAndSplitter) removeFromSeqStore(clientID int) error {
	_, err := f.seqStore.Call(seqstoreprotocol.EncodeRemoveRequest(clientID), seqStoreTimeout)
	return err
}

func (f *FilterAndSplitter) sendOutputs(output map[string][]byte) error {
	if body, ok := output[_EOF_OUTPUT_KEY]; ok {
		slog.Info("SEND OUTPUT")
		return f.eofOutput.Send(middleware.Message{Body: body})
	}

	for routingKey, body := range output {
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: routingKey}); err != nil {
			return fmt.Errorf("sending shard %s: %w", routingKey, err)
		}
	}
	return nil
}
