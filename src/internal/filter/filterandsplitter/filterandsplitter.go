package filterandsplitter

import (
	"encoding/binary"
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

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.FilterAndSpliterAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, seq uint64, total uint32, isCoordinator bool) error {
			if isCoordinator {
				handlerMessages.RemoveClient(clientID)

				return outputMiddleware.Send(newmiddleware.Message{
					Body:       batch.WriteEOF(clientID, config.QueryID, uint8(config.Id), seq, total),
					RoutingKey: newmiddleware.BroadcastRoutingKey,
				})
			}
			handlerMessages.RemoveClient(clientID)

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
		outputPersistPath:  filepath.Join(filepath.Dir(config.MonitorPersistPath), "output.bin"),
		inputMiddleware:    inputMiddleware,
		outputMiddleware:   outputMiddleware,
		eofInput:           eofInput,
		eofOutput:          eofOutput,
		eofHandler:         eofHandler,
		seqStore:           seqStore,
	}, nil
}

func (f *FilterAndSplitter) Run() {
	defer f.close()
	go f.eofHandler.Run()

	if err := f.inputMiddleware.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		f.newHandleInput(msg, ack, nack)
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
	if err := f.seqStore.Close(); err != nil {
		slog.Error("While closing seqstore client", "filter_id", f.id, "err", err)
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

	output := make(map[string][]byte, len(byShard))
	for routingKey, group := range byShard {
		output[routingKey] = splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), seq, group)
	}
	return output
}

func (f *FilterAndSplitter) sendEOF(clientID int, total uint32, seq uint64) error {
	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   f.handlerMessages.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(f.id),
		FilteredAmount: f.handlerMessages.GetForwardedMessagesAmountByClientId(clientID),
		Seq:            seq,
	}
	return f.eofOutput.Send(middleware.Message{Body: eofring.SerializeRingMessage(eofRingMessage)})
}

func (f *FilterAndSplitter) newHandleInput(msg middleware.Message, ack func(), nack func()) {
	input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		ack()
		return
	}

	byShard, err := f.processBatch(input)
	if err != nil {
		ack()
	}

	if err = f.writeTemp(); err != nil {
		nack()
	}

	if err = f.writeOutput(byShard); err != nil {
		nack()
	}

	accept, err := f.tryCommit(input.Seq)
	if err != nil {
		nack()
	}

	// A partir de acá damos el ack y trabajamos con lo que tenemos en disco ante caidas
	ack()

	if !accept {
		//Reject. El seqNum ya fue reclamado y procesado por otro nodo

		// Debo deshacer los cambios de mi estado interno volviendo a lo que tengo en backup.
		// Lo que tengo en .temp puede/debe descartarse
		return
	}

	if input.EOF {
		if err = f.sendEOF(input.ClientID, input.Total, input.Seq); err != nil {
			//Entro en recuperación
		}
		return
	}

	if err = f.sendOutputs(byShard); err != nil {
		//Entro en recuperación
	}

	// rename del .temp para escritura atómica
	// Borro .output

	//En recuperacion tengo que repetir el tryCommit
	//En reucperación miro primero si hay un .output. Si lo hay me lo traigo. Si hay un .temp tambien me lo traigo. Si no lo hay parto del backup directo (xq el .temp ya lo habría pisado)
}

func (f *FilterAndSplitter) processBatch(input batch.Msg[transfer.TransferAfterCurrency]) (map[string][]byte, error) {
	if input.EOF {
		return nil, nil
	}

	return f.handleBatch(input.ClientID, input.Seq, input.Records), nil
}

func (f *FilterAndSplitter) writeTemp() error {
	return nil
}

func (f *FilterAndSplitter) writeOutput(byShard map[string][]byte) error {
	return diskstore.WriteAtomic(f.outputPersistPath, byShard)
}

const seqStoreTimeout = 5 * time.Second

func (f *FilterAndSplitter) tryCommit(seq uint64) (bool, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)

	resp, err := f.seqStore.Call(buf, seqStoreTimeout)
	if err != nil {
		return false, fmt.Errorf("tryCommit rpc call: %w", err)
	}
	if len(resp) < 1 {
		return false, fmt.Errorf("tryCommit: empty response")
	}
	return resp[0] != 0, nil
}

func (f *FilterAndSplitter) sendOutputs(byShard map[string][]byte) error {
	for routingKey, body := range byShard {
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: routingKey}); err != nil {
			return fmt.Errorf("sending shard %s: %w", routingKey, err)
		}
	}
	return nil
}
