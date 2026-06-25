package averagefilter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/appendlog"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/q3filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/msgsend"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const avgFractionThreshold = 100

func NewAverageFilter(config AverageFilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputTransfersMiddleware newmiddleware.Middleware
		inputAvgsMiddleware      newmiddleware.Middleware
		outputQueue              newmiddleware.Middleware
		err                      error
	)

	defer func() {
		if err != nil {
			cleanup.Close(outputQueue, inputAvgsMiddleware, inputTransfersMiddleware)
		}
	}()

	transfersQueue := config.InputTransfersMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	transfersShardKey := fmt.Sprintf("shard-%d", config.Id)
	inputTransfersMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputTransfersMiddlewarePrefix, transfersQueue, transfersShardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input transfers middleware: %w", err)
	}

	avgsQueue := config.InputAvgsMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	inputAvgsMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.InputAvgsMiddlewarePrefix, avgsQueue)
	if err != nil {
		return nil, fmt.Errorf("creating input avgs middleware: %w", err)
	}

	outputQueue, err = newmiddleware.NewQueueMiddleware(connSettings, config.OutputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating output queue: %w", err)
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
			avgs:             map[string]float64{},
			transfersTracker: sendertracker.New(10_000_000),
			avgsTracker:      sendertracker.New(10_000_000),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &AverageFilter{
		id:                       uint32(config.Id),
		queryID:                  config.QueryID,
		inputTransfersMiddleware: inputTransfersMiddleware,
		inputAvgsMiddleware:      inputAvgsMiddleware,
		outputQueue:              outputQueue,
		expectedTransfersEofs:    config.ExpectedTransfersEofs,
		avgsExpectedEofs:         config.ExpectedAvgEofs,
		maxBatchSize:             config.MaxBatchSize,
		maxBatchBytes:            config.MaxBatchBytes,
		states:                   states,
		checkpoint:               ckpt,
		persistBatchSize:         config.PersistBatchSize,
		persistFlushInterval:     config.PersistFlushInterval,
		transferLogDir:           filepath.Join(config.PersistPath, "transfers"),
		transferLogs:             map[int]*appendlog.Log[transfer.TransferForQ3Filter]{},
	}, nil
}

func (af *AverageFilter) Run() {
	defer af.close()

	go af.consumeAvgs()
	if err := af.inputTransfersMiddleware.StartConsumingBatch(af.persistBatchSize, af.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		af.handleTransferBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming transfers", "err", err)
	}
}

func (af *AverageFilter) consumeAvgs() {
	if err := af.inputAvgsMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		af.handleAvgBatch(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming avgs", "err", err)
	}
}

func (af *AverageFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	af.stopConsuming()
}

func (af *AverageFilter) stopConsuming() {
	if err := af.inputAvgsMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input avgs consumer", "filter_id", af.id, "err", err)
	}
	if err := af.inputTransfersMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input transfers consumer", "filter_id", af.id, "err", err)
	}
}

func (af *AverageFilter) close() {
	af.lock.Lock()
	for clientID, log := range af.transferLogs {
		if err := log.Close(); err != nil {
			slog.Error("While closing transfer append log", "clientID", clientID, "err", err)
		}
		delete(af.transferLogs, clientID)
	}
	af.lock.Unlock()

	cleanup.Close(af.inputTransfersMiddleware, af.inputAvgsMiddleware, af.outputQueue)
}

func (af *AverageFilter) handleTransferBatch(msgs []newmiddleware.Message, ack, nack func()) {
	af.lock.Lock()
	defer af.lock.Unlock()

	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})
	pendingEntries := make(map[int][]appendlog.Entry[transfer.TransferForQ3Filter])
	aborted := make(map[int]bool)

	for _, msg := range msgs {
		input, err := q3filter.Read(msg.Body)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := af.states.For(clientID)
		tracker := state.transfersTracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			pendingEntries[clientID] = append(pendingEntries[clientID], appendlog.Entry[transfer.TransferForQ3Filter]{
				SenderID: input.SenderID,
				Seq:      input.Seq,
				Records:  input.Records,
			})
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[input.ClientID] = state

		if input.Abort || aborted[clientID] {
			aborted[clientID] = true
			modified[clientID] = state
			continue
		}

		if state.transfersTracker.IsComplete(af.expectedTransfersEofs) &&
			state.avgsTracker.IsComplete(af.avgsExpectedEofs) {
			if entries := pendingEntries[clientID]; len(entries) > 0 {
				if err := af.appendEntries(clientID, entries); err != nil {
					slog.Error("append entries failed", "clientID", clientID, "err", err)
					nack()
					af.stopConsuming()
					return
				}
				delete(pendingEntries, clientID)
			}
		}

		done, err := af.tryFinalize(clientID, state)
		if err != nil {
			slog.Error("finalize failed", "clientID", clientID, "err", err)
			nack()
			af.stopConsuming()
			return
		}
		if done {
			completed[clientID] = struct{}{}
		}
	}

	// Un solo write+sync por cliente no completado
	for clientID, entries := range pendingEntries {
		if err := af.appendEntries(clientID, entries); err != nil {
			slog.Error("append entries failed", "clientID", clientID, "err", err)
			nack()
			af.stopConsuming()
			return
		}
	}

	for clientID, state := range modified {
		if aborted[clientID] {
			af.states.Delete(clientID)
			af.checkpoint.DeleteClient(clientID)
			continue
		}
		if _, done := completed[clientID]; done {
			af.states.Delete(clientID)
			af.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := af.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			af.stopConsuming()
			return
		}
	}

	ack()
}

func (af *AverageFilter) appendEntries(clientID int, entries []appendlog.Entry[transfer.TransferForQ3Filter]) error {
	log, err := af.transferLog(clientID)
	if err != nil {
		return err
	}
	return log.AppendAll(entries)
}

func (af *AverageFilter) transferLog(clientID int) (*appendlog.Log[transfer.TransferForQ3Filter], error) {
	log := af.transferLogs[clientID]
	if log != nil {
		return log, nil
	}
	log, err := appendlog.Open(af.transferLogPath(clientID), q3filter.Codec)
	if err != nil {
		return nil, err
	}
	af.transferLogs[clientID] = log
	return log, nil
}

func (af *AverageFilter) closeTransferLog(clientID int) error {
	log := af.transferLogs[clientID]
	if log == nil {
		return nil
	}
	delete(af.transferLogs, clientID)
	return log.Close()
}

func (af *AverageFilter) transferLogPath(clientID int) string {
	return filepath.Join(af.transferLogDir, fmt.Sprintf("%d.log", clientID))
}

func (af *AverageFilter) handleAvgBatch(msg newmiddleware.Message, ack, nack func()) {
	af.lock.Lock()
	defer af.lock.Unlock()

	input, err := avgmethod.Read(msg.Body)
	if err != nil {
		slog.Error("decode failed", "err", err)
		ack()
		return
	}

	clientID := input.ClientID
	state := af.states.For(clientID)
	tracker := state.avgsTracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		tracker.RegisterBatch(int(input.SenderID))
		af.processAvgBatch(input, state)
	}

	tracker.Claim(int(input.SenderID), input.Seq)

	done, err := af.tryFinalize(clientID, state)
	if err != nil {
		slog.Error("finalize failed", "clientID", clientID, "err", err)
		nack()
		af.stopConsuming()
		return
	}
	if done {
		af.states.Delete(clientID)
		af.checkpoint.DeleteClient(clientID)
		ack()
		return
	}
	if err := af.checkpoint.SaveClient(clientID, state); err != nil {
		slog.Error("persist failed, stopping", "err", err)
		nack()
		af.stopConsuming()
		return
	}

	ack()
}

func (af *AverageFilter) processAvgBatch(input batch.Msg[transfer.AvgByMethod], state *clientState) {
	for _, record := range input.Records {
		state.avgs[record.Method] = record.Avg
	}
}

func (af *AverageFilter) tryFinalize(clientID int, state *clientState) (bool, error) {
	if !state.transfersTracker.IsComplete(af.expectedTransfersEofs) ||
		!state.avgsTracker.IsComplete(af.avgsExpectedEofs) {
		return false, nil
	}
	if err := af.finalize(clientID, state); err != nil {
		return false, err
	}
	return true, nil
}

func (af *AverageFilter) finalize(clientID int, state *clientState) error {
	if err := af.closeTransferLog(clientID); err != nil {
		return fmt.Errorf("closing transfer log before replay: %w", err)
	}
	log, err := appendlog.Open(af.transferLogPath(clientID), q3filter.Codec)
	if err != nil {
		return err
	}
	defer func() {
		if err := log.Close(); err != nil {
			slog.Error("While closing transfer append log", "clientID", clientID, "err", err)
		}
	}()

	replayTracker := sendertracker.New(10_000_000)
	ot := outputtracker.New()
	builder := batch.NewBuilder(af.maxBatchSize, af.maxBatchBytes, records.Query3ResultCodec)
	if err := log.ReadUnique(replayTracker, func(entry appendlog.Entry[transfer.TransferForQ3Filter]) error {
		if err := af.emitTransferResults(clientID, state, builder, ot, entry.Records); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !builder.IsEmpty() {
		if err := af.flushResultBatch(clientID, builder, ot); err != nil {
			return err
		}
	}

	total := ot.CountFor("")
	if err := msgsend.SendEOF(af.outputQueue, "", clientID, af.queryID, uint8(af.id), total+1, uint32(total)); err != nil {
		return err
	}

	if err := os.Remove(af.transferLogPath(clientID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing transfer log: %w", err)
	}
	return nil
}

func (af *AverageFilter) emitTransferResults(
	clientID int,
	state *clientState,
	builder *batch.Builder[queryresult.Query3Result],
	ot *outputtracker.OutputTracker,
	transfers []transfer.TransferForQ3Filter,
) error {
	for _, transfer := range transfers {
		result, ok := af.resultForTransfer(state, transfer)
		if !ok {
			continue
		}
		if !builder.TryAdd(&result) {
			if err := af.flushResultBatch(clientID, builder, ot); err != nil {
				return err
			}
			builder.TryAdd(&result)
		}
	}
	return nil
}

func (af *AverageFilter) resultForTransfer(state *clientState, t transfer.TransferForQ3Filter) (queryresult.Query3Result, bool) {
	avg, ok := state.avgs[t.PaymentFormat]
	if !ok || t.AmountPaid >= avg/avgFractionThreshold {
		return queryresult.Query3Result{}, false
	}
	return queryresult.Query3Result{
		FromBank:      t.FromBank,
		FromAccount:   t.FromBankAccount,
		PaymentFormat: t.PaymentFormat,
		Amount:        t.AmountPaid,
	}, true
}

func (af *AverageFilter) flushResultBatch(
	clientID int,
	builder *batch.Builder[queryresult.Query3Result],
	ot *outputtracker.OutputTracker,
) error {
	seq := ot.RegisterBatch("")
	body := builder.Flush(clientID, af.queryID, uint8(af.id), seq)
	return af.outputQueue.Send(newmiddleware.Message{Body: body})
}
