package averagefilter

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/q3filter"
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
		if err == nil {
			return
		}
		if outputQueue != nil {
			if err := outputQueue.Close(); err != nil {
				slog.Error("While closing output queue", "err", err)
			}
		}
		if inputAvgsMiddleware != nil {
			if err := inputAvgsMiddleware.Close(); err != nil {
				slog.Error("While closing input avgs middleware", "err", err)
			}
		}
		if inputTransfersMiddleware != nil {
			if err := inputTransfersMiddleware.Close(); err != nil {
				slog.Error("While closing input transfers middleware", "err", err)
			}
		}
	}()

	transfersQueue := config.InputTransfersMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	transfersShardKey := fmt.Sprintf("shard-%d", config.Id)
	inputTransfersMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputTransfersMiddlewarePrefix, transfersQueue, transfersShardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input transfers middleware: %w", err)
	}

	avgsQueue := config.InputAvgsMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	avgsShardKey := fmt.Sprintf("shard-%d", config.Id)
	inputAvgsMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputAvgsMiddlewarePrefix, avgsQueue, avgsShardKey)
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
			bufferFiles:      map[string]*os.File{},
			outputTracker:    outputtracker.New(),
			transfersTracker: sendertracker.New(10_000_000),
			avgsTracker:      sendertracker.New(10_000_000),
		}
	})
	for clientID, state := range recovered {
		state.bufferFiles = map[string]*os.File{}
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	bufferDir := filepath.Join(config.PersistPath, "buffers")
	if err := os.MkdirAll(bufferDir, 0755); err != nil {
		return nil, fmt.Errorf("creating buffer dir: %w", err)
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
		bufferDir:                bufferDir,
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
	if err := af.inputAvgsMiddleware.StartConsumingBatch(af.persistBatchSize, af.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		af.handleAvgBatch(msgs, ack, nack)
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
	for clientID := range af.states.All() {
		af.deleteRemainingFiles(clientID)
	}
	af.lock.Unlock()

	if err := af.inputTransfersMiddleware.Close(); err != nil {
		slog.Error("While closing input transfers middleware", "filter_id", af.id, "err", err)
	}
	if err := af.inputAvgsMiddleware.Close(); err != nil {
		slog.Error("While closing input avgs middleware", "filter_id", af.id, "err", err)
	}
	if err := af.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "filter_id", af.id, "err", err)
	}
}

func (af *AverageFilter) handleTransferBatch(msgs []newmiddleware.Message, ack, nack func()) {
	af.lock.Lock()
	defer af.lock.Unlock()

	modified := make(map[int]*clientState)

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
			for _, record := range input.Records {
				af.handleTransferRecordLocked(input.ClientID, record, state)
			}
			af.flushPendingLocked(input.ClientID, state)
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[input.ClientID] = state

		if af.isComplete(state) {
			if err := af.finalizeAndEmitEOFLocked(input.ClientID, state); err != nil {
				slog.Error("While finalizing average filter client", "err", err)
				nack()
				af.stopConsuming()
				return
			}
		}
	}

	if !af.persistModified(modified, nack) {
		return
	}
	ack()
}

func (af *AverageFilter) handleTransferRecordLocked(clientID int, t transfer.TransferForQ3Filter, state *clientState) {
	if _, ok := state.avgs[t.PaymentFormat]; ok {
		af.processTransferLocked(clientID, t, state)
		return
	}

	if state.avgsReady {
		return
	}

	af.saveTransferToFile(clientID, t, state)
}

func (af *AverageFilter) handleAvgBatch(msgs []newmiddleware.Message, ack, nack func()) {
	af.lock.Lock()
	defer af.lock.Unlock()

	modified := make(map[int]*clientState)

	for _, msg := range msgs {
		input, err := avgmethod.Read(msg.Body)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := af.states.For(clientID)
		tracker := state.avgsTracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
			if int(input.Total) > state.expectedAvgRecords {
				state.expectedAvgRecords = int(input.Total)
			}
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			for i := range input.Records {
				rec := input.Records[i]
				state.avgs[rec.Method] = rec.Avg
				af.drainFileForMethod(input.ClientID, rec.Method, state)
			}
			af.flushPendingLocked(input.ClientID, state)
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		af.checkAvgsReady(state)
		modified[input.ClientID] = state

		if af.isComplete(state) {
			if err := af.finalizeAndEmitEOFLocked(input.ClientID, state); err != nil {
				slog.Error("While finalizing average filter client", "err", err)
				nack()
				af.stopConsuming()
				return
			}
		}
	}

	if !af.persistModified(modified, nack) {
		return
	}
	ack()
}

func (af *AverageFilter) persistModified(modified map[int]*clientState, nack func()) bool {
	for clientID, state := range modified {
		if _, ok := af.states.All()[clientID]; !ok {
			continue
		}
		if err := af.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			af.stopConsuming()
			return false
		}
	}
	return true
}

func (af *AverageFilter) flushPendingLocked(clientID int, state *clientState) {
	if len(state.pending) == 0 {
		return
	}

	builder := batch.NewBuilder(af.maxBatchSize, af.maxBatchBytes, records.Query3ResultCodec)
	for i := range state.pending {
		if !builder.TryAdd(&state.pending[i]) {
			seq := state.outputTracker.RegisterBatch("")
			body := builder.Flush(clientID, af.queryID, uint8(af.id), seq)
			if err := af.outputQueue.Send(newmiddleware.Message{Body: body}); err != nil {
				slog.Error("While sending Q3 results batch", "err", err)
			}
			builder.TryAdd(&state.pending[i])
		}
	}
	if !builder.IsEmpty() {
		seq := state.outputTracker.RegisterBatch("")
		body := builder.Flush(clientID, af.queryID, uint8(af.id), seq)
		if err := af.outputQueue.Send(newmiddleware.Message{Body: body}); err != nil {
			slog.Error("While sending Q3 results batch", "err", err)
		}
	}
	state.pending = state.pending[:0]
}

func (af *AverageFilter) checkAvgsReady(state *clientState) {
	if state.avgsReady || !state.avgsTracker.IsComplete(af.avgsExpectedEofs) || len(state.avgs) < state.expectedAvgRecords {
		return
	}
	state.avgsReady = true
}

func (af *AverageFilter) isComplete(state *clientState) bool {
	return state.avgsReady && state.transfersTracker.IsComplete(af.expectedTransfersEofs)
}

func (af *AverageFilter) finalizeAndEmitEOFLocked(clientID int, state *clientState) error {
	af.finalizeClientLocked(clientID, state)
	total := state.outputTracker.CountFor("")
	seq := state.outputTracker.RegisterBatch("")
	eofBody := batch.WriteEOF(clientID, af.queryID, uint8(af.id), seq, uint32(total))
	if err := af.outputQueue.Send(newmiddleware.Message{Body: eofBody}); err != nil {
		return fmt.Errorf("sending EOF message: %w", err)
	}
	af.states.Delete(clientID)
	af.checkpoint.DeleteClient(clientID)
	return nil
}

func (af *AverageFilter) processTransferLocked(clientID int, t transfer.TransferForQ3Filter, state *clientState) {
	avg, ok := state.avgs[t.PaymentFormat]
	if !ok {
		return
	}
	if t.AmountPaid >= avg/avgFractionThreshold {
		return
	}
	state.pending = append(state.pending, queryresult.Query3Result{
		FromBank:      t.FromBank,
		FromAccount:   t.FromBankAccount,
		PaymentFormat: t.PaymentFormat,
		Amount:        t.AmountPaid,
	})
	if len(state.pending) >= af.maxBatchSize {
		af.flushPendingLocked(clientID, state)
	}
}

func (af *AverageFilter) finalizeClientLocked(clientID int, state *clientState) {
	for method := range state.avgs {
		af.drainFileForMethod(clientID, method, state)
	}
	af.deleteRemainingFiles(clientID)
	af.flushPendingLocked(clientID, state)
}

func (af *AverageFilter) bufferFileName(clientID int, method string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, method)
	return filepath.Join(af.bufferDir, fmt.Sprintf("avg_filter_%d_client_%d_%s.csv", af.id, clientID, safe))
}

func (af *AverageFilter) saveTransferToFile(clientID int, t transfer.TransferForQ3Filter, state *clientState) {
	file, ok := state.bufferFiles[t.PaymentFormat]
	if !ok {
		filename := af.bufferFileName(clientID, t.PaymentFormat)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			slog.Error("While opening buffer file", "filter_id", af.id, "err", err)
			return
		}
		state.bufferFiles[t.PaymentFormat] = f
		file = f
	}

	line := fmt.Sprintf("%s,%s,%s,%s\n",
		t.PaymentFormat, t.FromBank, t.FromBankAccount,
		strconv.FormatFloat(t.AmountPaid, 'f', -1, 64),
	)
	if _, err := file.WriteString(line); err != nil {
		slog.Error("While writing transfer to file", "err", err)
	}
}

func (af *AverageFilter) drainFileForMethod(clientID int, method string, state *clientState) {
	if wf, ok := state.bufferFiles[method]; ok {
		if err := wf.Close(); err != nil {
			slog.Error("While closing buffer file before drain", "filter_id", af.id, "err", err)
		}
		delete(state.bufferFiles, method)
	}

	filename := af.bufferFileName(clientID, method)
	file, err := os.Open(filename)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		slog.Error("While opening buffer file for drain", "filter_id", af.id, "err", err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("While closing drained file", "filter_id", af.id, "err", err)
		}
		if err := os.Remove(filename); err != nil {
			slog.Error("While removing buffer file", "err", err)
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cols := strings.SplitN(scanner.Text(), ",", 4)
		if len(cols) != 4 {
			slog.Error("While parsing transfer from file: unexpected column count", "filter_id", af.id)
			continue
		}
		amount, err := strconv.ParseFloat(cols[3], 64)
		if err != nil {
			slog.Error("While parsing amount from file", "filter_id", af.id, "err", err)
			continue
		}
		t := transfer.TransferForQ3Filter{
			PaymentFormat:   cols[0],
			FromBank:        cols[1],
			FromBankAccount: cols[2],
			AmountPaid:      amount,
		}
		af.processTransferLocked(clientID, t, state)
	}
}

func (af *AverageFilter) deleteRemainingFiles(clientID int) {
	if state, ok := af.states.All()[clientID]; ok {
		for method, f := range state.bufferFiles {
			if err := f.Close(); err != nil {
				slog.Error("While closing buffer file", "filter_id", af.id, "method", method, "err", err)
			}
			delete(state.bufferFiles, method)
		}
	}

	pattern := filepath.Join(af.bufferDir, fmt.Sprintf("avg_filter_%d_client_%d_*.csv", af.id, clientID))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		slog.Error("While globbing buffer files", "filter_id", af.id, "err", err)
		return
	}
	for _, f := range matches {
		if err := os.Remove(f); err != nil {
			slog.Error("While removing buffer file", "filter_id", af.id, "file", f, "err", err)
		}
	}
}
