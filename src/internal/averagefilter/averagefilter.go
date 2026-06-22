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

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/q3filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const avgFractionThreshold = 100

func NewAverageFilter(config AverageFilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	oldConnSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputTransfersMiddleware newmiddleware.Middleware
		inputAvgsMiddleware      newmiddleware.Middleware
		outputQueue              middleware.Middleware
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

	outputQueue, err = middleware.CreateQueueMiddleware(config.OutputQueue, oldConnSettings)
	if err != nil {
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	return &AverageFilter{
		id:                       uint32(config.Id),
		queryID:                  config.QueryID,
		inputTransfersMiddleware: inputTransfersMiddleware,
		inputAvgsMiddleware:      inputAvgsMiddleware,
		outputQueue:              outputQueue,
		transfersMonitor:         msgmonitor.NewMessageMonitor(),
		expectedTransfersEofs:    config.ExpectedTransfersEofs,
		avgsExpectedEofs:         config.ExpectedAvgEofs,
		state:                    map[int]*clientState{},
	}, nil
}

func (af *AverageFilter) Run() {
	defer af.close()

	go af.consumeAvgs()
	if err := af.inputTransfersMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		af.handleTransferInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming transfers", "err", err)
	}
}

func (af *AverageFilter) consumeAvgs() {
	if err := af.inputAvgsMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		af.handleAvgInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming avgs", "err", err)
	}
}

func (af *AverageFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	if err := af.inputAvgsMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input avgs consumer", "filter_id", af.id, "err", err)
	}
	if err := af.inputTransfersMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input transfers consumer", "filter_id", af.id, "err", err)
	}
}

func (af *AverageFilter) close() {
	af.lock.Lock()
	for clientID := range af.state {
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

func (af *AverageFilter) getOrInitState(clientID int) *clientState {
	s, ok := af.state[clientID]
	if !ok {
		s = &clientState{avgs: map[string]float64{}, bufferFiles: map[string]*os.File{}}
		af.state[clientID] = s
	}
	return s
}

func (af *AverageFilter) handleTransferInput(msg newmiddleware.Message, ack func()) {
	defer ack()

	input, err := q3filter.Read(msg.Body)
	if err != nil {
		slog.Error("While deserializing transfer batch", "err", err)
		return
	}

	if input.EOF {
		af.handleTransferEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		af.handleTransferRecord(input.ClientID, input.Records[i])
	}

	af.lock.Lock()
	if state, ok := af.state[input.ClientID]; ok {
		af.flushPendingLocked(input.ClientID, state)
	}
	af.lock.Unlock()
}

func (af *AverageFilter) handleTransferRecord(clientID int, t transfer.TransferForQ3Filter) {
	af.transfersMonitor.AddProcessedMessagesAmountByClientId(clientID, 1)

	af.lock.Lock()
	defer af.lock.Unlock()
	state := af.getOrInitState(clientID)

	if _, ok := state.avgs[t.PaymentFormat]; ok {
		af.processTransferLocked(clientID, t, state)
		return
	}

	if state.avgsReady {
		return
	}

	af.saveTransferToFile(clientID, t)
}

func (af *AverageFilter) handleTransferEOF(clientID int, total uint32) {
	af.lock.Lock()
	defer af.lock.Unlock()

	state := af.getOrInitState(clientID)
	state.transfersEofsReceived++

	if state.transfersEofsReceived < af.expectedTransfersEofs {
		return
	}

	if !state.avgsReady {
		state.transfersEofPending = true
		return
	}

	af.finalizeAndEmitEOFLocked(clientID, state)
}

func (af *AverageFilter) handleAvgInput(msg newmiddleware.Message, ack func()) {
	defer ack()

	input, err := avgmethod.Read(msg.Body)
	if err != nil {
		slog.Error("While deserializing avg batch", "err", err)
		return
	}

	af.lock.Lock()
	defer af.lock.Unlock()
	state := af.getOrInitState(input.ClientID)

	if input.EOF {
		af.handleAvgEOFLocked(input.ClientID, state, input.Total)
		return
	}

	for i := range input.Records {
		rec := input.Records[i]
		state.avgs[rec.Method] = rec.Avg
		af.drainFileForMethod(input.ClientID, rec.Method, state)
	}
	af.flushPendingLocked(input.ClientID, state)
	af.checkAvgsReady(input.ClientID, state)
}

const pendingFlushThreshold = 1000

func (af *AverageFilter) flushPendingLocked(clientID int, state *clientState) {
	if len(state.pending) == 0 {
		return
	}
	body := batch.Write(clientID, af.queryID, 0, 0, state.pending, records.Query3ResultCodec)
	if err := af.outputQueue.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("While sending Q3 results batch", "err", err)
	}
	state.pending = state.pending[:0]
}

func (af *AverageFilter) handleAvgEOFLocked(clientID int, state *clientState, totalAvgs uint32) {
	state.avgsEofsReceived++
	if int(totalAvgs) > state.expectedAvgRecords {
		state.expectedAvgRecords = int(totalAvgs)
	}
	af.checkAvgsReady(clientID, state)
}

func (af *AverageFilter) checkAvgsReady(clientID int, state *clientState) {
	if state.avgsReady || state.avgsEofsReceived < af.avgsExpectedEofs || len(state.avgs) < state.expectedAvgRecords {
		return
	}
	state.avgsReady = true
	af.deleteRemainingFiles(clientID)

	if state.transfersEofPending {
		state.transfersEofPending = false
		af.finalizeAndEmitEOFLocked(clientID, state)
	}
}

func (af *AverageFilter) finalizeAndEmitEOFLocked(clientID int, state *clientState) {
	af.finalizeClientLocked(clientID, state)
	forwarded := uint32(af.transfersMonitor.GetForwardedMessagesAmountByClientId(clientID))
	eofBody := batch.WriteEOF(clientID, af.queryID, 0, 0, forwarded)
	if err := af.outputQueue.Send(middleware.Message{Body: eofBody}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}
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
	af.transfersMonitor.AddForwardedMessagesAmountByClientId(clientID, 1)
	if len(state.pending) >= pendingFlushThreshold {
		af.flushPendingLocked(clientID, state)
	}
}

func (af *AverageFilter) finalizeClientLocked(clientID int, state *clientState) {
	for method := range state.avgs {
		af.drainFileForMethod(clientID, method, state)
	}
	af.deleteRemainingFiles(clientID)
	af.flushPendingLocked(clientID, state)
	delete(af.state, clientID)
}

func (af *AverageFilter) bufferFileName(clientID int, method string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, method)
	return fmt.Sprintf("avg_filter_%d_client_%d_%s.csv", af.id, clientID, safe)
}

func (af *AverageFilter) saveTransferToFile(clientID int, t transfer.TransferForQ3Filter) {
	state := af.state[clientID]
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
	if state, ok := af.state[clientID]; ok {
		for method, f := range state.bufferFiles {
			if err := f.Close(); err != nil {
				slog.Error("While closing buffer file", "filter_id", af.id, "method", method, "err", err)
			}
			delete(state.bufferFiles, method)
		}
	}

	pattern := fmt.Sprintf("avg_filter_%d_client_%d_*.csv", af.id, clientID)
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
