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
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const eofRingPrefix = "AVG_FILTER_T_"

const avgFractionThreshold = 100

type AverageFilterConfig struct {
	Id           int
	FilterAmount int

	MomHost string
	MomPort int

	InputTransfersQueue string
	InputAvgsQueue      string
	OutputQueue         string

	AvgsExpectedEofs int

	QueryID uint8
}

type AverageFilter struct {
	id      uint32
	queryID uint8

	inputTransfersQueue middleware.Middleware
	inputAvgsQueue      middleware.Middleware
	outputQueue         middleware.Middleware

	transfersEofOut  middleware.Middleware
	transfersEofIn   middleware.Middleware
	transfersRing    eofring.EofRingAlgorithm
	transfersMonitor msgmonitor.MessageMonitor

	avgsExpectedEofs int

	lock  sync.Mutex
	state map[int]*clientState
}

type clientState struct {
	avgs                map[string]float64
	avgsReady           bool
	ringeof             bool
	avgsEofsReceived    int
	transfersEofPending bool
	transfersEofRealAmt uint32
}

func getRingNextIndex(config AverageFilterConfig) int {
	if config.Id == config.FilterAmount-1 {
		return 0
	}
	return config.Id + 1
}

func NewAverageFilter(config AverageFilterConfig) (_ *AverageFilter, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputTransfersQueue middleware.Middleware
		inputAvgsQueue      middleware.Middleware
		outputQueue         middleware.Middleware
		transfersEofIn      middleware.Middleware
		transfersEofOut     middleware.Middleware
	)

	defer func() {
		if err == nil {
			return
		}
		if transfersEofOut != nil {
			transfersEofOut.Close()
		}
		if transfersEofIn != nil {
			transfersEofIn.Close()
		}
		if outputQueue != nil {
			outputQueue.Close()
		}
		if inputAvgsQueue != nil {
			inputAvgsQueue.Close()
		}
		if inputTransfersQueue != nil {
			inputTransfersQueue.Close()
		}
	}()

	inputTransfersQueue, err = middleware.CreateQueueMiddleware(config.InputTransfersQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input transfers queue: %w", err)
	}

	inputAvgsQueue, err = middleware.CreateQueueMiddleware(config.InputAvgsQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input avgs queue: %w", err)
	}

	outputQueue, err = middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	transfersEofIn, err = middleware.CreateQueueMiddleware(
		eofRingPrefix+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating transfers EOF input queue: %w", err)
	}

	transfersEofOut, err = middleware.CreateQueueMiddleware(
		eofRingPrefix+strconv.Itoa(getRingNextIndex(config)),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating transfers EOF output queue: %w", err)
	}

	expectedAvgEofs := config.AvgsExpectedEofs
	if expectedAvgEofs <= 0 {
		expectedAvgEofs = 1
	}

	transfersMonitor := msgmonitor.NewMessageMonitor()

	af := &AverageFilter{
		id:                  uint32(config.Id),
		queryID:             config.QueryID,
		inputTransfersQueue: inputTransfersQueue,
		inputAvgsQueue:      inputAvgsQueue,
		outputQueue:         outputQueue,
		transfersEofIn:      transfersEofIn,
		transfersEofOut:     transfersEofOut,
		transfersMonitor:    transfersMonitor,
		avgsExpectedEofs:    expectedAvgEofs,
		state:               map[int]*clientState{},
	}

	af.transfersRing = eofring.CreateEofRingAlgorithm(
		transfersEofIn,
		transfersEofOut,
		config.FilterAmount,
		uint32(config.Id),
		transfersMonitor,
		af.onTransfersRingConverged,
		config.QueryID,
	)

	return af, nil
}

func (af *AverageFilter) Run() {
	defer af.close()

	go af.transfersRing.Run()
	go af.consumeAvgs()
	if err := af.inputTransfersQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		af.handleTransferInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming transfers", "err", err)
	}
}

func (af *AverageFilter) consumeAvgs() {
	if err := af.inputAvgsQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
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

	af.inputAvgsQueue.StopConsuming()
	af.inputTransfersQueue.StopConsuming()
	af.transfersEofIn.StopConsuming()
}

func (af *AverageFilter) close() {
	af.lock.Lock()
	for clientID := range af.state {
		af.deleteRemainingFiles(clientID)
	}
	af.lock.Unlock()

	af.inputTransfersQueue.Close()
	af.inputAvgsQueue.Close()
	af.outputQueue.Close()
	af.transfersRing.Close()
	af.transfersEofIn.Close()
	af.transfersEofOut.Close()
}

func (af *AverageFilter) getOrInitState(clientID int) *clientState {
	s, ok := af.state[clientID]
	if !ok {
		s = &clientState{avgs: map[string]float64{}}
		af.state[clientID] = s
	}
	return s
}

func (af *AverageFilter) handleTransferInput(msg middleware.Message, ack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.TransferForQ3Filter](&msg)
	if err != nil {
		slog.Error("While deserializing transfer message", "err", err)
		return
	}

	if result.IsEOF() {
		af.handleTransferEOF(*result)
		return
	}

	af.handleTransferRecord(result.ClientID, result.Payload)
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

func (af *AverageFilter) handleTransferEOF(data inner.DataMsg[transfer.TransferForQ3Filter]) {
	af.lock.Lock()
	defer af.lock.Unlock()

	state := af.getOrInitState(data.ClientID)
	if !state.avgsReady {
		state.transfersEofPending = true
		state.transfersEofRealAmt = data.EOF.TotalMessages
		return
	}
	af.fireTransfersRingLocked(data.ClientID, data.EOF.TotalMessages)
}

func (af *AverageFilter) handleAvgInput(msg middleware.Message, ack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.AvgByMethod](&msg)
	if err != nil {
		slog.Error("While deserializing avg message", "err", err)
		return
	}

	af.lock.Lock()
	defer af.lock.Unlock()
	state := af.getOrInitState(result.ClientID)

	if result.IsEOF() {
		af.handleAvgEOFLocked(result.ClientID, state)
		return
	}

	method := result.Payload.Method
	state.avgs[method] = result.Payload.Avg
	af.drainFileForMethod(result.ClientID, method, state)
}

func (af *AverageFilter) handleAvgEOFLocked(clientID int, state *clientState) {
	state.avgsEofsReceived++
	if state.avgsEofsReceived < af.avgsExpectedEofs {
		return
	}
	state.avgsReady = true
	af.deleteRemainingFiles(clientID)

	if state.transfersEofPending {
		af.fireTransfersRingLocked(clientID, state.transfersEofRealAmt)
		state.transfersEofPending = false
	}

	if !state.ringeof {
		return
	}

	af.finalizeAndEmitEOFLocked(clientID, state)
}

func (af *AverageFilter) onTransfersRingConverged(clientID int, msg *middleware.Message, isCoordinator bool) error {
	af.lock.Lock()
	defer af.lock.Unlock()

	state, ok := af.state[clientID]
	if !ok {
		return nil
	}

	state.ringeof = true
	if !state.avgsReady {
		return nil
	}

	af.finalizeClientLocked(clientID, state)
	if err := af.outputQueue.Send(*msg); err != nil {
		return err
	}
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
	out := queryresult.Query3Result{
		FromBank:      t.FromBank,
		FromAccount:   t.FromBankAccount,
		PaymentFormat: t.PaymentFormat,
		Amount:        t.AmountPaid,
	}
	serialized, err := inner.SerializeData(inner.DataMsg[queryresult.Query3Result]{
		Payload:  out,
		ClientID: clientID,
		QueryID:  af.queryID,
	})
	if err != nil {
		slog.Error("While serializing Query3Result", "err", err)
		return
	}
	if err := af.outputQueue.Send(*serialized); err != nil {
		slog.Error("While sending Query3Result to output queue", "err", err)
		return
	}
	af.transfersMonitor.AddForwardedMessagesAmountByClientId(clientID, 1)
}

func (af *AverageFilter) fireTransfersRingLocked(clientID int, realAmount uint32) {
	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     realAmount,
		ActualAmount:   af.transfersMonitor.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(af.id),
		FilteredAmount: af.transfersMonitor.GetForwardedMessagesAmountByClientId(clientID),
	}
	serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
	if err != nil {
		slog.Error("While serializing EOF ring message", "err", err)
		return
	}
	if err := af.transfersEofOut.Send(*serialized); err != nil {
		slog.Error("While sending EOF to transfers ring", "err", err)
		return
	}
}

func (af *AverageFilter) finalizeClientLocked(clientID int, state *clientState) {
	for method := range state.avgs {
		af.drainFileForMethod(clientID, method, state)
	}
	af.deleteRemainingFiles(clientID)
	delete(af.state, clientID)
}

func (af *AverageFilter) finalizeAndEmitEOFLocked(clientID int, state *clientState) {
	af.finalizeClientLocked(clientID, state)

	serialized, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: clientID, QueryID: af.queryID})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return
	}
	if err := af.outputQueue.Send(*serialized); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}
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
	filename := af.bufferFileName(clientID, t.PaymentFormat)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("While opening buffer file", "filter_id", af.id, "err", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	line := fmt.Sprintf("%s,%s,%s,%s\n",
		t.PaymentFormat, t.FromBank, t.FromBankAccount,
		strconv.FormatFloat(t.AmountPaid, 'f', -1, 64),
	)
	if _, err := writer.WriteString(line); err != nil {
		slog.Error("While writing transfer to file", "err", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("While flushing buffer file", "err", err)
	}
}

func (af *AverageFilter) drainFileForMethod(clientID int, method string, state *clientState) {
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
		file.Close()
		if err := os.Remove(filename); err != nil {
			slog.Error("While removing buffer file", "err", err)
		}
	}()

	drained := 0
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
		drained++
	}
}

func (af *AverageFilter) deleteRemainingFiles(clientID int) {
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
