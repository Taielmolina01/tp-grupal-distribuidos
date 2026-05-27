package filter

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newAverageFilter(
	config FilterConfig,
	compareFunc func(float32, float32) bool,
	queryID uint8,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputTransfersQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputAvgsQueue, err := middleware.CreateQueueMiddleware(config.AvgInputQueue, connSettings)
	if err != nil {
		inputTransfersQueue.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputTransfersQueue.Close()
		inputAvgsQueue.Close()
		return nil, err
	}

	next := config.Id + 1
	if config.Id == config.FilterAmount-1 {
		next = 0
	}

	transfersEofIn, err := middleware.CreateQueueMiddleware(
		"AVG_FILTER_T_"+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	transfersEofOut, err := middleware.CreateQueueMiddleware(
		"AVG_FILTER_T_"+strconv.Itoa(next),
		connSettings,
	)
	if err != nil {
		transfersEofIn.Close()
		return nil, err
	}

	transfersMonitor := msgmonitor.NewMessageMonitor()

	expectedAvgEofs := config.AvgExpectedEofs
	if expectedAvgEofs <= 0 {
		expectedAvgEofs = 1
	}

	af := &AverageFilter{
		id:                  uint32(config.Id),
		queryID:             queryID,
		inputTransfersQueue: inputTransfersQueue,
		inputAvgsQueue:      inputAvgsQueue,
		outputQueue:         outputQueue,
		transfersMonitor:    transfersMonitor,
		transfersEofOut:     transfersEofOut,
		avgsExpectedEofs:    expectedAvgEofs,
		compareFunc:         compareFunc,
		state:               map[int]*avgFilterClientState{},
	}

	af.transfersRing = eofring.CreateEofRingAlgorithm(
		transfersEofIn,
		transfersEofOut,
		config.FilterAmount,
		uint32(config.Id),
		transfersMonitor,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			af.lock.Lock()
			delete(af.state, clientID)
			af.lock.Unlock()
			if !isCoordinator {
				return nil
			}
			if err := af.outputQueue.Send(*msg); err != nil {
				return err
			}
			slog.Info("AverageFilter emitted EOF to results_queue", "filter_id", af.id, "client_id", clientID)
			return nil
		},
		queryID,
	)

	return af, nil
}

func (af *AverageFilter) Run() {
	slog.Info("Starting average-filter consumers", "filter_id", af.id)
	go af.transfersRing.Run()
	go af.consumeAvgs()
	if err := af.inputTransfersQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		af.handleTransferMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming transfers", "err", err)
	}
}

func (af *AverageFilter) consumeAvgs() {
	if err := af.inputAvgsQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		af.handleAvgMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming avgs", "err", err)
	}
}

func (af *AverageFilter) getOrInitState(clientID int) *avgFilterClientState {
	s, ok := af.state[clientID]
	if !ok {
		s = &avgFilterClientState{
			avgs: map[string]float32{},
		}
		af.state[clientID] = s
	}
	return s
}

func (af *AverageFilter) handleAvgMessage(msg middleware.Message, ack, nack func()) {
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
		state.avgsEofsReceived++
		slog.Info("AverageFilter got avgs EOF", "filter_id", af.id, "client_id", result.ClientID, "count", state.avgsEofsReceived, "expected", af.avgsExpectedEofs)
		if state.avgsEofsReceived < af.avgsExpectedEofs {
			return
		}
		state.avgsReady = true
		// Lo que quede en disco son transfers de métodos para los que
		// nunca llegó un avg (no hubo transfers de ese método en el avg
		// period). No matchean nada, los descartamos.
		slog.Info("AverageFilter avgs ready, discarding leftover buffer files", "filter_id", af.id, "client_id", result.ClientID, "avgs", len(state.avgs))
		af.deleteRemainingFiles(result.ClientID)

		if state.transfersEofPending {
			af.fireTransfersRingLocked(result.ClientID, state.transfersEofRealAmt)
			state.transfersEofPending = false
		}
		return
	}

	method := result.Payload.Method
	state.avgs[method] = result.Payload.Avg

	// Drain incremental: procesar todos los transfers en disco que coincidan
	// con el método recién aprendido.
	af.drainFileForMethod(result.ClientID, method, state)
}

func (af *AverageFilter) handleTransferMessage(msg middleware.Message, ack, nack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.Transfer](&msg)
	if err != nil {
		slog.Error("While deserializing transfer message", "err", err)
		return
	}

	if result.IsEOF() {
		af.lock.Lock()
		state := af.getOrInitState(result.ClientID)
		if !state.avgsReady {
			state.transfersEofPending = true
			state.transfersEofRealAmt = result.EOF.TotalMessages
			slog.Info("AverageFilter deferring transfers EOF until avgs ready", "filter_id", af.id, "client_id", result.ClientID, "real_amount", result.EOF.TotalMessages)
			af.lock.Unlock()
			return
		}
		af.fireTransfersRingLocked(result.ClientID, result.EOF.TotalMessages)
		af.lock.Unlock()
		return
	}

	af.transfersMonitor.AddProcessedMessagesAmountByClientId(result.ClientID, 1)

	af.lock.Lock()
	defer af.lock.Unlock()
	state := af.getOrInitState(result.ClientID)

	if _, ok := state.avgs[result.Payload.PaymentFormat]; ok {
		af.processTransferLocked(result.ClientID, result.Payload, state)
		return
	}

	if state.avgsReady {
		// Ya llegaron todos los avgs y para este método no hay → descartar.
		return
	}

	af.saveTransferToFile(result.ClientID, result.Payload)
}

// processTransferLocked debe llamarse con af.lock tomado.
func (af *AverageFilter) processTransferLocked(clientID int, t transfer.Transfer, state *avgFilterClientState) {
	avg, ok := state.avgs[t.PaymentFormat]
	if !ok {
		return
	}
	if !af.compareFunc(t.AmountPaid, avg) {
		return
	}
	out := queryresult.Query3Result{
		FromBank:    t.FromBank,
		FromAccount: t.FromBankAccount,
		Amount:      t.AmountPaid,
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

// fireTransfersRingLocked debe llamarse con af.lock tomado.
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
	slog.Info("AverageFilter fired transfers ring", "filter_id", af.id, "client_id", clientID, "real_amount", realAmount, "actual_amount", ringMsg.ActualAmount, "filtered", ringMsg.FilteredAmount)
}

func (af *AverageFilter) bufferFileName(clientID int, method string) string {
	safe := strings.ReplaceAll(method, " ", "_")
	return fmt.Sprintf("avg_filter_%d_client_%d_%s.csv", af.id, clientID, safe)
}

func (af *AverageFilter) saveTransferToFile(clientID int, t transfer.Transfer) {
	filename := af.bufferFileName(clientID, t.PaymentFormat)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("While opening buffer file", "filter_id", af.id, "err", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	line := fmt.Sprintf("%s,%s,%s,%f\n", t.PaymentFormat, t.FromBank, t.FromBankAccount, t.AmountPaid)
	if _, err := writer.WriteString(line); err != nil {
		slog.Error("While writing transfer to file", "err", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("While flushing buffer file", "err", err)
	}
}

func (af *AverageFilter) drainFileForMethod(clientID int, method string, state *avgFilterClientState) {
	filename := af.bufferFileName(clientID, method)
	file, err := os.Open(filename)
	if os.IsNotExist(err) {
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
		amount, err := strconv.ParseFloat(cols[3], 32)
		if err != nil {
			slog.Error("While parsing amount from file", "filter_id", af.id, "err", err)
			continue
		}
		t := transfer.Transfer{
			PaymentFormat:   cols[0],
			FromBank:        cols[1],
			FromBankAccount: cols[2],
			AmountPaid:      float32(amount),
		}
		af.processTransferLocked(clientID, t, state)
		drained++
	}
	if drained > 0 {
		slog.Info("AverageFilter drained file for method", "filter_id", af.id, "client_id", clientID, "method", method, "drained", drained)
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
	if len(matches) > 0 {
		slog.Info("AverageFilter deleted remaining buffer files", "filter_id", af.id, "client_id", clientID, "count", len(matches))
	}
}

func (af *AverageFilter) HandleSignals() {
}

func (af *AverageFilter) Close() {
}
