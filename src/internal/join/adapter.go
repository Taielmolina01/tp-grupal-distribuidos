package join

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

const JOIN_STATE_FILE = "join_state_%d"

type TwoInputAdapter[L, R, O any] struct {
	id                int
	joinAmount        int
	queryID           uint8
	join              *Join[L, R, O]
	leftInput         middleware.Middleware
	rightInput        middleware.Middleware
	output            middleware.Middleware
	leftCodec         wire.Codec[L]
	rightCodec        wire.Codec[R]
	leftEofCount      map[int]int
	rightEofCount     map[int]int
	leftEofsExpected  int
	rightEofsExpected int
	fired             map[int]bool
	stateFilePath     string
	lock              sync.Mutex
}

func newTwoInputJoin[L, R, O any](
	config JoinConfig,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
	leftCombine func(L, L) L,
	leftCodec wire.Codec[L],
	rightCodec wire.Codec[R],
	outputCodec wire.Codec[O],
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	leftInput, err := middleware.CreateQueueMiddleware(config.LeftInputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	rightInput, err := middleware.CreateQueueMiddleware(config.RightInputQueue, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		return nil, err
	}

	output, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		if err := rightInput.Close(); err != nil {
			slog.Error("while closing right input", "err", err)
		}
		return nil, err
	}

	slog.Info("join started",
		"left_queue", config.LeftInputQueue,
		"right_queue", config.RightInputQueue,
		"output_queue", config.OutputQueue,
	)

	leftEofs := config.LeftEofsExpected
	if leftEofs <= 0 {
		leftEofs = 1
	}
	rightEofs := config.RightEofsExpected
	if rightEofs <= 0 {
		rightEofs = 1
	}

	adapter := &TwoInputAdapter[L, R, O]{
		id:                config.Id,
		joinAmount:        config.Amount,
		queryID:           config.QueryID,
		join:              newJoin(output, outputCodec, leftKey, rightKey, combine, leftCombine, config.QueryID),
		leftInput:         leftInput,
		rightInput:        rightInput,
		output:            output,
		leftCodec:         leftCodec,
		rightCodec:        rightCodec,
		leftEofCount:      map[int]int{},
		rightEofCount:     map[int]int{},
		leftEofsExpected:  leftEofs,
		rightEofsExpected: rightEofs,
		fired:             map[int]bool{},
		stateFilePath:     fmt.Sprintf(JOIN_STATE_FILE, config.Id),
	}

	if err := adapter.loadState(); err != nil {
		slog.Error("While loading join state from disk", "err", err)
	}

	return adapter, nil
}

func (a *TwoInputAdapter[L, R, O]) Run() {
	done := make(chan struct{})
	defer a.Close()
	go func() {
		if err := a.leftInput.StartConsuming(a.HandleLeft); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := a.rightInput.StartConsuming(a.HandleRight); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
}

func (a *TwoInputAdapter[L, R, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	if err := a.leftInput.StopConsuming(); err != nil {
		slog.Error("while stopping left input", "err", err)
	}
	if err := a.rightInput.StopConsuming(); err != nil {
		slog.Error("while stopping right input", "err", err)
	}
}

// Consuming handlers

func (a *TwoInputAdapter[L, R, O]) HandleLeft(msg middleware.Message, ack, nack func()) {
	input, err := batch.Read(msg.Body, a.leftCodec)
	if err != nil {
		slog.Error("while deserializing left batch", "err", err)
		ack()
		return
	}
	if input.EOF {
		a.HandleLeftEof(input.ClientID)
		a.checkClientIsFinishedAndSendEOF(input.ClientID)
		if err := a.saveState(); err != nil {
			slog.Error("While saving state after left EOF", "client_id", input.ClientID, "err", err)
		}
		ack()
		return
	}
	for i := range input.Records {
		a.join.HandleLeft(input.ClientID, input.Records[i])
	}

	if err := a.saveState(); err != nil {
		slog.Error("While saving state after right EOF", "client_id", input.ClientID, "err", err)
	}
	ack()
}

func (a *TwoInputAdapter[L, R, O]) HandleRight(msg middleware.Message, ack, nack func()) {
	input, err := batch.Read(msg.Body, a.rightCodec)
	if err != nil {
		slog.Error("while deserializing right batch", "err", err)
		ack()
		return
	}
	if input.EOF {
		a.HandleRightEof(input.ClientID)
		a.checkClientIsFinishedAndSendEOF(input.ClientID)
		if err := a.saveState(); err != nil {
			slog.Error("While saving state after right EOF", "client_id", input.ClientID, "err", err)
		}
		ack()
		return
	}
	for i := range input.Records {
		a.join.HandleRight(input.ClientID, input.Records[i])
	}

	if err := a.saveState(); err != nil {
		slog.Error("While saving state after right EOF", "client_id", input.ClientID, "err", err)
	}

	ack()
}

// Helpers with locking for EOF handling

func (a *TwoInputAdapter[L, R, O]) HandleLeftEof(clientID int) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.leftEofCount[clientID]++
}

func (a *TwoInputAdapter[L, R, O]) HandleRightEof(clientID int) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.rightEofCount[clientID]++
}
func (a *TwoInputAdapter[L, R, O]) checkClientIsFinishedAndSendEOF(clientID int) {
	if !a.isClientFinished(clientID) {
		return
	}

	a.join.HandleQueryEOF(clientID)

	eofBody := batch.WriteEOF(clientID, a.queryID, 0, 0, 0)
	if err := a.output.Send(middleware.Message{Body: eofBody}); err != nil {
		slog.Error("while sending join EOF downstream", "err", err)
	}
}

func (a *TwoInputAdapter[L, R, O]) isClientFinished(clientId int) bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.fired[clientId] || a.leftEofCount[clientId] < a.leftEofsExpected || a.rightEofCount[clientId] < a.rightEofsExpected {
		return false
	}
	a.fired[clientId] = true
	delete(a.leftEofCount, clientId)
	delete(a.rightEofCount, clientId)

	return true
}

// State persistence

func (a *TwoInputAdapter[L, R, O]) saveState() error {
	a.join.mu.Lock()
	a.lock.Lock()
	defer func() {
		a.lock.Unlock()
		a.join.mu.Unlock()
	}()

	w := wire.NewWriter()

	w.Uint32(uint32(len(a.join.leftBuffer)))
	for clientID, methods := range a.join.leftBuffer {
		w.Int32(int32(clientID))
		w.Uint32(uint32(len(methods)))
		for key, record := range methods {
			w.String(key)
			a.leftCodec.Marshal(w, &record)
		}
	}

	w.Uint32(uint32(len(a.join.rightBuffer)))
	for clientID, methods := range a.join.rightBuffer {
		w.Int32(int32(clientID))
		w.Uint32(uint32(len(methods)))
		for key, record := range methods {
			w.String(key)
			a.rightCodec.Marshal(w, &record)
		}
	}

	w.Uint32(uint32(len(a.join.pending)))
	for clientID, records := range a.join.pending {
		w.Int32(int32(clientID))
		w.Uint32(uint32(len(records)))
		for _, record := range records {
			a.join.outputCodec.Marshal(w, &record)
		}
	}

	w.Uint32(uint32(len(a.leftEofCount)))
	for clientID, count := range a.leftEofCount {
		w.Int32(int32(clientID))
		w.Int32(int32(count))
	}

	w.Uint32(uint32(len(a.rightEofCount)))
	for clientID, count := range a.rightEofCount {
		w.Int32(int32(clientID))
		w.Int32(int32(count))
	}

	w.Uint32(uint32(len(a.fired)))
	for clientID := range a.fired {
		w.Int32(int32(clientID))
	}

	tmp := a.stateFilePath + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, a.stateFilePath)
}

func (a *TwoInputAdapter[L, R, O]) loadState() error {
	data, err := os.ReadFile(a.stateFilePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	r := wire.NewReader(data)

	numClients := r.Uint32()
	for range numClients {
		clientID := int(r.Int32())
		numMethods := r.Uint32()
		methods := make(map[string]L, numMethods)
		for range numMethods {
			key := r.String()
			record := a.leftCodec.Unmarshal(r)
			methods[key] = record
		}
		if r.Err() != nil {
			return r.Err()
		}
		a.join.leftBuffer[clientID] = methods
	}

	numClients = r.Uint32()
	for range numClients {
		clientID := int(r.Int32())
		numMethods := r.Uint32()
		methods := make(map[string]R, numMethods)
		for range numMethods {
			key := r.String()
			record := a.rightCodec.Unmarshal(r)
			methods[key] = record
		}
		if r.Err() != nil {
			return r.Err()
		}
		a.join.rightBuffer[clientID] = methods
	}

	numClients = r.Uint32()
	for range numClients {
		clientID := int(r.Int32())
		numRecords := r.Uint32()
		records := make([]O, numRecords)
		for i := range numRecords {
			records[i] = a.join.outputCodec.Unmarshal(r)
		}
		if r.Err() != nil {
			return r.Err()
		}
		a.join.pending[clientID] = records
	}

	numClients = r.Uint32()
	for range numClients {
		clientID := int(r.Int32())
		count := int(r.Int32())
		a.leftEofCount[clientID] = count
	}

	numClients = r.Uint32()
	for range numClients {
		clientID := int(r.Int32())
		count := int(r.Int32())
		a.rightEofCount[clientID] = count
	}

	numClients = r.Uint32()
	for range numClients {
		clientID := int(r.Int32())
		a.fired[clientID] = true
	}

	return r.Err()
}

// Close helper

func (a *TwoInputAdapter[L, R, O]) Close() {
	if err := a.leftInput.Close(); err != nil {
		slog.Error("while closing left input", "err", err)
	}
	if err := a.rightInput.Close(); err != nil {
		slog.Error("while closing right input", "err", err)
	}
	if err := a.join.output.Close(); err != nil {
		slog.Error("while closing output", "err", err)
	}
}
