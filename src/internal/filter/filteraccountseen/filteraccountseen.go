package filteraccountseen

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountid"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/worker"
)

const FILTER_STATE_FILE = "filter_account_seen_%d"

func NewFilterAccountSeen(config FilterAccountSeenConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err == nil {
			return
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

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)

	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewQueueMiddleware(connSettings, config.OutputMiddleware)
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	f := &FilterAccountSeen{
		id:               config.Id,
		queryID:          config.QueryID,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		clientsState:     map[int]*clientState{},
		stateFilePath:    fmt.Sprintf(FILTER_STATE_FILE, config.Id),
		expectedEOFs:     config.ExpectedEOFs,
		maxBatchSize:     config.MaxBatchSize,
		maxBatchBytes:    config.MaxBatchBytes,
	}

	if err := f.loadState(); err != nil {
		slog.Error("While loading state from disk", "err", err)
	}

	return f, nil
}

func (f *FilterAccountSeen) Run() {
	defer f.close()

	if err := f.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (f *FilterAccountSeen) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "filter_id", f.id, "err", err)
	}
}

func (f *FilterAccountSeen) close() {
	if err := f.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "filter_id", f.id, "err", err)
	}
	if err := f.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "filter_id", f.id, "err", err)
	}
}

func (f *FilterAccountSeen) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	input, err := accountid.Read(msg.Body)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		f.handleEOF(input.ClientID, input.SenderID, input.Seq, input.Total)
		return
	}

	for i := range input.Records {
		f.handleRecord(input.ClientID, input.Records[i])
	}
}

func (f *FilterAccountSeen) handleRecord(clientID int, record account.AccountIdentifier) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := f.stateFor(clientID)

	if _, ok := state.seenAccounts[record]; ok {
		return
	}

	state.seenAccounts[record] = struct{}{}
	result := queryresult.Query4Result{
		BankId:        record.BankID,
		AccountNumber: record.AccountNumber,
	}
	if !state.builder.TryAdd(&result) {
		f.flushResults(clientID, state)
		state.builder.TryAdd(&result)
	}
}

func (f *FilterAccountSeen) handleEOF(clientID int, senderID uint8, seq uint64, total uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := f.stateFor(clientID)

	if state.isDuplicateEOF(int(senderID), seq) {
		slog.Warn("Discarding duplicate EOF", "clientID", clientID, "senderID", senderID, "seq", seq)
		return
	}

	state.eofAmt++

	if state.eofAmt < f.expectedEOFs {
		return
	}

	if !state.builder.IsEmpty() {
		f.flushResults(clientID, state)
	}

	eofBody := batch.WriteEOF(clientID, uint8(f.queryID), 0, 0, total)
	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: eofBody}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	delete(f.clientsState, clientID)

	if err := f.saveState(); err != nil {
		slog.Error("While saving state after EOF", "client_id", clientID, "err", err)
	}
}

func (f *FilterAccountSeen) flushResults(clientID int, state *clientState) {
	body := state.builder.Flush(clientID, uint8(f.queryID), 0, 0)
	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: body}); err != nil {
		slog.Error("While sending Q4 results batch", "err", err)
	}
}

func (f *FilterAccountSeen) stateFor(clientID int) *clientState {
	st, ok := f.clientsState[clientID]
	if !ok {
		st = &clientState{
			seenAccounts: map[account.AccountIdentifier]struct{}{},
			builder:      batch.NewBuilder(f.maxBatchSize, f.maxBatchBytes, records.Query4ResultCodec),
			seqReceived:  map[int]uint64{},
		}
		f.clientsState[clientID] = st
	}
	return st
}

func (f *FilterAccountSeen) saveState() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	w := wire.NewWriter()

	w.Uint32(uint32(len(f.clientsState)))
	for clientID, state := range f.clientsState {
		w.Int32(int32(clientID))
		w.Int32(int32(state.eofAmt))

		w.Uint32(uint32(len(state.seqReceived)))
		for senderID, seq := range state.seqReceived {
			w.Int32(int32(senderID))
			w.Uint64(seq)
		}

		w.Uint32(uint32(len(state.seenAccounts)))
		for acc := range state.seenAccounts {
			w.String(acc.BankID)
			w.String(acc.AccountNumber)
		}

		pending := state.builder.Records()
		w.Uint32(uint32(len(pending)))
		for i := range pending {
			records.Query4ResultCodec.Marshal(w, &pending[i])
		}
	}

	tmp := f.stateFilePath + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, f.stateFilePath)
}

func (f *FilterAccountSeen) loadState() error {
	data, err := os.ReadFile(f.stateFilePath)
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
		eofAmt := int(r.Int32())

		seqLen := r.Uint32()
		seqReceived := make(map[int]uint64, seqLen)
		for range seqLen {
			senderID := int(r.Int32())
			seq := r.Uint64()
			seqReceived[senderID] = seq
		}

		seenLen := r.Uint32()
		seenAccounts := make(map[account.AccountIdentifier]struct{}, seenLen)
		for range seenLen {
			bankID := r.String()
			accNumber := r.String()
			seenAccounts[account.AccountIdentifier{BankID: bankID, AccountNumber: accNumber}] = struct{}{}
		}

		pendingLen := r.Uint32()
		st := &clientState{
			eofAmt:       eofAmt,
			seenAccounts: seenAccounts,
			builder:      batch.NewBuilder(f.maxBatchSize, f.maxBatchBytes, records.Query4ResultCodec),
			seqReceived:  seqReceived,
		}
		for range pendingLen {
			result := records.Query4ResultCodec.Unmarshal(r)
			st.builder.TryAdd(&result)
		}

		if r.Err() != nil {
			return r.Err()
		}

		f.clientsState[clientID] = st
	}

	return r.Err()
}
