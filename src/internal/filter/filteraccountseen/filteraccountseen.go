package filteraccountseen

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountid"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
)

type FilterAccountSeenConfig struct {
	Id int

	ExpectedEOFs int

	OutputMiddleware string

	MomHost string
	MomPort int

	InputMiddlewarePrefix string
	QueryID               int
	MaxBatchSize          int
	MaxBatchBytes         int
}

type FilterAccountSeen struct {
	id int

	mu sync.Mutex

	expectedEOFs  int
	maxBatchSize  int
	maxBatchBytes int

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	clientsState map[int]*clientState

	queryID int
}

type clientState struct {
	eofAmt       int
	seenAccounts map[account.AccountIdentifier]struct{}
	builder      *batch.Builder[queryresult.Query4Result]
	seqReceived  map[int]uint64
}

func (s *clientState) isDuplicateEOF(senderID int, seq uint64) bool {
	if seq == 0 {
		return false
	}
	if seq <= s.seqReceived[senderID] {
		return true
	}
	s.seqReceived[senderID] = seq
	return false
}

func NewFilterAccountSeen(config FilterAccountSeenConfig) (_ *FilterAccountSeen, err error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
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

	return &FilterAccountSeen{
		id:               config.Id,
		queryID:          config.QueryID,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		clientsState:     map[int]*clientState{},
		expectedEOFs:     config.ExpectedEOFs,
		maxBatchSize:     config.MaxBatchSize,
		maxBatchBytes:    config.MaxBatchBytes,
	}, nil
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
	input, err := accountid.Read([]byte(msg.Body))
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
	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: string(eofBody)}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	delete(f.clientsState, clientID)
}

func (f *FilterAccountSeen) flushResults(clientID int, state *clientState) {
	body := state.builder.Flush(clientID, uint8(f.queryID), 0, 0)
	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: string(body)}); err != nil {
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
