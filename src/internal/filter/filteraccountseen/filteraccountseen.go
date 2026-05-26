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
	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
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
}

type FilterAccountSeen struct {
	id int

	mu sync.Mutex

	expectedEOFs int

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	clientsState map[int]*clientState

	queryID int
}

type clientState struct {
	eofAmt       int
	seenAccounts map[string]bool
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
				outputMiddleware.Close()
			}
			if inputMiddleware != nil {
				inputMiddleware.Close()
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

	f.inputMiddleware.StopConsuming()
}

func (f *FilterAccountSeen) close() {
	f.inputMiddleware.Close()
	f.outputMiddleware.Close()
}

func (f *FilterAccountSeen) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[account.AccountIdentifier](&msg)

	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		f.handleEOF(*m)
		return
	}

	f.handleRecord(m.ClientID, m.Payload)
}

func (f *FilterAccountSeen) handleRecord(clientID int, record account.AccountIdentifier) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := f.stateFor(clientID)

	if _, ok := state.seenAccounts[record.GetKey()]; ok {
		return
	}

	state.seenAccounts[record.GetKey()] = true

	msg, err := inner.SerializeData(inner.DataMsg[queryresult.Query4Result]{
		ClientID: clientID,
		QueryID:  uint8(f.queryID),
		Payload:  queryresult.Query4Result{BankId: record.BankID, AccountNumber: record.AccountNumber},
	})

	if err != nil {
		slog.Error("While serializing output message", "err", err)
		return
	}

	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body}); err != nil {
		slog.Error("While sending output message", "err", err)
	}
}

func (f *FilterAccountSeen) handleEOF(data inner.DataMsg[account.AccountIdentifier]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := f.stateFor(data.ClientID)
	state.eofAmt++

	if state.eofAmt < f.expectedEOFs {
		return
	}

	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID, QueryID: uint8(f.queryID)})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return
	}

	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	delete(f.clientsState, data.ClientID)
}

func (f *FilterAccountSeen) stateFor(clientID int) *clientState {
	st, ok := f.clientsState[clientID]
	if !ok {
		st = &clientState{seenAccounts: map[string]bool{}}
		f.clientsState[clientID] = st
	}
	return st
}
