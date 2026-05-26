package acumaccounts

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
	"tp-grupal-distribuidos/internal/common/shard"
)

type AcumAccountsConfig struct {
	Id int

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	MomHost string
	MomPort int

	ExpectedEOFs          int
	InputMiddlewarePrefix string

	QueryID int

	RequiredAmt int
}

type clientState struct {
	eofAmt int
	acum   map[string]int
}

type AcumAccounts struct {
	id int

	hasher shard.Hasher

	expectedEOFs     int
	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	requiredAmt  int
	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}

func NewAcumAccounts(config AcumAccountsConfig) (_ *AcumAccounts, err error) {
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

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	return &AcumAccounts{
		id:               config.Id,
		hasher:           shard.New(config.OutputMiddlewareAmount),
		expectedEOFs:     config.ExpectedEOFs,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		queryID:          config.QueryID,
		clientsState:     map[int]*clientState{},
		requiredAmt:      config.RequiredAmt,
	}, nil
}

func (a *AcumAccounts) Run() {
	defer a.close()

	if err := a.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		a.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (a *AcumAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	a.inputMiddleware.StopConsuming()
}

func (a *AcumAccounts) close() {
	a.inputMiddleware.Close()
	a.outputMiddleware.Close()
}

func (a *AcumAccounts) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[account.AccountChain](&msg)

	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		a.handleEOF(*m)
		return
	}

	a.handleRecord(m.ClientID, m.Payload)
}

func (a *AcumAccounts) handleRecord(clientID int, record account.AccountChain) {
	a.mu.Lock()
	defer a.mu.Unlock()

	state := a.stateFor(clientID)

	key := record.Left.GetKey() + "_" + record.Right.GetKey()

	if state.acum[key] >= a.requiredAmt {
		return
	}

	state.acum[key]++

	if state.acum[key] < a.requiredAmt {
		return
	}

	output := []account.AccountIdentifier{
		{BankID: record.Left.BankID, AccountNumber: record.Left.AccountNumber},
		{BankID: record.Right.BankID, AccountNumber: record.Right.AccountNumber},
	}

	for _, o := range output {
		msg, err := inner.SerializeData(inner.DataMsg[account.AccountIdentifier]{
			ClientID: clientID,
			QueryID:  uint8(a.queryID),
			Payload:  o,
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
			continue
		}

		routingKey := fmt.Sprintf("shard-%d", a.hasher.ShardFor(clientID, o.BankID, o.AccountNumber))
		if err := a.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: routingKey}); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (a *AcumAccounts) handleEOF(data inner.DataMsg[account.AccountChain]) {
	a.mu.Lock()
	defer a.mu.Unlock()

	state := a.stateFor(data.ClientID)
	state.eofAmt++

	if state.eofAmt < a.expectedEOFs {
		return
	}

	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID, QueryID: uint8(a.queryID)})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return
	}

	if err := a.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: newmiddleware.BroadcastRoutingKey}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	delete(a.clientsState, data.ClientID)
}

func (a *AcumAccounts) stateFor(clientID int) *clientState {
	st, ok := a.clientsState[clientID]
	if !ok {
		st = &clientState{
			acum: map[string]int{},
		}
		a.clientsState[clientID] = st
	}
	return st
}
