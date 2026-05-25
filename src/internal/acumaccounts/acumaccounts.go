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
	"tp-grupal-distribuidos/internal/common/middleware"
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

	expectedEOFs      int
	inputMiddleware   middleware.Middleware
	outputMiddlewares []middleware.Middleware

	requiredAmt  int
	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}

func declareOutputMiddlewares(config AcumAccountsConfig, connSettings middleware.ConnSettings) ([]middleware.Middleware, error) {
	outputMiddlewares := make([]middleware.Middleware, 0, config.OutputMiddlewareAmount)
	for i := range config.OutputMiddlewareAmount {
		q, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_%d", config.OutputMiddlewarePrefix, i), connSettings)
		if err != nil {
			for _, opened := range outputMiddlewares {
				opened.Close()
			}
			return nil, fmt.Errorf("creating output middleware %d: %w", i, err)
		}
		outputMiddlewares = append(outputMiddlewares, q)
	}
	return outputMiddlewares, nil
}

func NewAcumAccounts(config AcumAccountsConfig) (_ *AcumAccounts, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware   middleware.Middleware
		outputMiddlewares []middleware.Middleware
	)

	defer func() {
		if err != nil {
			for _, q := range outputMiddlewares {
				q.Close()
			}
			if inputMiddleware != nil {
				inputMiddleware.Close()
			}
		}
	}()

	inputMiddleware, err = middleware.CreateQueueMiddleware(
		config.InputMiddlewarePrefix+"_"+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddlewares, err = declareOutputMiddlewares(config, connSettings)
	if err != nil {
		return nil, fmt.Errorf("declaring output middlewares: %w", err)
	}

	return &AcumAccounts{
		id:                config.Id,
		hasher:            shard.New(config.OutputMiddlewareAmount),
		expectedEOFs:      config.ExpectedEOFs,
		inputMiddleware:   inputMiddleware,
		outputMiddlewares: outputMiddlewares,
		queryID:           config.QueryID,
		clientsState:      map[int]*clientState{},
		requiredAmt:       config.RequiredAmt,
	}, nil
}

func (a *AcumAccounts) Run() {
	defer a.close()

	if err := a.inputMiddleware.StartConsuming(func(msg middleware.Message, ack, _ func()) {
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

	for _, mw := range a.outputMiddlewares {
		mw.Close()
	}
}

func (a *AcumAccounts) handleInput(msg middleware.Message, ack func()) {
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
		outputIndex := a.hasher.ShardFor(clientID, o.BankID, o.AccountNumber)

		msg, err := inner.SerializeData(inner.DataMsg[account.AccountIdentifier]{
			ClientID: clientID,
			QueryID:  uint8(a.queryID),
			Payload:  o,
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		if err := a.outputMiddlewares[outputIndex].Send(*msg); err != nil {
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
	}

	for _, mw := range a.outputMiddlewares {
		if err := mw.Send(*msg); err != nil {
			slog.Error("While sending EOF message", "err", err)
		}
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
