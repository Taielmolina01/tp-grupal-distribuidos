package acumaccounts

import (
	"fmt"
	"hash/fnv"
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
)

type AcumAccountsConfig struct {
	Id int

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	MomHost string
	MomPort int

	ExpectedEOFs          int    //Cantidad de nodos del grupo anterior
	InputMiddlewarePrefix string //Es el output prefix del nodo anterior

	QueryID int

	RequiredAmt int
}

type clientState struct {
	eofAmt int
	acum   map[string]int
}

type AcumAccounts struct {
	id int

	outputMiddlewareAmount int

	expectedEOFs int
	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware

	requiredAmt  int
	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}

func declareOutputQueues(config AcumAccountsConfig, connSettings middleware.ConnSettings) ([]middleware.Middleware, error) {
	outputQueues := make([]middleware.Middleware, 0, config.OutputMiddlewareAmount)
	for i := range config.OutputMiddlewareAmount {
		q, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_%d", config.OutputMiddlewarePrefix, i), connSettings)
		if err != nil {
			for _, opened := range outputQueues {
				opened.Close()
			}
			return nil, fmt.Errorf("creating output queue %d: %w", i, err)
		}
		outputQueues = append(outputQueues, q)
	}
	return outputQueues, nil
}

func NewAcumAccounts(config AcumAccountsConfig) (_ *AcumAccounts, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputQueue   middleware.Middleware
		outputQueues []middleware.Middleware
	)

	defer func() {
		if err != nil {
			for _, q := range outputQueues {
				q.Close()
			}
			if inputQueue != nil {
				inputQueue.Close()
			}
		}
	}()

	inputQueue, err = middleware.CreateQueueMiddleware(
		config.InputMiddlewarePrefix+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	outputQueues, err = declareOutputQueues(config, connSettings)
	if err != nil {
		return nil, fmt.Errorf("declaring output queues: %w", err)
	}

	return &AcumAccounts{
		id:                     config.Id,
		outputMiddlewareAmount: config.OutputMiddlewareAmount,
		queryID:                config.QueryID,
		inputQueue:             inputQueue,
		outputQueues:           outputQueues,
		expectedEOFs:           config.ExpectedEOFs,
		clientsState:           map[int]*clientState{},
		requiredAmt:            config.RequiredAmt,
	}, nil
}

func (a *AcumAccounts) Run() {
	defer a.close()

	if err := a.inputQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		a.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (a *AcumAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	a.inputQueue.StopConsuming()
}

func (a *AcumAccounts) close() {
	a.inputQueue.Close()

	for _, queue := range a.outputQueues {
		queue.Close()
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
		output_index := a.shardFor(clientID, o.BankID, o.AccountNumber)

		msg, err := inner.SerializeData(inner.DataMsg[account.AccountIdentifier]{
			ClientID: clientID,
			QueryID:  uint8(a.queryID),
			Payload:  o,
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		if err := a.outputQueues[output_index].Send(*msg); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (a *AcumAccounts) shardFor(clientID int, key1, key2 string) int {
	h := fnv.New32a()
	fmt.Fprintf(h, "%d\x00%s\x00%s", clientID, key1, key2)
	return int(h.Sum32() % uint32(a.outputMiddlewareAmount))
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

	for _, q := range a.outputQueues {
		if err := q.Send(*msg); err != nil {
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
