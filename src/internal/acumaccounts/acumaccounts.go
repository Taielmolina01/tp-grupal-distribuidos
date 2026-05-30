package acumaccounts

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountchain"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountid"
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

	RequiredAmt int8
}

type clientState struct {
	eofAmt int
	acum   map[account.AccountPair]int8
}

type AcumAccounts struct {
	id int

	hasher shard.Hasher

	expectedEOFs     int
	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	requiredAmt int8

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
	input, err := accountchain.Read([]byte(msg.Body))
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		a.handleEOF(input.ClientID, input.Total)
		return
	}

	outgoing := map[string][]account.AccountIdentifier{}
	for i := range input.Records {
		for rk, ids := range a.collectRecord(input.ClientID, input.Records[i]) {
			outgoing[rk] = append(outgoing[rk], ids...)
		}
	}

	for routingKey, ids := range outgoing {
		body := accountid.WriteBatch(input.ClientID, uint8(a.queryID), ids)
		if err := a.outputMiddleware.Send(newmiddleware.Message{Body: string(body), RoutingKey: routingKey}); err != nil {
			slog.Error("While sending output batch", "err", err)
		}
	}
}

func (a *AcumAccounts) collectRecord(clientID int, record account.AccountChain) map[string][]account.AccountIdentifier {
	state := a.stateFor(clientID)

	pair := account.AccountPair{Left: record.Left, Right: record.Right}

	if state.acum[pair] >= a.requiredAmt {
		return nil
	}

	state.acum[pair]++

	if state.acum[pair] < a.requiredAmt {
		return nil
	}

	candidates := []account.AccountIdentifier{
		{BankID: record.Left.BankID, AccountNumber: record.Left.AccountNumber},
		{BankID: record.Right.BankID, AccountNumber: record.Right.AccountNumber},
	}

	result := map[string][]account.AccountIdentifier{}
	for _, o := range candidates {
		rk := fmt.Sprintf("shard-%d", a.hasher.ShardFor(clientID, o.BankID, o.AccountNumber))
		result[rk] = append(result[rk], o)
	}
	return result
}

func (a *AcumAccounts) handleEOF(clientID int, total uint32) {
	state := a.stateFor(clientID)
	state.eofAmt++

	if state.eofAmt < a.expectedEOFs {
		return
	}

	eofBody := accountid.WriteEOF(clientID, uint8(a.queryID), total)
	if err := a.outputMiddleware.Send(newmiddleware.Message{Body: string(eofBody), RoutingKey: newmiddleware.BroadcastRoutingKey}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	clear(state.acum)
	delete(a.clientsState, clientID)
}

func (a *AcumAccounts) stateFor(clientID int) *clientState {
	st, ok := a.clientsState[clientID]
	if !ok {
		st = &clientState{
			acum: map[account.AccountPair]int8{},
		}
		a.clientsState[clientID] = st
	}
	return st
}
