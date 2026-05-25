package joinaccounts

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
	"tp-grupal-distribuidos/internal/common/transfer"
)

type JoinAccountsConfig struct {
	Id int

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	MomHost string
	MomPort int

	InputMiddlewarePrefix string
	QueryID               int
}

type clientState struct {
	left  map[string]map[string]account.AccountPair
	right map[string]map[string]account.AccountPair
}

type JoinAccounts struct {
	id int

	hasher shard.Hasher

	inputMiddleware   middleware.Middleware
	outputMiddlewares []middleware.Middleware

	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}

func declareOutputMiddlewares(config JoinAccountsConfig, connSettings middleware.ConnSettings) ([]middleware.Middleware, error) {
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

func NewJoinAccounts(config JoinAccountsConfig) (_ *JoinAccounts, err error) {
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

	return &JoinAccounts{
		id:                config.Id,
		hasher:            shard.New(config.OutputMiddlewareAmount),
		queryID:           config.QueryID,
		inputMiddleware:   inputMiddleware,
		outputMiddlewares: outputMiddlewares,
		clientsState:      map[int]*clientState{},
	}, nil
}

func (j *JoinAccounts) Run() {
	defer j.close()

	if err := j.inputMiddleware.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		j.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (j *JoinAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	j.inputMiddleware.StopConsuming()
}

func (j *JoinAccounts) close() {
	j.inputMiddleware.Close()

	for _, mw := range j.outputMiddlewares {
		mw.Close()
	}
}

func (j *JoinAccounts) handleInput(msg middleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[transfer.SplittedTransfer](&msg)

	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		j.handleEOF(*m)
		return
	}

	j.handleRecord(m.ClientID, m.Payload)
}

func (j *JoinAccounts) handleRecord(clientID int, record transfer.SplittedTransfer) {
	if record.IsLeftPart {
		j.handleLeftPartRecord(clientID, record)
		return
	}
	j.handleRightPartRecord(clientID, record)
}

func (j *JoinAccounts) handleLeftPartRecord(clientID int, record transfer.SplittedTransfer) {
	j.mu.Lock()
	defer j.mu.Unlock()

	idenetifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	identifierKey := idenetifier.GetKey()

	rightIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	rightIdentifierKey := rightIdentifier.GetKey()

	state := j.stateFor(clientID)

	//Veo si existe el map para la acc que va a ser el medio (la shard key)
	accMap, ok := state.left[identifierKey]
	if !ok {
		accMap = map[string]account.AccountPair{}
		state.left[identifierKey] = accMap
	}

	//Veo si ya existe el par shardkey + esta key de la derecha
	_, ok = accMap[rightIdentifierKey]
	if ok {
		return
	}
	accMap[rightIdentifierKey] = account.AccountPair{
		Left:  idenetifier,
		Right: rightIdentifier,
	}

	//Ahora que existe, busco matches en el otro grupo
	accMapRight, ok := state.right[identifierKey]
	if !ok {
		return
	}

	for _, v := range accMapRight {
		msg, err := inner.SerializeData(inner.DataMsg[account.AccountChain]{
			ClientID: clientID,
			QueryID:  uint8(j.queryID),
			Payload: account.AccountChain{
				Left:   v.Left,
				Middle: idenetifier,
				Right:  rightIdentifier,
			},
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		outputIndex := j.hasher.ShardFor(clientID, v.Left.GetKey(), rightIdentifierKey)
		if err := j.outputMiddlewares[outputIndex].Send(*msg); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (j *JoinAccounts) handleRightPartRecord(clientID int, record transfer.SplittedTransfer) {
	j.mu.Lock()
	defer j.mu.Unlock()

	idenetifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	identifierKey := idenetifier.GetKey()

	leftIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	leftIdentifierKey := leftIdentifier.GetKey()

	state := j.stateFor(clientID)

	//Veo si existe el map para la acc que va a ser el medio (la shard key)
	accMap, ok := state.right[identifierKey]
	if !ok {
		accMap = map[string]account.AccountPair{}
		state.right[identifierKey] = accMap
	}

	//Veo si ya existe el par shardkey + esta key de la izquierda
	_, ok = accMap[leftIdentifierKey]
	if ok {
		return
	}
	accMap[leftIdentifierKey] = account.AccountPair{
		Left:  leftIdentifier,
		Right: idenetifier,
	}

	//Ahora que existe, busco matches en el otro grupo
	accMapRight, ok := state.left[identifierKey]
	if !ok {
		return
	}

	for _, v := range accMapRight {
		msg, err := inner.SerializeData(inner.DataMsg[account.AccountChain]{
			ClientID: clientID,
			QueryID:  uint8(j.queryID),
			Payload: account.AccountChain{
				Left:   leftIdentifier,
				Middle: idenetifier,
				Right:  v.Right,
			},
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		outputIndex := j.hasher.ShardFor(clientID, leftIdentifierKey, v.Right.GetKey())
		if err := j.outputMiddlewares[outputIndex].Send(*msg); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (j *JoinAccounts) handleEOF(data inner.DataMsg[transfer.SplittedTransfer]) {
	j.mu.Lock()
	defer j.mu.Unlock()

	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID, QueryID: uint8(j.queryID)})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
	}

	for _, mw := range j.outputMiddlewares {
		if err := mw.Send(*msg); err != nil {
			slog.Error("While sending EOF message", "err", err)
		}
	}

	delete(j.clientsState, data.ClientID)
}

func (j *JoinAccounts) stateFor(clientID int) *clientState {
	st, ok := j.clientsState[clientID]
	if !ok {
		st = &clientState{
			left:  map[string]map[string]account.AccountPair{},
			right: map[string]map[string]account.AccountPair{},
		}
		j.clientsState[clientID] = st
	}
	return st
}
