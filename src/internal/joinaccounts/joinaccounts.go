package joinaccounts

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
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

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	clientsState map[int]*clientState
	queryID      int
}

func NewJoinAccounts(config JoinAccountsConfig) (_ *JoinAccounts, err error) {
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

	return &JoinAccounts{
		id:               config.Id,
		hasher:           shard.New(config.OutputMiddlewareAmount),
		queryID:          config.QueryID,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		clientsState:     map[int]*clientState{},
	}, nil
}

func (j *JoinAccounts) Run() {
	defer j.close()

	if err := j.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
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
	j.outputMiddleware.Close()
}

func (j *JoinAccounts) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[transfer.SplittedTransfer](&middleware.Message{Body: msg.Body})

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
	var chains []account.AccountChain
	if record.IsLeftPart {
		chains = j.handleLeftPartRecord(clientID, record)
	} else {
		chains = j.handleRightPartRecord(clientID, record)
	}
	j.sendChains(clientID, chains)
}

func (j *JoinAccounts) handleLeftPartRecord(clientID int, record transfer.SplittedTransfer) []account.AccountChain {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	identifierKey := identifier.GetKey()

	rightIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	rightIdentifierKey := rightIdentifier.GetKey()

	state := j.stateFor(clientID)

	accMap, ok := state.left[identifierKey]
	if !ok {
		accMap = map[string]account.AccountPair{}
		state.left[identifierKey] = accMap
	}

	if _, ok = accMap[rightIdentifierKey]; ok {
		return nil
	}
	accMap[rightIdentifierKey] = account.AccountPair{Left: identifier, Right: rightIdentifier}

	accMapRight, ok := state.right[identifierKey]
	if !ok {
		return nil
	}

	chains := make([]account.AccountChain, 0, len(accMapRight))
	for _, v := range accMapRight {
		chains = append(chains, account.AccountChain{Left: v.Left, Middle: identifier, Right: rightIdentifier})
	}
	return chains
}

func (j *JoinAccounts) handleRightPartRecord(clientID int, record transfer.SplittedTransfer) []account.AccountChain {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	identifierKey := identifier.GetKey()

	leftIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	leftIdentifierKey := leftIdentifier.GetKey()

	state := j.stateFor(clientID)

	accMap, ok := state.right[identifierKey]
	if !ok {
		accMap = map[string]account.AccountPair{}
		state.right[identifierKey] = accMap
	}

	if _, ok = accMap[leftIdentifierKey]; ok {
		return nil
	}
	accMap[leftIdentifierKey] = account.AccountPair{Left: leftIdentifier, Right: identifier}

	accMapLeft, ok := state.left[identifierKey]
	if !ok {
		return nil
	}

	chains := make([]account.AccountChain, 0, len(accMapLeft))
	for _, v := range accMapLeft {
		chains = append(chains, account.AccountChain{Left: leftIdentifier, Middle: identifier, Right: v.Right})
	}
	return chains
}

func (j *JoinAccounts) sendChains(clientID int, chains []account.AccountChain) {
	if len(chains) == 0 {
		return
	}
	grouped := make(map[string][]account.AccountChain)
	for _, chain := range chains {
		rk := fmt.Sprintf("shard-%d", j.hasher.ShardFor(clientID, chain.Left.GetKey(), chain.Right.GetKey()))
		grouped[rk] = append(grouped[rk], chain)
	}
	for rk, batch := range grouped {
		msg, err := inner.SerializeData(inner.DataMsg[account.AccountChainBatch]{
			ClientID: clientID,
			QueryID:  uint8(j.queryID),
			Payload:  account.AccountChainBatch{Items: batch},
		})
		if err != nil {
			slog.Error("While serializing chain batch", "err", err)
			continue
		}
		if err := j.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: rk}); err != nil {
			slog.Error("While sending chain batch", "err", err)
		}
	}
}

func (j *JoinAccounts) handleEOF(data inner.DataMsg[transfer.SplittedTransfer]) {
	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID, QueryID: uint8(j.queryID)})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return
	}

	if err := j.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: newmiddleware.BroadcastRoutingKey}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	state, ok := j.clientsState[data.ClientID]
	if ok {
		clear(state.left)
		clear(state.right)
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
