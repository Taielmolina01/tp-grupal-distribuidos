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
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountchain"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/qualifiedaccount"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
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

	InputMiddlewarePrefix  string
	QualifiedInputExchange string
	PreFilterAmount        int

	QueryID int
}

type clientState struct {
	left  map[account.AccountIdentifier]map[account.AccountIdentifier]account.AccountPair
	right map[account.AccountIdentifier]map[account.AccountIdentifier]account.AccountPair

	qualifyingLeft  map[account.AccountIdentifier]bool
	qualifyingRight map[account.AccountIdentifier]bool

	transferEOFReceived bool
	transferEOFTotal    uint32
	prefilterEOFCount   int
}

type JoinAccounts struct {
	id int

	hasher shard.Hasher

	inputMiddleware     newmiddleware.Middleware
	qualifiedMiddleware newmiddleware.Middleware
	outputMiddleware    newmiddleware.Middleware

	preFilterAmount int

	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}

func NewJoinAccounts(config JoinAccountsConfig) (_ *JoinAccounts, err error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware     newmiddleware.Middleware
		qualifiedMiddleware newmiddleware.Middleware
		outputMiddleware    newmiddleware.Middleware
	)

	defer func() {
		if err != nil {
			if outputMiddleware != nil {
				outputMiddleware.Close()
			}
			if qualifiedMiddleware != nil {
				qualifiedMiddleware.Close()
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

	qualifiedQueue := fmt.Sprintf("%s_joinaccounts_%d", config.QualifiedInputExchange, config.Id)
	qualifiedMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.QualifiedInputExchange, qualifiedQueue)
	if err != nil {
		return nil, fmt.Errorf("creating qualified input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	return &JoinAccounts{
		id:                  config.Id,
		hasher:              shard.New(config.OutputMiddlewareAmount),
		queryID:             config.QueryID,
		inputMiddleware:     inputMiddleware,
		qualifiedMiddleware: qualifiedMiddleware,
		outputMiddleware:    outputMiddleware,
		preFilterAmount:     config.PreFilterAmount,
		clientsState:        map[int]*clientState{},
	}, nil
}

func (j *JoinAccounts) Run() {
	defer j.close()
	go j.consumeQualified()

	if err := j.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		j.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (j *JoinAccounts) consumeQualified() {
	if err := j.qualifiedMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		j.handleQualifiedInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from qualified middleware", "err", err)
	}
}

func (j *JoinAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	j.qualifiedMiddleware.StopConsuming()
	j.inputMiddleware.StopConsuming()
}

func (j *JoinAccounts) close() {
	j.inputMiddleware.Close()
	j.qualifiedMiddleware.Close()
	j.outputMiddleware.Close()
}

func (j *JoinAccounts) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	input, err := splittransfer.Read([]byte(msg.Body))
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if input.EOF {
		j.handleTransferEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		j.accumulate(input.ClientID, input.Records[i])
	}
}

func (j *JoinAccounts) handleQualifiedInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	input, err := qualifiedaccount.Read([]byte(msg.Body))
	if err != nil {
		slog.Error("While deserializing qualified accounts batch", "err", err)
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if input.EOF {
		j.handlePrefilterEOF(input.ClientID)
		return
	}

	state := j.stateFor(input.ClientID)
	for _, rec := range input.Records {
		if rec.IsLeft {
			state.qualifyingLeft[rec.Account] = true
		} else {
			state.qualifyingRight[rec.Account] = true
		}
	}
}

func (j *JoinAccounts) accumulate(clientID int, record transfer.SplittedTransfer) {
	if record.IsLeftPart {
		j.accumulateLeft(clientID, record)
	} else {
		j.accumulateRight(clientID, record)
	}
}

func (j *JoinAccounts) accumulateLeft(clientID int, record transfer.SplittedTransfer) {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	rightIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}

	state := j.stateFor(clientID)
	accMap, ok := state.left[identifier]
	if !ok {
		accMap = map[account.AccountIdentifier]account.AccountPair{}
		state.left[identifier] = accMap
	}
	accMap[rightIdentifier] = account.AccountPair{Left: identifier, Right: rightIdentifier}
}

func (j *JoinAccounts) accumulateRight(clientID int, record transfer.SplittedTransfer) {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	leftIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}

	state := j.stateFor(clientID)
	accMap, ok := state.right[identifier]
	if !ok {
		accMap = map[account.AccountIdentifier]account.AccountPair{}
		state.right[identifier] = accMap
	}
	accMap[leftIdentifier] = account.AccountPair{Left: leftIdentifier, Right: identifier}
}

func (j *JoinAccounts) handleTransferEOF(clientID int, total uint32) {
	state := j.stateFor(clientID)
	state.transferEOFReceived = true
	state.transferEOFTotal = total
	j.tryFinalize(clientID)
}

func (j *JoinAccounts) handlePrefilterEOF(clientID int) {
	state := j.stateFor(clientID)
	state.prefilterEOFCount++
	j.tryFinalize(clientID)
}

func (j *JoinAccounts) tryFinalize(clientID int) {
	state := j.stateFor(clientID)
	if !state.transferEOFReceived || state.prefilterEOFCount < j.preFilterAmount {
		return
	}
	j.finalize(clientID, state)
}

func (j *JoinAccounts) finalize(clientID int, state *clientState) {
	var chains []account.AccountChain

	for protagonistKey, rightMap := range state.right {
		leftMap, ok := state.left[protagonistKey]
		if !ok {
			continue
		}
		for _, aPair := range rightMap {
			if _, ok := state.qualifyingLeft[aPair.Left]; !ok {
				continue
			}
			for _, cPair := range leftMap {
				if _, ok := state.qualifyingRight[cPair.Right]; !ok {
					continue
				}
				chains = append(chains, account.AccountChain{
					Left:   aPair.Left,
					Middle: aPair.Right,
					Right:  cPair.Right,
				})
			}
		}
	}

	j.sendChains(clientID, chains)

	eofBody := accountchain.WriteEOF(clientID, uint8(j.queryID), state.transferEOFTotal)
	if err := j.outputMiddleware.Send(newmiddleware.Message{Body: string(eofBody), RoutingKey: newmiddleware.BroadcastRoutingKey}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	for _, inner := range state.left  { clear(inner) }
	for _, inner := range state.right { clear(inner) }
	clear(state.left)
	clear(state.right)
	clear(state.qualifyingLeft)
	clear(state.qualifyingRight)
	delete(j.clientsState, clientID)
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
	for rk, group := range grouped {
		body := accountchain.WriteBatch(clientID, uint8(j.queryID), group)
		if err := j.outputMiddleware.Send(newmiddleware.Message{Body: string(body), RoutingKey: rk}); err != nil {
			slog.Error("While sending chain batch", "err", err)
		}
	}
}

func (j *JoinAccounts) stateFor(clientID int) *clientState {
	st, ok := j.clientsState[clientID]
	if !ok {
		st = &clientState{
			left:            map[account.AccountIdentifier]map[account.AccountIdentifier]account.AccountPair{},
			right:           map[account.AccountIdentifier]map[account.AccountIdentifier]account.AccountPair{},
			qualifyingLeft:  map[account.AccountIdentifier]bool{},
			qualifyingRight: map[account.AccountIdentifier]bool{},
		}
		j.clientsState[clientID] = st
	}
	return st
}
