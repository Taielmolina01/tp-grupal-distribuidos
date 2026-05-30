package prefilterq4

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/qualifiedaccount"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type PreFilterQ4Config struct {
	Id int

	InputMiddlewarePrefix string
	OutputExchange        string

	Threshold int

	MomHost string
	MomPort int

	QueryID uint8
}

type accountState struct {
	id         account.AccountIdentifier
	leftConns  map[account.AccountIdentifier]bool
	rightConns map[account.AccountIdentifier]bool
}

type clientState struct {
	accounts map[account.AccountIdentifier]*accountState
}

type PreFilterQ4 struct {
	id        int
	threshold int
	queryID   uint8

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	clientsState map[int]*clientState
}

func NewPreFilterQ4(config PreFilterQ4Config) (_ *PreFilterQ4, err error) {
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

	inputQueue := config.InputMiddlewarePrefix + "_prefilter_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)

	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.OutputExchange, "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	return &PreFilterQ4{
		id:               config.Id,
		threshold:        config.Threshold,
		queryID:          config.QueryID,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		clientsState:     map[int]*clientState{},
	}, nil
}

func (p *PreFilterQ4) Run() {
	defer p.close()

	if err := p.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		p.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (p *PreFilterQ4) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	p.inputMiddleware.StopConsuming()
}

func (p *PreFilterQ4) close() {
	p.inputMiddleware.Close()
	p.outputMiddleware.Close()
}

func (p *PreFilterQ4) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()

	input, err := splittransfer.Read([]byte(msg.Body))
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		p.handleEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		p.observe(input.ClientID, input.Records[i])
	}
}

func (p *PreFilterQ4) observe(clientID int, rec transfer.SplittedTransfer) {
	var protagonist, peer account.AccountIdentifier

	if rec.IsLeftPart {
		protagonist = account.AccountIdentifier{BankID: rec.Transfer.FromBank, AccountNumber: rec.Transfer.FromBankAccount}
		peer = account.AccountIdentifier{BankID: rec.Transfer.ToBank, AccountNumber: rec.Transfer.ToBankAccount}
	} else {
		protagonist = account.AccountIdentifier{BankID: rec.Transfer.ToBank, AccountNumber: rec.Transfer.ToBankAccount}
		peer = account.AccountIdentifier{BankID: rec.Transfer.FromBank, AccountNumber: rec.Transfer.FromBankAccount}
	}

	state := p.stateFor(clientID)

	acc, ok := state.accounts[protagonist]
	if !ok {
		acc = &accountState{
			id:         protagonist,
			leftConns:  map[account.AccountIdentifier]bool{},
			rightConns: map[account.AccountIdentifier]bool{},
		}
		state.accounts[protagonist] = acc
	}

	if rec.IsLeftPart {
		acc.leftConns[peer] = true
	} else {
		acc.rightConns[peer] = true
	}
}

func (p *PreFilterQ4) handleEOF(clientID int, total uint32) {
	p.finalize(clientID, p.stateFor(clientID), total)
}

func (p *PreFilterQ4) finalize(clientID int, state *clientState, total uint32) {
	var qualified []qualifiedaccount.QualifiedAccount
	for _, acc := range state.accounts {
		if len(acc.leftConns) >= p.threshold {
			qualified = append(qualified, qualifiedaccount.QualifiedAccount{Account: acc.id, IsLeft: true})
		}
		if len(acc.rightConns) >= p.threshold {
			qualified = append(qualified, qualifiedaccount.QualifiedAccount{Account: acc.id, IsLeft: false})
		}
	}

	if len(qualified) > 0 {
		body := qualifiedaccount.WriteBatch(clientID, p.queryID, qualified)
		if err := p.outputMiddleware.Send(newmiddleware.Message{Body: string(body)}); err != nil {
			slog.Error("While sending qualified accounts batch", "err", err)
		}
	}

	eofBody := qualifiedaccount.WriteEOF(clientID, p.queryID, total)
	if err := p.outputMiddleware.Send(newmiddleware.Message{Body: string(eofBody)}); err != nil {
		slog.Error("While sending EOF", "err", err)
	}

	for _, acc := range state.accounts {
		clear(acc.leftConns)
		clear(acc.rightConns)
	}
	clear(state.accounts)
	delete(p.clientsState, clientID)
}

func (p *PreFilterQ4) stateFor(clientID int) *clientState {
	st, ok := p.clientsState[clientID]
	if !ok {
		st = &clientState{accounts: map[account.AccountIdentifier]*accountState{}}
		p.clientsState[clientID] = st
	}
	return st
}
