package filterbankidseen

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/shard"
)

type FilterBankIdSeenConfig struct {
	Id             int
	InputExchange  string
	OutputExchange string
	OutputAmount   int
	MomHost        string
	MomPort        int
	QueryID        int
}

type FilterBankIdSeen struct {
	id               int
	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware
	hasher           shard.Hasher
	queryID          int
	alreadySeen      map[int]map[string]bool
}

func NewFilterBankIdSeen(config FilterBankIdSeenConfig) (_ *FilterBankIdSeen, err error) {
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

	inputQueue := config.InputExchange + "_" + strconv.Itoa(config.Id)
	inputShardKey := fmt.Sprintf("shard-%d", config.Id)
	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputExchange, inputQueue, inputShardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputExchange, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	return &FilterBankIdSeen{
		id:               config.Id,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
		hasher:           shard.New(config.OutputAmount),
		queryID:          config.QueryID,
		alreadySeen:      map[int]map[string]bool{},
	}, nil
}

func (f *FilterBankIdSeen) Run() {
	defer f.close()

	slog.Info("Starting filter-bank-id-seen", "filter_id", f.id)
	if err := f.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (f *FilterBankIdSeen) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, stopping consumer", "filter_id", f.id)
	f.inputMiddleware.StopConsuming()
}

func (f *FilterBankIdSeen) close() {
	f.inputMiddleware.Close()
	f.outputMiddleware.Close()
}

func (f *FilterBankIdSeen) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()

	m, err := inner.DeserializeData[account.Account](&msg)
	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		f.handleEOF(msg)
		return
	}

	f.handleRecord(m.ClientID, m.Payload)
}

func (f *FilterBankIdSeen) handleRecord(clientID int, acc account.Account) {
	clientSeen, ok := f.alreadySeen[clientID]
	if !ok {
		clientSeen = map[string]bool{}
		f.alreadySeen[clientID] = clientSeen
	}

	if clientSeen[acc.BankId] {
		return
	}
	clientSeen[acc.BankId] = true

	msg, err := inner.SerializeData(inner.DataMsg[account.Account]{
		ClientID: clientID,
		QueryID:  uint8(f.queryID),
		Payload:  acc,
	})
	if err != nil {
		slog.Error("While serializing output message", "err", err)
		return
	}

	shardIdx := f.hasher.ShardFor(clientID, normalizer.NormalizeBankID(acc.BankId))
	routingKey := fmt.Sprintf("shard-%d", shardIdx)
	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: routingKey}); err != nil {
		slog.Error("While sending account to output exchange", "err", err)
	}
}

func (f *FilterBankIdSeen) handleEOF(msg newmiddleware.Message) {
	slog.Info("EOF received, broadcasting downstream", "filter_id", f.id)
	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body, RoutingKey: newmiddleware.BroadcastRoutingKey}); err != nil {
		slog.Error("While broadcasting EOF to output exchange", "err", err)
	}
}
