package bankdistinct

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/shard"
)

type Config struct {
	Id           int
	MomHost      string
	MomPort      int
	InputQueue   string
	OutputQueues []string
	QueryID      uint8
}

type BankDistinctFilter struct {
	id      uint32
	queryID uint8

	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware

	mu   sync.Mutex
	seen map[int]map[string]struct{}
}

func New(config Config) (_ *BankDistinctFilter, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputQueue   middleware.Middleware
		outputQueues []middleware.Middleware
	)

	defer func() {
		if err == nil {
			return
		}
		for _, q := range outputQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing output queue", "err", err)
			}
		}
		if inputQueue != nil {
			if err := inputQueue.Close(); err != nil {
				slog.Error("While closing input queue", "err", err)
			}
		}
	}()

	inputQueue, err = middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}

	outputQueues = make([]middleware.Middleware, 0, len(config.OutputQueues))
	for _, q := range config.OutputQueues {
		m, e := middleware.CreateQueueMiddleware(q, connSettings)
		if e != nil {
			err = fmt.Errorf("creating output queue %s: %w", q, e)
			return nil, err
		}
		outputQueues = append(outputQueues, m)
	}

	return &BankDistinctFilter{
		id:           uint32(config.Id),
		queryID:      config.QueryID,
		inputQueue:   inputQueue,
		outputQueues: outputQueues,
		seen:         map[int]map[string]struct{}{},
	}, nil
}

func (f *BankDistinctFilter) Run() {
	defer f.close()
	slog.Info("Starting bank-distinct filter consumers", "filter_id", f.id)
	if err := f.inputQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (f *BankDistinctFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.inputQueue.StopConsuming()
}

func (f *BankDistinctFilter) close() {
	if err := f.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	for _, q := range f.outputQueues {
		if err := q.Close(); err != nil {
			slog.Error("While closing output queue", "err", err)
		}
	}
}

func (f *BankDistinctFilter) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.AccountCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		f.handleEOF(input.ClientID, input.Total)
		return
	}

	f.handleBatch(input.ClientID, input.Records)
}

func (f *BankDistinctFilter) handleBatch(clientID int, accounts []account.Account) {
	f.mu.Lock()
	seen, ok := f.seen[clientID]
	if !ok {
		seen = map[string]struct{}{}
		f.seen[clientID] = seen
	}

	byShard := make(map[int][]account.Account)
	for i := range accounts {
		a := accounts[i]
		bank := normalizer.NormalizeBankID(a.BankId)
		if _, ok := seen[bank]; ok {
			continue
		}
		seen[bank] = struct{}{}
		idx := shard.CalculateIndexForShard(clientID, bank, len(f.outputQueues))
		byShard[idx] = append(byShard[idx], a)
	}
	f.mu.Unlock()

	for idx, group := range byShard {
		body := batch.Write(clientID, f.queryID, group, records.AccountCodec)
		if err := f.outputQueues[idx].Send(middleware.Message{Body: string(body)}); err != nil {
			slog.Error("While sending output batch", "err", err)
		}
	}
}

func (f *BankDistinctFilter) handleEOF(clientID int, total uint32) {
	eofBody := batch.WriteEOF(clientID, f.queryID, total)
	for _, q := range f.outputQueues {
		if err := q.Send(middleware.Message{Body: string(eofBody)}); err != nil {
			slog.Error("While broadcasting EOF to output queue", "err", err)
		}
	}
}
