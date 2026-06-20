package acumaccounts

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountchain"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountid"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewAcumAccounts(config AcumAccountsConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			if outputMiddleware != nil {
				if err := outputMiddleware.Close(); err != nil {
					slog.Error("While closing output middleware", "err", err)
				}
			}
			if inputMiddleware != nil {
				if err := inputMiddleware.Close(); err != nil {
					slog.Error("While closing input middleware", "err", err)
				}
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

	ckpt, err := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	recovered, err := ckpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			acum:            map[account.AccountPair]int8{},
			transferTracker: sendertracker.New(10_000_000),
			outputTracker:   outputtracker.New(),
		}
	})
	for clientID, cs := range recovered {
		states.Set(clientID, cs)
		slog.Info("recovered client state", "clientID", clientID,
			"acumEntries", len(cs.acum),
			"outputTotal", cs.outputTracker.Total(),
		)
	}

	return &AcumAccounts{
		id:                   config.Id,
		hasher:               shard.New(config.OutputMiddlewareAmount),
		outputAmount:         config.OutputMiddlewareAmount,
		expectedEOFs:         config.ExpectedEOFs,
		inputMiddleware:      inputMiddleware,
		outputMiddleware:     outputMiddleware,
		queryID:              config.QueryID,
		states:               states,
		requiredAmt:          config.RequiredAmt,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (a *AcumAccounts) Run() {
	defer a.close()

	if err := a.inputMiddleware.StartConsumingBatch(a.persistBatchSize, a.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		a.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (a *AcumAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	a.stopConsuming()
}

func (a *AcumAccounts) stopConsuming() {
	if err := a.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "acum_id", a.id, "err", err)
	}
}

func (a *AcumAccounts) close() {
	if err := a.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "acum_id", a.id, "err", err)
	}
	if err := a.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "acum_id", a.id, "err", err)
	}
}

func (a *AcumAccounts) handleBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := accountchain.Read(msg.Body)
		if err != nil {
			slog.Error("While deserializing input batch", "err", err)
			continue
		}

		clientID := input.ClientID
		state := a.states.For(clientID)
		tracker := state.transferTracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("Discarding duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq, "EOF", input.EOF)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			outgoing := map[string][]account.AccountIdentifier{}
			for i := range input.Records {
				for rk, ids := range a.collectRecord(clientID, input.Records[i]) {
					outgoing[rk] = append(outgoing[rk], ids...)
				}
			}
			for routingKey, ids := range outgoing {
				seq := state.outputTracker.RegisterBatch(routingKey)
				body := accountid.WriteBatch(clientID, uint8(a.queryID), uint8(a.id), seq, ids)
				if err := a.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: routingKey}); err != nil {
					slog.Error("While sending output batch", "err", err)
					nack()
					a.stopConsuming()
					return
				}
			}
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(a.expectedEOFs) {
			for i := range a.outputAmount {
				rk := fmt.Sprintf("shard-%d", i)
				total := state.outputTracker.CountFor(rk)
				eofBody := accountid.WriteEOF(clientID, uint8(a.queryID), uint8(a.id), total+1, uint32(total))
				if err := a.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
					slog.Error("While sending EOF message", "err", err)
					nack()
					a.stopConsuming()
					return
				}
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			a.states.Delete(clientID)
			a.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := a.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			a.stopConsuming()
			return
		}
	}

	ack()
}

func (a *AcumAccounts) collectRecord(clientID int, record account.AccountChain) map[string][]account.AccountIdentifier {
	state := a.states.For(clientID)

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
