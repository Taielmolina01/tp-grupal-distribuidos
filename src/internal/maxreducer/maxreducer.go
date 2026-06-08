package maxreducer

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const eofRingPrefix = "REDUCE"

type Config struct {
	Id               int
	ReducerAmount    int
	MomHost          string
	MomPort          int
	InputExchange    string
	InputQueue       string
	InputRoutingKeys []string
	OutputQueues     []string
	QueryID          uint8
}

type MaxReducer struct {
	id      int
	queryID uint8

	inputExchange middleware.Middleware
	outputQueues  []middleware.Middleware
	eofInput      middleware.Middleware
	eofOutput     middleware.Middleware
	eofHandler    eofring.EofRingAlgorithm
	msgMonitor    msgmonitor.MessageMonitor

	mu    sync.Mutex
	maxes map[int]map[string]transfer.TransferForQ2
}

func New(config Config) (_ *MaxReducer, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputExchange middleware.Middleware
		outputQueues  []middleware.Middleware
		eofInput      middleware.Middleware
		eofOutput     middleware.Middleware
	)

	defer func() {
		if err == nil {
			return
		}
		if eofOutput != nil {
			eofOutput.Close()
		}
		if eofInput != nil {
			eofInput.Close()
		}
		for _, q := range outputQueues {
			q.Close()
		}
		if inputExchange != nil {
			inputExchange.Close()
		}
	}()

	inputExchange, err = middleware.CreateExchangeMiddleware(config.InputExchange, config.InputQueue, config.InputRoutingKeys, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
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

	eofInputName, eofOutputName := eofring.GetInputAndOutputQueueNames(
		config.Id,
		config.ReducerAmount,
		eofRingPrefix,
		eofRingPrefix,
	)

	eofInput, err = middleware.CreateQueueMiddleware(eofInputName, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF input queue: %w", err)
	}

	eofOutput, err = middleware.CreateQueueMiddleware(eofOutputName, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF output queue: %w", err)
	}

	msgMonitor := msgmonitor.NewMessageMonitor()

	r := &MaxReducer{
		id:            config.Id,
		queryID:       config.QueryID,
		inputExchange: inputExchange,
		outputQueues:  outputQueues,
		eofInput:      eofInput,
		eofOutput:     eofOutput,
		msgMonitor:    msgMonitor,
		maxes:         map[int]map[string]transfer.TransferForQ2{},
	}

	r.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.ReducerAmount,
		uint32(config.Id),
		msgMonitor,
		r.onRingConverged,
		config.QueryID,
	)

	return r, nil
}

func (r *MaxReducer) Run() {
	defer r.close()
	slog.Info("Starting max-amount-by-bank consumers", "reducer_id", r.id)
	go r.eofHandler.Run()
	if err := r.inputExchange.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		r.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (r *MaxReducer) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	r.inputExchange.StopConsuming()
	r.eofInput.StopConsuming()
}

func (r *MaxReducer) close() {
	r.inputExchange.Close()
	r.eofInput.Close()
	r.eofOutput.Close()
	for _, q := range r.outputQueues {
		q.Close()
	}
}

func (r *MaxReducer) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		r.handleEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		r.handleRecord(input.ClientID, input.Records[i])
	}
}

func (r *MaxReducer) handleRecord(clientID int, t transfer.TransferAfterCurrency) {
	projected := transfer.ProjectForQ2(t)
	bank := normalizer.NormalizeBankID(projected.FromBank)

	r.mu.Lock()
	if r.maxes[clientID] == nil {
		r.maxes[clientID] = map[string]transfer.TransferForQ2{}
	}
	existing, ok := r.maxes[clientID][bank]
	if !ok {
		r.maxes[clientID][bank] = projected
		r.msgMonitor.AddForwardedMessagesAmountByClientId(clientID, 1)
	} else {
		r.maxes[clientID][bank] = mergeMax(existing, projected)
	}
	r.mu.Unlock()
	r.msgMonitor.AddProcessedMessagesAmountByClientId(clientID, 1)
}

func (r *MaxReducer) handleEOF(clientID int, total uint32) {
	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   r.msgMonitor.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(r.id),
		FilteredAmount: r.msgMonitor.GetForwardedMessagesAmountByClientId(clientID),
	}
	if err := r.eofOutput.Send(middleware.Message{Body: string(eofring.SerializeRingMessage(ringMsg))}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
		return
	}
}

func (r *MaxReducer) onRingConverged(clientID int, total uint32, _ bool) error {
	r.mu.Lock()
	byBank := r.maxes[clientID]
	delete(r.maxes, clientID)
	r.mu.Unlock()

	byShard := make(map[int][]transfer.TransferForQ2)
	for bank, t := range byBank {
		idx := shard.CalculateIndexForShard(clientID, bank, len(r.outputQueues))
		byShard[idx] = append(byShard[idx], t)
	}
	for idx, group := range byShard {
		body := batch.Write(clientID, r.queryID, group, records.TransferForQ2Codec)
		if err := r.outputQueues[idx].Send(middleware.Message{Body: string(body)}); err != nil {
			return err
		}
	}

	eofBody := batch.WriteEOF(clientID, r.queryID, total)
	for _, q := range r.outputQueues {
		if err := q.Send(middleware.Message{Body: string(eofBody)}); err != nil {
			return err
		}
	}
	return nil
}

func mergeMax(a, b transfer.TransferForQ2) transfer.TransferForQ2 {
	if a.AmountPaid > b.AmountPaid {
		return a
	}
	if a.AmountPaid == b.AmountPaid && a.FromBankAccount > b.FromBankAccount {
		return a
	}
	return b
}
