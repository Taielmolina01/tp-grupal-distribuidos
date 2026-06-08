package join

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
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type clientState struct {
	transfers map[string]transfer.TransferForQ2
	accounts  map[string]account.Account

	leftEofCount  int
	rightEofCount int
	fired         bool
}

type BankJoin struct {
	id      int
	queryID uint8

	leftInput  middleware.Middleware
	rightInput middleware.Middleware
	output     middleware.Middleware

	leftEofsExpected  int
	rightEofsExpected int

	mu           sync.Mutex
	clientsState map[int]*clientState
}

func New(config JoinConfig) (_ *BankJoin, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		leftInput  middleware.Middleware
		rightInput middleware.Middleware
		output     middleware.Middleware
	)

	defer func() {
		if err == nil {
			return
		}
		if output != nil {
			output.Close()
		}
		if rightInput != nil {
			rightInput.Close()
		}
		if leftInput != nil {
			leftInput.Close()
		}
	}()

	leftInput, err = middleware.CreateQueueMiddleware(config.LeftInputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating left input queue: %w", err)
	}

	rightInput, err = middleware.CreateQueueMiddleware(config.RightInputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating right input queue: %w", err)
	}

	output, err = middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	leftEofs := config.LeftEofsExpected
	if leftEofs <= 0 {
		leftEofs = 1
	}
	rightEofs := config.RightEofsExpected
	if rightEofs <= 0 {
		rightEofs = 1
	}

	slog.Info("join started",
		"left_queue", config.LeftInputQueue,
		"right_queue", config.RightInputQueue,
		"output_queue", config.OutputQueue,
	)

	return &BankJoin{
		id:                config.Id,
		queryID:           queryresult.Query2ID,
		leftInput:         leftInput,
		rightInput:        rightInput,
		output:            output,
		leftEofsExpected:  leftEofs,
		rightEofsExpected: rightEofs,
		clientsState:      map[int]*clientState{},
	}, nil
}

func (j *BankJoin) Run() {
	defer j.close()

	done := make(chan struct{})
	go func() {
		if err := j.leftInput.StartConsuming(func(msg middleware.Message, ack, _ func()) {
			j.handleLeft(msg, ack)
		}); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := j.rightInput.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		j.handleRight(msg, ack)
	}); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
}

func (j *BankJoin) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	j.leftInput.StopConsuming()
	j.rightInput.StopConsuming()
}

func (j *BankJoin) close() {
	j.leftInput.Close()
	j.rightInput.Close()
	j.output.Close()
}

func (j *BankJoin) handleLeft(msg middleware.Message, ack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.TransferForQ2Codec)
	if err != nil {
		slog.Error("while deserializing left batch", "err", err)
		return
	}

	if input.EOF {
		j.handleEOF(input.ClientID, true)
		return
	}

	j.mu.Lock()
	state := j.stateFor(input.ClientID)
	for i := range input.Records {
		t := input.Records[i]
		bank := normalizer.NormalizeBankID(t.FromBank)
		if existing, ok := state.transfers[bank]; ok {
			state.transfers[bank] = mergeMax(existing, t)
		} else {
			state.transfers[bank] = t
		}
	}
	j.mu.Unlock()
}

func (j *BankJoin) handleRight(msg middleware.Message, ack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.AccountCodec)
	if err != nil {
		slog.Error("while deserializing right batch", "err", err)
		return
	}

	if input.EOF {
		j.handleEOF(input.ClientID, false)
		return
	}

	j.mu.Lock()
	state := j.stateFor(input.ClientID)
	for i := range input.Records {
		a := input.Records[i]
		state.accounts[normalizer.NormalizeBankID(a.BankId)] = a
	}
	j.mu.Unlock()
}

func (j *BankJoin) handleEOF(clientID int, isLeft bool) {
	j.mu.Lock()
	state := j.stateFor(clientID)
	if isLeft {
		state.leftEofCount++
	} else {
		state.rightEofCount++
	}

	if state.fired || state.leftEofCount < j.leftEofsExpected || state.rightEofCount < j.rightEofsExpected {
		j.mu.Unlock()
		return
	}
	state.fired = true

	results := make([]queryresult.Query2Result, 0, len(state.transfers))
	for bank, t := range state.transfers {
		acc, ok := state.accounts[bank]
		if !ok {
			continue
		}
		results = append(results, queryresult.Query2Result{
			BankName:    acc.BankName,
			FromBank:    t.FromBank,
			FromAccount: t.FromBankAccount,
			Amount:      t.AmountPaid,
		})
	}
	delete(j.clientsState, clientID)
	j.mu.Unlock()

	if len(results) > 0 {
		body := batch.Write(clientID, j.queryID, results, records.Query2ResultCodec)
		if err := j.output.Send(middleware.Message{Body: string(body)}); err != nil {
			slog.Error("while sending join results", "err", err)
		}
	}

	eofBody := batch.WriteEOF(clientID, j.queryID, 0)
	if err := j.output.Send(middleware.Message{Body: string(eofBody)}); err != nil {
		slog.Error("while sending join EOF downstream", "err", err)
	}
}

func (j *BankJoin) stateFor(clientID int) *clientState {
	st, ok := j.clientsState[clientID]
	if !ok {
		st = &clientState{
			transfers: map[string]transfer.TransferForQ2{},
			accounts:  map[string]account.Account{},
		}
		j.clientsState[clientID] = st
	}
	return st
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
