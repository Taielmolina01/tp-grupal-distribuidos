package filteramount

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type FilterAmountConfig struct {
	Id int

	Amount float32

	OutputQueue string

	MomHost string
	MomPort int

	InputExchange string
	InputQueue    string
	QueryID       int
}

type FilterAmount struct {
	id int

	amount float32

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	queryID int
}

func NewFilterAmount(config FilterAmountConfig) (_ *FilterAmount, err error) {
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

	inputMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.InputExchange, config.InputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewQueueMiddleware(connSettings, config.OutputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	return &FilterAmount{
		id:               config.Id,
		queryID:          config.QueryID,
		amount:           config.Amount,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
	}, nil
}

func (f *FilterAmount) Run() {
	defer f.close()

	if err := f.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (f *FilterAmount) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.inputMiddleware.StopConsuming()
}

func (f *FilterAmount) close() {
	f.inputMiddleware.Close()
	f.outputMiddleware.Close()
}

func (f *FilterAmount) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()

	m, err := inner.DeserializeData[transfer.Transfer](&msg)
	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		f.handleEOF(*m)
		return
	}

	f.handleRecord(m.ClientID, m.Payload)
}

func (f *FilterAmount) handleRecord(clientID int, t transfer.Transfer) {
	if t.AmountPaid >= f.amount {
		return
	}

	msg, err := inner.SerializeData(inner.DataMsg[queryresult.Query1Result]{
		ClientID: clientID,
		QueryID:  uint8(f.queryID),
		Payload: queryresult.Query1Result{
			FromBank:    t.FromBank,
			FromAccount: t.FromBankAccount,
			ToBank:      t.ToBank,
			ToAccount:   t.ToBankAccount,
			Amount:      t.AmountPaid,
		},
	})
	if err != nil {
		slog.Error("While serializing output message", "err", err)
		return
	}

	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body}); err != nil {
		slog.Error("While sending output message", "err", err)
	}
}

func (f *FilterAmount) handleEOF(data inner.DataMsg[transfer.Transfer]) {
	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID, QueryID: uint8(f.queryID)})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return
	}

	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}
}
