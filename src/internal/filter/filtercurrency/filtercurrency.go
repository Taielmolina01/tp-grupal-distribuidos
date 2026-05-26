package filtercurrency

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type FilterCurrencyConfig struct {
	Id int

	Currencies []string

	MomHost string
	MomPort int

	InputExchange  string
	InputQueue     string
	OutputExchange string
	QueryID        int
}

type FilterCurrency struct {
	id int

	currencies []string

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	queryID int
}

func NewFilterCurrency(config FilterCurrencyConfig) (_ *FilterCurrency, err error) {
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

	outputMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.OutputExchange, "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	return &FilterCurrency{
		id:               config.Id,
		queryID:          config.QueryID,
		currencies:       config.Currencies,
		inputMiddleware:  inputMiddleware,
		outputMiddleware: outputMiddleware,
	}, nil
}

func (f *FilterCurrency) Run() {
	defer f.close()

	if err := f.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (f *FilterCurrency) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.inputMiddleware.StopConsuming()
}

func (f *FilterCurrency) close() {
	f.inputMiddleware.Close()
	f.outputMiddleware.Close()
}

func (f *FilterCurrency) handleInput(msg newmiddleware.Message, ack func()) {
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

func (f *FilterCurrency) handleRecord(clientID int, t transfer.Transfer) {
	if !slices.Contains(f.currencies, t.PaymentCurrency) {
		return
	}

	msg, err := inner.SerializeData(inner.DataMsg[transfer.Transfer]{
		ClientID: clientID,
		QueryID:  uint8(f.queryID),
		Payload:  t,
	})
	if err != nil {
		slog.Error("While serializing output message", "err", err)
		return
	}

	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body}); err != nil {
		slog.Error("While sending output message", "err", err)
	}
}

func (f *FilterCurrency) handleEOF(data inner.DataMsg[transfer.Transfer]) {
	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID, QueryID: uint8(f.queryID)})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return
	}

	if err := f.outputMiddleware.Send(newmiddleware.Message{Body: msg.Body}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}
}
