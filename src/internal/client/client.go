package client

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/account"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/messageprotocol/external"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
)

const maxBatchBytes = 8 * 1024

type ClientConfig struct {
	ServerHost               string
	ServerPort               string
	InputFileAccounts        string
	InputFileTrans           string
	OutputFile               string
	MaxBatchSize             int
	ConnectionAttempts       int
	ConnectionAttemptDelayMs int
}

type Client struct {
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	client.running.Store(true)
	return client, nil
}

func connectToServer(config ClientConfig) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range config.ConnectionAttempts {
		conn, err = net.Dial("tcp", config.ServerHost+":"+config.ServerPort)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(time.Duration(config.ConnectionAttemptDelayMs) * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer func() {
		if err := client.conn.Close(); err != nil {
			slog.Error("While closing client's socket", "err", err)
		}
	}()
	go client.handleSignals()

	if err := client.sendAccountRecords(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := client.sendTransRecords(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	// Acá tenemos que esperar las rtas pero todavía no estoy seguro como.
	// Obvio todas de entrada no, así que supongo que sería medio por batches.
	// Podemos recibir batches cruzados? Por ej, empezar a recibir la rta de la 1 y en el medio recibir la rta de la 2?
	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	if err := client.conn.Close(); err != nil {
		slog.Error("While closing client's socket from handleSignals", "err", err)
	}
}

func (client *Client) expectMsgType(expectedMsgType external.MsgType) error {
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Debug("Error while reading message type", "err", err)
		return err
	}
	if msgType != expectedMsgType {
		return errors.New("unexpected message type")
	}
	return nil
}

func accountSerializedSize(acc account.Account) int {
	return 5*4 + len(acc.BankName) + len(acc.BankId) +
		len(acc.AccountNumber) + len(acc.EntityId) + len(acc.EntityName)
}

func transSerializedSize(t transfer.Transfer) int {
	ts := t.Timestamp.Format(time.RFC3339)
	stringBytes := len(ts) + len(t.FromBank) + len(t.FromBankAccount) +
		len(t.ToBank) + len(t.ToBankAccount) + len(t.ReceivingCurrency) +
		len(t.PaymentCurrency) + len(t.PaymentFormat)
	return 8*4 + stringBytes + 4 + 4 + 1
}

func (client *Client) readNextAccountBatch(
	scanner *bufio.Scanner, pending *account.Account,
) ([]account.Account, *account.Account) {
	var batch []account.Account
	var batchBytes int

	if pending != nil {
		batch = append(batch, *pending)
		batchBytes += accountSerializedSize(*pending)
	}

	for len(batch) < client.config.MaxBatchSize && scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")
		if len(columns) < 5 {
			continue
		}
		acc := account.Account{
			BankName:      columns[0],
			BankId:        columns[1],
			AccountNumber: columns[2],
			EntityId:      columns[3],
			EntityName:    columns[4],
		}
		size := accountSerializedSize(acc)
		if batchBytes+size > maxBatchBytes {
			return batch, &acc
		}
		batch = append(batch, acc)
		batchBytes += size
	}
	return batch, nil
}

func (client *Client) readNextTransBatch(
	scanner *bufio.Scanner, pending *transfer.Transfer,
) ([]transfer.Transfer, *transfer.Transfer) {
	var batch []transfer.Transfer
	var batchBytes int

	if pending != nil {
		batch = append(batch, *pending)
		batchBytes += transSerializedSize(*pending)
	}

	for len(batch) < client.config.MaxBatchSize && scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")
		if len(columns) < 11 {
			continue
		}
		timestamp, err := time.Parse("2006/01/02", columns[0])
		if err != nil {
			slog.Debug("Error while parsing trans timestamp", "err", err)
			continue
		}
		amountReceived, err := strconv.ParseFloat(columns[5], 32)
		if err != nil {
			slog.Debug("Error while parsing trans amount received", "err", err)
			continue
		}
		amountPaid, err := strconv.ParseFloat(columns[7], 32)
		if err != nil {
			slog.Debug("Error while parsing trans amount paid", "err", err)
			continue
		}
		t := transfer.Transfer{
			Timestamp:         timestamp,
			FromBank:          columns[1],
			FromBankAccount:   columns[2],
			ToBank:            columns[3],
			ToBankAccount:     columns[4],
			AmountReceived:    float32(amountReceived),
			ReceivingCurrency: columns[6],
			AmountPaid:        float32(amountPaid),
			PaymentCurrency:   columns[8],
			PaymentFormat:     columns[9],
			IsLaundering:      columns[10] == "1",
		}
		size := transSerializedSize(t)
		if batchBytes+size > maxBatchBytes {
			return batch, &t
		}
		batch = append(batch, t)
		batchBytes += size
	}
	return batch, nil
}

func (client *Client) sendAccountRecords() error {
	file, err := os.Open(client.config.InputFileAccounts)
	if err != nil {
		slog.Debug("Error while opening accounts file", "err", err)
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("While closing accounts file", "err", err)
		}
	}()

	scanner := bufio.NewScanner(file)
	var pending *account.Account
	for {
		batch, next := client.readNextAccountBatch(scanner, pending)
		if len(batch) == 0 {
			break
		}
		pending = next
		if err := external.WriteAccountBatch(client.conn, batch); err != nil {
			return err
		}
		if err := client.expectMsgType(external.Ack); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	return client.expectMsgType(external.Ack)
}

func (client *Client) sendTransRecords() error {
	file, err := os.Open(client.config.InputFileTrans)
	if err != nil {
		slog.Debug("Error while opening trans file", "err", err)
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("While closing trans file", "err", err)
		}
	}()

	scanner := bufio.NewScanner(file)
	var pending *transfer.Transfer
	for {
		batch, next := client.readNextTransBatch(scanner, pending)
		if len(batch) == 0 {
			break
		}
		pending = next
		if err := external.WriteTransBatch(client.conn, batch); err != nil {
			return err
		}
		if err := client.expectMsgType(external.Ack); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	return client.expectMsgType(external.Ack)
}
