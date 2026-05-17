package client

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/external"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const (
	maxBatchBytes        = 8 * 1024
	numQueries           = 5
	transTimestampLayout = "2006/01/02"
)

type ClientConfig struct {
	ServerHost               string
	ServerPort               string
	InputFileAccounts        string
	InputFileTrans           string
	OutputFilePrefix         string
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

	if err := client.recvResults(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

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

	builder := external.NewAccountBatchBuilder(client.config.MaxBatchSize, maxBatchBytes)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
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
		if !builder.TryAdd(acc) {
			if err := builder.Flush(client.conn); err != nil {
				return err
			}
			if err := client.expectMsgType(external.Ack); err != nil {
				return err
			}
			builder.TryAdd(acc)
		}
	}
	if !builder.IsEmpty() {
		if err := builder.Flush(client.conn); err != nil {
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

	builder := external.NewTransBatchBuilder(client.config.MaxBatchSize, maxBatchBytes)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")
		if len(columns) < 11 {
			continue
		}
		timestamp, err := time.Parse(transTimestampLayout, columns[0])
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
		if !builder.TryAdd(t) {
			if err := builder.Flush(client.conn); err != nil {
				return err
			}
			if err := client.expectMsgType(external.Ack); err != nil {
				return err
			}
			builder.TryAdd(t)
		}
	}
	if !builder.IsEmpty() {
		if err := builder.Flush(client.conn); err != nil {
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

func (client *Client) recvResults() error {
	files := make([]*os.File, numQueries)
	writers := make([]*csv.Writer, numQueries)

	defer func() {
		for i, f := range files {
			if f != nil {
				writers[i].Flush()
				f.Close()
			}
		}
	}()

	for i := range numQueries {
		path := fmt.Sprintf("%s_%d.csv", client.config.OutputFilePrefix, i+1)
		f, err := os.Create(path)
		if err != nil {
			slog.Debug("Error while creating output file", "query", i+1, "err", err)
			return err
		}
		files[i] = f
		writers[i] = csv.NewWriter(f)
	}

	doneCount := 0
	for doneCount < numQueries {
		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			slog.Debug("Error while reading message type", "err", err)
			return err
		}

		switch msgType {
		case external.ResultBatch:
			results, err := external.ReadResultBatch(client.conn)
			if err != nil {
				slog.Debug("Error while reading result batch", "err", err)
				return err
			}
			client.flushBatchToWriters(results, writers)
			if err := external.WriteAck(client.conn); err != nil {
				slog.Debug("Error while writing ack", "err", err)
				return err
			}

		case external.QueryEOF:
			queryId, err := external.ReadQueryEOF(client.conn)
			if err != nil {
				slog.Debug("Error while reading query EOF", "err", err)
				return err
			}
			idx := int(queryId) - 1
			writers[idx].Flush()
			if err := files[idx].Close(); err != nil {
				slog.Error("While closing output file", "query", queryId, "err", err)
			}
			files[idx] = nil
			if err := external.WriteAck(client.conn); err != nil {
				slog.Debug("Error while writing ack for query EOF", "err", err)
				return err
			}
			doneCount++

		default:
			return errors.New("unexpected message type while receiving results")
		}
	}

	return nil
}

func (client *Client) flushBatchToWriters(results *queryresult.BatchResults, writers []*csv.Writer) {
	for _, r := range results.Query1 {
		writers[0].Write([]string{r.FromBank, r.FromAccount, r.ToBank, r.ToAccount, fmt.Sprintf("%.2f", r.Amount)})
	}
	for _, r := range results.Query2 {
		writers[1].Write([]string{r.BankName, r.FromBank, r.FromAccount, fmt.Sprintf("%.2f", r.Amount)})
	}
	for _, r := range results.Query3 {
		writers[2].Write([]string{r.FromBank, r.FromAccount, fmt.Sprintf("%.2f", r.Amount)})
	}
	for _, r := range results.Query4 {
		writers[3].Write([]string{r.BankId, r.AccountId})
	}
	for _, r := range results.Query5 {
		writers[4].Write([]string{strconv.FormatUint(uint64(r.Qty), 10)})
	}
}
