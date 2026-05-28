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
	// TODO: subir a 5 cuando se implementen Q3/Q4/Q5.
	numQueries           = 5
	transTimestampLayout = "2006/01/02 15:04"
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

	recvErrCh := make(chan error, 1)
	go func() {
		recvErrCh <- client.recvResults()
	}()

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

	if err := <-recvErrCh; err != nil {
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

	builder := external.NewAccountBatchBuilder(client.config.MaxBatchSize)
	scanner := bufio.NewScanner(file)

	scanner.Scan() // Skip header line
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
			if !builder.TryAdd(acc) {
				return fmt.Errorf("account record too large to fit in empty batch")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("while reading accounts file: %w", err)
	}
	if !builder.IsEmpty() {
		if err := builder.Flush(client.conn); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	return nil
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

	builder := external.NewTransBatchBuilder(client.config.MaxBatchSize)
	scanner := bufio.NewScanner(file)

	scanner.Scan() // Skip header line
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
		amountReceived, err := strconv.ParseFloat(columns[5], 64)
		if err != nil {
			slog.Debug("Error while parsing trans amount received", "err", err)
			continue
		}
		amountPaid, err := strconv.ParseFloat(columns[7], 64)
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
			AmountReceived:    amountReceived,
			ReceivingCurrency: columns[6],
			AmountPaid:        amountPaid,
			PaymentCurrency:   columns[8],
			PaymentFormat:     columns[9],
			IsLaundering:      columns[10] == "1",
		}
		if !builder.TryAdd(t) {
			if err := builder.Flush(client.conn); err != nil {
				return err
			}
			if !builder.TryAdd(t) {
				return fmt.Errorf("transfer record too large to fit in empty batch")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("while reading transfers file: %w", err)
	}
	if !builder.IsEmpty() {
		if err := builder.Flush(client.conn); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	return nil
}

func (client *Client) recvResults() error {
	files := make([]*os.File, numQueries)
	writers := make([]*csv.Writer, numQueries)

	defer func() {
		for i, f := range files {
			if f != nil {
				writers[i].Flush()
				if err := f.Close(); err != nil {
					slog.Error("While closing output file", "query", i+1, "err", err)
				}
			}
		}
	}()

	headers := [][]string{
		queryresult.Query1Result{}.GetHeaders(),
		queryresult.Query2Result{}.GetHeaders(),
		queryresult.Query3Result{}.GetHeaders(),
		queryresult.Query4Result{}.GetHeaders(),
		queryresult.Query5Result{}.GetHeaders(),
	}

	for i := range numQueries {
		path := fmt.Sprintf("%s_%d.csv", client.config.OutputFilePrefix, i+1)
		f, err := os.Create(path)
		if err != nil {
			slog.Debug("Error while creating output file", "query", i+1, "err", err)
			return err
		}
		files[i] = f
		writers[i] = csv.NewWriter(f)
		if err := writers[i].Write(headers[i]); err != nil {
			slog.Error("While writing header to output file", "query", i+1, "err", err)
			return err
		}
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
			doneCount++

		default:
			return errors.New("unexpected message type while receiving results")
		}
	}

	return nil
}

func (client *Client) flushBatchToWriters(results *queryresult.BatchResults, writers []*csv.Writer) {
	for _, r := range results.Query1 {
		if err := writers[0].Write([]string{r.FromBank, r.FromAccount, r.ToBank, r.ToAccount, fmt.Sprintf("%.2f", r.Amount)}); err != nil {
			slog.Error("While writing to output file", "query", 1, "err", err)
		}
	}
	for _, r := range results.Query2 {
		if err := writers[1].Write([]string{r.FromBank, r.FromAccount, r.BankName, fmt.Sprintf("%.2f", r.Amount)}); err != nil {
			slog.Error("While writing to output file", "query", 2, "err", err)
		}
	}
	for _, r := range results.Query3 {
		if err := writers[2].Write([]string{r.FromBank, r.FromAccount, r.PaymentFormat, fmt.Sprintf("%.2f", r.Amount)}); err != nil {
			slog.Error("While writing to output file", "query", 3, "err", err)
		}
	}
	for _, r := range results.Query4 {
		if err := writers[3].Write([]string{r.BankId, r.AccountNumber}); err != nil {
			slog.Error("While writing to output file", "query", 4, "err", err)
		}
	}
	for _, r := range results.Query5 {
		if err := writers[4].Write([]string{strconv.FormatUint(uint64(r.Qty), 10)}); err != nil {
			slog.Error("While writing to output file", "query", 5, "err", err)
		}
	}
}
