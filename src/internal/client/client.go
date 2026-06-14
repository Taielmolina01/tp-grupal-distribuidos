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
	"sync/atomic"
	"syscall"
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/csvutil"
	"tp-grupal-distribuidos/internal/common/messageprotocol/tcpproto"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const numQueries = 5

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

	builder := tcpproto.NewAccountBatchBuilder(client.config.MaxBatchSize)
	scanner := bufio.NewScanner(file)

	var cols [5][]byte
	scanner.Scan()
	for scanner.Scan() {
		if csvutil.SplitFields(scanner.Bytes(), cols[:]) < 5 {
			continue
		}
		acc := account.Account{
			BankName:      string(cols[0]),
			BankId:        string(cols[1]),
			AccountNumber: string(cols[2]),
			EntityId:      string(cols[3]),
			EntityName:    string(cols[4]),
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

	if err := tcpproto.WriteEndOfRecords(client.conn); err != nil {
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

	builder := tcpproto.NewTransBatchBuilder(client.config.MaxBatchSize)
	scanner := bufio.NewScanner(file)

	var cols [11][]byte
	scanner.Scan()
	for scanner.Scan() {
		if csvutil.SplitFields(scanner.Bytes(), cols[:]) < 11 {
			continue
		}
		tsUnix, ok := csvutil.ParseTimestampUnix(cols[0])
		if !ok {
			slog.Debug("Error while parsing trans timestamp", "raw", string(cols[0]))
			continue
		}
		amountReceived, err := strconv.ParseFloat(string(cols[5]), 64)
		if err != nil {
			slog.Debug("Error while parsing trans amount received", "err", err)
			continue
		}
		amountPaid, err := strconv.ParseFloat(string(cols[7]), 64)
		if err != nil {
			slog.Debug("Error while parsing trans amount paid", "err", err)
			continue
		}
		t := transfer.Transfer{
			Timestamp:         time.Unix(tsUnix, 0).UTC(),
			FromBank:          string(cols[1]),
			FromBankAccount:   string(cols[2]),
			ToBank:            string(cols[3]),
			ToBankAccount:     string(cols[4]),
			AmountReceived:    amountReceived,
			ReceivingCurrency: string(cols[6]),
			AmountPaid:        amountPaid,
			PaymentCurrency:   string(cols[8]),
			PaymentFormat:     string(cols[9]),
			IsLaundering:      len(cols[10]) > 0 && cols[10][0] == '1',
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

	if err := tcpproto.WriteEndOfRecords(client.conn); err != nil {
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
		msgType, err := tcpproto.ReadMsgType(client.conn)
		if err != nil {
			slog.Debug("Error while reading message type", "err", err)
			return err
		}

		switch msgType {
		case tcpproto.ResultBatch:
			results, err := tcpproto.ReadResultBatch(client.conn)
			if err != nil {
				slog.Debug("Error while reading result batch", "err", err)
				return err
			}
			client.flushBatchToWriters(results, writers)

		case tcpproto.QueryEOF:
			queryId, err := tcpproto.ReadQueryEOF(client.conn)
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
