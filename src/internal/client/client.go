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
	"sync"
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
	config    ClientConfig
	running   atomic.Bool
	sessionID uint32

	connMu sync.Mutex
	conn   net.Conn

	files       []*os.File
	writers     []*csv.Writer
	doneCount   int
	seenResults map[resultKey]struct{}
}

type resultKey struct {
	queryID  uint8
	senderID uint8
	seq      uint64
}

func NewClient(config ClientConfig) (*Client, error) {
	return &Client{config: config}, nil
}

func (client *Client) Run() error {
	client.running.Store(true)
	go client.handleSignals()

	if err := client.setupOutputFiles(); err != nil {
		return err
	}
	defer client.closeOutputFiles()

	backoff := time.Duration(client.config.ConnectionAttemptDelayMs) * time.Millisecond

	for client.running.Load() {
		conn, phase, resumeSeq, err := client.connectAndHandshake()
		if err != nil {
			if !client.running.Load() {
				return nil
			}
			slog.Warn("Connection failed, retrying", "err", err)
			time.Sleep(backoff)
			continue
		}
		client.setConn(conn)

		err = client.runSession(conn, phase, resumeSeq)
		client.closeConn()

		if err == nil {
			return nil
		}
		if !client.running.Load() {
			return nil
		}
		slog.Warn("Session interrupted, reconnecting", "err", err)
		time.Sleep(backoff)
	}
	return nil
}

func (client *Client) connectAndHandshake() (net.Conn, tcpproto.Phase, uint64, error) {
	conn, err := net.Dial("tcp", client.config.ServerHost+":"+client.config.ServerPort)
	if err != nil {
		return nil, 0, 0, err
	}

	if err := tcpproto.WriteHello(conn, client.sessionID); err != nil {
		conn.Close()
		return nil, 0, 0, err
	}
	msgType, err := tcpproto.ReadMsgType(conn)
	if err != nil {
		conn.Close()
		return nil, 0, 0, err
	}
	if msgType != tcpproto.Welcome {
		conn.Close()
		return nil, 0, 0, fmt.Errorf("expected WELCOME message, got %d", msgType)
	}
	sessionID, phase, resumeSeq, err := tcpproto.ReadWelcome(conn)
	if err != nil {
		conn.Close()
		return nil, 0, 0, err
	}

	client.sessionID = sessionID
	slog.Info("Session established", "session_id", sessionID, "phase", phase, "resume_seq", resumeSeq)
	return conn, phase, resumeSeq, nil
}

func (client *Client) runSession(conn net.Conn, phase tcpproto.Phase, resumeSeq uint64) error {
	var wg sync.WaitGroup
	var sendErr, recvErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		recvErr = client.recvResults(conn)
		if recvErr != nil {
			conn.Close()
		}
	}()

	if phase != tcpproto.PhaseResults {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendErr = client.sendRecords(conn, phase, resumeSeq)
			if sendErr != nil {
				conn.Close()
			}
		}()
	}

	wg.Wait()

	if recvErr != nil {
		return recvErr
	}
	return sendErr
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.closeConn()
}

func (client *Client) setConn(conn net.Conn) {
	client.connMu.Lock()
	defer client.connMu.Unlock()
	client.conn = conn
}

func (client *Client) closeConn() {
	client.connMu.Lock()
	defer client.connMu.Unlock()
	if client.conn != nil {
		if err := client.conn.Close(); err != nil {
			slog.Debug("While closing client's socket", "err", err)
		}
		client.conn = nil
	}
}

func (client *Client) sendRecords(conn net.Conn, phase tcpproto.Phase, resumeSeq uint64) error {
	var seq uint64
	skipAccounts := phase != tcpproto.PhaseAccounts
	if err := client.sendAccountRecords(conn, &seq, skipAccounts, resumeSeq); err != nil {
		return err
	}
	if err := client.sendTransRecords(conn, &seq, false, resumeSeq); err != nil {
		return err
	}
	return nil
}

func (client *Client) sendAccountRecords(conn net.Conn, seq *uint64, skip bool, resumeSeq uint64) error {
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

	flush := func() error {
		*seq++
		if skip || *seq <= resumeSeq {
			builder.Reset()
			return nil
		}
		return builder.Flush(conn, *seq)
	}

	var total uint32
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
			if err := flush(); err != nil {
				return err
			}
			if !builder.TryAdd(acc) {
				return fmt.Errorf("account record too large to fit in empty batch")
			}
		}
		total++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("while reading accounts file: %w", err)
	}
	if !builder.IsEmpty() {
		if err := flush(); err != nil {
			return err
		}
	}

	return client.sendEndOfRecords(conn, seq, skip, total)
}

func (client *Client) sendTransRecords(conn net.Conn, seq *uint64, skip bool, resumeSeq uint64) error {
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

	flush := func() error {
		*seq++
		if skip || *seq <= resumeSeq {
			builder.Reset()
			return nil
		}
		return builder.Flush(conn, *seq)
	}

	var total uint32
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
			if err := flush(); err != nil {
				return err
			}
			if !builder.TryAdd(t) {
				return fmt.Errorf("transfer record too large to fit in empty batch")
			}
		}
		total++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("while reading transfers file: %w", err)
	}
	if !builder.IsEmpty() {
		if err := flush(); err != nil {
			return err
		}
	}

	return client.sendEndOfRecords(conn, seq, skip, total)
}

func (client *Client) sendEndOfRecords(conn net.Conn, seq *uint64, skip bool, total uint32) error {
	*seq++
	if skip {
		return nil
	}
	return tcpproto.WriteEndOfRecords(conn, *seq, total)
}

func (client *Client) setupOutputFiles() error {
	client.files = make([]*os.File, numQueries)
	client.writers = make([]*csv.Writer, numQueries)
	client.seenResults = map[resultKey]struct{}{}

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
		client.files[i] = f
		client.writers[i] = csv.NewWriter(f)
		if err := client.writers[i].Write(headers[i]); err != nil {
			slog.Error("While writing header to output file", "query", i+1, "err", err)
			return err
		}
	}
	return nil
}

func (client *Client) closeOutputFiles() {
	for i, f := range client.files {
		if f != nil {
			client.writers[i].Flush()
			if err := f.Close(); err != nil {
				slog.Error("While closing output file", "query", i+1, "err", err)
			}
			client.files[i] = nil
		}
	}
}

func (client *Client) recvResults(conn net.Conn) error {
	for client.doneCount < numQueries {
		msgType, err := tcpproto.ReadMsgType(conn)
		if err != nil {
			slog.Debug("Error while reading message type", "err", err)
			return err
		}

		switch msgType {
		case tcpproto.ResultBatch:
			queryID, senderID, seq, results, err := tcpproto.ReadResultBatch(conn)
			if err != nil {
				slog.Debug("Error while reading result batch", "err", err)
				return err
			}
			if seq != 0 {
				key := resultKey{queryID: queryID, senderID: senderID, seq: seq}
				if _, seen := client.seenResults[key]; seen {
					continue
				}
				client.seenResults[key] = struct{}{}
			}
			client.flushBatchToWriters(results)

		case tcpproto.QueryEOF:
			queryId, err := tcpproto.ReadQueryEOF(conn)
			if err != nil {
				slog.Debug("Error while reading query EOF", "err", err)
				return err
			}
			idx := int(queryId) - 1
			if idx < 0 || idx >= numQueries || client.files[idx] == nil {
				slog.Debug("Ignoring QueryEOF for already-finished query", "query", queryId)
				continue
			}
			client.writers[idx].Flush()
			if err := client.files[idx].Close(); err != nil {
				slog.Error("While closing output file", "query", queryId, "err", err)
			}
			client.files[idx] = nil
			client.doneCount++

		default:
			return errors.New("unexpected message type while receiving results")
		}
	}

	return nil
}

func (client *Client) flushBatchToWriters(results *queryresult.BatchResults) {
	for _, r := range results.Query1 {
		if client.files[0] == nil {
			break
		}
		if err := client.writers[0].Write([]string{r.FromBank, r.FromAccount, r.ToBank, r.ToAccount, fmt.Sprintf("%.2f", r.Amount)}); err != nil {
			slog.Error("While writing to output file", "query", 1, "err", err)
		}
	}
	for _, r := range results.Query2 {
		if client.files[1] == nil {
			break
		}
		if err := client.writers[1].Write([]string{r.FromBank, r.FromAccount, r.BankName, fmt.Sprintf("%.2f", r.Amount)}); err != nil {
			slog.Error("While writing to output file", "query", 2, "err", err)
		}
	}
	for _, r := range results.Query3 {
		if client.files[2] == nil {
			break
		}
		if err := client.writers[2].Write([]string{r.FromBank, r.FromAccount, r.PaymentFormat, fmt.Sprintf("%.2f", r.Amount)}); err != nil {
			slog.Error("While writing to output file", "query", 3, "err", err)
		}
	}
	for _, r := range results.Query4 {
		if client.files[3] == nil {
			break
		}
		if err := client.writers[3].Write([]string{r.BankId, r.AccountNumber}); err != nil {
			slog.Error("While writing to output file", "query", 4, "err", err)
		}
	}
	for _, r := range results.Query5 {
		if client.files[4] == nil {
			break
		}
		if err := client.writers[4].Write([]string{strconv.FormatUint(uint64(r.Qty), 10)}); err != nil {
			slog.Error("While writing to output file", "query", 5, "err", err)
		}
	}
}
