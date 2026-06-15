package gateway

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"tp-grupal-distribuidos/internal/clientregistry"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/tcpproto"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/shard"
)

type GatewayConfig struct {
	AccountQueues        []string
	TransfersExchange    string
	TransfersRoutingKeys []string
	ResultsQueue         string
	ServerHost           string
	ServerPort           string
	MomHost              string
	MomPort              int
	MaxBatchSize         int
	QueryEOFsExpected    map[uint8]int
	SessionStorePath     string
}

const gatewaySenderID uint8 = 0

type Gateway struct {
	registry          clientregistry.ClientRegistry
	accountQueues     []middleware.Middleware
	transfersExchange middleware.Middleware
	resultsQueue      middleware.Middleware
	listener          net.Listener
	running           atomic.Bool
	sessions          *sessionStore
	queryEOFsExpected map[uint8]int
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	//Quiza las keys deberían ser config, quizá no. Quien sabe
	accountQueues := make([]middleware.Middleware, 0, len(config.AccountQueues))
	for _, queue := range config.AccountQueues {
		accountQueue, err := middleware.CreateQueueMiddleware(queue, connSettings)
		if err != nil {
			return nil, err
		}
		accountQueues = append(accountQueues, accountQueue)
	}

	// Las keys acá vienen x config xq son dinámicas
	// Se requiere sharding

	transfersExchange, err := middleware.CreateExchangeMiddleware(
		config.TransfersExchange,
		"",
		config.TransfersRoutingKeys,
		connSettings,
	)
	if err != nil {
		for _, q := range accountQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing accounts queue", "err", err)
			}
		}
		return nil, err
	}

	resultsQueue, err := middleware.CreateQueueMiddleware(config.ResultsQueue, connSettings)
	if err != nil {
		for _, q := range accountQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing accounts queue", "err", err)
			}
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		for _, q := range accountQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing accounts queue", "err", err)
			}
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		if err := resultsQueue.Close(); err != nil {
			slog.Error("While closing results queue", "err", err)
		}
		return nil, err
	}

	sessions, err := newSessionStore(config.SessionStorePath)
	if err != nil {
		for _, q := range accountQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing accounts queue", "err", err)
			}
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		if err := resultsQueue.Close(); err != nil {
			slog.Error("While closing results queue", "err", err)
		}
		if err := listener.Close(); err != nil {
			slog.Error("While closing acceptor socket", "err", err)
		}
		return nil, err
	}

	gateway := &Gateway{
		accountQueues:     accountQueues,
		transfersExchange: transfersExchange,
		resultsQueue:      resultsQueue,
		listener:          listener,
		sessions:          sessions,
		queryEOFsExpected: config.QueryEOFsExpected,
	}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.close()

	go func() {
		if err := gateway.resultsQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			gateway.handleClientResponse(msg, ack, nack)
		}); err != nil {
			slog.Error("While consuming results queue", "err", err)
		}
	}()
	go gateway.handleSignals()

	slog.Info("Accepting connections...")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			if closeErr := gateway.listener.Close(); closeErr != nil {
				slog.Error("While closing acceptor socket after accept error", "err", closeErr)
			}
			return err
		}

		go gateway.handleClientRequest(conn)
	}

	if err := gateway.resultsQueue.StopConsuming(); err != nil {
		slog.Error("While stopping results queue consumer", "err", err)
	}

	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, client := range clients {
			if err := client.Conn.Close(); err != nil {
				slog.Error("While closing client's socket", "err", err)
			}
		}
	})

	return nil
}

func (gateway *Gateway) close() {
	for _, q := range gateway.accountQueues {
		if err := q.Close(); err != nil {
			slog.Error("While closing account queue", "err", err)
		}
	}
	if err := gateway.transfersExchange.Close(); err != nil {
		slog.Error("While closing transfers exchange", "err", err)
	}
	if err := gateway.resultsQueue.Close(); err != nil {
		slog.Error("While closing results queue", "err", err)
	}
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	if err := gateway.listener.Close(); err != nil {
		slog.Error("While closing acceptor socket", "err", err)
	}
}

func (gateway *Gateway) handshake(conn net.Conn, r io.Reader) (clientregistry.ClientState, tcpproto.Phase, error) {
	msgType, err := tcpproto.ReadMsgType(r)
	if err != nil {
		return clientregistry.ClientState{}, 0, err
	}
	if msgType != tcpproto.Hello {
		return clientregistry.ClientState{}, 0, fmt.Errorf("expected HELLO message, got %d", msgType)
	}
	sessionID, err := tcpproto.ReadHello(r)
	if err != nil {
		return clientregistry.ClientState{}, 0, err
	}

	clientID := int(sessionID)
	phase := tcpproto.PhaseAccounts

	if session, ok := gateway.sessions.session(clientID); sessionID != 0 && ok {
		phase = session.phase
		gateway.registry.RemoveByID(clientID)
		slog.Info("Client resuming session", "client_id", clientID, "phase", phase)
	} else {
		if sessionID != 0 {
			slog.Warn("Unknown session, assigning new one", "requested_session_id", sessionID)
		}
		clientID, err = gateway.sessions.allocateClient()
		if err != nil {
			return clientregistry.ClientState{}, 0, err
		}
		slog.Info("Client connected", "client_id", clientID)
	}

	if err := tcpproto.WriteWelcome(conn, uint32(clientID), phase, 0); err != nil {
		return clientregistry.ClientState{}, 0, err
	}

	client := clientregistry.ClientState{ID: clientID, Conn: conn}
	gateway.registry.Add(client)
	return client, phase, nil
}

func (gateway *Gateway) handleClientRequest(conn net.Conn) {
	r := bufio.NewReaderSize(conn, 64*1024)

	client, phase, err := gateway.handshake(conn, r)
	if err != nil {
		slog.Debug("While handshaking with client", "err", err)
		if closeErr := conn.Close(); closeErr != nil {
			slog.Error("While closing client connection after handshake error", "err", closeErr)
		}
		return
	}

	completed := false
	defer func() {
		if !completed {
			slog.Info("Client disconnected before completing ingest", "client_id", client.ID)
			gateway.closeClient(client.ID)
		}
	}()

	if phase == tcpproto.PhaseResults {
		completed = true
		return
	}

	if phase == tcpproto.PhaseAccounts {
		if !gateway.runAccountsPhase(client, r) {
			return
		}
	}

	if !gateway.runTransfersPhase(client, r) {
		return
	}

	completed = true
}

func (gateway *Gateway) runAccountsPhase(client clientregistry.ClientState, r io.Reader) bool {
	for {
		msgType, err := tcpproto.ReadMsgType(r)
		if err != nil {
			slog.Debug("While reading message type (accounts phase)", "err", err)
			return false
		}

		switch msgType {
		case tcpproto.AccountBatch:
			if err := gateway.handleAccountBatch(client, r); err != nil {
				slog.Debug("While handling account batch", "err", err)
				return false
			}

		case tcpproto.EndOfRecords:
			if err := gateway.handleEndOfAccounts(client, r); err != nil {
				slog.Debug("While handling EOF accounts", "err", err)
				return false
			}
			return true

		default:
			slog.Debug("Unexpected message type in accounts phase", "got", msgType)
			return false
		}
	}
}

func (gateway *Gateway) runTransfersPhase(client clientregistry.ClientState, r io.Reader) bool {
	for {
		msgType, err := tcpproto.ReadMsgType(r)
		if err != nil {
			slog.Debug("While reading message type (transfers phase)", "err", err)
			return false
		}

		switch msgType {
		case tcpproto.TransBatch:
			if err := gateway.handleTransBatch(client, r); err != nil {
				slog.Debug("While handling trans batch", "err", err)
				return false
			}

		case tcpproto.EndOfRecords:
			if err := gateway.handleEndOfTransfers(client, r); err != nil {
				slog.Debug("While handling EOF transfers", "err", err)
				return false
			}
			return true

		default:
			slog.Debug("Unexpected message type in transfers phase", "got", msgType)
			return false
		}
	}
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	reader, info, err := batch.ReadHeader(msg.Body)
	if err != nil {
		slog.Error("While deserializing result header", "err", err)
		nack()
		return
	}

	client, ok := gateway.findClient(info.ClientID)
	if !ok {
		slog.Warn("Result for unknown client", "client_id", info.ClientID)
		ack()
		return
	}

	if info.EOF {
		shouldWrite, shouldClose, err := gateway.markQueryEOF(info.ClientID, info.QueryID)
		if err != nil {
			slog.Error("While marking QueryEOF", "client_id", info.ClientID, "query_id", info.QueryID, "err", err)
			nack()
			return
		}

		if !shouldWrite {
			ack()
			return
		}

		if err := tcpproto.WriteQueryEOF(client.Conn, info.QueryID); err != nil {
			slog.Error("While writing QueryEOF", "client_id", info.ClientID, "query_id", info.QueryID, "err", err)
			nack()
			return
		}
		if shouldClose {
			gateway.closeClient(info.ClientID)
		}
		ack()
		return
	}

	if err := gateway.forwardResult(client, info, reader); err != nil {
		slog.Error("While forwarding result batch", "client_id", info.ClientID, "query_id", info.QueryID, "err", err)
		nack()
		return
	}
	ack()
}

func (gateway *Gateway) forwardResult(client clientregistry.ClientState, info batch.Info, r *wire.Reader) error {
	var payload []byte
	var count int

	switch info.QueryID {
	case queryresult.Query1ID:
		recs, err := batch.ReadRecords(r, records.Query1ResultCodec)
		if err != nil {
			return err
		}
		payload, count = tcpproto.AppendQuery1Results(nil, recs), len(recs)
	case queryresult.Query2ID:
		recs, err := batch.ReadRecords(r, records.Query2ResultCodec)
		if err != nil {
			return err
		}
		payload, count = tcpproto.AppendQuery2Results(nil, recs), len(recs)
	case queryresult.Query3ID:
		recs, err := batch.ReadRecords(r, records.Query3ResultCodec)
		if err != nil {
			return err
		}
		payload, count = tcpproto.AppendQuery3Results(nil, recs), len(recs)
	case queryresult.Query4ID:
		recs, err := batch.ReadRecords(r, records.Query4ResultCodec)
		if err != nil {
			return err
		}
		payload, count = tcpproto.AppendQuery4Results(nil, recs), len(recs)
	case queryresult.Query5ID:
		recs, err := batch.ReadRecords(r, records.Query5ResultCodec)
		if err != nil {
			return err
		}
		payload, count = tcpproto.AppendQuery5Results(nil, recs), len(recs)
	default:
		return fmt.Errorf("unknown query id: %d", info.QueryID)
	}

	return tcpproto.WriteResultBatch(client.Conn, info.QueryID, info.SenderID, info.Seq, uint16(count), payload)
}

func (gateway *Gateway) markQueryEOF(clientID int, queryID uint8) (shouldWrite bool, shouldClose bool, err error) {
	counts, err := gateway.sessions.incEOF(clientID, queryID)
	if err != nil {
		return false, false, err
	}

	expected := gateway.queryEOFsExpected[queryID]
	if expected == 0 {
		expected = 1
	}
	count := counts[queryID]
	slog.Info("Received QueryEOF", "client_id", clientID, "query_id", queryID,
		"count", count, "expected", expected)

	shouldWrite = count == expected

	completed := 0
	for q, c := range counts {
		exp := gateway.queryEOFsExpected[q]
		if exp == 0 {
			exp = 1
		}
		if c >= exp {
			completed++
		}
	}
	shouldClose = completed >= len(gateway.queryEOFsExpected)
	return shouldWrite, shouldClose, nil
}

func (gateway *Gateway) closeClient(clientID int) {
	if client, ok := gateway.findClient(clientID); ok {
		if err := client.Conn.Close(); err != nil {
			slog.Error("While closing client connection", "client_id", clientID, "err", err)
		}
	}
	gateway.registry.RemoveByID(clientID)

	if err := gateway.sessions.removeClient(clientID); err != nil {
		slog.Error("While removing client from session store", "client_id", clientID, "err", err)
	}

	slog.Info("Client closed", "client_id", clientID)
}

func (gateway *Gateway) findClient(clientID int) (clientregistry.ClientState, bool) {
	var found clientregistry.ClientState
	var ok bool
	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, c := range clients {
			if c.ID == clientID {
				found = c
				ok = true
				return
			}
		}
	})
	return found, ok
}

func (gateway *Gateway) sendEOF(clientID int, seq uint64, total uint32, targets ...middleware.Middleware) error {
	body := batch.WriteEOF(clientID, 0, gatewaySenderID, seq, total)
	var errs []error
	for _, t := range targets {
		if err := t.Send(middleware.Message{Body: body}); err != nil {
			slog.Error("While sending EOF", "err", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (gateway *Gateway) handleAccountBatch(client clientregistry.ClientState, r io.Reader) error {
	seq, accounts, err := tcpproto.ReadAccountBatch(r)
	if err != nil {
		slog.Debug("While reading ACCOUNT_BATCH", "err", err)
		return err
	}

	byShard := make(map[int][]account.Account)
	for _, acc := range accounts {
		idx := shard.CalculateIndexForShard(
			client.ID,
			normalizer.NormalizeBankID(acc.BankId),
			len(gateway.accountQueues),
		)
		byShard[idx] = append(byShard[idx], acc)
	}
	for idx, group := range byShard {
		body := batch.Write(client.ID, 0, gatewaySenderID, seq, group, records.AccountCodec)
		if err := gateway.accountQueues[idx].Send(middleware.Message{Body: body}); err != nil {
			slog.Debug("While sending accounts batch", "err", err)
			return err
		}
	}
	return nil
}

func (gateway *Gateway) handleTransBatch(client clientregistry.ClientState, r io.Reader) error {
	seq, count, payload, err := tcpproto.ReadRawTransBatch(r)
	if err != nil {
		slog.Debug("While reading TRANS_BATCH", "err", err)
		return err
	}
	body := batch.WriteRaw(client.ID, 0, gatewaySenderID, seq, count, payload)
	if err := gateway.transfersExchange.Send(middleware.Message{Body: body}); err != nil {
		slog.Debug("While sending transfers batch", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleEndOfAccounts(client clientregistry.ClientState, r io.Reader) error {
	seq, total, err := tcpproto.ReadEndOfRecords(r)
	if err != nil {
		slog.Debug("While reading EOF accounts", "err", err)
		return err
	}
	slog.Info("Received EOF message", "kind", "accounts", "client_id", client.ID, "total", total)
	if err := gateway.sendEOF(client.ID, seq, total, gateway.accountQueues...); err != nil {
		return err
	}
	return gateway.sessions.setPhase(client.ID, tcpproto.PhaseTransfers)
}

func (gateway *Gateway) handleEndOfTransfers(client clientregistry.ClientState, r io.Reader) error {
	seq, total, err := tcpproto.ReadEndOfRecords(r)
	if err != nil {
		slog.Debug("While reading EOF transfers", "err", err)
		return err
	}
	slog.Info("Received EOF message", "kind", "transfers", "client_id", client.ID, "total", total)
	if err := gateway.sendEOF(client.ID, seq, total, gateway.transfersExchange); err != nil {
		return err
	}
	return gateway.sessions.setPhase(client.ID, tcpproto.PhaseResults)
}
