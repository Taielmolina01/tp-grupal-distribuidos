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
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"tp-grupal-distribuidos/internal/clientregistry"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/tcpproto"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"

	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/shard"
)

type GatewayConfig struct {
	AccountQueues      []string
	TransfersClusters  []shard.ClusterConfig
	ResultsQueue       string
	ServerHost         string
	ServerPort         string
	MomHost            string
	MomPort            int
	MaxBatchSize       int
	QueryEOFsExpected  map[uint8]int
	SessionStorePath   string
	SeqCheckpointEvery uint64
	ClientTimeout      time.Duration
	ReaperInterval     time.Duration
}

const gatewaySenderID uint8 = 0

const readBufferSize = 64 * 1024

type Gateway struct {
	registry           clientregistry.ClientRegistry
	accountQueues      []middleware.Middleware
	transferClusters   []newmiddleware.ShardedCluster
	resultsQueue       middleware.Middleware
	listener           net.Listener
	running            atomic.Bool
	sessions           *sessionStore
	buffer             *resultBuffer
	queryEOFsExpected  map[uint8]int
	seqCheckpointEvery uint64
	clientTimeout      time.Duration
	reaperInterval     time.Duration
	disconnectMu       sync.Mutex
	disconnectedAt     map[int]time.Time
	transfersTrackers  map[int]*outputtracker.OutputTracker
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	newConnSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	accountQueues := make([]middleware.Middleware, 0, len(config.AccountQueues))
	for _, queue := range config.AccountQueues {
		accountQueue, err := middleware.CreateQueueMiddleware(queue, connSettings)
		if err != nil {
			for _, q := range accountQueues {
				if closeErr := q.Close(); closeErr != nil {
					slog.Error("While closing accounts queue", "err", closeErr)
				}
			}
			return nil, err
		}
		accountQueues = append(accountQueues, accountQueue)
	}

	clusters := make([]newmiddleware.ShardedCluster, 0, len(config.TransfersClusters))
	for _, c := range config.TransfersClusters {
		m, err := newmiddleware.NewShardedMiddleware(newConnSettings, c.Prefix, "", "")
		if err != nil {
			for _, q := range accountQueues {
				if closeErr := q.Close(); closeErr != nil {
					slog.Error("While closing accounts queue", "err", closeErr)
				}
			}
			for _, cl := range clusters {
				if closeErr := cl.Middleware.Close(); closeErr != nil {
					slog.Error("While closing transfer cluster middleware", "err", closeErr)
				}
			}
			return nil, err
		}
		clusters = append(clusters, newmiddleware.ShardedCluster{
			Middleware: m,
			Hasher:     shard.New(c.NodeCount),
		})
	}

	//TODO PASAR A NUEVO MIDDLEAWRE
	resultsQueue, err := middleware.CreateQueueMiddleware(config.ResultsQueue, connSettings)
	if err != nil {
		for _, q := range accountQueues {
			if closeErr := q.Close(); closeErr != nil {
				slog.Error("While closing accounts queue", "err", closeErr)
			}
		}
		for _, cl := range clusters {
			if closeErr := cl.Middleware.Close(); closeErr != nil {
				slog.Error("While closing transfer cluster middleware", "err", closeErr)
			}
		}
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		for _, q := range accountQueues {
			if closeErr := q.Close(); closeErr != nil {
				slog.Error("While closing accounts queue", "err", closeErr)
			}
		}
		for _, cl := range clusters {
			if closeErr := cl.Middleware.Close(); closeErr != nil {
				slog.Error("While closing transfer cluster middleware", "err", closeErr)
			}
		}
		if closeErr := resultsQueue.Close(); closeErr != nil {
			slog.Error("While closing results queue", "err", closeErr)
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
		for _, cl := range clusters {
			if closeErr := cl.Middleware.Close(); closeErr != nil {
				slog.Error("While closing transfer cluster middleware", "err", closeErr)
			}
		}
		if err := resultsQueue.Close(); err != nil {
			slog.Error("While closing results queue", "err", err)
		}
		if err := listener.Close(); err != nil {
			slog.Error("While closing acceptor socket", "err", err)
		}
		return nil, err
	}

	buffer, err := newResultBuffer(filepath.Join(filepath.Dir(config.SessionStorePath), "result_buffers"))
	if err != nil {
		for _, q := range accountQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing accounts queue", "err", err)
			}
		}
		for _, cl := range clusters {
			if closeErr := cl.Middleware.Close(); closeErr != nil {
				slog.Error("While closing transfer cluster middleware", "err", closeErr)
			}
		}
		if err := resultsQueue.Close(); err != nil {
			slog.Error("While closing results queue", "err", err)
		}
		if err := listener.Close(); err != nil {
			slog.Error("While closing acceptor socket", "err", err)
		}
		return nil, err
	}

	checkpointEvery := config.SeqCheckpointEvery
	if checkpointEvery == 0 {
		checkpointEvery = 1
	}

	gateway := &Gateway{
		accountQueues:      accountQueues,
		transferClusters:   clusters,
		resultsQueue:       resultsQueue,
		listener:           listener,
		sessions:           sessions,
		buffer:             buffer,
		queryEOFsExpected:  config.QueryEOFsExpected,
		seqCheckpointEvery: checkpointEvery,
		clientTimeout:      config.ClientTimeout,
		reaperInterval:     config.ReaperInterval,
		disconnectedAt:     map[int]time.Time{},
		transfersTrackers:  map[int]*outputtracker.OutputTracker{},
	}

	now := time.Now()
	for _, clientID := range sessions.sessionIDs() {
		gateway.disconnectedAt[clientID] = now
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
	go gateway.reapDeadClients()

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

		enableKeepAlive(conn)
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
	for _, cl := range gateway.transferClusters {
		if err := cl.Middleware.Close(); err != nil {
			slog.Error("While closing transfer cluster middleware", "err", err)
		}
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
	var resumeSeq uint64

	if session, ok := gateway.sessions.session(clientID); sessionID != 0 && ok {
		phase = session.phase
		resumeSeq = session.confirmedSeq[phase]
		gateway.registry.RemoveByID(clientID)
		gateway.clearDisconnected(clientID)
		slog.Info("Client resuming session", "client_id", clientID, "phase", phase, "resume_seq", resumeSeq)
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

	if err := tcpproto.WriteWelcome(conn, uint32(clientID), phase, resumeSeq); err != nil {
		return clientregistry.ClientState{}, 0, err
	}

	client := clientregistry.ClientState{ID: clientID, Conn: conn}

	lock := gateway.buffer.lock(clientID)
	lock.Lock()
	defer lock.Unlock()
	if err := gateway.buffer.flush(clientID, func(body []byte) error {
		return gateway.deliverResult(client, body)
	}); err != nil {
		return clientregistry.ClientState{}, 0, err
	}
	gateway.registry.Add(client)
	return client, phase, nil
}

func (gateway *Gateway) handleClientRequest(conn net.Conn) {
	r := bufio.NewReaderSize(conn, readBufferSize)

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
			gateway.markDisconnected(client)
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
	var lastCheckpoint uint64
	for {
		msgType, err := tcpproto.ReadMsgType(r)
		if err != nil {
			slog.Debug("While reading message type (accounts phase)", "err", err)
			return false
		}

		switch msgType {
		case tcpproto.AccountBatch:
			seq, err := gateway.handleAccountBatch(client, r)
			if err != nil {
				slog.Debug("While handling account batch", "err", err)
				return false
			}
			gateway.maybeCheckpoint(client.ID, tcpproto.PhaseAccounts, seq, &lastCheckpoint)

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
	var lastCheckpoint uint64
	for {
		msgType, err := tcpproto.ReadMsgType(r)
		if err != nil {
			slog.Debug("While reading message type (transfers phase)", "err", err)
			return false
		}

		switch msgType {
		case tcpproto.TransBatch:
			seq, err := gateway.handleTransBatch(client, r)
			if err != nil {
				slog.Debug("While handling trans batch", "err", err)
				return false
			}
			gateway.maybeCheckpoint(client.ID, tcpproto.PhaseTransfers, seq, &lastCheckpoint)

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
	_, info, err := batch.ReadHeader(msg.Body)
	if err != nil {
		slog.Error("While deserializing result header", "err", err)
		nack()
		return
	}

	lock := gateway.buffer.lock(info.ClientID)
	lock.Lock()
	defer lock.Unlock()

	if client, ok := gateway.findClient(info.ClientID); ok {
		err := gateway.deliverResult(client, msg.Body)
		if err == nil {
			ack() //efectivamente se entrego, le aviso a rabbit que descarte
			return
		}
		slog.Debug("Delivery failed, treating client as disconnected", "client_id", info.ClientID, "err", err)
		gateway.markDisconnected(client)
	}

	if _, alive := gateway.sessions.session(info.ClientID); alive {
		if err := gateway.buffer.append(info.ClientID, msg.Body); err != nil {
			slog.Error("While buffering result for disconnected client", "client_id", info.ClientID, "err", err)
			nack() //fallo la escritura, le aviso a rabbit que no descarte
			return
		}
		ack() //efectivamente se escribio, le aviso a rabbit que descarte
		return
	}

	slog.Warn("Result for dead or unknown client, dropping", "client_id", info.ClientID)
	ack() //estado muerto o desconocido, no tiene sentido guardar esto, le aviso a rabbit que descarte
}

func (gateway *Gateway) deliverResult(client clientregistry.ClientState, body []byte) error {
	reader, info, err := batch.ReadHeader(body)
	if err != nil {
		return err
	}

	if info.EOF {
		shouldWrite, shouldClose, err := gateway.markQueryEOF(info.ClientID, info.QueryID, info.SenderID)
		if err != nil {
			return err
		}
		if !shouldWrite {
			return nil
		}
		if err := tcpproto.WriteQueryEOF(client.Conn, info.QueryID); err != nil {
			return err
		}
		if shouldClose {
			gateway.closeClient(info.ClientID)
		}
		return nil
	}

	return gateway.forwardResult(client, info, reader)
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

func (gateway *Gateway) markQueryEOF(clientID int, queryID uint8, senderID uint8) (shouldWrite bool, shouldClose bool, err error) {
	counts, err := gateway.sessions.incEOF(clientID, queryID, senderID)
	if err != nil {
		return false, false, err
	}
	if counts == nil {
		return false, false, nil
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
	if err := gateway.buffer.remove(clientID); err != nil {
		slog.Error("While removing client result buffer", "client_id", clientID, "err", err)
	}

	slog.Info("Client closed", "client_id", clientID)
}

func enableKeepAlive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     5 * time.Second,
		Interval: 3 * time.Second,
		Count:    3,
	}); err != nil {
		slog.Error("While enabling TCP keepalive", "err", err)
	}
}

func (gateway *Gateway) markDisconnected(client clientregistry.ClientState) {
	if err := client.Conn.Close(); err != nil {
		slog.Debug("While closing disconnected client connection", "client_id", client.ID, "err", err)
	}
	if !gateway.registry.RemoveByConn(client.ID, client.Conn) {
		return
	}
	gateway.disconnectMu.Lock()
	gateway.disconnectedAt[client.ID] = time.Now()
	gateway.disconnectMu.Unlock()
	slog.Info("Client disconnected, starting death timer", "client_id", client.ID)
}

func (gateway *Gateway) clearDisconnected(clientID int) {
	gateway.disconnectMu.Lock()
	delete(gateway.disconnectedAt, clientID)
	gateway.disconnectMu.Unlock()
}

func (gateway *Gateway) deadClients() []int {
	gateway.disconnectMu.Lock()
	defer gateway.disconnectMu.Unlock()
	now := time.Now()
	var dead []int
	for clientID, since := range gateway.disconnectedAt {
		if now.Sub(since) >= gateway.clientTimeout {
			dead = append(dead, clientID)
		}
	}
	return dead
}

func (gateway *Gateway) reapDeadClients() {
	ticker := time.NewTicker(gateway.reaperInterval)
	defer ticker.Stop()
	for gateway.running.Load() {
		<-ticker.C
		for _, clientID := range gateway.deadClients() {
			gateway.abortClient(clientID)
		}
		for _, clientID := range gateway.sessions.pendingAbortIDs() {
			gateway.dispatchAbort(clientID)
		}
	}
}

func (gateway *Gateway) abortClient(clientID int) {
	slog.Info("Client declared dead (no reconnect within timeout)", "client_id", clientID)
	gateway.clearDisconnected(clientID)
	if err := gateway.sessions.moveToPendingAbort(clientID); err != nil {
		slog.Error("While moving client to pending abort", "client_id", clientID, "err", err)
	}
	if err := gateway.buffer.remove(clientID); err != nil {
		slog.Error("While removing client result buffer", "client_id", clientID, "err", err)
	}
}

func (gateway *Gateway) dispatchAbort(clientID int) {
	if err := gateway.sendAbort(clientID); err != nil {
		slog.Error("While sending abort, will retry", "client_id", clientID, "err", err)
		return
	}
	if err := gateway.sessions.clearPendingAbort(clientID); err != nil {
		slog.Error("While clearing pending abort", "client_id", clientID, "err", err)
		return
	}
	slog.Info("Client aborted, notified pipeline", "client_id", clientID)
}

func (gateway *Gateway) sendAbort(clientID int) error {
	body := batch.WriteAbort(clientID, gatewaySenderID)
	rk := newmiddleware.BroadcastRoutingKey
	var errs []error
	for _, cluster := range gateway.transferClusters {
		if err := cluster.Middleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
			slog.Debug("While sending transfers batch", "err", err)
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
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
	slog.Info("EOF SENT", "seq", seq, "total", total)
	for _, t := range targets {
		if err := t.Send(middleware.Message{Body: body}); err != nil {
			slog.Error("While sending EOF", "err", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (gateway *Gateway) maybeCheckpoint(clientID int, phase tcpproto.Phase, seq uint64, lastCheckpoint *uint64) {
	if seq-*lastCheckpoint < gateway.seqCheckpointEvery {
		return
	}
	if err := gateway.sessions.advanceConfirmedSeq(clientID, phase, seq); err != nil {
		slog.Error("While checkpointing confirmed seq", "client_id", clientID, "phase", phase, "err", err)
		return
	}
	*lastCheckpoint = seq
}

func (gateway *Gateway) handleAccountBatch(client clientregistry.ClientState, r io.Reader) (uint64, error) {
	seq, accounts, err := tcpproto.ReadAccountBatch(r)
	if err != nil {
		slog.Debug("While reading ACCOUNT_BATCH", "err", err)
		return 0, err
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
			return 0, err
		}
	}
	return seq, nil
}

func (gateway *Gateway) handleTransBatch(client clientregistry.ClientState, r io.Reader) (uint64, error) {
	//mandarlo shardeado
	seq, count, payload, err := tcpproto.ReadRawTransBatch(r)
	if err != nil {
		slog.Debug("While reading TRANS_BATCH", "err", err)
		return 0, err
	}
	body := batch.WriteRaw(client.ID, 0, gatewaySenderID, seq, count, payload)
	tracker := gateway.transfersTrackerFor(client.ID)

	for ci, cluster := range gateway.transferClusters {
		slog.Info("LUCHO GOY")

		rk := fmt.Sprintf("shard-%d", cluster.Hasher.ShardFor(client.ID, strconv.FormatUint(seq, 10)))
		trackerKey := fmt.Sprintf("%d_%s", ci, rk)
		if err := cluster.Middleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
			slog.Debug("While sending transfers batch", "err", err)
			return 0, err
		}
		tracker.RegisterBatch(trackerKey)
	}

	return seq, nil
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
	//mandarlo shardeado
	seq, _, err := tcpproto.ReadEndOfRecords(r)
	if err != nil {
		slog.Debug("While reading EOF transfers", "err", err)
		return err
	}
	slog.Info("Received EOF message", "kind", "transfers", "client_id", client.ID)

	tracker := gateway.transfersTrackerFor(client.ID)

	for ci, cluster := range gateway.transferClusters {
		for i := range cluster.Hasher.TotalShards() {
			rk := fmt.Sprintf("shard-%d", i)
			trackerKey := fmt.Sprintf("%d_%s", ci, rk)
			total := tracker.CountFor(trackerKey)
			eofBody := batch.WriteEOF(client.ID, 0, gatewaySenderID, seq, uint32(total))
			if err := cluster.Middleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
				slog.Debug("While sending transfers batch", "err", err)
				return err
			}
			tracker.RegisterBatch(trackerKey)
		}
	}
	return gateway.sessions.setPhase(client.ID, tcpproto.PhaseResults)
}

func (gateway *Gateway) transfersTrackerFor(clientID int) *outputtracker.OutputTracker {
	t, ok := gateway.transfersTrackers[clientID]
	if !ok {
		t = outputtracker.New()
		gateway.transfersTrackers[clientID] = t
	}
	return t
}
