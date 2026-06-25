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
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/tcpproto"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/shard"
)

type GatewayConfig struct {
	TransfersClusters         []shard.ClusterConfig
	ResultsQueue              string
	ServerHost                string
	ServerPort                string
	MomHost                   string
	MomPort                   int
	MaxBatchSize              int
	QueryEOFsExpected         map[uint8]int
	SessionStorePath          string
	SeqCheckpointEvery        uint64
	ClientTimeout             time.Duration
	ReaperInterval            time.Duration
	ResultsBatchFlushInterval time.Duration
	AccountsClusterPrefix     string
	AccountsClusterAmount     int
}

const gatewaySenderID uint8 = 0

const readBufferSize = 64 * 1024

type Gateway struct {
	registry                  clientregistry.ClientRegistry
	transferClusters          []middleware.ShardedCluster
	resultsQueue              middleware.Middleware
	listener                  net.Listener
	running                   atomic.Bool
	sessions                  *sessionStore
	buffer                    *resultBuffer
	queryEOFsExpected         map[uint8]int
	seqCheckpointEvery        uint64
	clientTimeout             time.Duration
	reaperInterval            time.Duration
	disconnectMu              sync.Mutex
	disconnectedAt            map[int]time.Time
	transfersMu               sync.Mutex
	transfersTrackers         map[int]*outputtracker.OutputTracker
	transfersCountedSeq       map[int]uint64
	pendingClose              map[int]struct{}
	resultsBatchFlushInterval time.Duration
	accountsCluster           middleware.Middleware
	accountsAmount            int
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	newConnSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	accountsCluster, err := middleware.NewShardedMiddleware(newConnSettings, config.AccountsClusterPrefix, "", "")
	if err != nil {
		return nil, err
	}

	clusters := make([]middleware.ShardedCluster, 0, len(config.TransfersClusters))
	for _, c := range config.TransfersClusters {
		m, err := middleware.NewShardedMiddleware(newConnSettings, c.Prefix, "", "")
		if err != nil {
			if closeErr := accountsCluster.Close(); closeErr != nil {
				slog.Error("While closing accounts cluster", "err", closeErr)
			}
			for _, cl := range clusters {
				if closeErr := cl.Middleware.Close(); closeErr != nil {
					slog.Error("While closing transfer cluster middleware", "err", closeErr)
				}
			}
			return nil, err
		}
		clusters = append(clusters, middleware.ShardedCluster{
			Middleware: m,
			Hasher:     shard.New(c.NodeCount),
		})
	}

	resultsQueue, err := middleware.NewQueueMiddleware(newConnSettings, config.ResultsQueue)
	if err != nil {
		if closeErr := accountsCluster.Close(); closeErr != nil {
			slog.Error("While closing accounts cluster", "err", closeErr)
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
		if closeErr := accountsCluster.Close(); closeErr != nil {
			slog.Error("While closing accounts cluster", "err", closeErr)
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

	checkpointEvery := config.SeqCheckpointEvery
	if checkpointEvery == 0 {
		checkpointEvery = 1
	}

	sessions, err := newSessionStore(config.SessionStorePath)
	if err != nil {
		if closeErr := accountsCluster.Close(); closeErr != nil {
			slog.Error("While closing accounts cluster", "err", closeErr)
		}
		for _, cl := range clusters {
			if closeErr := cl.Middleware.Close(); closeErr != nil {
				slog.Error("While closing transfer cluster middleware", "err", closeErr)
			}
		}
		if closeErr := resultsQueue.Close(); closeErr != nil {
			slog.Error("While closing results queue", "err", closeErr)
		}
		if closeErr := listener.Close(); closeErr != nil {
			slog.Error("While closing acceptor socket", "err", closeErr)
		}
		return nil, err
	}

	buffer, err := newResultBuffer(filepath.Join(filepath.Dir(config.SessionStorePath), "result_buffers"))
	if err != nil {
		if closeErr := accountsCluster.Close(); closeErr != nil {
			slog.Error("While closing accounts cluster", "err", closeErr)
		}
		for _, cl := range clusters {
			if closeErr := cl.Middleware.Close(); closeErr != nil {
				slog.Error("While closing transfer cluster middleware", "err", closeErr)
			}
		}
		if closeErr := resultsQueue.Close(); closeErr != nil {
			slog.Error("While closing results queue", "err", closeErr)
		}
		if closeErr := listener.Close(); closeErr != nil {
			slog.Error("While closing acceptor socket", "err", closeErr)
		}
		return nil, err
	}

	gateway := &Gateway{
		transferClusters:          clusters,
		resultsQueue:              resultsQueue,
		listener:                  listener,
		sessions:                  sessions,
		buffer:                    buffer,
		queryEOFsExpected:         config.QueryEOFsExpected,
		seqCheckpointEvery:        checkpointEvery,
		clientTimeout:             config.ClientTimeout,
		reaperInterval:            config.ReaperInterval,
		disconnectedAt:            map[int]time.Time{},
		transfersTrackers:         map[int]*outputtracker.OutputTracker{},
		transfersCountedSeq:       map[int]uint64{},
		pendingClose:              map[int]struct{}{},
		resultsBatchFlushInterval: config.ResultsBatchFlushInterval,
		accountsCluster:           accountsCluster,
		accountsAmount:            config.AccountsClusterAmount,
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
		if err := gateway.resultsQueue.StartConsumingBatch(int(gateway.seqCheckpointEvery), gateway.resultsBatchFlushInterval, func(msgs []middleware.Message, ack, nack func()) {
			gateway.handleBatch(msgs, ack, nack)
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
	cleanup.Close(gateway.accountsCluster)
	for _, cl := range gateway.transferClusters {
		cleanup.Close(cl.Middleware)
	}
	cleanup.Close(gateway.resultsQueue)
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
	for _, queryID := range gateway.sessions.reportedQueries(clientID) {
		if err := tcpproto.WriteQueryEOF(conn, queryID); err != nil {
			return clientregistry.ClientState{}, 0, err
		}
	}
	if err := gateway.buffer.flush(clientID, func(body []byte) error {
		return gateway.deliverResult(client, body, nil)
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

	if phase == tcpproto.PhaseAccounts && !gateway.runAccountsPhase(client, r) {
		return
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

func (gateway *Gateway) handleBatch(msgs []middleware.Message, ack func(), nack func()) {
	failed := false
	for _, msg := range msgs {
		_, info, err := batch.ReadHeader(msg.Body)
		if err != nil {
			slog.Error("While deserializing result header", "err", err)
			continue
		}
		if err := gateway.processResult(info, msg.Body, gateway.pendingClose); err != nil {
			failed = true
		}
	}

	if !failed {
		if err := gateway.sessions.flush(); err != nil {
			slog.Error("While persisting results batch", "err", err)
			failed = true
		}
	}

	if failed {
		nack()
		return
	}

	for clientID := range gateway.pendingClose {
		lock := gateway.buffer.lock(clientID)
		lock.Lock()
		gateway.closeClient(clientID)
		lock.Unlock()
		delete(gateway.pendingClose, clientID)
	}
	ack()
}

func (gateway *Gateway) processResult(info batch.Info, body []byte, completed map[int]struct{}) error {
	lock := gateway.buffer.lock(info.ClientID)
	lock.Lock()
	defer lock.Unlock()

	if client, ok := gateway.findClient(info.ClientID); ok {
		if err := gateway.deliverResult(client, body, completed); err == nil {
			return nil
		} else {
			slog.Debug("Delivery failed, treating client as disconnected", "client_id", info.ClientID, "err", err)
			gateway.markDisconnected(client)
		}
	}

	if _, alive := gateway.sessions.session(info.ClientID); alive {
		if err := gateway.buffer.append(info.ClientID, body); err != nil {
			slog.Error("While buffering result for disconnected client", "client_id", info.ClientID, "err", err)
			return err
		}
		return nil
	}

	slog.Warn("Result for dead or unknown client, dropping", "client_id", info.ClientID)
	return nil
}

func (gateway *Gateway) deliverResult(client clientregistry.ClientState, body []byte, completed map[int]struct{}) error {
	reader, info, err := batch.ReadHeader(body)
	if err != nil {
		return err
	}

	if info.EOF {
		if err := gateway.sessions.registerEOFResult(info.ClientID, info.QueryID, info.SenderID, info.Total, info.Seq); err != nil {
			return err
		}
		return gateway.maybeCompleteQuery(client, info.QueryID, completed)
	}

	if !gateway.sessions.isDuplicateResult(info.ClientID, info.QueryID, info.SenderID, info.Seq) {
		if err := gateway.forwardResult(client, info, reader); err != nil {
			return err
		}
		if err := gateway.sessions.claimResult(info.ClientID, info.QueryID, info.SenderID, info.Seq); err != nil {
			return err
		}
	}
	return gateway.maybeCompleteQuery(client, info.QueryID, completed)
}

func (gateway *Gateway) maybeCompleteQuery(client clientregistry.ClientState, queryID uint8, completed map[int]struct{}) error {
	if gateway.sessions.queryReported(client.ID, queryID) {
		return nil
	}
	if !gateway.sessions.queryComplete(client.ID, queryID, gateway.expectedSenders(queryID)) {
		return nil
	}
	if err := tcpproto.WriteQueryEOF(client.Conn, queryID); err != nil {
		return err
	}
	allReported, err := gateway.sessions.markReported(client.ID, queryID, len(gateway.queryEOFsExpected))
	if err != nil {
		return err
	}
	slog.Info("Query completed", "client_id", client.ID, "query_id", queryID)
	if allReported {
		if completed != nil {
			completed[client.ID] = struct{}{}
		} else {
			gateway.closeClient(client.ID)
		}
	}
	return nil
}

func (gateway *Gateway) expectedSenders(queryID uint8) int {
	expected := gateway.queryEOFsExpected[queryID]
	if expected == 0 {
		expected = 1
	}
	return expected
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
	gateway.clearTransfersTracking(clientID)

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

	lock := gateway.buffer.lock(clientID)
	lock.Lock()
	defer lock.Unlock()

	if err := gateway.sessions.moveToPendingAbort(clientID); err != nil {
		slog.Error("While moving client to pending abort", "client_id", clientID, "err", err)
	}
	if err := gateway.buffer.remove(clientID); err != nil {
		slog.Error("While removing client result buffer", "client_id", clientID, "err", err)
	}
	gateway.clearTransfersTracking(clientID)
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
	body := batch.WriteAbort(clientID)
	rk := middleware.BroadcastRoutingKey
	var errs []error
	for _, cluster := range gateway.transferClusters {
		if err := cluster.Middleware.Send(middleware.Message{Body: body, RoutingKey: rk}); err != nil {
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
			gateway.accountsAmount,
		)
		byShard[idx] = append(byShard[idx], acc)
	}
	for idx, group := range byShard {
		body := batch.Write(client.ID, 0, gatewaySenderID, seq, group, records.AccountCodec)
		if err := gateway.accountsCluster.Send(middleware.Message{Body: body, RoutingKey: fmt.Sprintf("shard-%d", idx)}); err != nil {
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
	countThis := gateway.shouldCountTransfers(client.ID, seq)

	for ci, cluster := range gateway.transferClusters {
		rk := fmt.Sprintf("shard-%d", cluster.Hasher.ShardFor(client.ID, strconv.FormatUint(seq, 10)))
		trackerKey := fmt.Sprintf("%d_%s", ci, rk)
		if err := cluster.Middleware.Send(middleware.Message{Body: body, RoutingKey: rk}); err != nil {
			slog.Debug("While sending transfers batch", "err", err)
			return 0, err
		}
		if countThis {
			tracker.RegisterBatch(trackerKey)
		}
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
	eofBody := batch.WriteEOF(client.ID, 0, gatewaySenderID, seq, total)
	slog.Info("EOF SENT", "seq", seq, "total", total)
	if err := gateway.accountsCluster.Send(middleware.Message{Body: eofBody, RoutingKey: middleware.BroadcastRoutingKey}); err != nil {
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
			eofBody := batch.WriteEOF(client.ID, 0, gatewaySenderID, seq, total)
			if err := cluster.Middleware.Send(middleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
				slog.Debug("While sending transfers batch", "err", err)
				return err
			}
			tracker.RegisterBatch(trackerKey)
		}
	}
	return gateway.sessions.setPhase(client.ID, tcpproto.PhaseResults)
}

func (gateway *Gateway) transfersTrackerFor(clientID int) *outputtracker.OutputTracker {
	gateway.transfersMu.Lock()
	defer gateway.transfersMu.Unlock()
	t, ok := gateway.transfersTrackers[clientID]
	if !ok {
		t = outputtracker.New()
		gateway.transfersTrackers[clientID] = t
	}
	return t
}

func (gateway *Gateway) shouldCountTransfers(clientID int, seq uint64) bool {
	gateway.transfersMu.Lock()
	defer gateway.transfersMu.Unlock()
	if seq <= gateway.transfersCountedSeq[clientID] {
		return false
	}
	gateway.transfersCountedSeq[clientID] = seq
	return true
}

func (gateway *Gateway) clearTransfersTracking(clientID int) {
	gateway.transfersMu.Lock()
	defer gateway.transfersMu.Unlock()
	delete(gateway.transfersTrackers, clientID)
	delete(gateway.transfersCountedSeq, clientID)
}
