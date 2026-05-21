package gateway

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"tp-grupal-distribuidos/internal/clientregistry"
	"tp-grupal-distribuidos/internal/common/messageprotocol/external"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
)

type GatewayConfig struct {
	AccountsExchange  string
	TransfersExchange string
	ResultsQueue      string
	ServerHost        string
	ServerPort        string
	MomHost           string
	MomPort           int
	MaxBatchSize      int
}

type Gateway struct {
	registry          clientregistry.ClientRegistry
	accountsExchange  middleware.Middleware
	transfersExchange middleware.Middleware
	resultsQueue      middleware.Middleware
	listener          net.Listener
	running           atomic.Bool
	nextClientID      atomic.Int32
	countsMu          sync.Mutex
	accountsCount     map[int]uint32
	transfersCount    map[int]uint32
	queryEOFsByClient map[int]int
	resultBuilders    map[int]*external.ResultBatchBuilder
	maxBatchSize      int
	maxBatchBytes     int
}

const (
	TRANSFERS_Q5_KEY    = "TRANSFERS_Q5_KEY"
	TRANSFERS_Q1234_KEY = "TRANSFERS_Q1234_KEY"
	ACCOUNTS_QUEUE      = ""
	TRANSFER_QUEUE      = "TRANSFER_QUEUE"
)

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	//Quiza las keys deberían ser config, quizá no. Quien sabe
	accountsExchange, err := middleware.CreateExchangeMiddleware(config.AccountsExchange, ACCOUNTS_QUEUE, []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	// Las keys acá vienen x config xq son dinámicas
	// Se requiere sharding
	transfersExchange, err := middleware.CreateExchangeMiddleware(config.TransfersExchange, TRANSFER_QUEUE, []string{TRANSFERS_Q1234_KEY, TRANSFERS_Q5_KEY}, connSettings)
	if err != nil {
		if err := accountsExchange.Close(); err != nil {
			slog.Error("While closing accounts exchange", "err", err)
		}
		return nil, err
	}

	resultsQueue, err := middleware.CreateQueueMiddleware(config.ResultsQueue, connSettings)
	if err != nil {
		if err := accountsExchange.Close(); err != nil {
			slog.Error("While closing accounts exchange", "err", err)
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		if err := accountsExchange.Close(); err != nil {
			slog.Error("While closing accounts exchange", "err", err)
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		if err := resultsQueue.Close(); err != nil {
			slog.Error("While closing results queue", "err", err)
		}
		return nil, err
	}

	gateway := &Gateway{
		accountsExchange:  accountsExchange,
		transfersExchange: transfersExchange,
		resultsQueue:      resultsQueue,
		listener:          listener,
		accountsCount:     map[int]uint32{},
		transfersCount:    map[int]uint32{},
		queryEOFsByClient: map[int]int{},
		resultBuilders:    map[int]*external.ResultBatchBuilder{},
		maxBatchSize:      config.MaxBatchSize,
	}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer func() {
		if err := gateway.listener.Close(); err != nil {
			slog.Error("While closing gateway's acceptor socket", "err", err)
		}
	}()

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
			return err
		}

		clientID := int(gateway.nextClientID.Add(1))
		slog.Info("Client connected", "client_id", clientID)

		client := clientregistry.ClientState{ID: clientID, Conn: conn}

		gateway.registry.Add(client)

		go gateway.handleClientRequest(client)
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

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
accountsLoop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Debug("While reading message type (accounts phase)", "err", err)
			return
		}

		switch msgType {
		case external.AccountBatch:
			if err := gateway.handleAccountBatch(client); err != nil {
				slog.Debug("While handling account batch", "err", err)
				return
			}

		case external.EndOfRecords:
			if err := gateway.handleEndOfAccounts(client); err != nil {
				slog.Debug("While handling EOF accounts", "err", err)
				return
			}
			break accountsLoop

		default:
			slog.Debug("Unexpected message type in accounts phase", "got", msgType)
			return
		}
	}

transfersLoop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Debug("While reading message type (transfers phase)", "err", err)
			return
		}

		switch msgType {
		case external.TransBatch:
			if err := gateway.handleTransBatch(client); err != nil {
				slog.Debug("While handling trans batch", "err", err)
				return
			}

		case external.EndOfRecords:
			if err := gateway.handleEndOfTransfers(client); err != nil {
				slog.Debug("While handling EOF transfers", "err", err)
				return
			}
			break transfersLoop

		default:
			slog.Debug("Unexpected message type in transfers phase", "got", msgType)
			return
		}
	}
}

func (gateway *Gateway) getOrCreateBuilder(clientID int) *external.ResultBatchBuilder {
	if b, ok := gateway.resultBuilders[clientID]; ok {
		return b
	}
	b := external.NewResultBatchBuilder(gateway.maxBatchSize, gateway.maxBatchBytes)
	gateway.resultBuilders[clientID] = b
	return b
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	env, err := inner.DeserializeData[json.RawMessage](&msg)
	if err != nil {
		slog.Error("While deserializing result envelope", "err", err)
		nack()
		return
	}

	client, ok := gateway.findClient(env.ClientID)
	if !ok {
		slog.Warn("Result for unknown client", "client_id", env.ClientID)
		ack()
		return
	}

	builder := gateway.getOrCreateBuilder(env.ClientID)

	if env.IsEOF() {
		if !builder.IsEmpty() {
			if err := builder.Flush(client.Conn); err != nil {
				slog.Error("While flushing batch before QueryEOF", "client_id", env.ClientID, "err", err)
				nack()
				return
			}
		}
		if err := external.WriteQueryEOF(client.Conn, env.QueryID); err != nil {
			slog.Error("While writing QueryEOF", "client_id", env.ClientID, "query_id", env.QueryID, "err", err)
			nack()
			return
		}
		if gateway.markQueryEOF(env.ClientID) {
			gateway.closeClient(env.ClientID)
		}
		ack()
		return
	}
	added, err := addResultToBuilder(builder, env)
	if err != nil {
		slog.Error("While adding result to batch", "client_id", env.ClientID, "query_id", env.QueryID, "err", err)
		nack()
		return
	}
	if !added {
		if err := builder.Flush(client.Conn); err != nil {
			slog.Error("While flushing full batch", "client_id", env.ClientID, "err", err)
			nack()
			return
		}
		if _, err := addResultToBuilder(builder, env); err != nil {
			slog.Error("While adding result to batch after flush", "client_id", env.ClientID, "err", err)
			nack()
			return
		}
	}
	ack()
}

func addResultToBuilder(builder *external.ResultBatchBuilder, env *inner.DataMsg[json.RawMessage]) (bool, error) {
	switch env.QueryID {
	case inner.Query1ID:
		r, err := inner.Deserialize[queryresult.Query1Result](env.Payload)
		if err != nil {
			return false, err
		}
		return builder.TryAddQuery1(r), nil
	case inner.Query2ID:
		r, err := inner.Deserialize[queryresult.Query2Result](env.Payload)
		if err != nil {
			return false, err
		}
		return builder.TryAddQuery2(r), nil
	case inner.Query3ID:
		r, err := inner.Deserialize[queryresult.Query3Result](env.Payload)
		if err != nil {
			return false, err
		}
		return builder.TryAddQuery3(r), nil
	case inner.Query4ID:
		r, err := inner.Deserialize[queryresult.Query4Result](env.Payload)
		if err != nil {
			return false, err
		}
		return builder.TryAddQuery4(r), nil
	case inner.Query5ID:
		r, err := inner.Deserialize[queryresult.Query5Result](env.Payload)
		if err != nil {
			return false, err
		}
		return builder.TryAddQuery5(r), nil
	default:
		return false, fmt.Errorf("unknown query id: %d", env.QueryID)
	}
}

const totalQueries = 5

func (gateway *Gateway) markQueryEOF(clientID int) bool {
	gateway.countsMu.Lock()
	defer gateway.countsMu.Unlock()
	gateway.queryEOFsByClient[clientID]++
	return gateway.queryEOFsByClient[clientID] >= totalQueries
}

func (gateway *Gateway) closeClient(clientID int) {
	client, ok := gateway.findClient(clientID)
	if !ok {
		return
	}
	if err := client.Conn.Close(); err != nil {
		slog.Error("While closing client connection", "client_id", clientID, "err", err)
	}
	gateway.registry.RemoveByID(clientID)
	gateway.countsMu.Lock()
	delete(gateway.queryEOFsByClient, clientID)
	gateway.countsMu.Unlock()
	delete(gateway.resultBuilders, clientID)
	slog.Info("Client finished, connection closed", "client_id", clientID)
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

func wrapForClient[T any](clientID int, record T) (*middleware.Message, error) {
	return inner.SerializeData(inner.DataMsg[T]{ClientID: clientID, Payload: record})
}

func (gateway *Gateway) addCount(counts map[int]uint32, clientID int, n uint32) {
	gateway.countsMu.Lock()
	defer gateway.countsMu.Unlock()
	counts[clientID] += n
}

func (gateway *Gateway) takeCount(counts map[int]uint32, clientID int) uint32 {
	gateway.countsMu.Lock()
	defer gateway.countsMu.Unlock()
	total := counts[clientID]
	delete(counts, clientID)
	return total
}

func (gateway *Gateway) forwardEOF(client clientregistry.ClientState, kind string, total uint32, exchange middleware.Middleware) error {
	slog.Info("Received EOF message", "kind", kind, "client_id", client.ID, "total", total)
	msg, err := inner.SerializeData(inner.DataMsg[any]{
		ClientID: client.ID,
		EOF:      &inner.EOFInfo{Kind: kind, TotalMessages: total},
	})
	if err != nil {
		slog.Debug("While serializing EOF", "kind", kind, "err", err)
		return err
	}
	if err := exchange.Send(*msg); err != nil {
		slog.Debug("While sending EOF", "kind", kind, "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleAccountBatch(client clientregistry.ClientState) error {
	accounts, err := external.ReadAccountBatch(client.Conn)
	if err != nil {
		slog.Debug("While reading ACCOUNT_BATCH", "err", err)
		return err
	}
	for _, acc := range accounts {
		msg, err := wrapForClient(client.ID, acc)
		if err != nil {
			slog.Debug("While serializing account", "err", err)
			return err
		}
		if err := gateway.accountsExchange.Send(*msg); err != nil {
			slog.Debug("While sending account to accounts exchange", "err", err)
			return err
		}
	}
	gateway.addCount(gateway.accountsCount, client.ID, uint32(len(accounts)))
	return nil
}

func (gateway *Gateway) handleTransBatch(client clientregistry.ClientState) error {
	trans, err := external.ReadTransBatch(client.Conn)
	if err != nil {
		slog.Debug("While reading TRANS_BATCH", "err", err)
		return err
	}
	for _, t := range trans {
		msg, err := wrapForClient(client.ID, t)
		if err != nil {
			slog.Debug("While serializing transfer", "err", err)
			return err
		}
		if err := gateway.transfersExchange.Send(*msg); err != nil {
			slog.Debug("While sending transfer to transfers exchange", "err", err)
			return err
		}
	}
	gateway.addCount(gateway.transfersCount, client.ID, uint32(len(trans)))
	return nil
}

func (gateway *Gateway) handleEndOfAccounts(client clientregistry.ClientState) error {
	total := gateway.takeCount(gateway.accountsCount, client.ID)
	return gateway.forwardEOF(client, "accounts", total, gateway.accountsExchange)
}

func (gateway *Gateway) handleEndOfTransfers(client clientregistry.ClientState) error {
	total := gateway.takeCount(gateway.transfersCount, client.ID)
	return gateway.forwardEOF(client, "transfers", total, gateway.transfersExchange)
}
