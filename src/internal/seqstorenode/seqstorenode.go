package seqstorenode

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/diskstore"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/seqstore"
	"tp-grupal-distribuidos/internal/common/seqstoreprotocol"
)

type Config struct {
	MomHost      string
	MomPort      int
	RequestQueue string
	Capacity     uint64
	PersistPath  string
}

type SeqStoreNode struct {
	stores      map[int]*seqstore.SeqStore
	capacity    uint64
	persistPath string
	server      newmiddleware.RPCServer
}

func New(cfg Config) (*SeqStoreNode, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.PersistPath), 0755); err != nil {
		return nil, fmt.Errorf("seqstorenode: create persist dir: %w", err)
	}

	connSettings := newmiddleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}
	server, err := newmiddleware.NewRPCServerMiddleware(connSettings, cfg.RequestQueue)
	if err != nil {
		return nil, err
	}

	node := &SeqStoreNode{
		stores:      make(map[int]*seqstore.SeqStore),
		capacity:    cfg.Capacity,
		persistPath: cfg.PersistPath,
		server:      server,
	}

	if err := node.loadState(); err != nil {
		server.Close()
		return nil, err
	}

	return node, nil
}

func (n *SeqStoreNode) Run() {
	defer n.server.Close()

	if err := n.server.StartConsumingRPC(n.handle); err != nil {
		slog.Error("seqstorenode stopped", "err", err)
	}
}

func (n *SeqStoreNode) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, stopping seqstorenode")
	if err := n.server.StopConsuming(); err != nil {
		slog.Error("while stopping seqstorenode", "err", err)
	}
}

func (n *SeqStoreNode) handle(req newmiddleware.RPCMessage, reply func([]byte) error, nack func()) {
	msgType, err := seqstoreprotocol.DecodeType(req.Body)
	if err != nil {
		slog.Error("malformed request", "err", err)
		nack()
		return
	}
	switch msgType {
	case seqstoreprotocol.TypeClaim:
		n.handleClaim(req.Body, reply, nack)
	case seqstoreprotocol.TypeRemove:
		n.handleRemove(req.Body, reply, nack)
	default:
		slog.Error("unknown message type", "type", msgType)
		nack()
	}
}

func (n *SeqStoreNode) handleClaim(body []byte, reply func([]byte) error, nack func()) {
	clientID, seq, err := seqstoreprotocol.DecodeClaimRequest(body)
	if err != nil {
		slog.Error("malformed claim request", "err", err)
		nack()
		return
	}

	store, ok := n.stores[clientID]
	if !ok {
		store = seqstore.New(n.capacity)
		n.stores[clientID] = store
	}
	free := store.Claim(seq)

	if free {
		if err := diskstore.WriteAtomic(n.persistPath, n.snapshot()); err != nil {
			slog.Error("while persisting state, stopping server", "err", err)
			nack()
			go n.server.StopConsuming()
			return
		}
	}

	if err := reply(seqstoreprotocol.EncodeResponse(free)); err != nil {
		slog.Error("while sending reply", "clientID", clientID, "seq", seq, "err", err)
	}
}

func (n *SeqStoreNode) handleRemove(body []byte, reply func([]byte) error, nack func()) {
	clientID, err := seqstoreprotocol.DecodeRemoveRequest(body)
	if err != nil {
		slog.Error("malformed remove request", "err", err)
		nack()
		return
	}

	delete(n.stores, clientID)
	snapshot := n.snapshot()

	if err := diskstore.WriteAtomic(n.persistPath, snapshot); err != nil {
		slog.Error("while persisting remove, stopping server", "clientID", clientID, "err", err)
		nack()
		go n.server.StopConsuming()
		return
	}

	slog.Info("seqstore: client removed", "clientID", clientID)

	if err := reply(seqstoreprotocol.EncodeResponse(true)); err != nil {
		slog.Error("while sending remove reply", "clientID", clientID, "err", err)
	}
}

func (n *SeqStoreNode) snapshot() map[string][]byte {
	data := make(map[string][]byte, len(n.stores))
	for clientID, store := range n.stores {
		data[strconv.Itoa(clientID)] = store.Marshal()
	}
	return data
}

func (n *SeqStoreNode) loadState() error {
	data, err := diskstore.Read(n.persistPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("seqstorenode: load state: %w", err)
	}
	for key, val := range data {
		clientID, err := strconv.Atoi(key)
		if err != nil {
			return fmt.Errorf("seqstorenode: invalid client key %q: %w", key, err)
		}
		store, err := seqstore.NewFromBytes(val)
		if err != nil {
			return fmt.Errorf("seqstorenode: load store for client %d: %w", clientID, err)
		}
		n.stores[clientID] = store
	}
	return nil
}
