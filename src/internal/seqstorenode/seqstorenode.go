package seqstorenode

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

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

const clientEvictionDelay = time.Minute

type SeqStoreNode struct {
	stores      map[int]*seqstore.SeqStore
	mu          sync.Mutex
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
	clientID, seq, isEOF, err := seqstoreprotocol.DecodeRequest(req.Body)
	if err != nil {
		slog.Error("malformed request", "err", err)
		nack()
		return
	}

	n.mu.Lock()
	store, ok := n.stores[clientID]
	if !ok {
		store = seqstore.New(n.capacity)
		n.stores[clientID] = store
	}
	free := store.Claim(seq)
	var snapshot map[string][]byte
	if free {
		snapshot = n.snapshotLocked()
	}
	n.mu.Unlock()

	if free {
		if err := diskstore.WriteAtomic(n.persistPath, snapshot); err != nil {
			slog.Error("while persisting state, stopping server", "err", err)
			nack()
			go n.server.StopConsuming()
			return
		}
	}

	if isEOF && free {
		slog.Info("EOF", "seq", seq)
		time.AfterFunc(clientEvictionDelay, func() {
			n.mu.Lock()
			delete(n.stores, clientID)
			snapshot := n.snapshotLocked()
			n.mu.Unlock()

			if err := diskstore.WriteAtomic(n.persistPath, snapshot); err != nil {
				slog.Error("while persisitng remove", "clientID", clientID, "err", err)
			} else {
				slog.Info("seqstore: client remove", "clientID", clientID)
			}
		})
	}

	if err := reply(seqstoreprotocol.EncodeResponse(free)); err != nil {
		slog.Error("while sending reply", "clientID", clientID, "seq", seq, "err", err)
	}
}

func (n *SeqStoreNode) snapshotLocked() map[string][]byte {
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
