package seqstorenode

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/seqstore"
)

type Config struct {
	MomHost      string
	MomPort      int
	RequestQueue string
	Capacity     uint64
}

type SeqStoreNode struct {
	store  *seqstore.SeqStore
	server newmiddleware.RPCServer
}

func New(cfg Config) (*SeqStoreNode, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	server, err := newmiddleware.NewRPCServerMiddleware(connSettings, cfg.RequestQueue)
	if err != nil {
		return nil, err
	}

	return &SeqStoreNode{
		store:  seqstore.New(cfg.Capacity),
		server: server,
	}, nil
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
	seq, err := DecodeRequest(req.Body)
	if err != nil {
		slog.Error("malformed request", "err", err)
		nack()
		return
	}

	free := n.store.Claim(seq)
	if err := reply(EncodeResponse(free)); err != nil {
		slog.Error("while sending reply", "seq", seq, "err", err)
	}
}
