package seqstorenode

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/seqstore"
	"tp-grupal-distribuidos/internal/common/seqstoreprotocol"
)

type Config struct {
	MomHost      string
	MomPort      int
	RequestQueue string
	Capacity     uint64
}

type SeqStoreClient struct {
	claim   *seqstore.SeqStore
	confirm *seqstore.SeqStore
	lastSeq map[uint8]uint64
}

func newSeqStoreClient(capacity uint64) SeqStoreClient {
	return SeqStoreClient{
		claim:   seqstore.New(capacity),
		confirm: seqstore.New(capacity),
		lastSeq: make(map[uint8]uint64),
	}
}

type SeqStoreNode struct {
	stores   map[int]SeqStoreClient
	capacity uint64
	server   newmiddleware.RPCServer
}

func New(cfg Config) (*SeqStoreNode, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}
	server, err := newmiddleware.NewRPCServerMiddleware(connSettings, cfg.RequestQueue)
	if err != nil {
		return nil, err
	}

	return &SeqStoreNode{
		stores:   make(map[int]SeqStoreClient),
		capacity: cfg.Capacity,
		server:   server,
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
	msgType, err := seqstoreprotocol.DecodeType(req.Body)
	if err != nil {
		slog.Error("malformed request", "err", err)
		nack()
		return
	}
	switch msgType {
	case seqstoreprotocol.TypeClaim:
		n.handleClaim(req.Body, reply, nack)
	case seqstoreprotocol.TypeConfirm:
		n.handleConfirm(req.Body, reply, nack)
	case seqstoreprotocol.TypeRemove:
		n.handleRemove(req.Body, reply, nack)
	default:
		slog.Error("unknown message type", "type", msgType)
		nack()
	}
}

func (n *SeqStoreNode) handleClaim(body []byte, reply func([]byte) error, nack func()) {
	clientID, sender, seq, err := seqstoreprotocol.DecodeClaimRequest(body)
	if err != nil {
		slog.Error("malformed claim request", "err", err)
		nack()
		return
	}

	client, ok := n.stores[clientID]
	if !ok {
		client = newSeqStoreClient(n.capacity)
		n.stores[clientID] = client
	}
	free := client.claim.Claim(seq)
	if free {
		client.lastSeq[sender] = seq
	} else if client.lastSeq[sender] == seq && !client.confirm.IsSet(seq) {
		free = true
	}

	if err := reply(seqstoreprotocol.EncodeResponse(free)); err != nil {
		slog.Error("while sending reply", "clientID", clientID, "seq", seq, "err", err)
	}
}

func (n *SeqStoreNode) handleConfirm(body []byte, reply func([]byte) error, nack func()) {
	clientID, _, seq, err := seqstoreprotocol.DecodeConfirmRequest(body)
	if err != nil {
		slog.Error("malformed confirm request", "err", err)
		nack()
		return
	}

	client, ok := n.stores[clientID]
	if !ok {
		if err := reply(seqstoreprotocol.EncodeResponse(false)); err != nil {
			slog.Error("while sending confirm reply", "clientID", clientID, "err", err)
		}
		return
	}
	free := client.confirm.Claim(seq)

	if err := reply(seqstoreprotocol.EncodeResponse(free)); err != nil {
		slog.Error("while sending confirm reply", "clientID", clientID, "seq", seq, "err", err)
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
	slog.Info("seqstore: client removed", "clientID", clientID)

	if err := reply(seqstoreprotocol.EncodeResponse(true)); err != nil {
		slog.Error("while sending remove reply", "clientID", clientID, "err", err)
	}
}
