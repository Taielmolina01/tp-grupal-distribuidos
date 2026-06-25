package bully

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
	"tp-grupal-distribuidos/internal/common/socket"
)

const (
	_CONTAINER_PREFIX = "watchdog"
	_TIMEOUT          = 1000 * time.Millisecond
)

func CreateBullyAlgorithm(id int, peerCount int, basePort int) (Bully, error) {
	myPort := basePort + id
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", myPort))
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP listener on port %d: %v", myPort, err)
	}

	impl := &BullyImpl{
		id:           id,
		status:       normalStatus,
		leaderId:     -1,
		address:      net.TCPAddr{Port: myPort},
		peersMonitor: NewPeersMonitor(),
		socket:       listener,
		lastElection: lastElection{
			inProgress:       false,
			receivedResponse: false,
			mutex:            sync.Mutex{},
			ctxCancel:        nil,
		},
		basePort: basePort,
	}

	for i := range peerCount {
		peerPort := basePort + i
		if i == id || peerPort < myPort {
			continue
		}

		addr := fmt.Sprintf("%s_%d:%d", _CONTAINER_PREFIX, i, peerPort)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve peer %s_%d: %v", _CONTAINER_PREFIX, i, err)
		}

		node := NodeInfo{
			conn: socket.NewTCPSocket(conn),
			id:   i,
		}

		err = node.conn.SendMessage([]byte{byte(id)})
		if err != nil {
			return nil, fmt.Errorf("failed to send ID to peer %s_%d: %v", _CONTAINER_PREFIX, i, err)
		}

		impl.peersMonitor.AddPeer(node)

		go impl.handleClient(node)
	}

	return impl, nil
}

func (b *BullyImpl) Run() error {
	defer b.close()
	slog.Info("Starting bully", "id", b.id, "port", b.address.Port)

	go b.StartElection()

	for {
		conn, err := b.socket.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %v", err)
		}

		node := NodeInfo{
			conn: socket.NewTCPSocket(conn),
			id:   -1,
		}

		id, err := node.conn.ReadMessage(1)
		if err != nil {
			slog.Error("Failed to read node ID", "err", err)
			if err := node.conn.Close(); err != nil {
				slog.Error("Failed to close connection", "err", err)
			}
			return err
		}

		node.id = int(id[0])

		b.peersMonitor.AddPeer(node)
		go b.handleClient(node)
	}
}

func (b *BullyImpl) close() {
	if err := b.socket.Close(); err != nil {
		slog.Error("Failed to close socket", "err", err)
	}
	b.peersMonitor.DeleteAll()
}

func (b *BullyImpl) GetStatus() bullyStatus {
	return b.status
}

func (b *BullyImpl) handleClient(node NodeInfo) {
	defer func() {
		if err := node.conn.Close(); err != nil {
			slog.Error("Failed to close connection", "peer", node.id, "err", err)
		}
		b.removePeer(node)
	}()

	for {
		bytes, err := node.conn.ReadMessage(2)
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Info("Peer disconnected", "peer", node.id)
			} else {
				slog.Error("Failed to read from client", "err", err)
			}
			return
		}

		msgType := bullyMessage(bytes[0])
		switch msgType {
		case electionMsg:
			b.handleElectionMessage(bytes[1], node.conn)
		case answerMsg:
			b.handleAnswerMessage(bytes[1])
		case coordinatorMsg:
			b.handleCoordinatorMessage(bytes[1])
		case nodeIsAliveMsg:
			b.reconnectPeer(bytes[1])
		default:
			slog.Warn("Received unknown message type from client", "msgType", msgType)
		}
	}
}

func (b *BullyImpl) reconnectPeer(nodeId byte) {
	if nodeId < byte(b.id) {
		return
	}

	slog.Info("Reconnecting peer", "peerId", nodeId)

	peerPort := int(nodeId) + b.basePort
	addr := fmt.Sprintf("%s_%d:%d", _CONTAINER_PREFIX, nodeId, peerPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		slog.Error("Failed to reconnect to peer", "peerId", nodeId, "err", err)
		return
	}

	node := NodeInfo{
		conn: socket.NewTCPSocket(conn),
		id:   int(nodeId),
	}

	err = node.conn.SendMessage([]byte{byte(b.id)})
	if err != nil {
		slog.Error("Failed to send ID to reconnected peer", "peerId", nodeId, "err", err)
		if err := node.conn.Close(); err != nil {
			slog.Error("Failed to close connection", "err", err)
		}
		return
	}

	b.peersMonitor.AddPeer(node)
	go b.handleClient(node)
}

func (b *BullyImpl) removePeer(node NodeInfo) {
	b.peersMonitor.DeletePeer(node.id)
}

func (b *BullyImpl) StartElection() {
	slog.Info("Starting election from node", "id", b.id)
	b.status = electionStatus

	b.lastElection.mutex.Lock()
	b.lastElection.inProgress = true

	ctx, cancel := context.WithTimeout(context.Background(), _TIMEOUT)
	b.lastElection.ctxCancel = cancel
	b.lastElection.mutex.Unlock()

	go b.waitElectionResponse(ctx)

	b.peersMonitor.SendElectionMessage(b.id)
}

func (b *BullyImpl) GetLeaderId() (int, error) {
	if b.status != coordinatorStatus {
		return -1, fmt.Errorf("no leader elected yet")
	}
	return b.leaderId, nil
}

func (b *BullyImpl) handleElectionMessage(electorId byte, conn socket.TCPSocket) {
	slog.Info("Receiving election message from node", "electorId", electorId)

	err := conn.SendMessage([]byte{byte(answerMsg), byte(b.id)})
	if err != nil {
		slog.Error("Failed to send answer message", "electorId", electorId, "err", err)
	}
	b.StartElection()
}

func (b *BullyImpl) handleAnswerMessage(answerId byte) {
	slog.Info("Receiving answer message from node", "answerId", answerId)

	b.lastElection.mutex.Lock()
	defer b.lastElection.mutex.Unlock()

	if !b.lastElection.inProgress {
		return
	}

	b.lastElection.receivedResponse = true
	b.lastElection.inProgress = false
	if b.lastElection.ctxCancel != nil {
		b.lastElection.ctxCancel()
	}
}

func (b *BullyImpl) handleCoordinatorMessage(coordinatorId byte) {
	slog.Info("Receiving coordinator message from node", "coordinatorId", coordinatorId)

	b.status = coordinatorStatus
	b.leaderId = int(coordinatorId)

	b.lastElection.mutex.Lock()
	defer b.lastElection.mutex.Unlock()

	b.lastElection.inProgress = false
	b.lastElection.receivedResponse = false
	if b.onLeaderChange != nil {
		b.onLeaderChange(b.leaderId)
	}
}

func (b *BullyImpl) waitElectionResponse(ctx context.Context) {
	<-ctx.Done()
	b.lastElection.mutex.Lock()
	inProgress := b.lastElection.inProgress
	b.lastElection.mutex.Unlock()

	if inProgress {
		b.becomeLeader()
	}
}

func (b *BullyImpl) becomeLeader() {
	slog.Info("Becoming leader", "id", b.id)
	b.status = coordinatorStatus
	b.leaderId = b.id

	b.lastElection.mutex.Lock()
	defer b.lastElection.mutex.Unlock()

	b.lastElection.inProgress = false
	b.lastElection.receivedResponse = false

	b.peersMonitor.BroadcastCoordinatorMessage(b.id)
	if b.onLeaderChange != nil {
		b.onLeaderChange(b.leaderId)
	}
}

func (b *BullyImpl) SetLeaderChangeCallback(cb LeaderChangeCallback) {
	b.onLeaderChange = cb
}

func (b *BullyImpl) SendNodeIsAliveMessage(nodeId int) {
	slog.Info("Sending node is alive message for node", "nodeId", nodeId)

	go b.peersMonitor.SendNodeIsAliveMessage(nodeId)

	b.reconnectPeer(byte(nodeId))
}
