package bully

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	_CONTAINER_PREFIX = "watchdog"
	_TIMEOUT          = 1000 * time.Millisecond
)

func CreateBullyAlgorithm(id int, peerCount int, basePort int) (Bully, error) {
	myPort := basePort + id
	skt, err := CreateUDPSocket(myPort)
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP socket on port %d: %v", myPort, err)
	}

	peers := make([]net.UDPAddr, 0, peerCount-1)

	for i := range peerCount {
		if i == id {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s_%d:%d", _CONTAINER_PREFIX, i, basePort+i))
		if err != nil {
			return nil, fmt.Errorf("failed to resolve peer %s_%d: %v", _CONTAINER_PREFIX, i, err)
		}
		peers = append(peers, *addr)
	}

	return &BullyImpl{
		id:       id,
		status:   normalStatus,
		leaderId: -1,
		address:  net.UDPAddr{Port: myPort},
		peers:    peers,
		socket:   skt,
		lastElection: lastElection{
			inProgress:       false,
			receivedResponse: false,
			mutex:            sync.Mutex{},
			ctxCancel:        nil,
		},
	}, nil
}

func (b *BullyImpl) Run() error {
	slog.Info("Starting bully", "id", b.id, "port", b.address.Port, "peers", len(b.peers))
	b.StartElection()
	buf := make([]byte, 2)
	for {
		n, senderAddr, err := b.socket.ReceiveMessage(buf)
		if err != nil {
			return fmt.Errorf("failed to read from UDP: %v", err)
		}

		if n == 0 {
			continue
		}

		switch buf[0] {
		case byte(electionMsg):
			b.handleElectionMessage(buf[1], senderAddr)
		case byte(answerMsg):
			b.handleAnswerMessage(buf[1])
		case byte(coordinatorMsg):
			b.handleCoordinatorMessage(buf[1])
		default:
			slog.Info("Received unknown message type", "type", buf[0])
		}
	}
}

func (b *BullyImpl) GetStatus() bullyStatus {
	return b.status
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

	for _, peer := range b.peers {
		if peer.Port > b.address.Port {
			err := b.socket.SendMessage([]byte{byte(electionMsg), byte(b.id)}, peer)
			if err != nil {
				slog.Error("Failed to send election message", "peer", peer.String(), "err", err)
			}
		}
	}
}

func (b *BullyImpl) GetLeaderId() (int, error) {
	if b.status != coordinatorStatus {
		return -1, fmt.Errorf("no leader elected yet")
	}
	return b.leaderId, nil
}

func (b *BullyImpl) handleElectionMessage(electorId byte, senderAddr net.UDPAddr) {
	slog.Info("Receiving election message from node", "electorId", electorId)
	err := b.socket.SendMessage([]byte{byte(answerMsg), byte(b.id)}, senderAddr)
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

	for _, peer := range b.peers {
		if peer.Port != b.address.Port {
			err := b.socket.SendMessage([]byte{byte(coordinatorMsg), byte(b.id)}, peer)
			if err != nil {
				slog.Error("Failed to send coordinator message", "peer", peer.String(), "err", err)
			}
		}
	}
	if b.onLeaderChange != nil {
		b.onLeaderChange(b.leaderId)
	}
}

func (b *BullyImpl) SetLeaderChangeCallback(cb LeaderChangeCallback) {
	b.onLeaderChange = cb
}
