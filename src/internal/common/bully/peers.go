package bully

import (
	"log/slog"
	"sync"
)

type PeersMonitor interface {
	DeletePeer(int)
	AddPeer(NodeInfo)
	DeleteAll()
	SendElectionMessage(int)
	BroadcastCoordinatorMessage(int)
	SendNodeIsAliveMessage(int)
}

type peersMonitorImpl struct {
	peers map[int]NodeInfo
	mutex sync.Mutex
}

func NewPeersMonitor() PeersMonitor {
	return &peersMonitorImpl{
		peers: make(map[int]NodeInfo),
	}
}

func (p *peersMonitorImpl) DeletePeer(id int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	delete(p.peers, id)
}

func (p *peersMonitorImpl) AddPeer(node NodeInfo) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.peers[node.id] = node
}

func (p *peersMonitorImpl) DeleteAll() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for id := range p.peers {
		if err := p.peers[id].conn.Close(); err != nil {
			slog.Error("Failed to close connection", "err", err)
		}
		delete(p.peers, id)
	}
}

func (p *peersMonitorImpl) BroadcastCoordinatorMessage(id int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, peer := range p.peers {
		if peer.id != id {
			err := peer.conn.SendMessage([]byte{byte(coordinatorMsg), byte(id)})
			if err != nil {
				slog.Error("Failed to send coordinator message", "err", err)
			}
		}
	}
}

func (p *peersMonitorImpl) SendElectionMessage(id int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, peer := range p.peers {
		if peer.id > id {
			slog.Info("Sending message from node", "node", id, "toPeer", peer.id)
			err := peer.conn.SendMessage([]byte{byte(electionMsg), byte(id)})
			if err != nil {
				slog.Error("Failed to send election message", "err", err)
			}
		}
	}
}

func (p *peersMonitorImpl) SendNodeIsAliveMessage(aliveId int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, peer := range p.peers {
		if peer.id != aliveId {
			err := peer.conn.SendMessage([]byte{byte(nodeIsAliveMsg), byte(aliveId)})
			if err != nil {
				slog.Error("Failed to send node is alive message", "err", err)
			}
			return
		}
	}
}
