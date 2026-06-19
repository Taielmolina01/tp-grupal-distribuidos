package bully

import (
	"context"
	"net"
	"sync"
	"tp-grupal-distribuidos/internal/common/socket"
)

type bullyStatus int
type bullyMessage int

const (
	normalStatus bullyStatus = iota
	electionStatus
	coordinatorStatus

	electionMsg bullyMessage = iota
	answerMsg
	coordinatorMsg
	nodeIsAliveMsg
)

type Bully interface {
	Run() error
	GetStatus() bullyStatus
	StartElection()
	GetLeaderId() (int, error)
	SetLeaderChangeCallback(cb LeaderChangeCallback)
	SendNodeIsAliveMessage(int)
}

type lastElection struct {
	ctxCancel        context.CancelFunc
	mutex            sync.Mutex
	inProgress       bool
	receivedResponse bool
}

type LeaderChangeCallback func(leaderId int)

type BullyImpl struct {
	id             int
	status         bullyStatus
	leaderId       int
	socket         net.Listener
	address        net.TCPAddr
	lastElection   lastElection
	onLeaderChange LeaderChangeCallback
	peersMonitor   PeersMonitor
	basePort       int
}

type NodeInfo struct {
	conn socket.TCPSocket
	id   int
}
