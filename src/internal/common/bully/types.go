package bully

import (
	"context"
	"net"
	"sync"
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
)

type Bully interface {
	Run() error
	GetStatus() bullyStatus
	StartElection()
	GetLeaderId() (int, error)
	SetLeaderChangeCallback(cb LeaderChangeCallback)
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
	socket         *UDPSocket
	address        net.UDPAddr
	peers          []net.UDPAddr
	lastElection   lastElection
	onLeaderChange LeaderChangeCallback
}
