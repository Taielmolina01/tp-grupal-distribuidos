package client

import (
	"encoding/csv"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

type ClientConfig struct {
	ServerHost               string
	ServerPort               string
	InputFileAccounts        string
	InputFileTrans           string
	OutputFilePrefix         string
	MaxBatchSize             int
	MaxBatchBytes            int
	ConnectionAttempts       int
	ConnectionAttemptDelayMs int
}

type Client struct {
	config    ClientConfig
	running   atomic.Bool
	sessionID uint32

	connMu sync.Mutex
	conn   net.Conn

	files       []*os.File
	writers     []*csv.Writer
	doneCount   int
	seenResults map[resultKey]struct{}
}

type resultKey struct {
	queryID  uint8
	senderID uint8
	seq      uint64
}
