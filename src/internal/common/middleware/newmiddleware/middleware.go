package newmiddleware

import (
	"errors"
	"time"
)

const BroadcastRoutingKey = "broadcast"

var (
	ErrDisconnected = errors.New("middleware: disconnected")
	ErrSend         = errors.New("middleware: send error")
	ErrNoInputQueue = errors.New("middleware: no input queue configured")
)

type Message struct {
	Body       []byte
	RoutingKey string
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {
	StartConsuming(func(msg Message, ack func(), nack func())) error
	StartConsumingBatch(batchSize int, flushInterval time.Duration, fn func(msgs []Message, ack func(), nack func())) error
	StopConsuming() error
	Send(msg Message) error
	Close() error
}
