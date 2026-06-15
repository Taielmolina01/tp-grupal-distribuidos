package newmiddleware

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RPCClient interface {
	Call(body []byte, timeout time.Duration) ([]byte, error)
	Close() error
}

type rpcClientMiddleware struct {
	channel      *amqp.Channel
	connection   *amqp.Connection
	requestQueue string
	replyQueue   amqp.Queue
	replies      <-chan amqp.Delivery
}

func NewRPCClientMiddleware(settings ConnSettings, requestQueue string) (RPCClient, error) {
	conn, ch, err := setupConn(settings)
	if err != nil {
		return nil, err
	}

	replyQ, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrDisconnected
	}

	replies, err := ch.Consume(replyQ.Name, "", true, true, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrDisconnected
	}

	return &rpcClientMiddleware{
		channel:      ch,
		connection:   conn,
		requestQueue: requestQueue,
		replyQueue:   replyQ,
		replies:      replies,
	}, nil
}

func (c *rpcClientMiddleware) Call(body []byte, timeout time.Duration) ([]byte, error) {
	corrID := newCorrelationID()

	err := c.channel.Publish("", c.requestQueue, false, false, amqp.Publishing{
		ContentType:   "application/octet-stream",
		CorrelationId: corrID,
		ReplyTo:       c.replyQueue.Name,
		Body:          body,
	})
	if err != nil {
		return nil, fmt.Errorf("rpc publish: %w", err)
	}

	deadline := time.After(timeout)
	for {
		select {
		case d, ok := <-c.replies:
			if !ok {
				return nil, ErrDisconnected
			}
			if d.CorrelationId == corrID {
				return d.Body, nil
			}
		case <-deadline:
			return nil, fmt.Errorf("rpc timeout after %s waiting for corrID %s", timeout, corrID)
		}
	}
}

func (c *rpcClientMiddleware) Close() error {
	chErr := c.channel.Close()
	connErr := c.connection.Close()
	if chErr != nil {
		return chErr
	}
	return connErr
}

func newCorrelationID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
