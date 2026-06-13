package newmiddleware

import (
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RPCMessage struct {
	Body          []byte
	RoutingKey    string
	ReplyTo       string
	CorrelationID string
}

type RPCServer interface {
	StartConsumingRPC(func(req RPCMessage, reply func([]byte) error, nack func())) error
	StopConsuming() error
	Close() error
}

type rpcServerMiddleware struct {
	baseMiddleware
}

func NewRPCServerMiddleware(settings ConnSettings, queueName string) (RPCServer, error) {
	conn, ch, err := setupConn(settings)
	if err != nil {
		return nil, err
	}

	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrDisconnected
	}

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrDisconnected
	}

	return &rpcServerMiddleware{
		baseMiddleware: baseMiddleware{queue: q, channel: ch, connection: conn},
	}, nil
}

func (r *rpcServerMiddleware) StartConsumingRPC(
	handler func(req RPCMessage, reply func([]byte) error, nack func()),
) error {
	if !r.areConnsUp() {
		return ErrDisconnected
	}

	r.stopConsuming = make(chan struct{})
	stopCh := r.stopConsuming

	msgs, err := r.channel.Consume(r.queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return ErrDisconnected
	}

	connClosed := r.connection.NotifyClose(make(chan *amqp.Error, 1))
	channelClosed := r.channel.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return ErrDisconnected
			}
			req := RPCMessage{
				Body:          d.Body,
				RoutingKey:    d.RoutingKey,
				ReplyTo:       d.ReplyTo,
				CorrelationID: d.CorrelationId,
			}
			replyFn := func(responseBody []byte) error {
				if req.ReplyTo == "" {
					return nil
				}
				return r.channel.Publish("", req.ReplyTo, false, false, amqp.Publishing{
					ContentType:   "application/octet-stream",
					CorrelationId: req.CorrelationID,
					Body:          responseBody,
				})
			}
			nackFn := func() {
				if err := d.Nack(false, false); err != nil {
					slog.Error("rpc nack failed", "err", err)
				}
			}
			handler(req, replyFn, nackFn)
			if err := d.Ack(false); err != nil {
				slog.Error("rpc ack failed", "err", err)
			}
		case <-stopCh:
			return nil
		case <-connClosed:
			return ErrDisconnected
		case <-channelClosed:
			return ErrDisconnected
		}
	}
}

func (r *rpcServerMiddleware) StopConsuming() error { return r.baseMiddleware.StopConsuming() }
func (r *rpcServerMiddleware) Close() error         { return r.baseMiddleware.Close() }
