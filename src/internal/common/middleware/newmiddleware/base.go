package newmiddleware

import (
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	queue         amqp.Queue
	channel       *amqp.Channel
	connection    *amqp.Connection
	stopConsuming chan struct{}
}

func (b *baseMiddleware) areConnsUp() bool {
	return b.connection != nil && b.channel != nil &&
		!b.connection.IsClosed() && !b.channel.IsClosed()
}

func (b *baseMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if !b.areConnsUp() {
		return ErrDisconnected
	}
	if b.queue.Name == "" {
		return ErrNoInputQueue
	}

	b.stopConsuming = make(chan struct{})
	stopCh := b.stopConsuming

	msgs, err := b.channel.Consume(b.queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return ErrDisconnected
	}

	connClosed := b.connection.NotifyClose(make(chan *amqp.Error, 1))
	channelClosed := b.channel.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return ErrDisconnected
			}
			callbackFunc(
				Message{Body: string(d.Body), RoutingKey: d.RoutingKey},
				func() {
					if err := d.Ack(false); err != nil {
						slog.Error("ack failed", "err", err)
					}
				},
				func() {
					if err := d.Nack(false, false); err != nil {
						slog.Error("nack failed", "err", err)
					}
				},
			)
		case <-stopCh:
			return nil
		case <-connClosed:
			return ErrDisconnected
		case <-channelClosed:
			return ErrDisconnected
		}
	}
}

func (b *baseMiddleware) StopConsuming() error {
	if b.stopConsuming == nil {
		return nil
	}
	if !b.areConnsUp() {
		return ErrDisconnected
	}
	select {
	case <-b.stopConsuming:
	default:
		close(b.stopConsuming)
		b.stopConsuming = nil
	}
	return nil
}

func (b *baseMiddleware) Close() error {
	var chErr, connErr error
	if b.channel != nil {
		chErr = b.channel.Close()
	}
	if b.connection != nil {
		connErr = b.connection.Close()
	}
	if chErr != nil {
		return chErr
	}
	return connErr
}

func setupConn(settings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port))
	if err != nil {
		return nil, nil, ErrDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		if err := conn.Close(); err != nil {
			slog.Error("While closing connection on channel failure", "err", err)
		}
		return nil, nil, ErrDisconnected
	}

	if err := ch.Qos(1000, 0, false); err != nil {
		if err := ch.Close(); err != nil {
			slog.Error("While closing channel on Qos failure", "err", err)
		}
		if err := conn.Close(); err != nil {
			slog.Error("While closing connection on Qos failure", "err", err)
		}
		return nil, nil, ErrDisconnected
	}

	return conn, ch, nil
}

func startReturnHandler(ch *amqp.Channel) {
	returned := ch.NotifyReturn(make(chan amqp.Return, 10))
	go func() {
		for msg := range returned {
			slog.Warn("message not delivered", "message_id", msg.MessageId, "reply_code", msg.ReplyCode)
		}
	}()
}
