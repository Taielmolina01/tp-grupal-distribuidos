package middleware

import (
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	_DEFAULT_QOS = 1000
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
				Message{Body: d.Body, RoutingKey: d.RoutingKey},
				func() {
					if err := d.Ack(false); err != nil {
						slog.Error("ack failed", "err", err)
					}
				},
				func() {
					if err := d.Nack(false, true); err != nil {
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

func (b *baseMiddleware) StartConsumingBatch(batchSize int, flushInterval time.Duration, fn func(msgs []Message, ack func(), nack func())) error {
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

	batch := make([]Message, 0, batchSize)
	var deliveries []amqp.Delivery

	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	flush := func() {
		last := deliveries[len(deliveries)-1]
		fn(
			batch,
			func() {
				if err := last.Ack(true); err != nil {
					slog.Error("batch ack failed", "err", err)
				}
			},
			func() {
				if err := last.Nack(true, true); err != nil {
					slog.Error("batch nack failed", "err", err)
				}
			},
		)
		batch = batch[:0]
		deliveries = deliveries[:0]
	}

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(flushInterval)
	}

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return ErrDisconnected
			}
			batch = append(batch, Message{Body: d.Body, RoutingKey: d.RoutingKey})
			deliveries = append(deliveries, d)
			if len(batch) >= batchSize {
				flush()
				resetTimer()
			}
		case <-timer.C:
			if len(batch) > 0 {
				flush()
			}
			timer.Reset(flushInterval)
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

	if err := ch.Qos(_DEFAULT_QOS, 0, false); err != nil {
		if err := ch.Close(); err != nil {
			slog.Error("While closing channel on Qos failure", "err", err)
		}
		if err := conn.Close(); err != nil {
			slog.Error("While closing connection on Qos failure", "err", err)
		}
		return nil, nil, ErrDisconnected
	}

	if err := ch.Confirm(false); err != nil {
		if err := ch.Close(); err != nil {
			slog.Error("While closing channel on Confirm failure", "err", err)
		}
		if err := conn.Close(); err != nil {
			slog.Error("While closing connection on Confirm failure", "err", err)
		}
		return nil, nil, ErrDisconnected
	}

	return conn, ch, nil
}

func publish(ch *amqp.Channel, exchange, routingKey string, mandatory bool, publishing amqp.Publishing) error {
	publishing.DeliveryMode = amqp.Transient
	publishing.ContentType = "application/octet-stream"

	confirmation, err := ch.PublishWithDeferredConfirm(exchange, routingKey, mandatory, false, publishing)
	if err != nil {
		return ErrSend
	}
	if !confirmation.Wait() {
		return ErrSend
	}
	return nil
}

func startReturnHandler(ch *amqp.Channel) {
	returned := ch.NotifyReturn(make(chan amqp.Return, 10))
	go func() {
		for msg := range returned {
			slog.Warn("message not delivered", "message_id", msg.MessageId, "reply_code", msg.ReplyCode, "ReplyText", msg.ReplyText, "RK", msg.RoutingKey)
		}
	}()
}
