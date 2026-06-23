package newmiddleware

import (
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type shardedMiddleware struct {
	baseMiddleware
	exchange string
}

func NewShardedMiddleware(settings ConnSettings, exchange string, inputQueue string, shardKey string) (Middleware, error) {
	conn, ch, err := setupConn(settings)
	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		if err := ch.Close(); err != nil {
			slog.Error("while closing channel after exchange declare failure", "err", err)
		}
		if err := conn.Close(); err != nil {
			slog.Error("while closing connection after exchange declare failure", "err", err)
		}
		return nil, ErrDisconnected
	}

	mw := &shardedMiddleware{
		baseMiddleware: baseMiddleware{
			connection: conn,
			channel:    ch,
		},
		exchange: exchange,
	}

	if inputQueue != "" {
		q, err := ch.QueueDeclare(inputQueue, true, false, false, false, nil)
		if err != nil {
			if err := ch.Close(); err != nil {
				slog.Error("while closing channel after queue declare failure", "err", err)
			}
			if err := conn.Close(); err != nil {
				slog.Error("while closing connection after queue declare failure", "err", err)
			}
			return nil, ErrDisconnected
		}

		for _, key := range []string{shardKey, BroadcastRoutingKey} {
			if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
				if err := ch.Close(); err != nil {
					slog.Error("while closing channel after queue bind failure", "err", err)
				}
				if err := conn.Close(); err != nil {
					slog.Error("while closing connection after queue bind failure", "err", err)
				}
				return nil, ErrDisconnected
			}
		}

		mw.queue = q
	}

	startReturnHandler(ch)

	return mw, nil
}

func (s *shardedMiddleware) Send(msg Message) error {
	if !s.areConnsUp() {
		return ErrDisconnected
	}

	if msg.RoutingKey == "" {
		return ErrSend
	}

	if err := publishPersistent(s.channel, s.exchange, msg.RoutingKey, true, amqp.Publishing{Body: msg.Body}); err != nil {
		return ErrSend
	}

	return nil
}
