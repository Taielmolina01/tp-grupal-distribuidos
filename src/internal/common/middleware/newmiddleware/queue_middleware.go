package newmiddleware

import (
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	baseMiddleware
}

func NewQueueMiddleware(settings ConnSettings, queueName string) (Middleware, error) {
	conn, ch, err := setupConn(settings)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		if err := ch.Close(); err != nil {
			slog.Error("while closing channel after queue declare failure", "err", err)
		}
		if err := conn.Close(); err != nil {
			slog.Error("while closing connection after queue declare failure", "err", err)
		}
		return nil, ErrDisconnected
	}

	startReturnHandler(ch)

	return &queueMiddleware{
		baseMiddleware: baseMiddleware{
			queue:      q,
			channel:    ch,
			connection: conn,
		},
	}, nil
}

func (q *queueMiddleware) Send(msg Message) error {
	if !q.areConnsUp() {
		return ErrDisconnected
	}

	err := publishPersistent(q.channel, "", q.queue.Name, true, amqp.Publishing{
		Body: msg.Body,
	})
	if err != nil {
		return ErrSend
	}
	return nil
}
