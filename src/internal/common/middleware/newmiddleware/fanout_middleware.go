package newmiddleware

import (
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fanoutMiddleware struct {
	baseMiddleware
	exchange string
}

func NewFanoutMiddleware(settings ConnSettings, exchange string, inputQueue string) (Middleware, error) {
	conn, ch, err := setupConn(settings)
	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(exchange, amqp.ExchangeFanout, false, false, false, false, nil); err != nil {
		if err := ch.Close(); err != nil {
			slog.Error("while closing channel after exchange declare failure", "err", err)
		}
		if err := conn.Close(); err != nil {
			slog.Error("while closing connection after exchange declare failure", "err", err)
		}
		return nil, ErrDisconnected
	}

	mw := &fanoutMiddleware{
		baseMiddleware: baseMiddleware{
			connection: conn,
			channel:    ch,
		},
		exchange: exchange,
	}

	if inputQueue != "" {
		q, err := ch.QueueDeclare(inputQueue, false, false, false, false, nil)
		if err != nil {
			if err := ch.Close(); err != nil {
				slog.Error("while closing channel after queue declare failure", "err", err)
			}
			if err := conn.Close(); err != nil {
				slog.Error("while closing connection after queue declare failure", "err", err)
			}
			return nil, ErrDisconnected
		}

		if err := ch.QueueBind(q.Name, "", exchange, false, nil); err != nil {
			if err := ch.Close(); err != nil {
				slog.Error("while closing channel after queue bind failure", "err", err)
			}
			if err := conn.Close(); err != nil {
				slog.Error("while closing connection after queue bind failure", "err", err)
			}
			return nil, ErrDisconnected
		}

		mw.queue = q
	}

	startReturnHandler(ch)

	return mw, nil
}

func (f *fanoutMiddleware) Send(msg Message) error {
	if !f.areConnsUp() {
		return ErrDisconnected
	}

	err := f.channel.Publish(f.exchange, "", true, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg.Body),
	})
	if err != nil {
		return ErrSend
	}
	return nil
}
