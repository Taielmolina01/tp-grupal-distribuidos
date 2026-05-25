package newmiddleware

import amqp "github.com/rabbitmq/amqp091-go"

type queueMiddleware struct {
	baseMiddleware
}

func NewQueueMiddleware(settings ConnSettings, queueName string) (Middleware, error) {
	conn, ch, err := setupConn(settings)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
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

	err := q.channel.Publish("", q.queue.Name, true, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg.Body),
	})
	if err != nil {
		return ErrSend
	}
	return nil
}
