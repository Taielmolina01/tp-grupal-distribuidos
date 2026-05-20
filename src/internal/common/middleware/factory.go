package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	return CreateQueueMiddlewareHelper(
		queueName,
		connectionSettings,
	)
}

func CreateExchangeMiddleware(exchange string, queue string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	return CreateExchangeMiddlewareHelper(
		exchange,
		queue,
		keys,
		connectionSettings,
	)
}
