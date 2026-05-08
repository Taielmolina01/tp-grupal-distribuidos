package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	return CreateQueueMiddlewareHelper(
		queueName,
		connectionSettings,
	)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	return CreateExchangeMiddlewareHelper(
		exchange,
		keys,
		connectionSettings,
	)
}
