package filter

import (
	"log/slog"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

const DATE_LAYOUT = "2006-01-02"

func newConvertedAmountFilter[T, S comparable](
	config FilterConfig,
	compareFunc func(t T, s S) bool,
	leftKeyFunc func(S) string,
	leftSecondKeyFunc func(S) string,
	leftValueFunc func(S) float32,
	rightKeyFunc func(T) string,
	rightsecondKeyFunc func(T) string,
	rightValueFunc func(T) float32,
	conversionFunc func(T, float32) S,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	leftInputQueue, err := middleware.CreateQueueMiddleware(
		config.LeftInputQueue,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	rightInputQueue, err := middleware.CreateQueueMiddleware(
		config.RightInputQueue,
		connSettings,
	)

	if err != nil {
		if err := leftInputQueue.Close(); err != nil {
			slog.Error("while closing left input queue", "err", err)
		}
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(
		config.OutputQueue,
		connSettings,
	)

	if err != nil {
		if err := leftInputQueue.Close(); err != nil {
			slog.Error("while closing left input queue", "err", err)
		}
		if err := rightInputQueue.Close(); err != nil {
			slog.Error("while closing right input queue", "err", err)
		}
		return nil, err
	}

	return &ConvertedAmountFilter[T, S]{
		leftInputQueue:     leftInputQueue,
		rightInputQueue:    rightInputQueue,
		outputQueue:        outputQueue,
		compareFunc:        compareFunc,
		queryId:            config.QueryId,
		conversionsByDay:   make(map[string]map[string]float32),
		leftKeyFunc:        leftKeyFunc,
		leftSecondKeyFunc:  leftSecondKeyFunc,
		leftValueFunc:      leftValueFunc,
		rightKeyFunc:       rightKeyFunc,
		rightsecondKeyFunc: rightsecondKeyFunc,
		rightValueFunc:     rightValueFunc,
		conversionFunc:     conversionFunc,
	}, nil
}

func (filter *ConvertedAmountFilter[T, S]) Run() {
	go func() {
		if err := filter.leftInputQueue.StartConsuming(filter.consumeLeft); err != nil {
			slog.Error("while starting consuming from left input queue", "err", err)
			return
		}
	}()
	if err := filter.rightInputQueue.StartConsuming(filter.consumeRight); err != nil {
		slog.Error("while starting consuming from right input queue", "err", err)
		return
	}
}

func (filter *ConvertedAmountFilter[T, S]) consumeLeft(msg middleware.Message, ack, nack func()) {
	defer ack()

	result, err := inner.DeserializeData[S](&msg)
	if err != nil {
		slog.Error("while deserializing transfer", "err", err)
		return
	}

	filter.conversionsByDay[filter.leftKeyFunc(result.Payload)][filter.leftSecondKeyFunc(result.Payload)] = filter.leftValueFunc(result.Payload)
}

func (filter *ConvertedAmountFilter[T, S]) consumeRight(msg middleware.Message, ack, nack func()) {
	defer ack()

	result, err := inner.DeserializeData[T](&msg)
	if err != nil {
		slog.Error("while deserializing transfer", "err", err)
		return
	}

	payload := result.Payload

	key := filter.rightKeyFunc(payload)
	if _, ok := filter.conversionsByDay[key]; !ok {
		slog.Error("no conversion rate for today", "date", key)
		// deberia guardar esta transaccion a disco
	} else {
		conversion := filter.conversionsByDay[key][filter.rightsecondKeyFunc(payload)]
		if filter.compareFunc(payload, filter.conversionFunc(
			payload,
			conversion,
		)) {
			msgOutput, err := inner.SerializeData(inner.DataMsg[T]{
				ClientID: result.ClientID,
				QueryID:  filter.queryId,
				Payload:  payload,
				EOF:      nil,
			})
			if err != nil {
				slog.Error("while serializing message", "err", err)
				return
			}
			if err := filter.outputQueue.Send(*msgOutput); err != nil {
				slog.Error("while publishing message to output queue", "err", err)
				return
			}
		}
	}
}

func (filter *ConvertedAmountFilter[T, S]) HandleSignals() {
	if err := filter.leftInputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from left input queue", "err", err)
	}
	if err := filter.leftInputQueue.Close(); err != nil {
		slog.Error("while closing left input queue", "err", err)
	}
	if err := filter.rightInputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from right input queue", "err", err)
	}
}
