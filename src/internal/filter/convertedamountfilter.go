package filter

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	DATE_LAYOUT           = "2006-01-02"
	DATE_LAYOUT_WITH_HOUR = "2006-01-02 15:04"
	FILE_LAYOUT           = "%s_%s.csv"
)

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
	toSaveFunc func(T, int) string,
	fromSaveFunc func(string) (T, int, error),
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
		toSaveFunc:         toSaveFunc,
		fromSaveFunc:       fromSaveFunc,
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
	if _, ok := filter.conversionsByDay[filter.leftKeyFunc(result.Payload)]; !ok {
		filter.conversionsByDay[filter.leftKeyFunc(result.Payload)] = make(map[string]float32)
	} else if _, ok := filter.conversionsByDay[filter.leftKeyFunc(result.Payload)][filter.leftSecondKeyFunc(result.Payload)]; !ok {
		filter.conversionsByDay[filter.leftKeyFunc(result.Payload)][filter.leftSecondKeyFunc(result.Payload)] = filter.leftValueFunc(result.Payload)
		filter.CheckTransfersWithoutConversion(result.Payload)
	}
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
		filter.saveTransfersInFile(payload, result.ClientID)
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

func (filter *ConvertedAmountFilter[T, S]) CheckTransfersWithoutConversion(s S) {
	_, err := os.Stat(fmt.Sprintf(FILE_LAYOUT, filter.leftKeyFunc(s), filter.leftSecondKeyFunc(s)))

	if errors.Is(err, os.ErrNotExist) {
		return
	} else {
		file, err := os.Open(fmt.Sprintf(FILE_LAYOUT, filter.leftKeyFunc(s), filter.leftSecondKeyFunc(s)))
		if err != nil {
			slog.Error("while opening file", "err", err)
			return
		}
		defer func() {
			if err := file.Close(); err != nil {
				slog.Error("while closing file", "err", err)
			}
		}()

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()
			transfer, clientID, err := filter.fromSaveFunc(line)
			if err != nil {
				slog.Error("while parsing line", "err", err)
				continue
			}
			if filter.compareFunc(transfer, s) {
				msgOutput, err := inner.SerializeData(inner.DataMsg[T]{
					ClientID: clientID,
					QueryID:  filter.queryId,
					Payload:  transfer,
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
}

func (filter *ConvertedAmountFilter[T, S]) saveTransfersInFile(transfer T, clientID int) {
	// https://www.solvetic.com/tutoriales/article/1458-entender-los-permisos-linux-chmod/
	file, err := os.OpenFile(
		"data.csv",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)

	if err != nil {
		slog.Error("while opening file", "err", err)
		return
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("while closing file", "err", err)
		}
	}()

	writer := bufio.NewWriter(file)

	line := filter.toSaveFunc(transfer, clientID) + "\n"

	_, err = writer.WriteString(line)
	if err != nil {
		slog.Error("while writing to file", "err", err)
	}

	if err := writer.Flush(); err != nil {
		slog.Error("while flushing writer", "err", err)
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
