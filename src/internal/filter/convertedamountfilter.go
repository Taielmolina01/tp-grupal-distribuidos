package filter

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	DATE_LAYOUT           = "2006-01-02"
	DATE_LAYOUT_WITH_HOUR = "2006-01-02 15:04"
	FILE_LAYOUT           = "%s_%s.csv"
	eofRingQueuePrefix    = "FILTER_CONVERTED_AMOUNT"
)

var datasetToFrank = map[string]string{
	"Australian Dollar": "AUD",
	"Bitcoin":           "BTC",
	"Brazil Real":       "BRL",
	"Canadian Dollar":   "CAD",
	"Euro":              "EUR",
	"Mexican Peso":      "MXN",
	"Ruble":             "RUB",
	"Rupee":             "INR",
	"Saudi Riyal":       "SAR",
	"Shekel":            "ILS",
	"Swiss Franc":       "CHF",
	"UK Pound":          "GBP",
	"US Dollar":         "USD",
	"Yen":               "JPY",
	"Yuan":              "CNY",
}

func newConvertedAmountFilter[T, S comparable](
	config FilterConfig,
	compareFunc func(t T, s S) bool,
	leftKeyFunc func(S) string,
	leftSecondKeyFunc func(S) string,
	leftValueFunc func(S) float64,
	rightKeyFunc func(T) string,
	rightsecondKeyFunc func(T) string,
	rightValueFunc func(T) float64,
	conversionFunc func(T, float64) S,
	toSaveFunc func(T, int) string,
	fromSaveFunc func(string) (T, int, error),
	toIgnoreFunc func(T) bool,
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

	rightInputQueue, err := middleware.CreateExchangeMiddleware(
		config.RightInputExchange,
		config.RightInputQueue,
		config.RightInputRoutingKeys,
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

	eofInputName, eofOutputName := eofring.GetInputAndOutputQueueNames(
		config.Id,
		config.FilterAmount,
		eofRingQueuePrefix,
		eofRingQueuePrefix,
	)

	eofInput, err := middleware.CreateQueueMiddleware(
		eofInputName,
		connSettings,
	)

	if err != nil {
		slog.Error("creating eof input", "err", err)
	}

	eofOutput, err := middleware.CreateQueueMiddleware(
		eofOutputName,
		connSettings,
	)

	if err != nil {
		slog.Error("creating eof output", "err", err)
	}

	messagesMonitor := msgmonitor.NewMessageMonitor()

	eofring := eofring.CreateEofRingAlgorithm(
		eofInput, eofOutput,
		config.FilterAmount,
		uint32(config.Id),
		messagesMonitor,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			if isCoordinator {
				if err := outputQueue.Send(*msg); err != nil {
					slog.Error("while sending message to output queue from ring callback", "err", err)
					return err
				}
			}
			return nil
		},
		config.QueryId,
	)

	return &ConvertedAmountFilter[T, S]{
		leftInputQueue:     leftInputQueue,
		rightInputQueue:    rightInputQueue,
		outputQueue:        outputQueue,
		compareFunc:        compareFunc,
		queryId:            config.QueryId,
		conversionsByDay:   make(map[string]map[string]float64),
		leftKeyFunc:        leftKeyFunc,
		leftSecondKeyFunc:  leftSecondKeyFunc,
		leftValueFunc:      leftValueFunc,
		rightKeyFunc:       rightKeyFunc,
		rightsecondKeyFunc: rightsecondKeyFunc,
		rightValueFunc:     rightValueFunc,
		conversionFunc:     conversionFunc,
		toSaveFunc:         toSaveFunc,
		fromSaveFunc:       fromSaveFunc,
		eofRing:            eofring,
		eofOutputQueue:     eofOutput,
		handlerMessages:    messagesMonitor,
		id:                 uint32(config.Id),
		toIgnoreFunc:       toIgnoreFunc,
		quote:              config.Quote,
		amountThreshold:    config.Amount,
		fileMutex:          sync.Mutex{},
		mapMutex:           sync.Mutex{},
	}, nil
}

func (filter *ConvertedAmountFilter[T, S]) Run() {
	defer filter.close()

	go func() {
		if err := filter.leftInputQueue.StartConsuming(filter.consumeLeft); err != nil {
			slog.Error("while starting consuming from left input queue", "err", err)
			return
		}
	}()
	go filter.eofRing.Run()

	if err := filter.rightInputQueue.StartConsuming(filter.consumeRight); err != nil {
		slog.Error("while starting consuming from right input queue", "err", err)
		return
	}
}

func (filter *ConvertedAmountFilter[T, S]) consumeLeft(msg middleware.Message, ack, _ func()) {
	defer ack()

	result, err := inner.DeserializeData[S](&msg)
	if err != nil {
		slog.Error("while deserializing transfer", "err", err)
		return
	}
	if result.IsEOF() {
		msgToOutput, err := inner.SerializeData(inner.DataMsg[any]{
			ClientID: result.ClientID,
			EOF:      &inner.EOFInfo{},
			Payload:  nil,
			QueryID:  result.QueryID,
		})
		if err != nil {
			slog.Error("while serializing message for finish callback", "client_id", result.ClientID, "err", err)
		} else {
			if err := filter.outputQueue.Send(*msgToOutput); err != nil {
				slog.Error("while calling finish callback", "client_id", result.ClientID, "err", err)
				return
			}
		}
		return
	}

	filter.mapMutex.Lock()
	if _, ok := filter.conversionsByDay[filter.leftKeyFunc(result.Payload)]; !ok {
		filter.conversionsByDay[filter.leftKeyFunc(result.Payload)] = make(map[string]float64)
	}

	filter.conversionsByDay[filter.leftKeyFunc(result.Payload)][filter.leftSecondKeyFunc(result.Payload)] = filter.leftValueFunc(result.Payload)
	filter.mapMutex.Unlock()
	filter.CheckTransfersWithoutConversion(result.Payload)
}

func (filter *ConvertedAmountFilter[T, S]) consumeRight(msg middleware.Message, ack, _ func()) {
	defer ack()

	result, err := inner.DeserializeData[T](&msg)
	if err != nil {
		slog.Error("while deserializing transfer", "err", err)
		return
	}

	if result.IsEOF() {
		filter.closeClientFiles(result.ClientID)
		eofRingMessage := eofmessagetypes.EofRingMessage{
			RealAmount:     result.EOF.TotalMessages,
			ActualAmount:   filter.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			CoordinatorId:  uint32(filter.id),
			FilteredAmount: filter.handlerMessages.GetForwardedMessagesAmountByClientId(result.ClientID),
		}
		serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(eofRingMessage)
		if err != nil {
			slog.Error("While serializing EOF ring message", "err", err)
			return
		}
		if err := filter.eofOutputQueue.Send(
			*serializedEofRingMessage,
		); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
		}
		return
	}

	filter.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)

	payload := result.Payload

	key := filter.rightKeyFunc(payload)

	if filter.toIgnoreFunc(result.Payload) {
		return
	}

	filter.mapMutex.Lock()
	conversion, ok := filter.conversionsByDay[key][datasetToFrank[filter.rightsecondKeyFunc(payload)]]
	filter.mapMutex.Unlock()

	if ok || filter.rightsecondKeyFunc(payload) == filter.quote {
		if filter.compareFunc(payload, filter.conversionFunc(
			payload,
			conversion,
		)) || filter.rightValueFunc(payload) < filter.amountThreshold {
			filter.handlerMessages.AddForwardedMessagesAmountByClientId(result.ClientID, 1)
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
	} else {
		filter.saveTransfersInFile(payload, result.ClientID)
	}
}

func (filter *ConvertedAmountFilter[T, S]) CheckTransfersWithoutConversion(s S) {
	filter.fileMutex.Lock()
	defer filter.fileMutex.Unlock()

	firstKeyFile := strings.ReplaceAll(filter.leftKeyFunc(s), " ", "")
	secondKeyFile := strings.ReplaceAll(filter.leftSecondKeyFunc(s), " ", "")
	filePath := fmt.Sprintf(FILE_LAYOUT, firstKeyFile, secondKeyFile)
	_, err := os.Stat(filePath)

	if err != nil {
		return
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			slog.Error("while opening file", "err", err)
			return
		}
		defer func() {
			if err := file.Close(); err != nil {
				slog.Error("while closing file", "err", err)
			}
			if err := os.Remove(filePath); err != nil {
				slog.Error("while removing file", "err", err)
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

			if filter.toIgnoreFunc(transfer) {
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
	filter.mapMutex.Lock()
	secondKey := datasetToFrank[filter.rightsecondKeyFunc(transfer)]
	filter.mapMutex.Unlock()

	filter.fileMutex.Lock()
	defer filter.fileMutex.Unlock()

	// https://www.solvetic.com/tutoriales/article/1458-entender-los-permisos-linux-chmod/
	filePath := fmt.Sprintf(
		FILE_LAYOUT,
		strings.ReplaceAll(filter.rightKeyFunc(transfer), " ", ""),
		secondKey,
	)
	file, err := os.OpenFile(
		filePath,
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

func (filter *ConvertedAmountFilter[T, S]) closeClientFiles(clientID int) {
	// quizas deberia tener un mutex por file? no se que tan costoso es eso
	filter.fileMutex.Lock()
	defer filter.fileMutex.Unlock()

	pattern := "*_*.csv"
	files, err := filepath.Glob(pattern)
	if err != nil {
		slog.Error("while globbing files for client cleanup", "err", err)
		return
	}

	for _, filePath := range files {
		f, err := os.Open(filePath)
		if err != nil {
			slog.Error("while opening file for client cleanup", "file", filePath, "err", err)
			continue
		}

		scanner := bufio.NewScanner(f)
		var remainingLines []string

		for scanner.Scan() {
			line := scanner.Text()
			transfer, cid, err := filter.fromSaveFunc(line)
			if err != nil {
				slog.Error("while parsing saved line during cleanup", "file", filePath, "err", err)
				continue
			}

			if filter.toIgnoreFunc(transfer) {
				continue
			}

			if cid != clientID || !filter.compareFunc(transfer, filter.conversionFunc(
				transfer,
				filter.conversionsByDay[filter.rightKeyFunc(transfer)][datasetToFrank[filter.rightsecondKeyFunc(transfer)]],
			)) {
				remainingLines = append(remainingLines, line)
			} else {
				msgOutput, err := inner.SerializeData(inner.DataMsg[T]{
					ClientID: cid,
					QueryID:  filter.queryId,
					Payload:  transfer,
					EOF:      nil,
				})
				if err != nil {
					slog.Error("while serializing saved transfer during cleanup", "file", filePath, "err", err)
					continue
				}
				if err := filter.outputQueue.Send(*msgOutput); err != nil {
					slog.Error("while sending saved transfer during cleanup", "file", filePath, "err", err)
					continue
				}
				filter.handlerMessages.AddForwardedMessagesAmountByClientId(cid, 1)
			}
		}

		if err := f.Close(); err != nil {
			slog.Error("while closing file during client cleanup", "file", filePath, "err", err)
		}

		// podria hacer un directorio por cliente y guardar distinto en los archivos. quizas sea más facil para manejar esto. Incluso las sin procesar podrían ser millones
		// y estoy levantando en memoria un monton de data. para la proxima iteración CAMBIAR.
		if len(remainingLines) == 0 {
			if err := os.Remove(filePath); err != nil {
				slog.Error("while removing emptied file during cleanup", "file", filePath, "err", err)
			}
			continue
		}

		dir := filepath.Dir(filePath)
		tmpFile, err := os.CreateTemp(dir, ".tmp-*")
		if err != nil {
			slog.Error("while creating temp file during cleanup", "err", err)
			continue
		}

		w := bufio.NewWriter(tmpFile)
		for _, l := range remainingLines {
			if _, err := w.WriteString(l + "\n"); err != nil {
				slog.Error("while writing to temp file during cleanup", "err", err)
			}
		}
		if err := w.Flush(); err != nil {
			slog.Error("while flushing temp file during cleanup", "err", err)
		}
		if err := tmpFile.Close(); err != nil {
			slog.Error("while closing temp file during cleanup", "err", err)
		}

		if err := os.Rename(tmpFile.Name(), filePath); err != nil {
			slog.Error("while replacing original file during cleanup", "file", filePath, "err", err)
		}
	}
}

func (filter *ConvertedAmountFilter[T, S]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := filter.leftInputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from left input queue", "err", err)
	}
	if err := filter.rightInputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from right input queue", "err", err)
	}
}

func (filter *ConvertedAmountFilter[T, S]) close() {

	if err := filter.leftInputQueue.Close(); err != nil {
		slog.Error("while closing left input queue", "err", err)
	}
	if err := filter.rightInputQueue.Close(); err != nil {
		slog.Error("while closing right input queue", "err", err)
	}
	if err := filter.outputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
