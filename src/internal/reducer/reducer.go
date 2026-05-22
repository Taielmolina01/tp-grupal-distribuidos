package reducer

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/worker"
)

type ReducerConfig struct {
	Id                int
	ReducerAmount     int
	MomHost           string
	MomPort           int
	InputExchange     string
	QueryId           uint8
	InputQueue        string
	OutputQueues      []string
	InputRoutingKeys  []string
	InputEofsExpected int
	JoinsAmount       int
}

type Reducer[T comparable] struct {
	id                int
	inputExchange     middleware.Middleware
	outputQueues      []middleware.Middleware
	actualValues      map[int]map[string]T
	callback          func(T, T) T
	keyFunc           func(T) string
	eofHandler        eofring.EofRingAlgorithm
	handlerMessages   msgmonitor.MessageMonitor
	outputQueueEof    middleware.Middleware
	queryId           uint8
	inputEofsExpected int
	inputEofCount     map[int]int
	totalRealAmount   map[int]uint32
	lock              sync.Mutex
	joinsAmount       int
}

func newReducer[T comparable](
	config ReducerConfig,
	callback func(T, T) T,
	keyFunc func(T) string,
	queryId uint8,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.InputQueue, config.InputRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	for _, outputQueue := range config.OutputQueues {
		_, err := middleware.CreateQueueMiddleware(outputQueue, connSettings)
		if err != nil {
			return nil, err
		}
	}

	next := config.Id + 1
	if config.Id == config.ReducerAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		"REDUCE_"+strconv.Itoa(config.Id),
		connSettings,
	)

	eofOutput, err := middleware.CreateQueueMiddleware(
		"REDUCE_"+strconv.Itoa(next),
		connSettings,
	)

	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	expectedEofs := config.InputEofsExpected
	if expectedEofs <= 0 {
		expectedEofs = 1
	}

	reducer := &Reducer[T]{
		id:                config.Id,
		inputExchange:     inputExchange,
		outputQueues:      []middleware.Middleware{},
		actualValues:      map[int]map[string]T{},
		callback:          callback,
		keyFunc:           keyFunc,
		inputEofsExpected: expectedEofs,
		inputEofCount:     map[int]int{},
		totalRealAmount:   map[int]uint32{},
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.ReducerAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message) error {
			reducer.lock.Lock()
			values := reducer.actualValues[clientID]
			delete(reducer.actualValues, clientID)
			reducer.lock.Unlock()

			for _, v := range values {
				msgOutput, err := inner.SerializeData(inner.DataMsg[T]{
					Payload:  v,
					ClientID: clientID,
					QueryID:  reducer.queryId,
				})
				if err != nil {
					return err
				}
				slog.Info("Reducer sending message to output exchange", "client_id", clientID, "payload", v)
				if err := reducer.outputQueues[reducer.calculateIndexForShard(clientID, keyFunc(v))].Send(*msgOutput); err != nil { // shard by client_id_from_Bank
					return err
				}
			}

			for _, outputQueue := range reducer.outputQueues {
				if err := outputQueue.Send(*msg); err != nil { // shard by client_id_from_Bank
					return err
				}
			}
			return nil
		},
		queryId,
	)

	reducer.eofHandler = eofHandler
	reducer.handlerMessages = handlerMessages
	reducer.outputQueueEof = eofOutput
	reducer.queryId = queryId

	return reducer, nil
}

func (reducer *Reducer[T]) Run() {
	go reducer.eofHandler.Run()
	if err := reducer.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		reducer.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (reducer *Reducer[T]) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	result, err := inner.DeserializeData[T](&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if result.IsEOF() {
		reducer.inputEofCount[result.ClientID]++
		reducer.totalRealAmount[result.ClientID] = result.EOF.TotalMessages
		// slog.Info("input EOF received", "client_id", result.ClientID, "count", reducer.inputEofCount[result.ClientID], "expected", reducer.inputEofsExpected)

		eofRingMessage := eofmessagetypes.EofRingMessage{
			RealAmount:     reducer.totalRealAmount[result.ClientID],
			ActualAmount:   reducer.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			Leader:         uint32(reducer.id),
			FilteredAmount: reducer.handlerMessages.GetFilteredMessagesAmountByClientId(result.ClientID),
		}
		delete(reducer.inputEofCount, result.ClientID)
		delete(reducer.totalRealAmount, result.ClientID)

		serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(eofRingMessage)

		if err != nil {
			slog.Error("While serializing EOF message", "err", err)
			return
		}

		if err := reducer.outputQueueEof.Send(
			*serializedEofRingMessage,
		); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
		}
		// slog.Info("EOF message sent to EOF ring", "reducer_id", reducer.id, "client_id", eofRingMessage.ClientId, "real_amount", eofRingMessage.RealAmount, "actual_amount", eofRingMessage.ActualAmount)
	} else {
		key := reducer.keyFunc(result.Payload)
		reducer.lock.Lock()
		if reducer.actualValues[result.ClientID] == nil {
			reducer.actualValues[result.ClientID] = map[string]T{}
		}
		existing, ok := reducer.actualValues[result.ClientID][key]
		if !ok {
			reducer.actualValues[result.ClientID][key] = result.Payload
			reducer.handlerMessages.AddFilteredMessagesAmountByClientId(result.ClientID, 1)
		} else {
			reducer.actualValues[result.ClientID][key] = reducer.callback(existing, result.Payload)
		}
		reducer.lock.Unlock()
		reducer.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)
	}

}

func (reducer *Reducer[T]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := reducer.Close(); err != nil {
		slog.Error("While closing reducer node", "err", err)
	}
}

func (reducer *Reducer[T]) Close() error {
	if err := reducer.inputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := reducer.inputExchange.Close(); err != nil {
		return err
	}
	for _, outputQueue := range reducer.outputQueues {
		if err := outputQueue.StopConsuming(); err != nil {
			return err
		}
		if err := outputQueue.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (reducer *Reducer[T]) convertToBytes(FromBank string, clientID int) []byte {
	return []byte(fmt.Sprintf("%v%d", FromBank, clientID))
}

func (reducer *Reducer[T]) calculateIndexForShard(clientID int, FromBank string) int {
	bytes := reducer.convertToBytes(FromBank, clientID)
	hash := fnv.New64a()
	hash.Write(bytes)
	return int(hash.Sum64() % uint64(reducer.joinsAmount))
}
