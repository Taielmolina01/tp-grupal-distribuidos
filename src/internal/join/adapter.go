package join

import (
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

type TwoInputAdapter[L, R, O any] struct {
	id                int
	joinAmount        int
	handlerMessages   msgmonitor.MessageMonitor
	eofHandler        eofring.EofRingAlgorithm
	outputQueueEof    middleware.Middleware
	totalRealAmount   map[int]uint32
	ringFired         map[int]bool
	queryID           uint8
	join              *Join[L, R, O]
	leftInput         middleware.Middleware
	rightInput        middleware.Middleware
	leftEofCount      map[int]int
	rightEofCount     map[int]int
	leftEofsExpected  int
	rightEofsExpected int
	lock              sync.Mutex
}

func newTwoInputJoin[L, R, O any](
	config JoinConfig,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
	leftCombine func(L, L) L,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	leftInput, err := middleware.CreateExchangeMiddleware(config.LeftInputExchange, config.LeftInputQueue, config.LeftRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	rightInput, err := middleware.CreateExchangeMiddleware(config.RightInputExchange, config.RightInputQueue, config.RightRoutingKeys, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		return nil, err
	}

	output, err := middleware.CreateExchangeMiddleware(config.OutputExchange, config.OutputQueue, config.OutputRoutingKeys, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		if err := rightInput.Close(); err != nil {
			slog.Error("while closing right input", "err", err)
		}
		return nil, err
	}

	slog.Info("join started",
		"left_exchange", config.LeftInputExchange, "left_queue", config.LeftInputQueue, "left_keys", config.LeftRoutingKeys,
		"right_exchange", config.RightInputExchange, "right_queue", config.RightInputQueue, "right_keys", config.RightRoutingKeys,
		"output_exchange", config.OutputExchange, "output_keys", config.OutputRoutingKeys,
	)

	leftEofs := config.LeftEofsExpected
	if leftEofs <= 0 {
		leftEofs = 1
	}
	rightEofs := config.RightEofsExpected
	if rightEofs <= 0 {
		rightEofs = 1
	}

	next := config.Id + 1
	if config.Id == config.Amount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		"JOIN_Q2_"+strconv.Itoa(config.Id),
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	eofOutput, err := middleware.CreateQueueMiddleware(
		"JOIN_Q2_"+strconv.Itoa(next),
		connSettings,
	)

	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	adapter := &TwoInputAdapter[L, R, O]{
		id:                config.Id,
		joinAmount:        config.Amount,
		handlerMessages:   handlerMessages,
		outputQueueEof:    eofOutput,
		totalRealAmount:   map[int]uint32{},
		ringFired:         map[int]bool{},
		queryID:           config.QueryID,
		join:              newJoin[L, R, O](output, leftKey, rightKey, combine, leftCombine, config.QueryID),
		leftInput:         leftInput,
		rightInput:        rightInput,
		leftEofCount:      map[int]int{},
		rightEofCount:     map[int]int{},
		leftEofsExpected:  leftEofs,
		rightEofsExpected: rightEofs,
	}

	adapter.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.Amount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message) error {
			adapter.join.HandleQueryEOF(clientID)

			return output.Send(*msg)
		},
		config.QueryID,
	)

	return adapter, nil
}

func (a *TwoInputAdapter[L, R, O]) Run() {
	done := make(chan struct{})
	go a.eofHandler.Run()
	go func() {
		if err := a.leftInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			data, err := inner.DeserializeData[L](&msg)
			if err != nil {
				slog.Error("while deserializing left message", "err", err)
				return
			}
			if data.IsEOF() {
				slog.Info("join got left EOF", "client_id", data.ClientID, "total_messages", data.EOF.TotalMessages)
				a.handleEOF(data.ClientID, true, data.EOF.TotalMessages)
				return
			}
			a.join.HandleLeft(data.ClientID, data.Payload)
			a.handlerMessages.AddProcessedMessagesAmountByClientId(data.ClientID, 1)
		}); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := a.rightInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		defer ack()
		data, err := inner.DeserializeData[R](&msg)
		if err != nil {
			slog.Error("while deserializing right message", "err", err)
			return
		}
		if data.IsEOF() {
			slog.Info("join got right EOF", "client_id", data.ClientID)
			a.handleEOF(data.ClientID, false, 0)
			return
		}
		a.join.HandleRight(data.ClientID, data.Payload)
	}); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
}

func (a *TwoInputAdapter[L, R, O]) handleEOF(clientID int, isLeft bool, totalMessages uint32) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if isLeft {
		a.leftEofCount[clientID]++
		if totalMessages > a.totalRealAmount[clientID] {
			a.totalRealAmount[clientID] = totalMessages
		}
	} else {
		a.rightEofCount[clientID]++
	}

	if a.leftEofCount[clientID] < a.leftEofsExpected || a.rightEofCount[clientID] < a.rightEofsExpected {
		return
	}
	if a.ringFired[clientID] {
		return
	}
	a.ringFired[clientID] = true

	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     a.totalRealAmount[clientID],
		ActualAmount:   a.handlerMessages.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		Leader:         uint32(a.id),
		FilteredAmount: a.handlerMessages.GetFilteredMessagesAmountByClientId(clientID),
	}

	delete(a.leftEofCount, clientID)
	delete(a.rightEofCount, clientID)
	delete(a.totalRealAmount, clientID)

	serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
	if err != nil {
		slog.Error("while serializing ring message", "err", err)
		return
	}
	if err := a.outputQueueEof.Send(*serialized); err != nil {
		slog.Error("while sending ring message", "err", err)
	}
	slog.Info("join sent EOF ring message", "join_id", a.id, "client_id", clientID, "real_amount", ringMsg.RealAmount, "actual_amount", ringMsg.ActualAmount)
}

type SingleInputAdapter[T, O any] struct {
	join   *Join[T, T, O]
	input  middleware.Middleware
	isLeft func(T) bool
}

func newSingleInputJoin[T, O any](
	config JoinConfig,
	isLeft func(T) bool,
	leftKey func(T) string,
	rightKey func(T) string,
	combine func(T, T) O,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	input, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.InputQueue, config.InputRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	output, err := middleware.CreateExchangeMiddleware(config.OutputExchange, config.OutputQueue, config.OutputRoutingKeys, connSettings)
	if err != nil {
		if err := input.Close(); err != nil {
			slog.Error("while closing input", "err", err)
		}
		return nil, err
	}

	return &SingleInputAdapter[T, O]{
		join:   newJoin[T, T, O](output, leftKey, rightKey, combine, nil, config.QueryID),
		input:  input,
		isLeft: isLeft,
	}, nil
}

func (a *SingleInputAdapter[T, O]) Run() {
	if err := a.input.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		defer ack()
		data, err := inner.DeserializeData[T](&msg)
		if err != nil {
			slog.Error("while deserializing message", "err", err)
			return
		}
		if data.IsEOF() {
			a.join.HandleQueryEOF(data.ClientID)
			return
		}
		if a.isLeft(data.Payload) {
			a.join.HandleLeft(data.ClientID, data.Payload)
		} else {
			a.join.HandleRight(data.ClientID, data.Payload)
		}
	}); err != nil {
		slog.Error("while consuming input", "err", err)
	}
}

func (a *SingleInputAdapter[T, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := a.input.StopConsuming(); err != nil {
		slog.Error("while stopping input", "err", err)
	}
	if err := a.input.Close(); err != nil {
		slog.Error("while closing input", "err", err)
	}
	if err := a.join.output.Close(); err != nil {
		slog.Error("while closing output", "err", err)
	}
}

func (a *TwoInputAdapter[L, R, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := a.leftInput.StopConsuming(); err != nil {
		slog.Error("while stopping left input", "err", err)
	}
	if err := a.rightInput.StopConsuming(); err != nil {
		slog.Error("while stopping right input", "err", err)
	}
	if err := a.leftInput.Close(); err != nil {
		slog.Error("while closing left input", "err", err)
	}
	if err := a.rightInput.Close(); err != nil {
		slog.Error("while closing right input", "err", err)
	}
	if err := a.join.output.Close(); err != nil {
		slog.Error("while closing output", "err", err)
	}
}
