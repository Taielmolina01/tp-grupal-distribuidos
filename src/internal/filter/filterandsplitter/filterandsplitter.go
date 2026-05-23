package filterandsplitter

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type FilterAndSplitterConfig struct {
	Id        int
	StartDate time.Time
	EndDate   time.Time

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	FilterAndSpliterAmount int

	MomHost string
	MomPort int

	InputExchange    string
	InputQueue       string
	InputRoutingKeys []string

	QueryID int
}

type clientState struct {
	msgCount     uint64
	msgSentCount uint64
}

type FilterAndSplitter struct {
	id        int
	startDate time.Time
	endDate   time.Time

	outputMiddlewareAmount int

	inputExchange middleware.Middleware
	outputQueues  []middleware.Middleware
	eofInput      middleware.Middleware
	eofOutput     middleware.Middleware
	eofHandler    eofring.EofRingAlgorithm

	handlerMessages msgmonitor.MessageMonitor

	queryID int
}

func declareOutputQueues(config FilterAndSplitterConfig, connSettings middleware.ConnSettings) ([]middleware.Middleware, error) {
	outputQueues := make([]middleware.Middleware, 0, config.OutputMiddlewareAmount)
	for i := range config.OutputMiddlewareAmount {
		q, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_%d", config.OutputMiddlewarePrefix, i), connSettings)
		if err != nil {
			for _, opened := range outputQueues {
				opened.Close()
			}
			return nil, fmt.Errorf("creating output queue %d: %w", i, err)
		}
		outputQueues = append(outputQueues, q)
	}
	return outputQueues, nil
}

func getRingNextIndex(config FilterAndSplitterConfig) int {
	if config.Id == config.FilterAndSpliterAmount-1 {
		return 0
	}
	return config.Id + 1
}

func NewFilterAndSplitter(config FilterAndSplitterConfig) (_ *FilterAndSplitter, err error) {
	const ring_prefix = "FILTER_AND_SPLIITER_EOF_"
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	handlerMessages := msgmonitor.NewMessageMonitor()

	var (
		inputExchange middleware.Middleware
		outputQueues  []middleware.Middleware
		eofInput      middleware.Middleware
		eofOutput     middleware.Middleware
	)

	defer func() {
		if err != nil {
			if eofOutput != nil {
				eofOutput.Close()
			}
			if eofInput != nil {
				eofInput.Close()
			}
			for _, q := range outputQueues {
				q.Close()
			}
			if inputExchange != nil {
				inputExchange.Close()
			}
		}
	}()

	inputExchange, err = middleware.CreateExchangeMiddleware(
		config.InputExchange,
		config.InputQueue,
		config.InputRoutingKeys,
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	outputQueues, err = declareOutputQueues(config, connSettings)
	if err != nil {
		return nil, fmt.Errorf("declaring output queues: %w", err)
	}

	eofInput, err = middleware.CreateQueueMiddleware(ring_prefix+strconv.Itoa(config.Id), connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF input queue: %w", err)
	}

	eofOutput, err = middleware.CreateQueueMiddleware(ring_prefix+strconv.Itoa(getRingNextIndex(config)), connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating EOF output queue: %w", err)
	}

	eofHandler := eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.FilterAndSpliterAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			handlerMessages.RemoveClient(clientID)

			if isCoordinator {
				return outputQueues[clientID].Send(*msg)
			}
			return nil
		},
		uint8(config.QueryID),
	)

	return &FilterAndSplitter{
		id:                     config.Id,
		startDate:              config.StartDate,
		endDate:                config.EndDate,
		outputMiddlewareAmount: config.OutputMiddlewareAmount,
		queryID:                config.QueryID,
		handlerMessages:        handlerMessages,
		inputExchange:          inputExchange,
		outputQueues:           outputQueues,
		eofInput:               eofInput,
		eofOutput:              eofOutput,
		eofHandler:             eofHandler,
	}, nil
}

func (f *FilterAndSplitter) Run() {
	defer f.close()
	go f.eofHandler.Run()

	if err := f.inputExchange.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		f.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (f *FilterAndSplitter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.inputExchange.StopConsuming()
	f.eofInput.StopConsuming()
}

func (f *FilterAndSplitter) close() {
	f.inputExchange.Close()
	f.eofInput.Close()
	f.eofOutput.Close()

	for _, queue := range f.outputQueues {
		queue.Close()
	}
}

func (f *FilterAndSplitter) handleInput(msg middleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[transfer.Transfer](&msg)

	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		f.handleEOF(*m)
		return
	}

	f.handleRecord(m.ClientID, m.Payload)
}

func (f *FilterAndSplitter) handleRecord(clientID int, record transfer.Transfer) {
	f.handlerMessages.AddProcessedMessagesAmountByClientId(clientID, 1)

	if record.Timestamp.Before(f.startDate) || record.Timestamp.After(f.endDate) {
		return
	}

	if record.FromBankAccount == record.ToBankAccount && record.FromBank == record.ToBank {
		return
	}

	output := []transfer.SplittedTransfer{
		{Transfer: record, IsLeftPart: true},
		{Transfer: record, IsLeftPart: false},
	}

	for _, o := range output {
		var bank, acc string
		if o.IsLeftPart {
			bank = o.Transfer.FromBank
			acc = o.Transfer.FromBankAccount
		} else {
			bank = o.Transfer.ToBank
			acc = o.Transfer.ToBankAccount
		}

		output_index := f.shardFor(clientID, bank, acc)

		msg, err := inner.SerializeData(inner.DataMsg[transfer.SplittedTransfer]{
			ClientID: clientID,
			QueryID:  uint8(f.queryID),
			Payload:  o,
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		f.handlerMessages.AddFilteredMessagesAmountByClientId(clientID, 1)
		if err := f.outputQueues[output_index].Send(*msg); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (f *FilterAndSplitter) shardFor(clientID int, bank, acc string) int {
	h := fnv.New32a()
	fmt.Fprintf(h, "%d\x00%s\x00%s", clientID, bank, acc)
	return int(h.Sum32() % uint32(f.outputMiddlewareAmount))
}

func (f *FilterAndSplitter) handleEOF(data inner.DataMsg[transfer.Transfer]) {
	eofRingMessage := eofmessagetypes.EofRingMessage{
		RealAmount:     data.EOF.TotalMessages,
		ActualAmount:   f.handlerMessages.GetProcessedMessagesAmountByClientId(data.ClientID),
		ClientId:       data.ClientID,
		CoordinatorId:  uint32(f.id),
		FilteredAmount: f.handlerMessages.GetFilteredMessagesAmountByClientId(data.ClientID),
	}
	serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(eofRingMessage)
	if err != nil {
		slog.Error("While serializing EOF ring message", "err", err)
		return
	}
	if err := f.eofOutput.Send(
		*serializedEofRingMessage,
	); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
	}
	slog.Info("EOF message sent to EOF ring", "filter_id", f.id, "client_id", eofRingMessage.ClientId, "real_amount", eofRingMessage.RealAmount, "actual_amount", eofRingMessage.ActualAmount)
	slog.Info("Total messages processed by filter", "filter_id", f.id, "client_id", f.id, "processed_messages", f.handlerMessages.GetProcessedMessagesAmountByClientId(int(f.id)))
}
