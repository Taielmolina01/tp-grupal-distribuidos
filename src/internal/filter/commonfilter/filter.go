package commonfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"strings"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewFilter[T any, O any](
	config filter.FilterConfig,
	filterFunction func(T) bool,
	inputToOutput func(T) O,
	shardKeys func(O) []string,
	inputCodec wire.Codec[T],
	outputCodec wire.Codec[O],
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	newConnSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(
		config.InputExchange,
		config.InputQueue,
		config.InputRoutingKeys,
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	outputExchange, err := newmiddleware.NewShardedMiddleware(newConnSettings, config.OutputExchange, "", "")
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input exchange after output exchange creation failure", "err", err)
		}
		return nil, err
	}

	return &Filter[T, O]{
		id:              uint32(config.Id),
		queryId:         config.QueryId,
		filterType:      config.Type,
		filterFunction:  filterFunction,
		outputTransform: inputToOutput,
		shardKeys:       shardKeys,
		inputCodec:      inputCodec,
		outputCodec:     outputCodec,
		inputExchange:   inputExchange,
		outputExchange:  outputExchange,
		multiHasher:     shard.NewMultiCluster(config.OutputClusters),
		inputAmount:     config.FilterAmount, // Igual que en el otro, reempalzar por el N de nodos de la etapa anterior
		states: statemap.New(func() *clientState {
			return &clientState{
				tracker:       sendertracker.New(10_000_000),
				outputTracker: outputtracker.New(),
			}
		}),
	}, nil
}

func (f *Filter[T, O]) Run() {
	defer f.close()

	if err := f.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		f.handleInput(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (f *Filter[T, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	f.stopConsuming()
}

func (f *Filter[T, O]) stopConsuming() {
	if err := f.inputExchange.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "err", err)
	}
}

func (f *Filter[T, O]) close() {
	if err := f.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := f.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}

func (f *Filter[T, O]) handleInput(msg middleware.Message, ack func(), nack func()) {
	input, err := batch.Read(msg.Body, f.inputCodec)
	if err != nil {
		slog.Error("decode failed", "err", err)
		ack()
		return
	}

	clientID := input.ClientID

	state := f.states.For(clientID)
	tracker := state.tracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		tracker.RegisterBatch(int(input.SenderID))
		if err := f.processBatch(input, state); err != nil {
			slog.Error("process batch failed", "err", err)
			nack()
			f.stopConsuming()
			return
		}
	}

	tracker.Claim(int(input.SenderID), input.Seq)

	if tracker.IsComplete(f.inputAmount) {
		if err := f.finishStep(clientID, state); err != nil {
			slog.Error("finish step failed", "err", err)
			nack()
			f.stopConsuming()
			return
		}
		f.states.Delete(clientID)
	}

	ack()
}

func (f *Filter[T, O]) processBatch(input batch.Msg[T], state *clientState) error {
	outputs := make([]O, 0)
	slog.Info("MSG", "seq", input.Seq, "EOF", input.EOF)
	for _, record := range input.Records {

		if f.filterFunction(record) {
			outputs = append(outputs, f.outputTransform(record))
		}
	}

	if len(outputs) == 0 {
		return nil
	}

	byKeys := make(map[string]struct {
		keys    []string
		outputs []O
	})
	for _, o := range outputs {
		keys := f.multiHasher.RoutingKeysFor(input.ClientID, f.shardKeys(o)...)
		mapKey := strings.Join(keys, "|")
		entry := byKeys[mapKey]
		entry.keys = keys
		entry.outputs = append(entry.outputs, o)
		byKeys[mapKey] = entry
	}

	for _, entry := range byKeys {
		body := batch.Write(input.ClientID, f.queryId, uint8(f.id), input.Seq, entry.outputs, f.outputCodec)
		if err := f.outputExchange.Send(newmiddleware.Message{Body: body, RoutingKeys: entry.keys}); err != nil {
			return err
		}
		for _, k := range entry.keys {
			state.outputTracker.RegisterBatch(k)
		}
	}
	return nil
}

func (f *Filter[T, O]) finishStep(clientID int, state *clientState) error {
	eofSeq := state.tracker.GetEOFSeq()
	var sendErr error
	for _, rk := range f.multiHasher.AllRoutingKeys() {
		if sendErr != nil {
			break
		}
		total := state.outputTracker.CountFor(rk)
		eofBody := batch.WriteEOF(clientID, f.queryId, uint8(f.id), eofSeq, uint32(total))
		if err := f.outputExchange.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			slog.Error("finish step: send EOF failed", "routingKey", rk, "err", err)
			sendErr = err
		}
	}
	return sendErr
}
