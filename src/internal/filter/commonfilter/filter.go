package commonfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
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
	routeRecord func(clientID int, o O) []string,
	inputCodec wire.Codec[T],
	outputCodec wire.Codec[O],
	outputMiddleware newmiddleware.Middleware,
	router shard.OutputRouter,
) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputExchange newmiddleware.Middleware
		constructErr  error
	)
	defer func() {
		if constructErr != nil {
			if inputExchange != nil {
				if err := inputExchange.Close(); err != nil {
					slog.Error("While closing input exchange after construction failure", "err", err)
				}
			}
			if err := outputMiddleware.Close(); err != nil {
				slog.Error("While closing output middleware after construction failure", "err", err)
			}
		}
	}()

	inputExchange, constructErr = newmiddleware.NewShardedMiddleware(
		connSettings, config.InputExchange, config.InputQueue, config.InputRoutingKeys[0],
	)
	if constructErr != nil {
		return nil, constructErr
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if err != nil {
		constructErr = err
		return nil, err
	}

	recovered, err := ckpt.Load()
	if err != nil {
		constructErr = err
		return nil, err
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			tracker:       sendertracker.New(10_000_000),
			outputTracker: outputtracker.New(),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &Filter[T, O]{
		id:                   uint32(config.Id),
		queryId:              config.QueryId,
		filterType:           config.Type,
		filterFunction:       filterFunction,
		outputTransform:      inputToOutput,
		routeRecord:          routeRecord,
		inputCodec:           inputCodec,
		outputCodec:          outputCodec,
		inputExchange:        inputExchange,
		outputExchange:       outputMiddleware,
		router:               router,
		inputAmount:          config.FilterAmount,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (f *Filter[T, O]) Run() {
	defer f.close()

	if err := f.inputExchange.StartConsumingBatch(f.persistBatchSize, f.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		f.handleBatch(msgs, ack, nack)
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

func (f *Filter[T, O]) handleBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, f.inputCodec)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := f.states.For(clientID)
		tracker := state.tracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
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
		modified[clientID] = state

		if tracker.IsComplete(f.inputAmount) {
			if err := f.finishStep(clientID, state); err != nil {
				slog.Error("finish step failed", "err", err)
				nack()
				f.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			f.states.Delete(clientID)
			f.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := f.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			f.stopConsuming()
			return
		}
	}

	ack()
}

func (f *Filter[T, O]) processBatch(input batch.Msg[T], state *clientState) error {
	outputs := make([]O, 0)
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
		keys := f.routeRecord(input.ClientID, o)
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
	for _, rk := range f.router.AllRoutingKeys() {
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
