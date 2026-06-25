package commonfilter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/msgsend"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewFilter[T any, O any](
	config filter.FilterConfig,
	filterFunction func(T) bool,
	inputToOutput func(T) O,
	shardKeys func(O, uint64) []string,
	inputCodec wire.Codec[T],
	outputCodec wire.Codec[O],
	outputClusters []newmiddleware.ShardedCluster,
) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputExchange newmiddleware.Middleware
		constructErr  error
	)
	defer func() {
		if constructErr == nil {
			return
		}
		cleanup.Close(inputExchange)
		for _, cl := range outputClusters {
			cleanup.Close(cl.Middleware)
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputExchange, constructErr = newmiddleware.NewShardedMiddleware(
		connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey,
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
		queryId:              config.QueryID,
		filterFunction:       filterFunction,
		outputTransform:      inputToOutput,
		shardKeys:            shardKeys,
		inputCodec:           inputCodec,
		outputCodec:          outputCodec,
		inputExchange:        inputExchange,
		outputClusters:       outputClusters,
		inputAmount:          config.FilterAmount,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
		isLastNode:           config.IsLastNode,
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
	cleanup.Close(f.inputExchange)
	for _, cluster := range f.outputClusters {
		cleanup.Close(cluster.Middleware)
	}
}

func (f *Filter[T, O]) handleBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})
	aborted := make(map[int]bool)

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, f.inputCodec)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := f.states.For(clientID)
		tracker := state.tracker

		if input.Abort || aborted[clientID] {
			aborted[clientID] = true
			modified[clientID] = state
			continue
		}

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
		if aborted[clientID] && !f.isLastNode {
			for _, cl := range f.outputClusters {
				if err := msgsend.SendAbort(cl.Middleware, newmiddleware.BroadcastRoutingKey, clientID); err != nil {
					slog.Error("While emitting abort", "err", err)
					nack()
					f.stopConsuming()
					return
				}
			}
			f.states.Delete(clientID)
			f.checkpoint.DeleteClient(clientID)
			continue
		}
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
	type clusterKey struct {
		index int
		rk    string
	}
	byCluster := make(map[clusterKey][]O)

	for _, record := range input.Records {
		if !f.filterFunction(record) {
			continue
		}
		o := f.outputTransform(record)
		keys := f.shardKeys(o, input.Seq)

		for index, cluster := range f.outputClusters {
			rk := fmt.Sprintf("shard-%d", cluster.Hasher.ShardFor(input.ClientID, keys...))
			ck := clusterKey{index, rk}
			byCluster[ck] = append(byCluster[ck], o)
		}
	}

	for ck, group := range byCluster {
		cluster := f.outputClusters[ck.index]
		body := batch.Write(input.ClientID, f.queryId, uint8(f.id), input.Seq, group, f.outputCodec)
		if err := cluster.Middleware.Send(newmiddleware.Message{Body: body, RoutingKey: ck.rk}); err != nil {
			return err
		}
		state.outputTracker.RegisterBatch(fmt.Sprintf("%d_%s", ck.index, ck.rk))
	}
	return nil
}

func (f *Filter[T, O]) finishStep(clientID int, state *clientState) error {
	eofSeq := state.tracker.GetEOFSeq()
	for ci, cluster := range f.outputClusters {
		for i := range cluster.Hasher.TotalShards() {
			rk := fmt.Sprintf("shard-%d", i)
			total := state.outputTracker.CountFor(fmt.Sprintf("%d_%s", ci, rk))
			eofBody := batch.WriteEOF(clientID, f.queryId, uint8(f.id), eofSeq, uint32(total))
			if err := cluster.Middleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
				slog.Error("finish step: send EOF failed", "cluster", ci, "routingKey", rk, "err", err)
				return err
			}
		}
	}
	return nil
}
