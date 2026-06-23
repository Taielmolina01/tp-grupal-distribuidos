package averagefilter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/q3filter"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const avgFractionThreshold = 100

func NewAverageFilter(config AverageFilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputTransfersMiddleware newmiddleware.Middleware
		inputAvgsMiddleware      newmiddleware.Middleware
		outputQueue              newmiddleware.Middleware
		err                      error
	)

	defer func() {
		if err == nil {
			return
		}
		if outputQueue != nil {
			if err := outputQueue.Close(); err != nil {
				slog.Error("While closing output queue", "err", err)
			}
		}
		if inputAvgsMiddleware != nil {
			if err := inputAvgsMiddleware.Close(); err != nil {
				slog.Error("While closing input avgs middleware", "err", err)
			}
		}
		if inputTransfersMiddleware != nil {
			if err := inputTransfersMiddleware.Close(); err != nil {
				slog.Error("While closing input transfers middleware", "err", err)
			}
		}
	}()

	transfersQueue := config.InputTransfersMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	transfersShardKey := fmt.Sprintf("shard-%d", config.Id)
	inputTransfersMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputTransfersMiddlewarePrefix, transfersQueue, transfersShardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input transfers middleware: %w", err)
	}

	avgsQueue := config.InputAvgsMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	inputAvgsMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.InputAvgsMiddlewarePrefix, avgsQueue)
	if err != nil {
		return nil, fmt.Errorf("creating input avgs middleware: %w", err)
	}

	outputQueue, err = newmiddleware.NewQueueMiddleware(connSettings, config.OutputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	recovered, err := ckpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			avgs:             map[string]float64{},
			transfersTracker: sendertracker.New(10_000_000),
			avgsTracker:      sendertracker.New(10_000_000),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &AverageFilter{
		id:                       uint32(config.Id),
		queryID:                  config.QueryID,
		inputTransfersMiddleware: inputTransfersMiddleware,
		inputAvgsMiddleware:      inputAvgsMiddleware,
		outputQueue:              outputQueue,
		expectedTransfersEofs:    config.ExpectedTransfersEofs,
		avgsExpectedEofs:         config.ExpectedAvgEofs,
		maxBatchSize:             config.MaxBatchSize,
		maxBatchBytes:            config.MaxBatchBytes,
		states:                   states,
		checkpoint:               ckpt,
		persistBatchSize:         config.PersistBatchSize,
		persistFlushInterval:     config.PersistFlushInterval,
	}, nil
}

func (af *AverageFilter) Run() {
	defer af.close()

	go af.consumeAvgs()
	if err := af.inputTransfersMiddleware.StartConsumingBatch(af.persistBatchSize, af.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		af.handleTransferBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming transfers", "err", err)
	}
}

func (af *AverageFilter) consumeAvgs() {
	if err := af.inputAvgsMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		af.handleAvgBatch(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming avgs", "err", err)
	}
}

func (af *AverageFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	af.stopConsuming()
}

func (af *AverageFilter) stopConsuming() {
	if err := af.inputAvgsMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input avgs consumer", "filter_id", af.id, "err", err)
	}
	if err := af.inputTransfersMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input transfers consumer", "filter_id", af.id, "err", err)
	}
}

func (af *AverageFilter) close() {
	if err := af.inputTransfersMiddleware.Close(); err != nil {
		slog.Error("While closing input transfers middleware", "filter_id", af.id, "err", err)
	}
	if err := af.inputAvgsMiddleware.Close(); err != nil {
		slog.Error("While closing input avgs middleware", "filter_id", af.id, "err", err)
	}
	if err := af.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "filter_id", af.id, "err", err)
	}
}

func (af *AverageFilter) handleTransferBatch(msgs []newmiddleware.Message, ack, nack func()) {
	af.lock.Lock()
	defer af.lock.Unlock()

	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := q3filter.Read(msg.Body)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := af.states.For(clientID)
		tracker := state.transfersTracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			if err := af.processTransferBatch(input, state); err != nil {
				slog.Error("process batch failed", "err", err)
				nack()
				af.stopConsuming()
				return
			}
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[input.ClientID] = state

		if tracker.IsComplete(af.expectedTransfersEofs) {
			//TODO: Try finalize (verificando que el otro tb haya terminado)
			if state.avgsTracker.IsComplete(af.avgsExpectedEofs) {
				if err := af.finalize(state); err != nil {

				}
				completed[clientID] = struct{}{}
			}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			af.states.Delete(clientID)
			af.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := af.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			af.stopConsuming()
			return
		}
	}

	ack()
}

func (af *AverageFilter) processTransferBatch(input batch.Msg[transfer.TransferForQ3Filter], state *clientState) error {
	//TODO: Bajar a disco append olny
	//Para esto armo un array de pending x cliente y lo tengo en su status (pero sin persistirlo). El persist definirlo con lucho
	return nil
}

func (af *AverageFilter) handleAvgBatch(msg newmiddleware.Message, ack, nack func()) {
	af.lock.Lock()
	defer af.lock.Unlock()

	input, err := avgmethod.Read(msg.Body)
	if err != nil {
		slog.Error("decode failed", "err", err)
		ack()
		return
	}

	clientID := input.ClientID
	state := af.states.For(clientID)
	tracker := state.avgsTracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		tracker.RegisterBatch(int(input.SenderID))
		if err := af.processAvgBatch(input, state); err != nil {
			slog.Error("process batch failed", "err", err)
			nack()
			af.stopConsuming()
			return
		}
	}

	tracker.Claim(int(input.SenderID), input.Seq)

	if tracker.IsComplete(af.avgsExpectedEofs) {
		//TODO: Try finalize (verificando que el otro tb haya terminado)
		if state.transfersTracker.IsComplete(af.expectedTransfersEofs) {
			if err := af.finalize(state); err != nil {

			}
		}

	}

	// for clientID, state := range modified {
	// 	if _, done := completed[clientID]; done {
	// 		af.states.Delete(clientID)
	// 		af.checkpoint.DeleteClient(clientID)
	// 		continue
	// 	}
	// 	if err := af.checkpoint.SaveClient(clientID, state); err != nil {
	// 		slog.Error("persist failed, stopping", "err", err)
	// 		nack()
	// 		af.stopConsuming()
	// 		return
	// 	}
	// }

	ack()
}

func (af *AverageFilter) processAvgBatch(input batch.Msg[transfer.AvgByMethod], state *clientState) error {
	for _, record := range input.Records {
		state.avgs[record.Method] = record.Avg
	}
	return nil
}

func (af *AverageFilter) finalize(state *clientState) error {
	// ot := outputtracker.New()
	// builder := batch.NewBuilder(af.maxBatchSize, af.maxBatchBytes, records.Query3ResultCodec)

	// TODO: Abrir el archivo del cliente y empezar a iterar
	// Esto es el ejemplo del filteraccountseen
	// for acc := range state.seenAccounts {
	// 	result := queryresult.Query4Result{
	// 		BankId:        acc.BankID,
	// 		AccountNumber: acc.AccountNumber,
	// 	}
	// 	if !builder.TryAdd(&result) {
	// 		seq := ot.RegisterBatch("")
	// 		body := builder.Flush(clientID, uint8(af.queryID), uint8(af.id), seq)
	// 		if err := af.outputMiddleware.Send(newmiddleware.Message{Body: body}); err != nil {
	// 			return err
	// 		}
	// 		builder.TryAdd(&result)
	// 	}
	// }

	// if !builder.IsEmpty() {
	// 	seq := ot.RegisterBatch("")
	// 	body := builder.Flush(clientID, uint8(af.queryID), uint8(af.id), seq)
	// 	if err := af.outputMiddleware.Send(newmiddleware.Message{Body: body}); err != nil {
	// 		return err
	// 	}
	// }

	// total := ot.CountFor("")
	// eofBody := batch.WriteEOF(clientID, uint8(af.queryID), uint8(af.id), total+1, uint32(total))
	// return f.outputMiddleware.Send(newmiddleware.Message{Body: eofBody})
	return nil
}
