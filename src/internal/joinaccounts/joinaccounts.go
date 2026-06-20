package joinaccounts

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountchain"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/qualifiedaccount"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewJoinAccounts(config JoinAccountsConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware           newmiddleware.Middleware
		qualifiedInputMiddleware  newmiddleware.Middleware
		qualifiedOutputMiddleware newmiddleware.Middleware
		outputMiddleware          newmiddleware.Middleware
		err                       error
	)

	defer func() {
		if err != nil {
			if outputMiddleware != nil {
				if err := outputMiddleware.Close(); err != nil {
					slog.Error("While closing output middleware", "err", err)
				}
			}
			if qualifiedOutputMiddleware != nil {
				if err := qualifiedOutputMiddleware.Close(); err != nil {
					slog.Error("While closing qualified output middleware", "err", err)
				}
			}
			if qualifiedInputMiddleware != nil {
				if err := qualifiedInputMiddleware.Close(); err != nil {
					slog.Error("While closing qualified input middleware", "err", err)
				}
			}
			if inputMiddleware != nil {
				if err := inputMiddleware.Close(); err != nil {
					slog.Error("While closing input middleware", "err", err)
				}
			}
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)

	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	qualifiedQueue := fmt.Sprintf("%s_joinaccounts_%d", config.QualifiedExchange, config.Id)
	qualifiedInputMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.QualifiedExchange, qualifiedQueue)
	if err != nil {
		return nil, fmt.Errorf("creating qualified input middleware: %w", err)
	}

	qualifiedOutputMiddleware, err = newmiddleware.NewFanoutMiddleware(connSettings, config.QualifiedExchange, "")
	if err != nil {
		return nil, fmt.Errorf("creating qualified output middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	transferCkpt, err := checkpoint.New(config.PersistPath+"/transfer", marshalTransferState, unmarshalTransferState)
	if err != nil {
		return nil, fmt.Errorf("creating transfer checkpoint: %w", err)
	}

	qualifiedCkpt, err := checkpoint.New(config.PersistPath+"/qualified", marshalQualifiedState, unmarshalQualifiedState)
	if err != nil {
		return nil, fmt.Errorf("creating qualified checkpoint: %w", err)
	}

	transferRecovered, err := transferCkpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading transfer checkpoint: %w", err)
	}

	qualifiedRecovered, err := qualifiedCkpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading qualified checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			left:                   map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
			right:                  map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
			qualifyingLeft:         map[account.AccountIdentifier]struct{}{},
			qualifyingRight:        map[account.AccountIdentifier]struct{}{},
			transferTracker:        sendertracker.New(10_000_000),
			qualifiedTracker:       sendertracker.New(10_000_000),
			qualifiedOutputTracker: outputtracker.New(),
			chainOutputTracker:     outputtracker.New(),
		}
	})

	recoveredStates := mergeRecoveredStates(transferRecovered, qualifiedRecovered, config.Threshold)
	for clientID, cs := range recoveredStates {
		states.Set(clientID, cs)
		slog.Info("recovered client state", "clientID", clientID,
			"transferProcessed", cs.transferTracker.GetMsgCount(),
			"qualifiedProcessed", cs.qualifiedTracker.GetMsgCount(),
		)
	}

	return &JoinAccounts{
		id:                        config.Id,
		hasher:                    shard.New(config.OutputMiddlewareAmount),
		outputAmount:              config.OutputMiddlewareAmount,
		queryID:                   config.QueryID,
		inputMiddleware:           inputMiddleware,
		inputMiddlewareAmt:        config.InputMiddlewareAmt,
		qualifiedInputMiddleware:  qualifiedInputMiddleware,
		qualifiedOutputMiddleware: qualifiedOutputMiddleware,
		outputMiddleware:          outputMiddleware,
		peerAmount:                config.PeerAmount,
		threshold:                 config.Threshold,
		maxBatchSize:              config.MaxBatchSize,
		maxBatchBytes:             config.MaxBatchBytes,
		states:                    states,
		transferCheckpoint:        transferCkpt,
		qualifiedCheckpoint:       qualifiedCkpt,
		persistBatchSize:          config.PersistBatchSize,
		persistFlushInterval:      config.PersistFlushInterval,
	}, nil
}

func mergeRecoveredStates(
	transferRecovered map[int]*transferPartialState,
	qualifiedRecovered map[int]*qualifiedPartialState,
	threshold int,
) map[int]*clientState {
	merged := make(map[int]*clientState, len(transferRecovered))

	for clientID, ts := range transferRecovered {
		cs := &clientState{
			transferTracker:        ts.transferTracker,
			qualifiedOutputTracker: ts.qualifiedOutputTracker,
			left:                   ts.left,
			right:                  ts.right,
			qualifiedTracker:       sendertracker.New(10_000_000),
			chainOutputTracker:     outputtracker.New(),
			qualifyingLeft:         map[account.AccountIdentifier]struct{}{},
			qualifyingRight:        map[account.AccountIdentifier]struct{}{},
		}
		cs.pendingQualified = reconstructPendingQualified(ts.left, ts.right, threshold)
		merged[clientID] = cs
	}

	for clientID, qs := range qualifiedRecovered {
		cs, ok := merged[clientID]
		if !ok {
			cs = &clientState{
				transferTracker:        sendertracker.New(10_000_000),
				qualifiedOutputTracker: outputtracker.New(),
				left:                   map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
				right:                  map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
			}
			merged[clientID] = cs
		}
		cs.qualifiedTracker = qs.qualifiedTracker
		cs.chainOutputTracker = qs.chainOutputTracker
		cs.qualifyingLeft = qs.qualifyingLeft
		cs.qualifyingRight = qs.qualifyingRight
	}

	return merged
}

func reconstructPendingQualified(
	left map[account.AccountIdentifier]map[account.AccountIdentifier]struct{},
	right map[account.AccountIdentifier]map[account.AccountIdentifier]struct{},
	threshold int,
) []qualifiedaccount.QualifiedAccount {
	var pending []qualifiedaccount.QualifiedAccount
	for identifier, accMap := range left {
		if len(accMap) >= threshold {
			pending = append(pending, qualifiedaccount.QualifiedAccount{Account: identifier, IsLeft: true})
		}
	}
	for identifier, accMap := range right {
		if len(accMap) >= threshold {
			pending = append(pending, qualifiedaccount.QualifiedAccount{Account: identifier, IsLeft: false})
		}
	}
	return pending
}

func (j *JoinAccounts) Run() {
	defer j.close()
	go j.consumeQualified()

	if err := j.inputMiddleware.StartConsumingBatch(j.persistBatchSize, j.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		j.handleTransferBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (j *JoinAccounts) consumeQualified() {
	if err := j.qualifiedInputMiddleware.StartConsumingBatch(j.persistBatchSize, j.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		j.handleQualifiedBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from qualified middleware", "err", err)
	}
}

func (j *JoinAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	j.stopConsuming()
}

func (j *JoinAccounts) stopConsuming() {
	if err := j.qualifiedInputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping qualified input consumer", "join_id", j.id, "err", err)
	}
	if err := j.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "join_id", j.id, "err", err)
	}
}

func (j *JoinAccounts) close() {
	if err := j.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "join_id", j.id, "err", err)
	}
	if err := j.qualifiedInputMiddleware.Close(); err != nil {
		slog.Error("While closing qualified input middleware", "join_id", j.id, "err", err)
	}
	if err := j.qualifiedOutputMiddleware.Close(); err != nil {
		slog.Error("While closing qualified output middleware", "join_id", j.id, "err", err)
	}
	if err := j.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "join_id", j.id, "err", err)
	}
}

func (j *JoinAccounts) handleTransferBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	j.mu.Lock()
	defer j.mu.Unlock()

	modified := make(map[int]*clientState)

	for _, msg := range msgs {
		input, err := splittransfer.Read(msg.Body)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := j.states.For(clientID)
		tracker := state.transferTracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			if err := j.processTransferBatch(input, state); err != nil {
				slog.Error("process batch failed", "err", err)
				nack()
				j.stopConsuming()
				return
			}
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(j.inputMiddlewareAmt) {
			slog.Info("TRANSFERS COMPLETE")
			if err := j.finishTransfersStep(clientID, state); err != nil {
				slog.Error("finishing transfers step failed", "err", err)
				nack()
				j.stopConsuming()
				return
			}
		}
	}

	for clientID, state := range modified {
		ts := &transferPartialState{
			transferTracker:        state.transferTracker,
			qualifiedOutputTracker: state.qualifiedOutputTracker,
			left:                   state.left,
			right:                  state.right,
		}
		if err := j.transferCheckpoint.SaveClient(clientID, ts); err != nil {
			slog.Error("transfer persist failed, stopping", "err", err)
			nack()
			j.stopConsuming()
			return
		}
	}

	ack()
}

func (j *JoinAccounts) processTransferBatch(input splittransfer.Msg, state *clientState) error {
	for _, record := range input.Records {
		if record.IsLeftPart {
			j.accumulateLeft(record, state)
		} else {
			j.accumulateRight(record, state)
		}
	}
	return nil
}

func (j *JoinAccounts) accumulateLeft(record transfer.SplittedTransfer, state *clientState) {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	rightIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}

	accMap, ok := state.left[identifier]
	if !ok {
		accMap = map[account.AccountIdentifier]struct{}{}
		state.left[identifier] = accMap
	}
	accMap[rightIdentifier] = struct{}{}

	if len(accMap) == j.threshold {
		j.prepareQuallified(identifier, true, state)
	}
}

func (j *JoinAccounts) accumulateRight(record transfer.SplittedTransfer, state *clientState) {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	leftIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}

	accMap, ok := state.right[identifier]
	if !ok {
		accMap = map[account.AccountIdentifier]struct{}{}
		state.right[identifier] = accMap
	}
	accMap[leftIdentifier] = struct{}{}

	if len(accMap) == j.threshold {
		j.prepareQuallified(identifier, false, state)
	}
}

func (j *JoinAccounts) prepareQuallified(acc account.AccountIdentifier, isLeft bool, state *clientState) {
	state.pendingQualified = append(state.pendingQualified, qualifiedaccount.QualifiedAccount{Account: acc, IsLeft: isLeft})
}

func (j *JoinAccounts) finishTransfersStep(clientID int, state *clientState) error {
	b := qualifiedaccount.NewBatchBuilder(j.maxBatchSize, j.maxBatchBytes)
	for _, qualified := range state.pendingQualified {
		if !b.TryAdd(&qualified) {
			seq := state.qualifiedOutputTracker.RegisterBatch("")
			body := b.Flush(clientID, uint8(j.queryID), uint8(j.id), seq)
			if err := j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: body}); err != nil {
				slog.Error("While sending qualified batch", "err", err)
				return err
			}
			b.TryAdd(&qualified)
		}
	}
	if !b.IsEmpty() {
		seq := state.qualifiedOutputTracker.RegisterBatch("")
		body := b.Flush(clientID, uint8(j.queryID), uint8(j.id), seq)
		if err := j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: body}); err != nil {
			slog.Error("While sending qualified batch", "err", err)
			return err
		}
	}

	eofSeq := state.qualifiedOutputTracker.RegisterBatch("")
	eofBody := qualifiedaccount.WriteEOF(clientID, uint8(j.queryID), uint8(j.id), eofSeq, uint32(eofSeq-1))
	if err := j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: eofBody}); err != nil {
		slog.Error("While sending qualified EOF", "err", err)
		return err
	}
	return nil
}

func (j *JoinAccounts) handleQualifiedBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	j.mu.Lock()
	defer j.mu.Unlock()

	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := qualifiedaccount.Read(msg.Body)
		if err != nil {
			slog.Error("qualified decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := j.states.For(clientID)
		tracker := state.qualifiedTracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			for _, rec := range input.Records {
				if rec.IsLeft {
					state.qualifyingLeft[rec.Account] = struct{}{}
				} else {
					state.qualifyingRight[rec.Account] = struct{}{}
				}
			}
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(int(j.peerAmount)) {
			slog.Info("QUALIFIED COMPLETE")
			if err := j.finishQualifiedStep(clientID, state); err != nil {
				slog.Error("finishing qualified step failed", "err", err)
				nack()
				j.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			j.states.Delete(clientID)
			j.transferCheckpoint.DeleteClient(clientID)
			j.qualifiedCheckpoint.DeleteClient(clientID)
			continue
		}
		qs := &qualifiedPartialState{
			qualifiedTracker:   state.qualifiedTracker,
			chainOutputTracker: state.chainOutputTracker,
			qualifyingLeft:     state.qualifyingLeft,
			qualifyingRight:    state.qualifyingRight,
		}
		if err := j.qualifiedCheckpoint.SaveClient(clientID, qs); err != nil {
			slog.Error("qualified persist failed, stopping", "err", err)
			nack()
			j.stopConsuming()
			return
		}
	}

	ack()
}

func (j *JoinAccounts) finishQualifiedStep(clientID int, state *clientState) error {
	batches := make(map[string]*batch.Builder[account.AccountChain])

	for protagonistKey, rightMap := range state.right {
		leftMap, ok := state.left[protagonistKey]
		if !ok {
			continue
		}
		for r := range rightMap {
			if _, ok := state.qualifyingLeft[r]; !ok {
				continue
			}
			for l := range leftMap {
				if _, ok := state.qualifyingRight[l]; !ok {
					continue
				}
				//Me quedó re confuso. Comentario para yo del futuro:
				// El state.right desde la vista de la sharding key (B) tiene las transf de A->B dado que la shardingKey está a la derecha de la operación
				// El state.left desde la vista de la sharding key (B) tiene las transf de B->C dado que la shardingKey está a la izquierda de la operación
				// Acá parece que queda al reves, pero left es R=A, protagonist es B, L=C
				chain := account.AccountChain{
					Left:   r,
					Middle: protagonistKey,
					Right:  l,
				}
				rk := fmt.Sprintf("shard-%d", j.hasher.ShardFor(clientID, chain.Left.GetKey(), chain.Right.GetKey()))
				b := j.builderFor(batches, rk)
				if !b.TryAdd(&chain) {
					seq := state.chainOutputTracker.RegisterBatch(rk)
					if err := j.flushChainBatch(clientID, rk, seq, b); err != nil {
						return err
					}
					b.TryAdd(&chain)
				}
			}
		}
	}

	for rk, b := range batches {
		if !b.IsEmpty() {
			seq := state.chainOutputTracker.RegisterBatch(rk)

			if err := j.flushChainBatch(clientID, rk, seq, b); err != nil {
				return err
			}
		}
	}

	var sendErr error
	for i := range j.outputAmount {
		if sendErr != nil {
			break
		}
		rk := fmt.Sprintf("shard-%d", i)
		total := state.chainOutputTracker.CountFor(rk)
		seq := state.chainOutputTracker.RegisterBatch(rk)
		eofBody := accountchain.WriteEOF(clientID, uint8(j.queryID), uint8(j.id), seq, uint32(total))
		if err := j.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			slog.Error("While sending EOF message", "routingKey", rk, "err", err)
			sendErr = err
		}
	}
	return sendErr
}

func (j *JoinAccounts) builderFor(batches map[string]*batch.Builder[account.AccountChain], rk string) *batch.Builder[account.AccountChain] {
	b := batches[rk]
	if b == nil {
		b = accountchain.NewBatchBuilder(j.maxBatchSize, j.maxBatchBytes)
		batches[rk] = b
	}
	return b
}

func (j *JoinAccounts) flushChainBatch(clientID int, rk string, seqNumber uint64, b *batch.Builder[account.AccountChain]) error {
	body := b.Flush(clientID, uint8(j.queryID), uint8(j.id), seqNumber)
	return j.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk})
}
