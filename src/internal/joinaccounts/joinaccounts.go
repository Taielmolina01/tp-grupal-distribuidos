package joinaccounts

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
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

	return &JoinAccounts{
		id:                        config.Id,
		hasher:                    shard.New(config.OutputMiddlewareAmount),
		queryID:                   config.QueryID,
		inputMiddleware:           inputMiddleware,
		qualifiedInputMiddleware:  qualifiedInputMiddleware,
		qualifiedOutputMiddleware: qualifiedOutputMiddleware,
		outputMiddleware:          outputMiddleware,
		peerAmount:                config.PeerAmount,
		threshold:                 config.Threshold,
		maxBatchSize:              config.MaxBatchSize,
		maxBatchBytes:             config.MaxBatchBytes,

		states: statemap.New(func() *clientState {
			return &clientState{
				left:                   map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
				right:                  map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
				qualifyingLeft:         map[account.AccountIdentifier]struct{}{},
				qualifyingRight:        map[account.AccountIdentifier]struct{}{},
				qualifiedBatch:         qualifiedaccount.NewBatchBuilder(config.MaxBatchSize, config.MaxBatchBytes),
				transferTracker:        sendertracker.New(10_000_000),
				qualifiedTracker:       sendertracker.New(10_000_000),
				qualifiedOutputTracker: outputtracker.New(),
				chainOutputTracker:     outputtracker.New(),
			}
		}),
	}, nil
}

func (j *JoinAccounts) Run() {
	defer j.close()
	go j.consumeQualified()

	if err := j.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		j.handleTransferInput(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (j *JoinAccounts) consumeQualified() {
	if err := j.qualifiedInputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		j.handleQualifiedInput(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from qualified middleware", "err", err)
	}
}

func (j *JoinAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	j.StopConsuming()
}

func (j *JoinAccounts) StopConsuming() {
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

func (j *JoinAccounts) handleTransferInput(msg newmiddleware.Message, ack func(), nack func()) {
	//Podría usar el ProjectForQ4 creo
	input, err := splittransfer.Read(msg.Body)
	if err != nil {
		slog.Error("decode failed", "err", err)
		ack()
		return
	}

	clientID := input.ClientID

	j.mu.Lock()
	defer j.mu.Unlock()

	state := j.states.For(clientID)
	tracker := state.transferTracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		// SI O SI TENGO QUE VERIF DUPLICADOS, SINO ACÁ SUMO CUALQUIER COSA
		// Por ende no puedo mandar los outputs hasta terminar
		// O puedo mandar el output con el mismo seqNumber que recibí, pero tengo q garantizar que el output de todo input entra en un solo batch
		// Las quallifiedaccounts que mando al siguiente step son más chicas que el input recibido ais que puedo
		tracker.RegisterBatch(int(input.SenderID), uint64(len(input.Records)))
		if err := j.processTransferBatch(input, state); err != nil {
			slog.Error("process batch failed", "err", err)
			nack()
			j.StopConsuming()
			return
		}
	}

	tracker.Claim(int(input.SenderID), input.Seq)

	if tracker.IsComplete(int(j.inputMiddlewareAmt)) {
		if err := j.finishTransfersStep(clientID, state); err != nil {
			slog.Error("finishing transfers step failed", "err", err)
			nack()
			j.StopConsuming()
			return
		}
	}

	// if err := h.persist(); err != nil {
	// 	slog.Error("persist failed, stopping", "err", err)
	// 	nack()
	//	j.StopConsuming()
	// 	return
	// }

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

	if state.qualifiedBatch.IsEmpty() {
		return nil
	}
	if err := j.flushQualifiedBatch(input.ClientID, input.Seq, state.qualifiedBatch); err != nil {
		return err
	}
	state.qualifiedOutputTracker.RegisterBatch("")
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
	qa := qualifiedaccount.QualifiedAccount{Account: acc, IsLeft: isLeft}
	state.qualifiedBatch.Add(&qa)
}

func (j *JoinAccounts) flushQualifiedBatch(clientID int, seqNumber uint64, b *batch.Builder[qualifiedaccount.QualifiedAccount]) error {
	// Acá funciona medio de suerte
	// Si tengo 3 origenes con secuencias independientes no puedo garantizar que usandoel seqNumber todas sean diferentes.
	// Si llega origen 1 seq 1, y luego origen 2 seq 1 me arruina
	body := b.Flush(clientID, uint8(j.queryID), uint8(j.id), seqNumber)
	return j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: body})
}

func (j *JoinAccounts) finishTransfersStep(clientID int, state *clientState) error {
	eofBody := qualifiedaccount.WriteEOF(clientID, uint8(j.queryID), uint8(j.id), state.transferTracker.GetEOFSeq(), uint32(state.qualifiedOutputTracker.Total()))
	if err := j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: eofBody}); err != nil {
		slog.Error("While sending qualified EOF", "err", err)
		return err
	}

	return nil
}

func (j *JoinAccounts) handleQualifiedInput(msg newmiddleware.Message, ack func(), nack func()) {
	input, err := qualifiedaccount.Read(msg.Body)
	if err != nil {
		slog.Error("qualified decode failed", "err", err)
		ack()
		return
	}

	clientID := input.ClientID

	j.mu.Lock()
	defer j.mu.Unlock()

	state := j.states.For(clientID)
	tracker := state.qualifiedTracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		// slog.Warn("quailified duplicate", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq, "EOF", input.EOF)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		tracker.RegisterBatch(int(input.SenderID), uint64(len(input.Records)))
		for _, rec := range input.Records {
			if rec.IsLeft {
				state.qualifyingLeft[rec.Account] = struct{}{}
			} else {
				state.qualifyingRight[rec.Account] = struct{}{}
			}
		}
	}

	tracker.Claim(int(input.SenderID), input.Seq)

	if tracker.IsComplete(int(j.peerAmount)) {
		if err := j.finishQualifiedStep(clientID, state); err != nil {
			slog.Error("finishing transfers step failed", "err", err)
			nack()
			j.StopConsuming()
			return
		}
	}

	// // if err := h.persist(); err != nil {
	// // 	slog.Error("persist failed, stopping", "err", err)
	// // 	nack()
	// //	j.StopConsuming()
	// // 	return
	// // }

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
	state.chainOutputTracker.ForEach(func(rk string, total uint64) {
		if sendErr != nil {
			return
		}
		seq := state.chainOutputTracker.RegisterBatch(rk)
		eofBody := accountchain.WriteEOF(clientID, uint8(j.queryID), uint8(j.id), seq, uint32(total))
		if err := j.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			slog.Error("While sending EOF message", "routingKey", rk, "err", err)
			sendErr = err
		}
	})
	j.states.Delete(clientID)
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
