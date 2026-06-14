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
	"tp-grupal-distribuidos/internal/common/shard"
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
		if err == nil {
			return
		}
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
		clientsState:              map[int]*clientState{},
	}, nil
}

func (j *JoinAccounts) Run() {
	defer j.close()
	go j.consumeQualified()

	if err := j.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		j.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (j *JoinAccounts) consumeQualified() {
	if err := j.qualifiedInputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		j.handleQualifiedInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from qualified middleware", "err", err)
	}
}

func (j *JoinAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

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

func (j *JoinAccounts) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	input, err := splittransfer.Read(msg.Body)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if input.EOF {
		j.handleTransferEOF(input.ClientID, input.SenderID, input.Seq, input.Total)
		return
	}

	for i := range input.Records {
		j.accumulate(input.ClientID, input.Records[i])
	}
}

func (j *JoinAccounts) handleQualifiedInput(msg newmiddleware.Message, ack func()) {
	defer ack()
	input, err := qualifiedaccount.Read(msg.Body)
	if err != nil {
		slog.Error("While deserializing qualified accounts batch", "err", err)
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if input.EOF {
		if j.stateFor(input.ClientID).isDuplicateQualified(int(input.SenderID), input.Seq) {
			slog.Warn("Discarding duplicate qualified EOF", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
			return
		}
		j.handleQualifiedEOF(input.ClientID)
		return
	}

	state := j.stateFor(input.ClientID)
	for _, rec := range input.Records {
		if rec.IsLeft {
			state.qualifyingLeft[rec.Account] = struct{}{}
		} else {
			state.qualifyingRight[rec.Account] = struct{}{}
		}
	}
}

func (j *JoinAccounts) accumulate(clientID int, record transfer.SplittedTransfer) {
	if record.IsLeftPart {
		j.accumulateLeft(clientID, record)
	} else {
		j.accumulateRight(clientID, record)
	}
}

func (j *JoinAccounts) accumulateLeft(clientID int, record transfer.SplittedTransfer) {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	rightIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}

	state := j.stateFor(clientID)
	accMap, ok := state.left[identifier]
	if !ok {
		accMap = map[account.AccountIdentifier]struct{}{}
		state.left[identifier] = accMap
	}
	accMap[rightIdentifier] = struct{}{}

	if len(accMap) == j.threshold {
		j.broadcastQualified(clientID, identifier, true)
	}
}

func (j *JoinAccounts) accumulateRight(clientID int, record transfer.SplittedTransfer) {
	identifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	leftIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}

	state := j.stateFor(clientID)
	accMap, ok := state.right[identifier]
	if !ok {
		accMap = map[account.AccountIdentifier]struct{}{}
		state.right[identifier] = accMap
	}
	accMap[leftIdentifier] = struct{}{}

	if len(accMap) == j.threshold {
		j.broadcastQualified(clientID, identifier, false)
	}
}

func (j *JoinAccounts) broadcastQualified(clientID int, acc account.AccountIdentifier, isLeft bool) {
	state := j.stateFor(clientID)
	qa := qualifiedaccount.QualifiedAccount{Account: acc, IsLeft: isLeft}
	if !state.qualifiedBatch.TryAdd(&qa) {
		j.flushQualifiedBatch(clientID, state.qualifiedBatch)
		state.qualifiedBatch.TryAdd(&qa)
	}
}

func (j *JoinAccounts) flushQualifiedBatch(clientID int, b *batch.Builder[qualifiedaccount.QualifiedAccount]) {
	seq := j.stateFor(clientID).nextSeq()
	body := b.Flush(clientID, uint8(j.queryID), uint8(j.id), seq)
	if err := j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: body}); err != nil {
		slog.Error("While flushing qualified batch", "err", err)
	}
}

func (j *JoinAccounts) handleTransferEOF(clientID int, senderID uint8, seq uint64, total uint32) {
	if j.stateFor(clientID).isDuplicateTransfer(int(senderID), seq) {
		slog.Warn("Discarding duplicate EOF", "clientID", clientID, "senderID", senderID, "seq", seq)
		return
	}
	state := j.stateFor(clientID)
	state.transferEOFReceived = true
	state.transferEOFTotal = total

	if !state.qualifiedBatch.IsEmpty() {
		j.flushQualifiedBatch(clientID, state.qualifiedBatch)
	}

	eofBody := qualifiedaccount.WriteEOF(clientID, uint8(j.queryID), uint8(j.id), j.stateFor(clientID).nextSeq(), 0)
	if err := j.qualifiedOutputMiddleware.Send(newmiddleware.Message{Body: eofBody}); err != nil {
		slog.Error("While sending qualified EOF", "err", err)
	}

	j.tryFinalize(clientID)
}

func (j *JoinAccounts) handleQualifiedEOF(clientID int) {
	state := j.stateFor(clientID)
	state.qualifiedEOFCount++
	j.tryFinalize(clientID)
}

func (j *JoinAccounts) tryFinalize(clientID int) {
	state := j.stateFor(clientID)
	if !state.transferEOFReceived || state.qualifiedEOFCount < j.peerAmount {
		return
	}
	j.finalize(clientID, state)
}

func (j *JoinAccounts) finalize(clientID int, state *clientState) {
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
					j.flushChainBatch(clientID, rk, b)
					b.TryAdd(&chain)
				}
			}
		}
	}

	for rk, b := range batches {
		if !b.IsEmpty() {
			j.flushChainBatch(clientID, rk, b)
		}
	}

	eofBody := accountchain.WriteEOF(clientID, uint8(j.queryID), uint8(j.id), state.nextSeq(), state.transferEOFTotal)
	if err := j.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: newmiddleware.BroadcastRoutingKey}); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}

	for _, inner := range state.left {
		clear(inner)
	}
	for _, inner := range state.right {
		clear(inner)
	}
	clear(state.left)
	clear(state.right)
	clear(state.qualifyingLeft)
	clear(state.qualifyingRight)
	delete(j.clientsState, clientID)
}

func (j *JoinAccounts) builderFor(batches map[string]*batch.Builder[account.AccountChain], rk string) *batch.Builder[account.AccountChain] {
	b := batches[rk]
	if b == nil {
		b = accountchain.NewBatchBuilder(j.maxBatchSize, j.maxBatchBytes)
		batches[rk] = b
	}
	return b
}

func (j *JoinAccounts) flushChainBatch(clientID int, rk string, b *batch.Builder[account.AccountChain]) {
	seq := j.stateFor(clientID).nextSeq()
	body := b.Flush(clientID, uint8(j.queryID), uint8(j.id), seq)
	if err := j.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
		slog.Error("While sending chain batch", "err", err)
	}
}

func (j *JoinAccounts) stateFor(clientID int) *clientState {
	st, ok := j.clientsState[clientID]
	if !ok {
		st = &clientState{
			left:                 map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
			right:                map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}{},
			qualifyingLeft:       map[account.AccountIdentifier]struct{}{},
			qualifyingRight:      map[account.AccountIdentifier]struct{}{},
			qualifiedBatch:       qualifiedaccount.NewBatchBuilder(j.maxBatchSize, j.maxBatchBytes),
			transferSeqReceived:  map[int]uint64{},
			qualifiedSeqReceived: map[int]uint64{},
		}
		j.clientsState[clientID] = st
	}
	return st
}
