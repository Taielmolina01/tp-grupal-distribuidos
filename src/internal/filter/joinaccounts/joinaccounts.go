package joinaccounts

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type JoinAccountsConfig struct {
	Id int

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	MomHost string
	MomPort int

	ExpectedEOFs          int    //Cantidad de nodos del grupo anterior
	InputMiddlewarePrefix string //Es el output prefix del nodo anterior

	QueryID int
}

type clientState struct {
	eofAmt int

	left  map[string]map[string]account.AccountPair
	right map[string]map[string]account.AccountPair
}

type JoinAccounts struct {
	id int

	outputMiddlewareAmount int

	expectedEOFs int
	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware

	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}

func declareOutputQueues(config JoinAccountsConfig, connSettings middleware.ConnSettings) ([]middleware.Middleware, error) {
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

func NewJoinAccounts(config JoinAccountsConfig) (_ *JoinAccounts, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputQueue   middleware.Middleware
		outputQueues []middleware.Middleware
	)

	defer func() {
		if err != nil {
			for _, q := range outputQueues {
				q.Close()
			}
			if inputQueue != nil {
				inputQueue.Close()
			}
		}
	}()

	inputQueue, err = middleware.CreateQueueMiddleware(
		config.InputMiddlewarePrefix+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	outputQueues, err = declareOutputQueues(config, connSettings)
	if err != nil {
		return nil, fmt.Errorf("declaring output queues: %w", err)
	}

	return &JoinAccounts{
		id:                     config.Id,
		outputMiddlewareAmount: config.OutputMiddlewareAmount,
		queryID:                config.QueryID,
		inputQueue:             inputQueue,
		outputQueues:           outputQueues,
		expectedEOFs:           config.ExpectedEOFs,
		clientsState:           map[int]*clientState{},
	}, nil
}

func (j *JoinAccounts) Run() {
	defer j.close()

	if err := j.inputQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		j.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (j *JoinAccounts) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	j.inputQueue.StopConsuming()
}

func (j *JoinAccounts) close() {
	j.inputQueue.Close()

	for _, queue := range j.outputQueues {
		queue.Close()
	}
}

func (j *JoinAccounts) handleInput(msg middleware.Message, ack func()) {
	defer ack()
	m, err := inner.DeserializeData[transfer.SplittedTransfer](&msg)

	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		j.handleEOF(*m)
		return
	}

	j.handleRecord(m.ClientID, m.Payload)
}

func (j *JoinAccounts) handleRecord(clientID int, record transfer.SplittedTransfer) {
	if record.IsLeftPart {
		j.handleLeftPartRecord(clientID, record)
		return
	}
	j.handleRightPartRecord(clientID, record)
	// j.handlerMessages.AddProcessedMessagesAmountByClientId(clientID, 1)

	// if record.Timestamp.Before(j.startDate) || record.Timestamp.After(j.endDate) {
	// 	return
	// }

	// output := []transfer.SplittedTransfer{
	// 	{Transfer: record, IsLeftPart: true},
	// 	{Transfer: record, IsLeftPart: false},
	// }

	// for _, o := range output {
	// 	var bank, acc string
	// 	if o.IsLeftPart {
	// 		bank = o.Transfer.FromBank
	// 		acc = o.Transfer.FromBankAccount
	// 	} else {
	// 		bank = o.Transfer.ToBank
	// 		acc = o.Transfer.ToBankAccount
	// 	}

	// 	output_index := j.shardFor(clientID, bank, acc)

	// 	msg, err := inner.SerializeData(inner.DataMsg[transfer.SplittedTransfer]{
	// 		ClientID: clientID,
	// 		QueryID:  uint8(j.queryID),
	// 		Payload:  o,
	// 	})

	// 	if err != nil {
	// 		slog.Error("While serializing output message", "err", err)
	// 	}

	// 	j.handlerMessages.AddFilteredMessagesAmountByClientId(clientID, 1)
	// 	if err := j.outputQueues[output_index].Send(*msg); err != nil {
	// 		slog.Error("While sending output message", "err", err)
	// 	}
	// }
}

func (j *JoinAccounts) handleLeftPartRecord(clientID int, record transfer.SplittedTransfer) {
	j.mu.Lock()
	defer j.mu.Unlock()

	idenetifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	identifierKey := idenetifier.GetKey()

	rightIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	rightIdentifierKey := rightIdentifier.GetKey()

	state := j.stateFor(clientID)

	//Veo si existe el map para la acc que va a ser el medio (la shard key)
	accMap, ok := state.left[identifierKey]
	if !ok {
		accMap = map[string]account.AccountPair{}
		state.left[identifierKey] = accMap
	}

	//Veo si ya existe el par shardkey + esta key de la derecha
	_, ok = accMap[rightIdentifierKey]
	if ok {
		return
	}
	accMap[rightIdentifierKey] = account.AccountPair{
		Left:  idenetifier,
		Right: rightIdentifier,
	}

	//Ahora que existe, busco matches en el otro grupo
	accMapRight, ok := state.right[identifierKey]
	if !ok {
		return
	}

	for _, v := range accMapRight {

		msg, err := inner.SerializeData(inner.DataMsg[account.AccountChain]{
			ClientID: clientID,
			QueryID:  uint8(j.queryID),
			Payload: account.AccountChain{
				Left:   v.Left,
				Middle: idenetifier,
				Right:  rightIdentifier,
			},
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		output_index := j.shardFor(clientID, identifierKey, rightIdentifierKey)
		slog.Info("LEFT A B C", "shard", output_index, "A", v.Left, "B", idenetifier, "C", rightIdentifier)
		if err := j.outputQueues[output_index].Send(*msg); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}
func (j *JoinAccounts) handleRightPartRecord(clientID int, record transfer.SplittedTransfer) {
	j.mu.Lock()
	defer j.mu.Unlock()

	idenetifier := account.AccountIdentifier{
		BankID:        record.Transfer.ToBank,
		AccountNumber: record.Transfer.ToBankAccount,
	}
	identifierKey := idenetifier.GetKey()

	leftIdentifier := account.AccountIdentifier{
		BankID:        record.Transfer.FromBank,
		AccountNumber: record.Transfer.FromBankAccount,
	}
	leftIdentifierKey := leftIdentifier.GetKey()

	state := j.stateFor(clientID)

	//Veo si existe el map para la acc que va a ser el medio (la shard key)
	accMap, ok := state.right[identifierKey]
	if !ok {
		accMap = map[string]account.AccountPair{}
		state.right[identifierKey] = accMap
	}

	//Veo si ya existe el par shardkey + esta key de la izquierda
	_, ok = accMap[leftIdentifierKey]
	if ok {
		return
	}
	accMap[leftIdentifierKey] = account.AccountPair{
		Left:  leftIdentifier,
		Right: idenetifier,
	}

	//Ahora que existe, busco matches en el otro grupo
	accMapRight, ok := state.left[identifierKey]
	if !ok {
		return
	}

	for _, v := range accMapRight {

		msg, err := inner.SerializeData(inner.DataMsg[account.AccountChain]{
			ClientID: clientID,
			QueryID:  uint8(j.queryID),
			Payload: account.AccountChain{
				Left:   leftIdentifier,
				Middle: idenetifier,
				Right:  v.Right,
			},
		})

		if err != nil {
			slog.Error("While serializing output message", "err", err)
		}

		output_index := j.shardFor(clientID, identifierKey, leftIdentifierKey)
		slog.Info("RIGHT A B C", "shard", output_index, "A", leftIdentifier, "B", idenetifier, "C", v.Right)
		if err := j.outputQueues[output_index].Send(*msg); err != nil {
			slog.Error("While sending output message", "err", err)
		}
	}
}

func (j *JoinAccounts) shardFor(clientID int, key1, key2 string) int {
	h := fnv.New32a()
	fmt.Fprintf(h, "%d\x00%s\x00%s", clientID, key1, key2)
	return int(h.Sum32() % uint32(j.outputMiddlewareAmount))
}

func (j *JoinAccounts) handleEOF(data inner.DataMsg[transfer.SplittedTransfer]) {
	j.mu.Lock()
	defer j.mu.Unlock()

	state := j.stateFor(data.ClientID)

	state.eofAmt++

	if state.eofAmt < j.expectedEOFs {
		return
	}

	msg, err := inner.SerializeEofMessage(eofmessage.EofMessage{ClientID: data.ClientID})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
	}

	for _, q := range j.outputQueues {
		if err := q.Send(*msg); err != nil {
			slog.Error("While sending EOF message", "err", err)
		}
	}
}

func (j *JoinAccounts) stateFor(clientID int) *clientState {
	st, ok := j.clientsState[clientID]
	if !ok {
		st = &clientState{
			left:  map[string]map[string]account.AccountPair{},
			right: map[string]map[string]account.AccountPair{},
		}
		j.clientsState[clientID] = st
	}
	return st
}
