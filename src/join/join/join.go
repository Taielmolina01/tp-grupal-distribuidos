package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	topByClients      map[int][]fruititem.FruitItemFromClient
	eofByClient       map[int]map[int]bool
	completedClients  map[int]bool
	topSize           int
	aggregationAmount int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
			inputQueue:        inputQueue,
			outputQueue:       outputQueue,
			topByClients:      map[int][]fruititem.FruitItemFromClient{},
			eofByClient:       map[int]map[int]bool{},
			completedClients:  map[int]bool{},
			topSize:           config.TopSize,
			aggregationAmount: config.AggregationAmount,
		},
		nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	if eofMsg, isAggregationEof, err := inner.DeserializeAggregationEofMessage(&msg); err != nil {
		slog.Error("While deserializing aggregation EOF", "err", err)
		return
	} else if isAggregationEof {
		join.handleAggregationEof(*eofMsg)
		return
	}

	fruitTop, _, isEof, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing msg", "err", err)
		return
	}

	if isEof {
		slog.Info("Ignoring legacy EOF without aggregation ID", "client_id", fruitTop.ClientId)
		return
	}

	if _, done := join.completedClients[fruitTop.ClientId]; done {
		// Si el cliente ya había terminado de ser procesado esto es un duplicado simplemente asique lo ignoro.
		return
	}

	if _, ok := join.topByClients[fruitTop.ClientId]; !ok {
		join.topByClients[fruitTop.ClientId] = []fruititem.FruitItemFromClient{*fruitTop}
		return
	}

	join.topByClients[fruitTop.ClientId] = append(join.topByClients[fruitTop.ClientId], *fruitTop)
}

func (join *Join) handleAggregationEof(eofMsg eofmessage.AggregationEofMessage) {
	if _, done := join.completedClients[eofMsg.ClientID]; done {
		// Si el cliente ya había terminado de procesar sus EOF esto es un duplicado simplemente asique lo ignoro.
		return
	}

	if _, ok := join.eofByClient[eofMsg.ClientID]; !ok {
		join.eofByClient[eofMsg.ClientID] = map[int]bool{}
	}

	if _, exists := join.eofByClient[eofMsg.ClientID][eofMsg.AggregationID]; exists {
		// Si el cliente ya había enviado el EOF para este aggregation, esto es un duplicado simplemente asique lo ignoro.
		return
	}

	join.eofByClient[eofMsg.ClientID][eofMsg.AggregationID] = true
	currentCount := len(join.eofByClient[eofMsg.ClientID])

	if currentCount < join.aggregationAmount {
		// Si no llegaron los EOF de todos los aggregations, sigo para no calcular el top.
		return
	}

	top := join.CalculateTop(eofMsg.ClientID)
	message, err := inner.SerializeMessage(top)
	if err != nil {
		slog.Error("While serializing top", "client_id", eofMsg.ClientID, "err", err)
		return
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Error("While sending top", "client_id", eofMsg.ClientID, "err", err)
		return
	}

	join.completedClients[eofMsg.ClientID] = true
	delete(join.eofByClient, eofMsg.ClientID)
	delete(join.topByClients, eofMsg.ClientID)
}

func (join *Join) CalculateTop(clientID int) fruititem.FruitItemFromClient {
	amountByFruit := map[string]fruititem.FruitItem{}
	for i := range join.topByClients[clientID] {
		for _, fruitRecord := range join.topByClients[clientID][i].FruitItems {
			if current, ok := amountByFruit[fruitRecord.Fruit]; ok {
				amountByFruit[fruitRecord.Fruit] = current.Sum(fruitRecord)
			} else {
				amountByFruit[fruitRecord.Fruit] = fruitRecord
			}
		}
	}
	fruitItemsFromClient := fruititem.FruitItemFromClient{
		ClientId:   clientID,
		FruitItems: make([]fruititem.FruitItem, 0, len(amountByFruit)),
	}

	for _, item := range amountByFruit {
		fruitItemsFromClient.FruitItems = append(fruitItemsFromClient.FruitItems, item)
	}

	sort.SliceStable(fruitItemsFromClient.FruitItems, func(i, j int) bool {
		return fruitItemsFromClient.FruitItems[j].Less(fruitItemsFromClient.FruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItemsFromClient.FruitItems))
	fruitItemsFromClient.FruitItems = fruitItemsFromClient.FruitItems[:finalTopSize]
	return fruitItemsFromClient
}

func (join *Join) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.Close()
}

func (join *Join) Close() error {
	if err := join.inputQueue.StopConsuming(); err != nil {
		return err
	}
	if err := join.inputQueue.Close(); err != nil {
		return err
	}
	if err := join.outputQueue.StopConsuming(); err != nil {
		return err
	}
	if err := join.outputQueue.Close(); err != nil {
		return err
	}
	clear(join.completedClients)
	clear(join.eofByClient)
	clear(join.topByClients)
	return nil
}
