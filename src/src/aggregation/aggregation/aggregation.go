package aggregation

import (
	"fmt"
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

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	id                    int
	outputQueue           middleware.Middleware
	inputExchange         middleware.Middleware
	fruitItemMapPerClient map[int]map[string]fruititem.FruitItem
	eofCountPerClient     map[int]int
	topSize               int
	sumAmount             int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		id:                    config.Id,
		outputQueue:           outputQueue,
		inputExchange:         inputExchange,
		fruitItemMapPerClient: map[int]map[string]fruititem.FruitItem{},
		topSize:               config.TopSize,
		sumAmount:             config.SumAmount,
		eofCountPerClient:     map[int]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, _, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(*fruitRecords); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	}

	aggregation.handleDataMessage(*fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(fruitItemsFromClient fruititem.FruitItemFromClient) error {
	aggregation.eofCountPerClient[fruitItemsFromClient.ClientId]++

	if aggregation.eofCountPerClient[fruitItemsFromClient.ClientId] != aggregation.sumAmount {
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(fruitItemsFromClient.ClientId)

	message, err := inner.SerializeMessage(fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Error("While sending top message", "err", err)
		return err
	}

	eofMessage, err := inner.SerializeAggregationEofMessage(eofmessage.AggregationEofMessage{
		ClientID:      fruitItemsFromClient.ClientId,
		AggregationID: aggregation.id,
	})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*eofMessage); err != nil {
		slog.Error("While sending EOF message", "err", err)
		return err
	}

	delete(aggregation.eofCountPerClient, fruitItemsFromClient.ClientId)
	delete(aggregation.fruitItemMapPerClient, fruitItemsFromClient.ClientId)

	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitItemsForClient fruititem.FruitItemFromClient) {
	if _, ok := aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId]; !ok {
		aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitItemsForClient.FruitItems {
		if _, ok := aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit] = aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId int) fruititem.FruitItemFromClient {
	fruitItemsFromClient := fruititem.FruitItemFromClient{
		ClientId:   clientId,
		FruitItems: make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMapPerClient[clientId])),
	}
	for _, item := range aggregation.fruitItemMapPerClient[clientId] {
		fruitItemsFromClient.FruitItems = append(fruitItemsFromClient.FruitItems, item)
	}
	sort.SliceStable(fruitItemsFromClient.FruitItems, func(i, j int) bool {
		return fruitItemsFromClient.FruitItems[j].Less(fruitItemsFromClient.FruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItemsFromClient.FruitItems))
	fruitItemsFromClient.FruitItems = fruitItemsFromClient.FruitItems[:finalTopSize]
	return fruitItemsFromClient
}

func (aggregation *Aggregation) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.Close()
}

func (aggregation *Aggregation) Close() error {
	if err := aggregation.inputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := aggregation.inputExchange.Close(); err != nil {
		return err
	}
	if err := aggregation.outputQueue.StopConsuming(); err != nil {
		return err
	}
	if err := aggregation.outputQueue.Close(); err != nil {
		return err
	}
	clear(aggregation.eofCountPerClient)
	clear(aggregation.fruitItemMapPerClient)
	return nil
}
