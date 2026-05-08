package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofringmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                 uint32
	inputQueue         middleware.Middleware
	outputExchanges    []middleware.Middleware
	eofOutput          middleware.Middleware
	eofInput           middleware.Middleware
	sumMonitor         MonitorSum
	shutdown           chan struct{}
	shutdownOnce       sync.Once
	sumAmount          int
	aggregationsAmount int
}

// Inicializadores

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchanges := make([]middleware.Middleware, 0, config.AggregationAmount)
	for _, routeKey := range outputExchangeRouteKeys {
		outputExchange, exchangeErr := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{routeKey}, connSettings)
		if exchangeErr != nil {
			inputQueue.Close()
			for _, exchange := range outputExchanges {
				exchange.Close()
			}
			return nil, exchangeErr
		}

		outputExchanges = append(outputExchanges, outputExchange)
	}

	next := config.Id + 1

	if config.Id == config.SumAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		strconv.Itoa(config.Id),
		connSettings,
	)

	eofOutput, err := middleware.CreateQueueMiddleware(
		strconv.Itoa(next),
		connSettings,
	)

	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		id:                 uint32(config.Id),
		inputQueue:         inputQueue,
		outputExchanges:    outputExchanges,
		sumMonitor:         NewSumMonitor(),
		eofInput:           eofInput,
		eofOutput:          eofOutput,
		shutdown:           make(chan struct{}),
		sumAmount:          config.SumAmount,
		aggregationsAmount: config.AggregationAmount,
	}, nil
}

func (sum *Sum) Run() {
	slog.Info("Starting sum consumers", "sum_id", sum.id)
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
	go sum.eofInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleEofMessageFromQueue(msg, ack, nack)
	})

	<-sum.shutdown
}

// Handler para la working queue que comparten las distintas intancias de sum.

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitsFromClient, eofMessage, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing input message", "sum_id", sum.id, "err", err)
		return
	}

	if isEof {
		if err := sum.handleEOFMessage(*eofMessage); err != nil {
			slog.Error("While handling end of record message", "sum_id", sum.id, "client_id", eofMessage.ClientID, "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(*fruitsFromClient); err != nil {
		slog.Error("While handling data message", "sum_id", sum.id, "client_id", fruitsFromClient.ClientId, "err", err)
	}
}

func (sum *Sum) handleEOFMessage(eofMessage eofmessage.EofMessage) error {

	sum.sendFinalMessagesToAggregation(eofMessage.ClientID)

	amount := sum.sumMonitor.GetProccessedMessagesAmountByClientId(eofMessage.ClientID)

	eofMessageRequest, err := inner.SerializeEofFromQueueMsg(
		eofringmessage.EofRingMessage{
			ActualAmount: amount,
			RealAmount:   eofMessage.TotalMessages,
			ClientId:     eofMessage.ClientID,
			Leader:       uint32(sum.id),
		},
	)
	if err != nil {
		slog.Error("Error serializing EOF from queue", "sum_id", sum.id, "client_id", eofMessage.ClientID, "err", err)
		return err
	}
	if err := sum.eofOutput.Send(*eofMessageRequest); err != nil {
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitsFromClient fruititem.FruitItemFromClient) error {
	sum.sumMonitor.CountNewFruitsFromClient(fruitsFromClient)
	return nil
}

// Handlers para la queue de productor consumidor del ring.

func (sum *Sum) handleEofMessageFromQueue(msg middleware.Message, ack, nack func()) {
	eofRingMessage, eofRingCommitMessage, err := inner.DeserializeRingMessage(&msg)
	if err != nil {
		slog.Error("Error deserializing EOF ring message", "sum_id", sum.id, "err", err)
		return
	}

	if eofRingCommitMessage != nil {
		if err := sum.handleEOFCommitMessage(eofRingCommitMessage); err != nil {
			slog.Error("Error handling EOF commit", "sum_id", sum.id, "client_id", eofRingCommitMessage.ClientID, "err", err)
		} else {
			slog.Info("EOF commit sent to aggregation", "sum_id", sum.id, "client_id", eofRingCommitMessage.ClientID)
			ack()
		}
		return
	}

	if eofRingMessage == nil {
		slog.Error("EOF ring message without processed payload", "sum_id", sum.id)
		return
	}

	if eofRingMessage.Leader == sum.id && eofRingMessage.ActualAmount == eofRingMessage.RealAmount {
		// Si soy el líder y la cantidad de todos los mensajes enviados por el cliente (contados por el gateway) y la suma de lo que cada uno
		// de los sums me dice que proceso, entonces envio el commit otra vez en forma de anillo para que cada uno le pase al exchange de los
		// aggregations sus mensajes.
		sum.sendEofCommitToReplicas(eofRingMessage, ack)
	} else {
		if eofRingMessage.Leader == sum.id {
			// Si soy el líder y la cantidad de todos los mensajes enviados por el cliente (contados por el gateway) y la suma de lo que cada uno
			// de los sums me dice que proceso no coinciden, simplemente inicio el anillo de nuevo. Esto porque estoy asumiendo que lo único que paso
			// es que un sum no había terminado de procesar los mensajes de un cliente en particular. Como asumimos que no hay caida, eventualmente va a converger
			// al primer caso.
			value := sum.sumMonitor.GetProccessedMessagesAmountByClientId(eofRingMessage.ClientId)
			eofRingMessage.ActualAmount = value
		} else if eofRingMessage.Leader != sum.id {
			// Si no soy el líder simplemente sumo los mensajes que yo leí del cliente X y lo sumo al mensaje del ring y lo forwardeo.

			value := sum.sumMonitor.GetProccessedMessagesAmountByClientId(eofRingMessage.ClientId)
			eofRingMessage.ActualAmount += value
		}

		sum.sendEofMessageToQueue(eofRingMessage, ack)
	}
}

func (sum *Sum) sendEofCommitToReplicas(eofRingMessage *eofringmessage.EofRingMessage, ack func()) {
	defer ack()
	msg, err := inner.SerializeEofMessageCommit(eofringmessage.EofMessageCommit{ClientID: eofRingMessage.ClientId, Hops: 0})
	if err != nil {
		slog.Error("Error serializing EOF commit", "sum_id", sum.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
	if err = sum.eofOutput.Send(*msg); err != nil {
		slog.Error("Error sending EOF commit to ring", "sum_id", sum.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
	slog.Info("EOF commit sent to ring", "sum_id", sum.id, "client_id", eofRingMessage.ClientId)
}

func (sum *Sum) sendEofMessageToQueue(eofRingMessage *eofringmessage.EofRingMessage, ack func()) {
	defer ack()
	serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(*eofRingMessage)
	if err != nil {
		slog.Error("Error serializing forwarded EOF ring message", "sum_id", sum.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
	if err := sum.eofOutput.Send(*serializedEofRingMessage); err != nil {
		slog.Error("Error forwarding EOF ring message", "sum_id", sum.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
}

// Handlers para cuando se recibe un EOF commit del ring.

func (sum *Sum) convertToBytes(fruitName string, clientID int) []byte {
	return []byte(fmt.Sprintf("%v%d", fruitName, clientID))
}

func (sum *Sum) calculateIndexForShard(fruitItem fruititem.FruitItem, clientID int) int {
	bytes := sum.convertToBytes(fruitItem.Fruit, clientID)
	hash := fnv.New64a()
	hash.Write(bytes)
	return int(hash.Sum64() % uint64(sum.aggregationsAmount))
}

func (sum *Sum) sendMessageToAggregation(fruitItemFromClient *fruititem.FruitItemFromClient) error {
	message, err := inner.SerializeMessage(*fruitItemFromClient)
	if err != nil {
		slog.Debug("While serializing message", "err", err)
		return err
	}
	for _, fruitItem := range fruitItemFromClient.FruitItems {
		index := sum.calculateIndexForShard(fruitItem, fruitItemFromClient.ClientId)
		if err := sum.outputExchanges[index].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) broadcastEofMessageToAggregation(clientID int) error {
	eofMessage := fruititem.FruitItemFromClient{
		ClientId:   clientID,
		FruitItems: []fruititem.FruitItem{},
	}
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing message", "err", err)
		return err
	}
	for i := range sum.outputExchanges {
		if err := sum.outputExchanges[i].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) sendFinalMessagesToAggregation(clientID int) {
	for _, value := range sum.sumMonitor.GetFruitsByClientID(clientID) {
		sum.sendMessageToAggregation(&fruititem.FruitItemFromClient{
			ClientId:   clientID,
			FruitItems: []fruititem.FruitItem{value},
		})
	}

	sum.broadcastEofMessageToAggregation(clientID)
}

func (sum *Sum) handleEOFCommitMessage(msg *eofringmessage.EofMessageCommit) error {

	sum.sendFinalMessagesToAggregation(msg.ClientID)

	msg.Hops++

	if msg.Hops == sum.sumAmount-1 {
		// Si soy el último nodo antes del lider simplemente no le forwardeo el mensaje de commit porque el líder antes de iniciar
		// el anillo envia sus datos al aggregation.
		return nil
	}

	toSend, err := inner.SerializeEofMessageCommit(*msg)

	if err != nil {
		slog.Debug("While sending EOF commit", "err", err)
		return err
	}

	if err = sum.eofOutput.Send(*toSend); err != nil {
		slog.Error("Error sending EOF commit to ring", "sum_id", sum.id, "client_id", msg.ClientID, "err", err)
		return err
	}

	return nil
}

func (sum *Sum) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := sum.Close(); err != nil {
		slog.Error("While closing sum", "err", err)
	}
	sum.shutdownOnce.Do(func() {
		close(sum.shutdown)
	})
}

func (sum *Sum) Close() error {
	if err := sum.inputQueue.StopConsuming(); err != nil {
		return err
	}
	if err := sum.inputQueue.Close(); err != nil {
		return err
	}
	for i := range sum.outputExchanges {
		if err := sum.outputExchanges[i].StopConsuming(); err != nil {
			return err
		}
		if err := sum.outputExchanges[i].Close(); err != nil {
			return err
		}
	}
	if err := sum.eofOutput.Close(); err != nil {
		return err
	}
	sum.sumMonitor.Close()
	return nil
}
