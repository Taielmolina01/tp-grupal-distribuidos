package eofring

import (
	"fmt"
	"log/slog"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type EofRingAlgorithm interface {
	Run()
}

type eofRingAlgorithmImpl struct {
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	amountReplicas  int
	id              uint32
	messagesMonitor msgmonitor.MessageMonitor
	outputExchange  middleware.Middleware
	// typeOfNode should be an enum defined somewhere in common, but for simplicity I just left it as a string
	typeOfNode    string
	totalMessages *uint32
	finishCallback func() error
	queryId uint8
}

func CreateEofRingAlgorithm(
	inputQueue, outputQueue middleware.Middleware,
	amountReplicas int,
	id uint32,
	outputExchange middleware.Middleware,
	messageMonitor msgmonitor.MessageMonitor,
	finishCallback func() error,
	queryId uint8,
) EofRingAlgorithm {
	return &eofRingAlgorithmImpl{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		amountReplicas:  amountReplicas,
		id:              id,
		messagesMonitor: messageMonitor,
		outputExchange:  outputExchange,
		totalMessages:   nil,
		queryId:         queryId,
		finishCallback:  finishCallback,
	}
}

func (eofring *eofRingAlgorithmImpl) Run() {
	slog.Info("Starting EOF ring consumer", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id)
	if err := eofring.inputQueue.StartConsuming(eofring.handleEofMessageFromQueue); err != nil {
		slog.Error("While consuming from EOF ring queue", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "err", err)
	}
}

func (eofring *eofRingAlgorithmImpl) handleEofMessageFromQueue(msg middleware.Message, ack, nack func()) {
	eofRingMessage, eofRingCommitMessage, err := inner.DeserializeRingMessage(&msg)
	if err != nil {
		slog.Error("Error deserializing EOF ring message", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "err", err)
		ack()
		return
	}

	if eofRingCommitMessage != nil {
		if err := eofring.handleEOFCommitMessage(eofRingCommitMessage); err != nil {
			slog.Error("Error handling EOF commit", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingCommitMessage.ClientID, "err", err)
			nack()
		} else {
			slog.Info("EOF commit sent to aggregation", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingCommitMessage.ClientID)
			ack()
		}
		return
	}

	if eofRingMessage == nil {
		slog.Error("EOF ring message without processed payload", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id)
		ack()
		return
	}

	if eofring.totalMessages == nil {
		eofring.totalMessages = &eofRingMessage.ActualAmount
	}

	if eofRingMessage.Leader == eofring.id && eofRingMessage.ActualAmount == eofRingMessage.RealAmount {
		// Si soy el líder y la cantidad de todos los mensajes enviados por el cliente (contados por el gateway) y la suma de lo que cada uno
		// de los nodos me dice que proceso, entonces envio el commit otra vez en forma de anillo para que cada uno le pase al exchange de los
		// aggregations sus mensajes.
		slog.Info("EOF commit sent to replicas", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId)
		eofring.sendEofCommitToReplicas(eofRingMessage, ack, nack)
	} else {
		value := eofring.messagesMonitor.GetProcessedMessagesAmountByClientId(eofRingMessage.ClientId)
		if eofRingMessage.Leader == eofring.id {
			// Si soy el líder y la cantidad de todos los mensajes enviados por el cliente (contados por el gateway) y la suma de lo que cada uno
			// de los nodos me dice que proceso no coinciden, simplemente inicio el anillo de nuevo. Esto porque estoy asumiendo que lo único que paso
			// es que un nodo no había terminado de procesar los mensajes de un cliente en particular. Como asumimos que no hay caida, eventualmente va a converger
			// al primer caso.
			eofRingMessage.ActualAmount = value
			eofRingMessage.FilteredAmount = eofring.messagesMonitor.GetFilteredMessagesAmountByClientId(eofRingMessage.ClientId)

		} else {
			// Si no soy el líder simplemente sumo los mensajes que yo leí del cliente X y lo sumo al mensaje del ring y lo forwardeo.
			eofRingMessage.ActualAmount += value
			eofRingMessage.FilteredAmount += eofring.messagesMonitor.GetFilteredMessagesAmountByClientId(eofRingMessage.ClientId)
		}

		eofring.sendEofMessageToQueue(eofRingMessage, ack)
	}
}

func (eofring *eofRingAlgorithmImpl) sendEofCommitToReplicas(eofRingMessage *eofmessagetypes.EofRingMessage, ack, nack func()) {
	msg, err := inner.SerializeEofMessageCommit(eofmessagetypes.EofMessageCommit{ClientID: eofRingMessage.ClientId, Hops: 0, FilteredAmount: eofRingMessage.FilteredAmount})
	if err != nil {
		slog.Error("Error serializing EOF commit", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId, "err", err)
		ack()
		return
	}
	if err = eofring.outputQueue.Send(*msg); err != nil {
		slog.Error("Error sending EOF commit to ring", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId, "err", err)
		nack()
		return
	}
	slog.Info("EOF commit sent to ring", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId)
	ack()
}

func (eofring *eofRingAlgorithmImpl) sendEofMessageToQueue(eofRingMessage *eofmessagetypes.EofRingMessage, ack func()) {
	defer ack()
	serializedEofRingMessage, err := inner.SerializeEofFromQueueMsg(*eofRingMessage)
	if err != nil {
		slog.Error("Error serializing forwarded EOF ring message", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
	if err := eofring.outputQueue.Send(*serializedEofRingMessage); err != nil {
		slog.Error("Error forwarding EOF ring message", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
}

func (eofring *eofRingAlgorithmImpl) handleEOFCommitMessage(msg *eofmessagetypes.EofMessageCommit) error {

	msgToOutput, err := inner.SerializeData(inner.DataMsg[any]{
		ClientID: msg.ClientID,
		EOF: &inner.EOFInfo{
			Kind:          "commit",           // ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
			TotalMessages: msg.FilteredAmount, // aca si pasan de la primera instancia de filter deberia solamente decir q los mensajes totales son los que pasaron mi filtro justamente
		},
		Payload: nil,
		QueryID: eofring.queryId,
	})

	if err := eofring.finishCallback(); err != nil {
		slog.Error("Error finishing callback", "err", err)
	}
	eofring.outputExchange.Send(*msgToOutput)

	msg.Hops++

	if msg.Hops >= eofring.amountReplicas-1 {
		return nil
	}

	toSend, err := inner.SerializeEofMessageCommit(*msg)

	if err != nil {
		slog.Error("While sending EOF commit", "err", err)
		return err
	}

	if err = eofring.outputQueue.Send(*toSend); err != nil {
		slog.Error("Error sending EOF commit to ring", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", msg.ClientID, "err", err)
		return err
	}

	return nil
}
