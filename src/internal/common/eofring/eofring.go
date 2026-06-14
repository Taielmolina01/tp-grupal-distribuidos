package eofring

import (
	"fmt"
	"log/slog"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

const (
	typeRing uint8 = iota + 1
	typeCommit
)

func SerializeRingMessage(m eofmessagetypes.EofRingMessage) []byte {
	w := wire.NewWriter()
	w.Uint8(typeRing)
	w.Uint32(m.CoordinatorId)
	w.Uint32(m.ActualAmount)
	w.Uint32(m.RealAmount)
	w.Uint32(uint32(m.ClientId))
	w.Uint32(m.FilteredAmount)
	w.Uint64(m.Seq)
	return w.Bytes()
}

func serializeCommit(m eofmessagetypes.EofMessageCommit) []byte {
	w := wire.NewWriter()
	w.Uint8(typeCommit)
	w.Uint32(uint32(m.ClientID))
	w.Uint32(uint32(m.Hops))
	w.Uint32(m.FilteredAmount)
	w.Uint64(m.Seq)
	return w.Bytes()
}

func deserializeRing(body []byte) (*eofmessagetypes.EofRingMessage, *eofmessagetypes.EofMessageCommit, error) {
	r := wire.NewReader(body)
	switch r.Uint8() {
	case typeRing:
		m := &eofmessagetypes.EofRingMessage{
			CoordinatorId:  r.Uint32(),
			ActualAmount:   r.Uint32(),
			RealAmount:     r.Uint32(),
			ClientId:       int(r.Uint32()),
			FilteredAmount: r.Uint32(),
			Seq:            r.Uint64(),
		}
		return m, nil, r.Err()
	case typeCommit:
		m := &eofmessagetypes.EofMessageCommit{
			ClientID:       int(r.Uint32()),
			Hops:           int(r.Uint32()),
			FilteredAmount: r.Uint32(),
			Seq:            r.Uint64(),
		}
		return nil, m, r.Err()
	default:
		return nil, nil, fmt.Errorf("eofring: unknown message type")
	}
}

type EofRingAlgorithm interface {
	Run()
	Close() error
}

type eofRingAlgorithmImpl struct {
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	amountReplicas  int
	id              uint32
	messagesMonitor msgmonitor.MessageMonitor
	typeOfNode      string
	totalMessages   *uint32
	finishCallback  FinishCallback
	queryId         uint8
}

type FinishCallback func(clientID int, seq uint64, total uint32, isCoordinator bool) error

func CreateEofRingAlgorithm(
	inputQueue, outputQueue middleware.Middleware,
	amountReplicas int,
	id uint32,
	messageMonitor msgmonitor.MessageMonitor,
	finishCallback FinishCallback,
	queryId uint8,
) EofRingAlgorithm {
	return &eofRingAlgorithmImpl{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		amountReplicas:  amountReplicas,
		id:              id,
		messagesMonitor: messageMonitor,
		totalMessages:   nil,
		queryId:         queryId,
		finishCallback:  finishCallback,
	}
}

func (eofring *eofRingAlgorithmImpl) Run() {
	if err := eofring.inputQueue.StartConsuming(eofring.handleEofMessageFromQueue); err != nil {
		slog.Error("While consuming from EOF ring queue", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "err", err)
	}
}

func (eofring *eofRingAlgorithmImpl) Close() error {
	if err := eofring.inputQueue.Close(); err != nil {
		slog.Error("While closing EOF ring input queue", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "err", err)
		return err
	}
	eofring.messagesMonitor.Close()
	return nil
}

func (eofring *eofRingAlgorithmImpl) handleEofMessageFromQueue(msg middleware.Message, ack, nack func()) {
	eofRingMessage, eofRingCommitMessage, err := deserializeRing(msg.Body)
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

	if eofRingMessage.CoordinatorId == eofring.id && eofRingMessage.ActualAmount == eofRingMessage.RealAmount {
		// Si soy el líder y la cantidad de todos los mensajes enviados por el cliente (contados por el gateway) y la suma de lo que cada uno
		// de los nodos me dice que proceso, entonces envio el commit otra vez en forma de anillo para que cada uno le pase al exchange de los
		// aggregations sus mensajes.
		eofring.sendEofCommitToReplicas(eofRingMessage, ack, nack)
	} else {
		value := eofring.messagesMonitor.GetProcessedMessagesAmountByClientId(eofRingMessage.ClientId)
		if eofRingMessage.CoordinatorId == eofring.id {
			// Si soy el líder y la cantidad de todos los mensajes enviados por el cliente (contados por el gateway) y la suma de lo que cada uno
			// de los nodos me dice que proceso no coinciden, simplemente inicio el anillo de nuevo. Esto porque estoy asumiendo que lo único que paso
			// es que un nodo no había terminado de procesar los mensajes de un cliente en particular. Como asumimos que no hay caida, eventualmente va a converger
			// al primer caso.
			eofRingMessage.ActualAmount = value
			eofRingMessage.FilteredAmount = eofring.messagesMonitor.GetForwardedMessagesAmountByClientId(eofRingMessage.ClientId)
		} else {
			// Si no soy el líder simplemente sumo los mensajes que yo leí del cliente X y lo sumo al mensaje del ring y lo forwardeo.
			eofRingMessage.ActualAmount += value
			eofRingMessage.FilteredAmount += eofring.messagesMonitor.GetForwardedMessagesAmountByClientId(eofRingMessage.ClientId)
		}

		eofring.sendEofMessageToQueue(eofRingMessage, ack)
	}
}

func (eofring *eofRingAlgorithmImpl) sendEofCommitToReplicas(eofRingMessage *eofmessagetypes.EofRingMessage, ack, nack func()) {
	body := serializeCommit(eofmessagetypes.EofMessageCommit{
		CoordinatorId:  eofRingMessage.CoordinatorId,
		ClientID:       eofRingMessage.ClientId,
		Hops:           0,
		FilteredAmount: eofRingMessage.FilteredAmount,
		Seq:            eofRingMessage.Seq,
	})
	if err := eofring.outputQueue.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("Error sending EOF commit to ring", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId, "err", err)
		nack()
		return
	}
	ack()
}

func (eofring *eofRingAlgorithmImpl) sendEofMessageToQueue(eofRingMessage *eofmessagetypes.EofRingMessage, ack func()) {
	defer ack()
	body := SerializeRingMessage(*eofRingMessage)
	if err := eofring.outputQueue.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("Error forwarding EOF ring message", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", eofRingMessage.ClientId, "err", err)
		return
	}
}

func (eofring *eofRingAlgorithmImpl) handleEOFCommitMessage(msg *eofmessagetypes.EofMessageCommit) error {

	if err := eofring.finishCallback(msg.ClientID, msg.Seq, msg.FilteredAmount, msg.CoordinatorId == eofring.id); err != nil {
		return fmt.Errorf("finish callback: %w", err)
	}

	msg.Hops++

	if msg.Hops >= eofring.amountReplicas {
		return nil
	}

	if err := eofring.outputQueue.Send(middleware.Message{Body: serializeCommit(*msg)}); err != nil {
		slog.Error("Error sending EOF commit to ring", fmt.Sprintf("%s_id", eofring.typeOfNode), eofring.id, "client_id", msg.ClientID, "err", err)
		return err
	}

	return nil
}
