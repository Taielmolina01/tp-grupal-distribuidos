package messagehandler

import (
	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/fruititem"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
)

var clientId = 0

type MessageHandler struct {
	clientId          int
	processedMessages uint32
}

func NewMessageHandler() MessageHandler {
	response := MessageHandler{clientId: clientId}
	clientId++
	return response
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := fruititem.FruitItemFromClient{
		ClientId:   messageHandler.clientId,
		FruitItems: []fruititem.FruitItem{fruitRecord},
	}
	messageHandler.processedMessages += 1
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := eofmessage.EofMessage{
		ClientID:      messageHandler.clientId,
		TotalMessages: messageHandler.processedMessages,
	}
	return inner.SerializeEofMessage(data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, _, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if fruitRecords == nil || fruitRecords.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords.FruitItems, nil
}

func (messageHandler *MessageHandler) DeserializeRingMessage(message *middleware.Message) (*eofmessagetypes.EofRingMessage, *eofmessagetypes.EofMessageCommit, error) {
	return inner.DeserializeRingMessage(message)
}

func (messageHandler *MessageHandler) SerializeEofFromQueueMsg(msg eofmessagetypes.EofRingMessage) (*middleware.Message, error) {
	return inner.SerializeEofFromQueueMsg(msg)
}
