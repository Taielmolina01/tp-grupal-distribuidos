package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofringmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var clientId = 0

type MessageHandler struct {
	clientId          int
	processedMessages uint32
}

type MessageEof struct {
	actualAmount uint32
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

func (messageHandler *MessageHandler) DeserializeRingMessage(message *middleware.Message) (*eofringmessage.EofRingMessage, *eofringmessage.EofMessageCommit, error) {
	return inner.DeserializeRingMessage(message)
}

func (messageHandler *MessageHandler) SerializeEofFromQueueMsg(msg eofringmessage.EofRingMessage) (*middleware.Message, error) {
	return inner.SerializeEofFromQueueMsg(msg)
}
