package inner

import (
	"encoding/json"
	"errors"
	"math"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofringmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const _AGGREGATION_ID = "agg"

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func parseClientID(raw interface{}) (int, error) {
	clientIDAsFloat, ok := raw.(float64)
	if !ok {
		return 0, errors.New("Client ID is not a valid number")
	}
	if clientIDAsFloat < 0 || math.Trunc(clientIDAsFloat) != clientIDAsFloat {
		return 0, errors.New("Client ID must be a non-negative integer")
	}
	return int(clientIDAsFloat), nil
}

func SerializeMessage(fruitRecords fruititem.FruitItemFromClient) (*middleware.Message, error) {
	data := []interface{}{}
	data = append(data, fruitRecords.ClientId)
	for _, fruitRecord := range fruitRecords.FruitItems {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*fruititem.FruitItemFromClient, *eofmessage.EofMessage, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, nil, false, err
	}
	result := fruititem.FruitItemFromClient{}
	var clientId int
	for i, datum := range data {
		if i == 0 {
			_clientId, err := parseClientID(datum)
			if err != nil {
				return nil, nil, false, err
			}
			clientId = _clientId
			continue
		}

		fruitPair, ok := datum.([]interface{})
		if !ok {
			if amountOfMessages, ok := datum.(float64); ok {
				return nil, &eofmessage.EofMessage{
						TotalMessages: uint32(amountOfMessages),
						ClientID:      clientId,
					},
					true,
					nil
			}
			return nil, nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		result.FruitItems = append(result.FruitItems, fruitRecord)
	}

	result.ClientId = clientId
	return &result, nil, len(result.FruitItems) == 0, nil
}

func DeserializeEofRingMessage(data []interface{}) (*eofringmessage.EofRingMessage, error) {
	leaderAsFloat, ok := data[0].(float64)
	if !ok {
		return nil, errors.New("LeaderID is not a non negative number")
	}
	actualAmountAsFloat, ok := data[1].(float64)
	if !ok {
		return nil, errors.New("Actual amount of messages handled is not a non negative number")
	}
	realAmountAsFloat, ok := data[2].(float64)
	if !ok {
		return nil, errors.New("Real amount of messages handled is not a non negative number")
	}
	parsedClientID, err := parseClientID(data[3])
	if err != nil {
		return nil, err
	}

	return &eofringmessage.EofRingMessage{
		Leader:       uint32(leaderAsFloat),
		ActualAmount: uint32(actualAmountAsFloat),
		RealAmount:   uint32(realAmountAsFloat),
		ClientId:     parsedClientID,
	}, nil
}

func DeserializeEofCommitRingMessage(data []interface{}) (*eofringmessage.EofMessageCommit, error) {
	parsedClientID, err := parseClientID(data[0])
	if err != nil {
		return nil, err
	}

	hopsAsFloat, ok := data[1].(float64)
	if !ok {
		return nil, errors.New("Hops is not a valid number")
	}

	return &eofringmessage.EofMessageCommit{ClientID: parsedClientID, Hops: int(hopsAsFloat)}, nil
}

func DeserializeRingMessage(message *middleware.Message) (*eofringmessage.EofRingMessage, *eofringmessage.EofMessageCommit, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return nil, nil, err
	}

	if len(data) != 2 && len(data) != 4 {
		return nil, nil, errors.New("EOF message has an invalid shape")
	}

	if len(data) == 2 {
		eofCommitRingMessage, err := DeserializeEofCommitRingMessage(data)
		return nil, eofCommitRingMessage, err
	}

	eofRingMessage, err := DeserializeEofRingMessage(data)
	return eofRingMessage, nil, err

}

func SerializeEofFromQueueMsg(msg eofringmessage.EofRingMessage) (*middleware.Message, error) {
	data, err := serializeJson([]interface{}{msg.Leader, msg.ActualAmount, msg.RealAmount, msg.ClientId})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{
		Body: string(data),
	}, nil
}

func SerializeEofMessage(msg eofmessage.EofMessage) (*middleware.Message, error) {
	data, err := serializeJson([]interface{}{msg.ClientID, msg.TotalMessages})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{
		Body: string(data),
	}, nil
}

func SerializeEofMessageCommit(msg eofringmessage.EofMessageCommit) (*middleware.Message, error) {
	data, err := serializeJson([]interface{}{msg.ClientID, msg.Hops})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{
		Body: string(data),
	}, nil
}

func SerializeAggregationEofMessage(msg eofmessage.AggregationEofMessage) (*middleware.Message, error) {
	data, err := serializeJson([]interface{}{_AGGREGATION_ID, msg.ClientID, msg.AggregationID})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{
		Body: string(data),
	}, nil
}

func DeserializeAggregationEofMessage(message *middleware.Message) (*eofmessage.AggregationEofMessage, bool, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return nil, false, err
	}

	if len(data) != 3 {
		return nil, false, nil
	}

	tag, ok := data[0].(string)
	if !ok || tag != _AGGREGATION_ID {
		return nil, false, nil
	}

	clientID, err := parseClientID(data[1])
	if err != nil {
		return nil, false, errors.New("Aggregation EOF message has invalid client ID")
	}

	aggregationIDRaw, ok := data[2].(float64)
	if !ok {
		return nil, false, errors.New("Aggregation EOF message has invalid aggregation ID")
	}

	return &eofmessage.AggregationEofMessage{
		ClientID:      clientID,
		AggregationID: int(aggregationIDRaw),
	}, true, nil
}
