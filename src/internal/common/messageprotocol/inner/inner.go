package inner

import (
	"encoding/json"
	"errors"
	"math"

	"tp-grupal-distribuidos/internal/common/eofmessage"
	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/middleware"
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
		return 0, errors.New("client ID is not a valid number")
	}
	if clientIDAsFloat < 0 || math.Trunc(clientIDAsFloat) != clientIDAsFloat {
		return 0, errors.New("client ID must be a non-negative integer")
	}
	return int(clientIDAsFloat), nil
}

func DeserializeEofRingMessage(data []interface{}) (*eofmessagetypes.EofRingMessage, error) {
	leaderAsFloat, ok := data[0].(float64)
	if !ok {
		return nil, errors.New("leaderID is not a non negative number")
	}
	actualAmountAsFloat, ok := data[1].(float64)
	if !ok {
		return nil, errors.New("actual amount of messages handled is not a non negative number")
	}
	realAmountAsFloat, ok := data[2].(float64)
	if !ok {
		return nil, errors.New("real amount of messages handled is not a non negative number")
	}
	parsedClientID, err := parseClientID(data[3])
	if err != nil {
		return nil, err
	}
	filteredAmount, ok := data[4].(float64)
	if !ok {
		return nil, errors.New("filtered amount of messages handled is not a non negative number")
	}

	return &eofmessagetypes.EofRingMessage{
		CoordinatorId:  uint32(leaderAsFloat),
		ActualAmount:   uint32(actualAmountAsFloat),
		RealAmount:     uint32(realAmountAsFloat),
		ClientId:       parsedClientID,
		FilteredAmount: uint32(filteredAmount),
	}, nil
}

func DeserializeEofCommitRingMessage(data []interface{}) (*eofmessagetypes.EofMessageCommit, error) {
	parsedClientID, err := parseClientID(data[0])
	if err != nil {
		return nil, err
	}

	hopsAsFloat, ok := data[1].(float64)
	if !ok {
		return nil, errors.New("hops is not a valid number")
	}

	filteredAmountAsFloat, ok := data[2].(float64)
	if !ok {
		return nil, errors.New("filtered amount is not a valid number")
	}

	return &eofmessagetypes.EofMessageCommit{ClientID: parsedClientID, Hops: int(hopsAsFloat), FilteredAmount: uint32(filteredAmountAsFloat)}, nil
}

func DeserializeRingMessage(message *middleware.Message) (*eofmessagetypes.EofRingMessage, *eofmessagetypes.EofMessageCommit, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return nil, nil, err
	}

	if len(data) != 3 && len(data) != 5 {
		return nil, nil, errors.New("EOF message has an invalid shape")
	}

	if len(data) == 3 {
		eofCommitRingMessage, err := DeserializeEofCommitRingMessage(data)
		return nil, eofCommitRingMessage, err
	}

	eofRingMessage, err := DeserializeEofRingMessage(data)
	return eofRingMessage, nil, err

}

func SerializeEofFromQueueMsg(msg eofmessagetypes.EofRingMessage) (*middleware.Message, error) {
	data, err := serializeJson([]interface{}{msg.CoordinatorId, msg.ActualAmount, msg.RealAmount, msg.ClientId, msg.FilteredAmount})
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

func SerializeEofMessageCommit(msg eofmessagetypes.EofMessageCommit) (*middleware.Message, error) {
	data, err := serializeJson([]interface{}{msg.ClientID, msg.Hops, msg.FilteredAmount})
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
		return nil, false, errors.New("aggregation EOF message has invalid client ID")
	}

	aggregationIDRaw, ok := data[2].(float64)
	if !ok {
		return nil, false, errors.New("aggregation EOF message has invalid aggregation ID")
	}

	return &eofmessage.AggregationEofMessage{
		ClientID:      clientID,
		AggregationID: int(aggregationIDRaw),
	}, true, nil
}
