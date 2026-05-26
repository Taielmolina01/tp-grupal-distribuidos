package inner

import (
	"encoding/json"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
)

const (
	Query1ID uint8 = 1
	Query2ID uint8 = 2
	Query3ID uint8 = 3
	Query4ID uint8 = 4
	Query5ID uint8 = 5
)

type DataMsg[T any] struct {
	ClientID int      `json:"client_id"`
	QueryID  uint8    `json:"query_id"`
	Payload  T        `json:"payload,omitempty"`
	EOF      *EOFInfo `json:"eof,omitempty"`
}

type EOFInfo struct {
	Kind          string `json:"kind"`
	TotalMessages uint32 `json:"total_messages"`
}

func SerializeData[T any](d DataMsg[T]) (*newmiddleware.Message, error) {
	body, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &newmiddleware.Message{Body: string(body)}, nil
}

func DeserializeData[T any](msg *newmiddleware.Message) (*DataMsg[T], error) {
	d := &DataMsg[T]{}
	if err := json.Unmarshal([]byte(msg.Body), d); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DataMsg[T]) IsEOF() bool {
	return d.EOF != nil
}

func Deserialize[T any](payload json.RawMessage) (T, error) {
	var result T
	if err := json.Unmarshal(payload, &result); err != nil {
		return result, err
	}
	return result, nil
}
