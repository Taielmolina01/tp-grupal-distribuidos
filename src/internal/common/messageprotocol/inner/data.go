package inner

import (
	"encoding/json"

	"tp-grupal-distribuidos/internal/common/middleware"
)

type DataMsg[T any] struct {
	ClientID int      `json:"client_id"`
	Payload  T        `json:"payload,omitempty"`
	EOF      *EOFInfo `json:"eof,omitempty"`
}

type EOFInfo struct {
	Kind          string `json:"kind"`
	TotalMessages uint32 `json:"total_messages"`
}

func SerializeData[T any](d DataMsg[T]) (*middleware.Message, error) {
	body, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeData[T any](msg *middleware.Message) (*DataMsg[T], error) {
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
