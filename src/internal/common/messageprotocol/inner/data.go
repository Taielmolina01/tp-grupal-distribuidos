package inner

import (
	"encoding/json"

	"tp-grupal-distribuidos/internal/common/middleware"
)

type DataMsg struct {
	ClientID int             `json:"client_id"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	EOF      *EOFInfo        `json:"eof,omitempty"`
}

type EOFInfo struct {
	Kind          string `json:"kind"`
	TotalMessages uint32 `json:"total_messages"`
}

// a chequear esto
type Serializable interface {
	Serialize() ([]byte, error)
}

func SerializeData(d DataMsg) (*middleware.Message, error) {
	body, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeData(msg *middleware.Message) (*DataMsg, error) {
	d := &DataMsg{}
	if err := json.Unmarshal([]byte(msg.Body), d); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DataMsg) IsEOF() bool {
	return d.EOF != nil
}
