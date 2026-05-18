package inner

import (
	"encoding/json"

	"tp-grupal-distribuidos/internal/common/middleware"
)

const (
	Query1ID uint8 = 1
	Query2ID uint8 = 2
	Query3ID uint8 = 3
	Query4ID uint8 = 4
	Query5ID uint8 = 5
)

type ResultMsg struct {
	ClientID   int             `json:"client_id"`
	QueryID    uint8           `json:"query_id"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	IsQueryEOF bool            `json:"eof_query,omitempty"`
}

func SerializeResult(env ResultMsg) (*middleware.Message, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeResult(msg *middleware.Message) (*ResultMsg, error) {
	env := &ResultMsg{}
	if err := json.Unmarshal([]byte(msg.Body), env); err != nil {
		return nil, err
	}
	return env, nil
}
