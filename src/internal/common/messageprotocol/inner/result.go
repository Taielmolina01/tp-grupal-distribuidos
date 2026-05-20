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

type ResultMsg[T any] struct {
	ClientID   int   `json:"client_id"`
	QueryID    uint8 `json:"query_id"`
	Payload    T     `json:"payload,omitempty"`
	IsQueryEOF bool  `json:"eof_query,omitempty"`
}

func SerializeResult[T any](env ResultMsg[T]) (*middleware.Message, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeResult[T any](msg *middleware.Message) (*ResultMsg[T], error) {
	env := &ResultMsg[T]{}
	if err := json.Unmarshal([]byte(msg.Body), env); err != nil {
		return nil, err
	}
	return env, nil
}
