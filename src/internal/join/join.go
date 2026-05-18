package join

import (
	"encoding/json"
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
)

func newJoin[L, R, O any](
	output middleware.Middleware,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
	queryID uint8,
) *Join[L, R, O] {
	return &Join[L, R, O]{
		output:      output,
		leftBuffer:  map[int]map[string]L{},
		rightBuffer: map[int]map[string]R{},
		leftKey:     leftKey,
		rightKey:    rightKey,
		combine:     combine,
		queryID:     queryID,
	}
}

func (j *Join[L, R, O]) HandleLeft(clientID int, record L) {
	key := j.leftKey(record)

	if rightMap, ok := j.rightBuffer[clientID]; ok {
		if rightRecord, ok := rightMap[key]; ok {
			j.emit(clientID, j.combine(record, rightRecord))
			return
		}
	}

	if j.leftBuffer[clientID] == nil {
		j.leftBuffer[clientID] = map[string]L{}
	}
	j.leftBuffer[clientID][key] = record
}

func (j *Join[L, R, O]) HandleRight(clientID int, record R) {
	key := j.rightKey(record)

	if leftMap, ok := j.leftBuffer[clientID]; ok {
		if leftRecord, ok := leftMap[key]; ok {
			j.emit(clientID, j.combine(leftRecord, record))
			return
		}
	}

	if j.rightBuffer[clientID] == nil {
		j.rightBuffer[clientID] = map[string]R{}
	}
	j.rightBuffer[clientID][key] = record
}

func (j *Join[L, R, O]) emit(clientID int, result O) {
	payload, err := json.Marshal(result)
	if err != nil {
		slog.Error("while marshaling result", "err", err)
		return
	}

	msg, err := inner.SerializeResult(inner.ResultMsg{
		ClientID: clientID,
		QueryID:  j.queryID,
		Payload:  payload,
	})
	if err != nil {
		slog.Error("while serializing result", "err", err)
		return
	}

	if err := j.output.Send(*msg); err != nil {
		slog.Error("while sending result", "err", err)
	}
}
