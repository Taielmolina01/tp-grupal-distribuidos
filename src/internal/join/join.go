package join

import (
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

	j.mu.Lock()
	if rightMap, ok := j.rightBuffer[clientID]; ok {
		if rightRecord, ok := rightMap[key]; ok {
			j.mu.Unlock()
			j.emit(clientID, j.combine(record, rightRecord))
			return
		}
	}

	if j.leftBuffer[clientID] == nil {
		j.leftBuffer[clientID] = map[string]L{}
	}
	j.leftBuffer[clientID][key] = record
	j.mu.Unlock()
}

func (j *Join[L, R, O]) HandleRight(clientID int, record R) {
	key := j.rightKey(record)

	j.mu.Lock()
	if leftMap, ok := j.leftBuffer[clientID]; ok {
		if leftRecord, ok := leftMap[key]; ok {
			j.mu.Unlock()
			j.emit(clientID, j.combine(leftRecord, record))
			return
		}
	}

	if j.rightBuffer[clientID] == nil {
		j.rightBuffer[clientID] = map[string]R{}
	}
	j.rightBuffer[clientID][key] = record
	j.mu.Unlock()
}

func (j *Join[L, R, O]) HandleQueryEOF(clientID int) {
	slog.Info("join sending QueryEOF", "client_id", clientID, "query_id", j.queryID)
	j.mu.Lock()
	delete(j.leftBuffer, clientID)
	delete(j.rightBuffer, clientID)
	j.mu.Unlock()

	msg, err := inner.SerializeData(inner.DataMsg[O]{
		ClientID: clientID,
		QueryID:  j.queryID,
		EOF:      &inner.EOFInfo{Kind: "query_eof"},
	})
	if err != nil {
		slog.Error("while serializing query EOF", "err", err)
		return
	}
	if err := j.output.Send(*msg); err != nil {
		slog.Error("while sending query EOF", "err", err)
	}
}

func (j *Join[L, R, O]) emit(clientID int, result O) {
	msg, err := inner.SerializeData(inner.DataMsg[O]{
		ClientID: clientID,
		QueryID:  j.queryID,
		Payload:  result,
	})
	if err != nil {
		slog.Error("while serializing result", "err", err)
		return
	}

	if err := j.output.Send(*msg); err != nil {
		slog.Error("while sending result", "err", err)
	}
}
