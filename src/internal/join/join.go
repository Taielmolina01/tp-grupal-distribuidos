package join

import (
	"log/slog"
	"strconv"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
)

func newJoin[L, R, O any](
	output middleware.Middleware,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
	leftCombine func(L, L) L,
	queryID uint8,
) *Join[L, R, O] {
	return &Join[L, R, O]{
		output:      output,
		leftBuffer:  map[int]map[string]L{},
		rightBuffer: map[int]map[string]R{},
		leftKey:     leftKey,
		rightKey:    rightKey,
		combine:     combine,
		leftCombine: leftCombine,
		queryID:     queryID,
	}
}

func (j *Join[L, R, O]) HandleLeft(clientID int, record L) {
	key := j.leftKey(record)

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.leftCombine != nil {
		if j.leftBuffer[clientID] == nil {
			j.leftBuffer[clientID] = map[string]L{}
		}
		if existing, ok := j.leftBuffer[clientID][key]; ok {
			slog.Info("client_id", strconv.Itoa(clientID), "before combine", "existing", existing, "new", record)
			j.leftBuffer[clientID][key] = j.leftCombine(existing, record)
			slog.Info("client_id", strconv.Itoa(clientID), "after combine", "combined", j.leftBuffer[clientID][key])
		} else {
			j.leftBuffer[clientID][key] = record
			slog.Info("client_id", strconv.Itoa(clientID), "new register", "key", key, "record", j.leftBuffer[clientID][key])
		}
		return
	}

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

	j.mu.Lock()
	if j.leftCombine != nil {
		if j.rightBuffer[clientID] == nil {
			j.rightBuffer[clientID] = map[string]R{}
		}
		j.rightBuffer[clientID][key] = record
		j.mu.Unlock()
		return
	}

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
	slog.Info("join emitting results", "client_id", clientID, "query_id", j.queryID)

	j.mu.Lock()
	if j.leftCombine != nil {
		leftMap := j.leftBuffer[clientID]
		rightMap := j.rightBuffer[clientID]
		for key, leftRecord := range leftMap {
			if rightRecord, ok := rightMap[key]; ok {
				j.emit(clientID, j.combine(leftRecord, rightRecord))
			}
		}
	}
	delete(j.leftBuffer, clientID)
	delete(j.rightBuffer, clientID)
	j.mu.Unlock()
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

	// esto es un recurso compartido con 119
	if err := j.output.Send(*msg); err != nil {
		slog.Error("while sending result", "err", err)
	}
}
