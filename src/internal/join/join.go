package join

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
)

func newJoin[L, R, O any](
	output middleware.Middleware,
	outputCodec wire.Codec[O],
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
	leftCombine func(L, L) L,
	queryID uint8,
) *Join[L, R, O] {
	return &Join[L, R, O]{
		output:      output,
		outputCodec: outputCodec,
		leftBuffer:  map[int]map[string]L{},
		rightBuffer: map[int]map[string]R{},
		pending:     map[int][]O{},
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
			j.leftBuffer[clientID][key] = j.leftCombine(existing, record)
		} else {
			j.leftBuffer[clientID][key] = record
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
	defer j.mu.Unlock()

	if j.leftCombine != nil {
		if j.rightBuffer[clientID] == nil {
			j.rightBuffer[clientID] = map[string]R{}
		}
		j.rightBuffer[clientID][key] = record
		return
	}

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

func (j *Join[L, R, O]) HandleQueryEOF(clientID int) {
	results := j.handleMapsEof(clientID)

	if len(results) == 0 {
		return
	}
	body := batch.Write(clientID, j.queryID, 0, 0, results, j.outputCodec)
	if err := j.output.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("while sending join results", "err", err)
	}
}

func (j *Join[L, R, O]) handleMapsEof(clientID int) []O {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.leftCombine != nil {
		leftMap := j.leftBuffer[clientID]
		rightMap := j.rightBuffer[clientID]
		for key, leftRecord := range leftMap {
			if rightRecord, ok := rightMap[key]; ok {
				j.emit(clientID, j.combine(leftRecord, rightRecord))
			}
		}
	}
	results := j.pending[clientID]
	delete(j.pending, clientID)
	delete(j.leftBuffer, clientID)
	delete(j.rightBuffer, clientID)
	return results
}

func (j *Join[L, R, O]) emit(clientID int, result O) {
	j.pending[clientID] = append(j.pending[clientID], result)
}
