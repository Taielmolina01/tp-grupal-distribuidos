package join

import (
	"encoding/json"
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newJoin[L, R, O any](
	config JoinConfig,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	leftInput, err := middleware.CreateExchangeMiddleware(config.LeftInputExchange, []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	rightInput, err := middleware.CreateExchangeMiddleware(config.RightInputExchange, []string{}, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		return nil, err
	}

	output, err := middleware.CreateExchangeMiddleware(config.OutputExchange, []string{}, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		if err := rightInput.Close(); err != nil {
			slog.Error("while closing right input", "err", err)
		}
		return nil, err
	}

	return &Join[L, R, O]{
		leftInput:   leftInput,
		rightInput:  rightInput,
		output:      output,
		leftBuffer:  map[int]map[string]L{},
		rightBuffer: map[int]map[string]R{},
		leftKey:     leftKey,
		rightKey:    rightKey,
		combine:     combine,
		queryID:     config.QueryID,
	}, nil
}

func (j *Join[L, R, O]) Run() {
	done := make(chan struct{})

	go func() {
		if err := j.leftInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			j.handleLeft(msg, ack, nack)
		}); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := j.rightInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleRight(msg, ack, nack)
	}); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
}

func (j *Join[L, R, O]) HandleSignals() {}

func (j *Join[L, R, O]) handleLeft(msg middleware.Message, ack, nack func()) {
	defer ack()

	env, err := inner.DeserializeData(&msg)
	if err != nil {
		slog.Error("while deserializing left message", "err", err)
		return
	}

	if env.IsEOF() {
		return
	}

	var record L
	if err := json.Unmarshal(env.Payload, &record); err != nil {
		slog.Error("while unmarshaling left record", "err", err)
		return
	}

	clientID := env.ClientID
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

func (j *Join[L, R, O]) handleRight(msg middleware.Message, ack, nack func()) {
	defer ack()

	env, err := inner.DeserializeData(&msg)
	if err != nil {
		slog.Error("while deserializing right message", "err", err)
		return
	}

	if env.IsEOF() {
		return
	}

	var record R
	if err := json.Unmarshal(env.Payload, &record); err != nil {
		slog.Error("while unmarshaling right record", "err", err)
		return
	}

	clientID := env.ClientID
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
