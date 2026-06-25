package join

import (
	"log/slog"
	"sort"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

func (j *Join) processMessage(msg newmiddleware.Message, isLeft bool) (int, bool) {
	if isLeft {
		input, err := batch.Read(msg.Body, records.TransferForQ2Codec)
		if err != nil {
			slog.Error("decode left failed", "err", err)
			return 0, false
		}
		state := j.states.For(input.ClientID)
		tracker := state.leftTracker
		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate left", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
			return 0, false
		}
		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			j.accumulateLeft(input.Records, state)
		}
		tracker.Claim(int(input.SenderID), input.Seq)
		return input.ClientID, true
	}

	input, err := batch.Read(msg.Body, records.AccountCodec)
	if err != nil {
		slog.Error("decode right failed", "err", err)
		return 0, false
	}
	state := j.states.For(input.ClientID)
	tracker := state.rightTracker
	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate right", "clientID", input.ClientID, "senderID", input.SenderID, "seq", input.Seq)
		return 0, false
	}
	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
	} else {
		tracker.RegisterBatch(int(input.SenderID))
		j.accumulateRight(input.Records, state)
	}
	tracker.Claim(int(input.SenderID), input.Seq)
	return input.ClientID, true
}

func (j *Join) accumulateLeft(recs []transfer.TransferForQ2, state *clientState) {
	for i := range recs {
		t := recs[i]
		key := normalizer.NormalizeBankID(t.FromBank)
		existing, ok := state.leftBuffer[key]
		if !ok {
			state.leftBuffer[key] = t
		} else {
			state.leftBuffer[key] = maxTransfer(existing, t)
		}
	}
}

func (j *Join) accumulateRight(recs []account.Account, state *clientState) {
	for i := range recs {
		a := recs[i]
		state.rightBuffer[normalizer.NormalizeBankID(a.BankId)] = a
	}
}

func maxTransfer(t1, t2 transfer.TransferForQ2) transfer.TransferForQ2 {
	if t1.AmountPaid > t2.AmountPaid {
		return t1
	} else if t1.AmountPaid == t2.AmountPaid {
		if t1.FromBankAccount > t2.FromBankAccount {
			return t1
		}
		return t2
	}
	return t2
}

func (j *Join) maybeEmit(clientID int, state *clientState) error {
	if state.emitted {
		return nil
	}
	if !state.leftTracker.IsComplete(j.leftEofsExpected) || !state.rightTracker.IsComplete(j.rightEofsExpected) {
		return nil
	}
	if err := j.emit(clientID, state); err != nil {
		return err
	}
	state.emitted = true
	return nil
}

func (j *Join) emit(clientID int, state *clientState) error {
	keys := make([]string, 0, len(state.leftBuffer))
	for k := range state.leftBuffer {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	results := make([]queryresult.Query2Result, 0, len(keys))
	for _, key := range keys {
		t := state.leftBuffer[key]
		acc, ok := state.rightBuffer[key]
		if !ok {
			continue
		}
		results = append(results, queryresult.Query2Result{
			BankName:    acc.BankName,
			FromBank:    t.FromBank,
			FromAccount: t.FromBankAccount,
			Amount:      t.AmountPaid,
		})
	}

	var seq uint64
	var total uint32
	if len(results) > 0 {
		body := batch.Write(clientID, j.queryID, uint8(j.id), seq, results, records.Query2ResultCodec)
		if err := j.output.Send(newmiddleware.Message{Body: body}); err != nil {
			return err
		}
		seq++
		total = 1
	}

	eofBody := batch.WriteEOF(clientID, j.queryID, uint8(j.id), seq, total)
	return j.output.Send(newmiddleware.Message{Body: eofBody})
}
