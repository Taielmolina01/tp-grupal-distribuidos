package records

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const minSizeTime = serializer.INT64_SIZE

const MinSizeTransferForQ4 = 4 * serializer.UINT16_SIZE

func MarshalTransferForQ4(w *wire.Writer, t *transfer.TransferForQ4) {
	w.String(t.FromBank)
	w.String(t.FromBankAccount)
	w.String(t.ToBank)
	w.String(t.ToBankAccount)
}

func UnmarshalTransferForQ4(r *wire.Reader) transfer.TransferForQ4 {
	return transfer.TransferForQ4{
		FromBank:        r.String(),
		FromBankAccount: r.String(),
		ToBank:          r.String(),
		ToBankAccount:   r.String(),
	}
}

const MinSizeTransfer = minSizeTime + 7*serializer.UINT16_SIZE + 2*serializer.UINT64_SIZE + serializer.BOOL_SIZE

func MarshalTransfer(w *wire.Writer, t *transfer.Transfer) {
	w.Time(t.Timestamp)
	w.String(t.FromBank)
	w.String(t.FromBankAccount)
	w.String(t.ToBank)
	w.String(t.ToBankAccount)
	w.Float64(t.AmountReceived)
	w.String(t.ReceivingCurrency)
	w.Float64(t.AmountPaid)
	w.String(t.PaymentCurrency)
	w.String(t.PaymentFormat)
	w.Bool(t.IsLaundering)
}

func UnmarshalTransfer(r *wire.Reader) transfer.Transfer {
	return transfer.Transfer{
		Timestamp:         r.Time(),
		FromBank:          r.String(),
		FromBankAccount:   r.String(),
		ToBank:            r.String(),
		ToBankAccount:     r.String(),
		AmountReceived:    r.Float64(),
		ReceivingCurrency: r.String(),
		AmountPaid:        r.Float64(),
		PaymentCurrency:   r.String(),
		PaymentFormat:     r.String(),
		IsLaundering:      r.Bool(),
	}
}

const MinSizeTransferAfterCurrency = minSizeTime + 5*serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalTransferAfterCurrency(w *wire.Writer, t *transfer.TransferAfterCurrency) {
	w.Time(t.Timestamp)
	w.String(t.FromBank)
	w.String(t.FromBankAccount)
	w.String(t.ToBank)
	w.String(t.ToBankAccount)
	w.Float64(t.AmountPaid)
	w.String(t.PaymentFormat)
}

func UnmarshalTransferAfterCurrency(r *wire.Reader) transfer.TransferAfterCurrency {
	return transfer.TransferAfterCurrency{
		Timestamp:       r.Time(),
		FromBank:        r.String(),
		FromBankAccount: r.String(),
		ToBank:          r.String(),
		ToBankAccount:   r.String(),
		AmountPaid:      r.Float64(),
		PaymentFormat:   r.String(),
	}
}

const MinSizeTransferForQ5Filter = minSizeTime + serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalTransferForQ5Filter(w *wire.Writer, t *transfer.TransferForQ5Filter) {
	w.Time(t.Timestamp)
	w.String(t.Currency)
	w.Float64(t.AmountPaid)
}

func UnmarshalTransferForQ5Filter(r *wire.Reader) transfer.TransferForQ5Filter {
	return transfer.TransferForQ5Filter{
		Timestamp:  r.Time(),
		Currency:   r.String(),
		AmountPaid: r.Float64(),
	}
}

const MinSizeQuery1Result = 4*serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalQuery1Result(w *wire.Writer, q *queryresult.Query1Result) {
	w.String(q.FromBank)
	w.String(q.FromAccount)
	w.String(q.ToBank)
	w.String(q.ToAccount)
	w.Float64(q.Amount)
}

func UnmarshalQuery1Result(r *wire.Reader) queryresult.Query1Result {
	return queryresult.Query1Result{
		FromBank:    r.String(),
		FromAccount: r.String(),
		ToBank:      r.String(),
		ToAccount:   r.String(),
		Amount:      r.Float64(),
	}
}

const MinSizeTransferForQ2 = 2*serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalTransferForQ2(w *wire.Writer, t *transfer.TransferForQ2) {
	w.String(t.FromBank)
	w.String(t.FromBankAccount)
	w.Float64(t.AmountPaid)
}

func UnmarshalTransferForQ2(r *wire.Reader) transfer.TransferForQ2 {
	return transfer.TransferForQ2{
		FromBank:        r.String(),
		FromBankAccount: r.String(),
		AmountPaid:      r.Float64(),
	}
}

var (
	TransferCodec = wire.Codec[transfer.Transfer]{
		Marshal: MarshalTransfer, Unmarshal: UnmarshalTransfer, MinSize: MinSizeTransfer,
	}
	TransferForQ2Codec = wire.Codec[transfer.TransferForQ2]{
		Marshal: MarshalTransferForQ2, Unmarshal: UnmarshalTransferForQ2, MinSize: MinSizeTransferForQ2,
	}
	TransferAfterCurrencyCodec = wire.Codec[transfer.TransferAfterCurrency]{
		Marshal: MarshalTransferAfterCurrency, Unmarshal: UnmarshalTransferAfterCurrency, MinSize: MinSizeTransferAfterCurrency,
	}
	TransferForQ5FilterCodec = wire.Codec[transfer.TransferForQ5Filter]{
		Marshal: MarshalTransferForQ5Filter, Unmarshal: UnmarshalTransferForQ5Filter, MinSize: MinSizeTransferForQ5Filter,
	}
	Query1ResultCodec = wire.Codec[queryresult.Query1Result]{
		Marshal: MarshalQuery1Result, Unmarshal: UnmarshalQuery1Result, MinSize: MinSizeQuery1Result,
	}
	Query2ResultCodec = wire.Codec[queryresult.Query2Result]{
		Marshal: MarshalQuery2Result, Unmarshal: UnmarshalQuery2Result, MinSize: MinSizeQuery2Result,
	}
	Query3ResultCodec = wire.Codec[queryresult.Query3Result]{
		Marshal: MarshalQuery3Result, Unmarshal: UnmarshalQuery3Result, MinSize: MinSizeQuery3Result,
	}
	Query4ResultCodec = wire.Codec[queryresult.Query4Result]{
		Marshal: MarshalQuery4Result, Unmarshal: UnmarshalQuery4Result, MinSize: MinSizeQuery4Result,
	}
	Query5ResultCodec = wire.Codec[queryresult.Query5Result]{
		Marshal: MarshalQuery5Result, Unmarshal: UnmarshalQuery5Result, MinSize: MinSizeQuery5Result,
	}
	AccountCodec = wire.Codec[account.Account]{
		Marshal: MarshalAccount, Unmarshal: UnmarshalAccount, MinSize: MinSizeAccount,
	}
)

const MinSizeAccount = 5 * serializer.UINT16_SIZE

func MarshalAccount(w *wire.Writer, a *account.Account) {
	w.String(a.BankName)
	w.String(a.BankId)
	w.String(a.AccountNumber)
	w.String(a.EntityId)
	w.String(a.EntityName)
}

func UnmarshalAccount(r *wire.Reader) account.Account {
	return account.Account{
		BankName:      r.String(),
		BankId:        r.String(),
		AccountNumber: r.String(),
		EntityId:      r.String(),
		EntityName:    r.String(),
	}
}

const MinSizeQuery2Result = 3*serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalQuery2Result(w *wire.Writer, q *queryresult.Query2Result) {
	w.String(q.BankName)
	w.String(q.FromBank)
	w.String(q.FromAccount)
	w.Float64(q.Amount)
}

func UnmarshalQuery2Result(r *wire.Reader) queryresult.Query2Result {
	return queryresult.Query2Result{
		BankName:    r.String(),
		FromBank:    r.String(),
		FromAccount: r.String(),
		Amount:      r.Float64(),
	}
}

const MinSizeQuery3Result = 3*serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalQuery3Result(w *wire.Writer, q *queryresult.Query3Result) {
	w.String(q.FromBank)
	w.String(q.FromAccount)
	w.String(q.PaymentFormat)
	w.Float64(q.Amount)
}

func UnmarshalQuery3Result(r *wire.Reader) queryresult.Query3Result {
	return queryresult.Query3Result{
		FromBank:      r.String(),
		FromAccount:   r.String(),
		PaymentFormat: r.String(),
		Amount:        r.Float64(),
	}
}

const MinSizeQuery4Result = 2 * serializer.UINT16_SIZE

func MarshalQuery4Result(w *wire.Writer, q *queryresult.Query4Result) {
	w.String(q.BankId)
	w.String(q.AccountNumber)
}

func UnmarshalQuery4Result(r *wire.Reader) queryresult.Query4Result {
	return queryresult.Query4Result{
		BankId:        r.String(),
		AccountNumber: r.String(),
	}
}

const MinSizeQuery5Result = serializer.UINT32_SIZE

func MarshalQuery5Result(w *wire.Writer, q *queryresult.Query5Result) {
	w.Uint32(q.Qty)
}

func UnmarshalQuery5Result(r *wire.Reader) queryresult.Query5Result {
	return queryresult.Query5Result{Qty: r.Uint32()}
}

const MinSizeAccountIdentifier = 2 * serializer.UINT16_SIZE

func MarshalAccountIdentifier(w *wire.Writer, a *account.AccountIdentifier) {
	w.String(a.BankID)
	w.String(a.AccountNumber)
}

func UnmarshalAccountIdentifier(r *wire.Reader) account.AccountIdentifier {
	return account.AccountIdentifier{
		BankID:        r.String(),
		AccountNumber: r.String(),
	}
}

var AccountIdentifierCodec = wire.Codec[account.AccountIdentifier]{
	Marshal: MarshalAccountIdentifier, Unmarshal: UnmarshalAccountIdentifier, MinSize: MinSizeAccountIdentifier,
}

const MinSizeFetcherResponse = 2*serializer.UINT16_SIZE + serializer.UINT64_SIZE

func MarshalFetcherResponse(w *wire.Writer, f *fetcherresponse.FetcherResponse) {
	w.String(f.Date)
	w.String(f.Quote)
	w.Float64(f.Rate)
}

func UnmarshalFetcherResponse(r *wire.Reader) fetcherresponse.FetcherResponse {
	return fetcherresponse.FetcherResponse{
		Date:  r.String(),
		Quote: r.String(),
		Rate:  r.Float64(),
	}
}

var FetcherResponseCodec = wire.Codec[fetcherresponse.FetcherResponse]{
	Marshal: MarshalFetcherResponse, Unmarshal: UnmarshalFetcherResponse, MinSize: MinSizeFetcherResponse,
}

var FinalTransferForQ5Codec = wire.Codec[transfer.FinalTransferForQ5]{
	Marshal:   func(*wire.Writer, *transfer.FinalTransferForQ5) {},
	Unmarshal: func(*wire.Reader) transfer.FinalTransferForQ5 { return transfer.FinalTransferForQ5{} },
	MinSize:   0,
}
