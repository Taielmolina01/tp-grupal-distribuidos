package transfer

import "time"

type Transfer struct {
	Timestamp         time.Time
	FromBank          string
	FromBankAccount   string
	ToBank            string
	ToBankAccount     string
	AmountReceived    float64
	ReceivingCurrency string
	AmountPaid        float64
	PaymentCurrency   string
	PaymentFormat     string
	IsLaundering      bool
}

type TransferAfterCurrency struct {
	Timestamp       time.Time
	FromBank        string
	FromBankAccount string
	ToBank          string
	ToBankAccount   string
	AmountPaid      float64
	PaymentFormat   string
}

type TransferForQ2 struct {
	FromBank        string
	FromBankAccount string
	AmountPaid      float64
}

func ProjectForQ2(t TransferAfterCurrency) TransferForQ2 {
	return TransferForQ2{
		FromBank:        t.FromBank,
		FromBankAccount: t.FromBankAccount,
		AmountPaid:      t.AmountPaid,
	}
}

type TransferForQ3Avg struct {
	PaymentFormat string
	AmountPaid    float64
}

func ProjectForQ3Avg(t TransferAfterCurrency) TransferForQ3Avg {
	return TransferForQ3Avg{
		PaymentFormat: t.PaymentFormat,
		AmountPaid:    t.AmountPaid,
	}
}

type TransferForQ3Filter struct {
	PaymentFormat   string
	AmountPaid      float64
	FromBank        string
	FromBankAccount string
}

func ProjectForQ3Filter(t TransferAfterCurrency) TransferForQ3Filter {
	return TransferForQ3Filter{
		PaymentFormat:   t.PaymentFormat,
		AmountPaid:      t.AmountPaid,
		FromBank:        t.FromBank,
		FromBankAccount: t.FromBankAccount,
	}
}

type TransferForQ4 struct {
	FromBank        string
	FromBankAccount string
	ToBank          string
	ToBankAccount   string
}

func ProjectForQ4(t TransferAfterCurrency) TransferForQ4 {
	return TransferForQ4{
		FromBank:        t.FromBank,
		FromBankAccount: t.FromBankAccount,
		ToBank:          t.ToBank,
		ToBankAccount:   t.ToBankAccount,
	}
}

func ProjectAfterCurrency(t Transfer) TransferAfterCurrency {
	return TransferAfterCurrency{
		Timestamp:       t.Timestamp,
		FromBank:        t.FromBank,
		FromBankAccount: t.FromBankAccount,
		ToBank:          t.ToBank,
		ToBankAccount:   t.ToBankAccount,
		AmountPaid:      t.AmountPaid,
		PaymentFormat:   t.PaymentFormat,
	}
}

type TransferForQ5Filter struct {
	Timestamp  time.Time
	Currency   string
	AmountPaid float64
}

func ProjectForQ5Filter(t Transfer) TransferForQ5Filter {
	return TransferForQ5Filter{
		Timestamp:  t.Timestamp,
		Currency:   t.PaymentCurrency,
		AmountPaid: t.AmountPaid,
	}
}

type FinalTransferForQ5 struct{}

func ProjectForQ5Final() FinalTransferForQ5 {
	return FinalTransferForQ5{}
}

type SplittedTransfer struct {
	Transfer   TransferForQ4
	IsLeftPart bool
}

type SumByMethod struct {
	Sum    float64
	Amount int
	Method string
}

type AvgByMethod struct {
	Avg    float64
	Method string
}
