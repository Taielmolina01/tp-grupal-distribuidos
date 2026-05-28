package transfer

import "time"

type Transfer struct {
	Timestamp         time.Time `json:"timestamp"`
	FromBank          string    `json:"from_bank"`
	FromBankAccount   string    `json:"from_bank_account"`
	ToBank            string    `json:"to_bank"`
	ToBankAccount     string    `json:"to_bank_account"`
	AmountReceived    float64   `json:"amount_received"`
	ReceivingCurrency string    `json:"receiving_currency"`
	AmountPaid        float64   `json:"amount_paid"`
	PaymentCurrency   string    `json:"payment_currency"`
	PaymentFormat     string    `json:"payment_format"`
	IsLaundering      bool      `json:"is_laundering"`
}

type TransferAfterCurrency struct {
	Timestamp       time.Time `json:"timestamp"`
	FromBank        string    `json:"from_bank"`
	FromBankAccount string    `json:"from_bank_account"`
	ToBank          string    `json:"to_bank"`
	ToBankAccount   string    `json:"to_bank_account"`
	AmountPaid      float64   `json:"amount_paid"`
	PaymentFormat   string    `json:"payment_format"`
}

type TransferForQ2 struct {
	FromBank        string  `json:"from_bank"`
	FromBankAccount string  `json:"from_bank_account"`
	AmountPaid      float64 `json:"amount_paid"`
}

func ProjectForQ2(t TransferAfterCurrency) TransferForQ2 {
	return TransferForQ2{
		FromBank:        t.FromBank,
		FromBankAccount: t.FromBankAccount,
		AmountPaid:      t.AmountPaid,
	}
}

type TransferForQ3Avg struct {
	PaymentFormat string  `json:"payment_format"`
	AmountPaid    float64 `json:"amount_paid"`
}

func ProjectForQ3Avg(t TransferAfterCurrency) TransferForQ3Avg {
	return TransferForQ3Avg{
		PaymentFormat: t.PaymentFormat,
		AmountPaid:    t.AmountPaid,
	}
}

type TransferForQ3Filter struct {
	PaymentFormat   string  `json:"payment_format"`
	AmountPaid      float64 `json:"amount_paid"`
	FromBank        string  `json:"from_bank"`
	FromBankAccount string  `json:"from_bank_account"`
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
	FromBank        string `json:"from_bank"`
	FromBankAccount string `json:"from_bank_account"`
	ToBank          string `json:"to_bank"`
	ToBankAccount   string `json:"to_bank_account"`
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

type SplittedTransfer struct {
	Transfer   TransferForQ4 `json:"transfer"`
	IsLeftPart bool          `json:"is_left_part"`
}

type SumByMethod struct {
	Sum    float64 `json:"sum"`
	Amount int     `json:"amount"`
	Method string  `json:"method"`
}

type AvgByMethod struct {
	Avg    float64 `json:"average"`
	Method string  `json:"method"`
}

// Capaz sobren comparaciones idk
func (t Transfer) Equals(other Transfer) bool {
	return t.Timestamp.Equal(other.Timestamp) &&
		t.FromBank == other.FromBank &&
		t.FromBankAccount == other.FromBankAccount &&
		t.ToBank == other.ToBank &&
		t.ToBankAccount == other.ToBankAccount &&
		t.AmountReceived == other.AmountReceived &&
		t.ReceivingCurrency == other.ReceivingCurrency &&
		t.AmountPaid == other.AmountPaid &&
		t.PaymentCurrency == other.PaymentCurrency &&
		t.PaymentFormat == other.PaymentFormat &&
		t.IsLaundering == other.IsLaundering
}
