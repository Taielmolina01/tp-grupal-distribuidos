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

type SplittedTransfer struct {
	Transfer   Transfer `json:"transfer"`
	IsLeftPart bool     `json:"is_left_part"`
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
