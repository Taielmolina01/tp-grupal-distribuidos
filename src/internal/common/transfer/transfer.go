package transfer

import "time"

type Transfer struct {
	Timestamp         time.Time
	FromBank          string
	FromBankAccount   string
	ToBank            string
	ToBankAccount     string
	AmountReceived    float32
	ReceivingCurrency string
	AmountPaid        float32
	PaymentCurrency   string
	PaymentFormat     string
	IsLaundering      bool
}

type SplittedTransfer struct {
	Timestamp         time.Time
	FromBank          string
	FromBankAccount   string
	ToBank            string
	ToBankAccount     string
	AmountReceived    float32
	ReceivingCurrency string
	AmountPaid        float32
	PaymentCurrency   string
	PaymentFormat     string
	IsLaundering      bool
	IsLeftPart        bool
}
