package transfer

import "time"

type Transfer struct {
	Timestamp         time.Time
	FromBank          string
	FromBankAccount   string
	ToBank            string
	ToBankAccount     string
	AmountReceived    float
	ReceivingCurrency string
	AmountPaid        float
	PaymentCurrency   string
	PaymentFormat     string
	IsLaundering      bool
}
