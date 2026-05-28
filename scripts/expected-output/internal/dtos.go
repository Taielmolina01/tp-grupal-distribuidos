package internal

type Transaction struct {
	Timestamp         string
	FromBank          string
	FromAccount       string
	ToBank            string
	ToAccount         string
	AmountPaid        float64
	ReceivingCurrency string
	PaymentCurrency   string
	PaymentFormat     string
}

type Account struct {
	BankName string
	BankID   string
}

type BankAcc struct {
	Bank    string
	Account string
}
