package queryresult

import "tp-grupal-distribuidos/internal/common/messageprotocol/inner"

type Query1Result struct {
	FromBank    string  `json:"from_bank"`
	FromAccount string  `json:"from_account"`
	ToBank      string  `json:"to_bank"`
	ToAccount   string  `json:"to_account"`
	Amount      float32 `json:"amount"`
}

type Query2Result struct {
	BankName    string  `json:"bank_name"`
	FromBank    string  `json:"from_bank"`
	FromAccount string  `json:"from_account"`
	Amount      float32 `json:"amount"`
}

type Query3Result struct {
	FromBank    string  `json:"from_bank"`
	FromAccount string  `json:"from_account"`
	Amount      float32 `json:"amount"`
}

type Query4Result struct {
	BankId        string `json:"bank_id"`
	AccountNumber string `json:"account_number"`
}

type Query5Result struct {
	Qty uint32 `json:"qty"`
}

type BatchResults struct {
	Query1 []Query1Result
	Query2 []Query2Result
	Query3 []Query3Result
	Query4 []Query4Result
	Query5 []Query5Result
}

type QueryResult interface {
	GetQueryId() uint8
	GetHeaders() []string
}

func (q Query1Result) GetQueryId() uint8 { return inner.Query1ID }
func (q Query2Result) GetQueryId() uint8 { return inner.Query2ID }
func (q Query3Result) GetQueryId() uint8 { return inner.Query3ID }
func (q Query4Result) GetQueryId() uint8 { return inner.Query4ID }
func (q Query5Result) GetQueryId() uint8 { return inner.Query5ID }

func (q Query1Result) GetHeaders() []string {
	return []string{"From Bank", "Account", "To Bank", "Account.1", "Amount Paid"}
}

func (q Query2Result) GetHeaders() []string {
	return []string{"From Bank", "Account", "Bank Name", "Amount Paid"}
}

func (q Query3Result) GetHeaders() []string {
	return []string{"From Bank", "Account", "Payment Format", "Amount Paid"}
}

func (q Query4Result) GetHeaders() []string {
	return []string{"Bank", "Account"}
}

func (q Query5Result) GetHeaders() []string {
	return []string{"Size"}
}
