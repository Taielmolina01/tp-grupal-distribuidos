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
	BankId    string `json:"bank_id"`
	AccountId string `json:"account_id"`
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
	GetPayload() any
}

func (q Query1Result) GetQueryId() uint8 {
	return inner.Query1ID
}

func (q Query2Result) GetQueryId() uint8 {
	return inner.Query2ID
}

func (q Query3Result) GetQueryId() uint8 {
	return inner.Query3ID
}

func (q Query4Result) GetQueryId() uint8 {
	return inner.Query4ID
}

func (q Query5Result) GetQueryId() uint8 {
	return inner.Query5ID
}
