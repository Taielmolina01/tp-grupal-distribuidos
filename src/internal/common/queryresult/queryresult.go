package queryresult

type Query1Result struct {
	FromBank    string
	FromAccount string
	ToBank      string
	ToAccount   string
	Amount      float32
}

type Query2Result struct {
	BankName    string
	FromBank    string
	FromAccount string
	Amount      float32
}

type Query3Result struct {
	FromBank    string
	FromAccount string
	Amount      float32
}

type Query4Result struct {
	BankId    string
	AccountId string
}

type Query5Result struct {
	Qty uint32
}

type BatchResults struct {
	Query1 []Query1Result
	Query2 []Query2Result
	Query3 []Query3Result
	Query4 []Query4Result
	Query5 []Query5Result
}
