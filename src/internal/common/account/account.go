package account

type Account struct {
	BankName      string
	BankId        string
	AccountNumber string
	EntityId      string
	EntityName    string
}

type AccountChain struct {
	// Left -> Middle -> Right
	Left   AccountIdentifier
	Middle AccountIdentifier
	Right  AccountIdentifier
}

type AccountPair struct {
	// Left -> Right
	Left  AccountIdentifier
	Right AccountIdentifier
}

type AccountIdentifier struct {
	BankID        string
	AccountNumber string
}

func (a AccountIdentifier) GetKey() string {
	return a.BankID + "_" + a.AccountNumber
}
