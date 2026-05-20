package account

type Account struct {
	BankName      string `json:"bank_name"`
	BankId        string `json:"bank_id"`
	AccountNumber string `json:"account_number"`
	EntityId      string `json:"entity_id"`
	EntityName    string `json:"entity_name"`
}

// Capaz sobren comparaciones idk
func (account Account) Equals(other Account) bool {
	return account.BankName == other.BankName &&
		account.BankId == other.BankId &&
		account.AccountNumber == other.AccountNumber &&
		account.EntityId == other.EntityId &&
		account.EntityName == other.EntityName
}

type AccountChain struct {
	// A -> B -> C
	A AccountIdentifier `json:"A"`
	B AccountIdentifier `json:"B"`
	C AccountIdentifier `json:"C"`
}

type AccountIdentifier struct {
	BankID        string `json:"bank_id"`
	AccountNumber string `json:"account_number"`
}

type Bank struct {
	ID   string `json:"bank_id"`
	Name string `json:"bank_name"`
}
