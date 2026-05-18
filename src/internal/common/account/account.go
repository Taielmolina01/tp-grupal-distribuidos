package account

type Account struct {
	BankName      string
	BankId        string
	AccountNumber string
	EntityId      string
	EntityName    string
}

// Capaz sobren comparaciones idk
func (account Account) Equals(other Account) bool {
	return account.BankName == other.BankName &&
		account.BankId == other.BankId &&
		account.AccountNumber == other.AccountNumber &&
		account.EntityId == other.EntityId &&
		account.EntityName == other.EntityName
}
