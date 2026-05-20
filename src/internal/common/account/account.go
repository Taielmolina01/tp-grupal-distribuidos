package account

import "encoding/json"

type Account struct {
	BankName      string
	BankId        string
	AccountNumber string
	EntityId      string
	EntityName    string
}

func (account Account) Serialize() ([]byte, error) {
	return json.Marshal(account)
}

func Deserialize(b []byte) (Account, error) {
	var a Account
	err := json.Unmarshal(b, &a)
	return a, err
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
	// a -> b -> c
}

type AccountIdentifier struct{}

type Bank struct {
	Name string
	Id   string
}
