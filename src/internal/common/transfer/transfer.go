package transfer

import (
	"encoding/json"
	"time"
)

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

func (t Transfer) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

func Deserialize(b []byte) (Transfer, error) {
	var t Transfer
	err := json.Unmarshal(b, &t)
	return t, err
}

func (t SplittedTransfer) SerializeSplitted() ([]byte, error) {
	return json.Marshal(t)
}

func DeserializeSplitted(b []byte) (SplittedTransfer, error) {
	var t SplittedTransfer
	err := json.Unmarshal(b, &t)
	return t, err
}

// Capaz sobren comparaciones idk
func (t Transfer) Equals(other Transfer) bool {
	return t.Timestamp.Equal(other.Timestamp) &&
		t.FromBank == other.FromBank &&
		t.FromBankAccount == other.FromBankAccount &&
		t.ToBank == other.ToBank &&
		t.ToBankAccount == other.ToBankAccount &&
		t.AmountReceived == other.AmountReceived &&
		t.ReceivingCurrency == other.ReceivingCurrency &&
		t.AmountPaid == other.AmountPaid &&
		t.PaymentCurrency == other.PaymentCurrency &&
		t.PaymentFormat == other.PaymentFormat &&
		t.IsLaundering == other.IsLaundering
}

type SplittedTransfer struct {
	Transfer   Transfer
	IsLeftPart bool
}

type SumByMethod struct {
	Sum    float32
	Amount int
	Method string
}

type AvgByMethod struct {
	Avg    float32
	Method string
}
