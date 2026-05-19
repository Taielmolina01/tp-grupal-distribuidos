package queryresult

import "encoding/json"

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

func (r Query1Result) Serialize() ([]byte, error) { return json.Marshal(r) }
func (r Query2Result) Serialize() ([]byte, error) { return json.Marshal(r) }
func (r Query3Result) Serialize() ([]byte, error) { return json.Marshal(r) }
func (r Query4Result) Serialize() ([]byte, error) { return json.Marshal(r) }
func (r Query5Result) Serialize() ([]byte, error) { return json.Marshal(r) }

func DeserializeQuery1(b []byte) (Query1Result, error) {
	var r Query1Result
	err := json.Unmarshal(b, &r)
	return r, err
}

func DeserializeQuery2(b []byte) (Query2Result, error) {
	var r Query2Result
	err := json.Unmarshal(b, &r)
	return r, err
}

func DeserializeQuery3(b []byte) (Query3Result, error) {
	var r Query3Result
	err := json.Unmarshal(b, &r)
	return r, err
}

func DeserializeQuery4(b []byte) (Query4Result, error) {
	var r Query4Result
	err := json.Unmarshal(b, &r)
	return r, err
}

func DeserializeQuery5(b []byte) (Query5Result, error) {
	var r Query5Result
	err := json.Unmarshal(b, &r)
	return r, err
}
