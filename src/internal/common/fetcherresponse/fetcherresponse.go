package fetcherresponse

type FetcherResponse struct {
	Date  string  `json:"date"`
	Quote string  `json:"quote"`
	Rate  float32 `json:"rate"`
}
