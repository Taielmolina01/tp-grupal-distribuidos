package fetcherresponse

type FetcherResponse struct {
	Date  string  `json:"date"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}
