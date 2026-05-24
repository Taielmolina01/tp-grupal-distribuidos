package fetcherresponse

type FetcherResponse struct {
	Date string  `json:"date"`
	Base string  `json:"base"`
	Rate float64 `json:"rate"`
}
