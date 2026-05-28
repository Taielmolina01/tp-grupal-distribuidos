package currencyconversion

type CurrencyConversion struct {
	FromCurrency   string  `json:"from_currency"`
	ToCurrency     string  `json:"to_currency"`
	ConversionRate float64 `json:"conversion_rate"`
}
