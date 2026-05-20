package currencyconversion

type CurrencyConversion struct {
	FromCurrency   string  `json:"from_currency"`
	ToCurrency     string  `json:"to_currency"`
	ConversionRate float32 `json:"conversion_rate"`
}
