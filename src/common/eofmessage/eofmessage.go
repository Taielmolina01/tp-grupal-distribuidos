package eofmessage

type EofMessage struct {
	TotalMessages uint32
	ClientID      int
}

type AggregationEofMessage struct {
	ClientID      int
	AggregationID int
}
