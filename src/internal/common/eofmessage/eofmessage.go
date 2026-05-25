package eofmessage

type EofMessage struct {
	TotalMessages uint32
	ClientID      int
	QueryID       uint8
}

type AggregationEofMessage struct {
	ClientID      int
	AggregationID int
}
