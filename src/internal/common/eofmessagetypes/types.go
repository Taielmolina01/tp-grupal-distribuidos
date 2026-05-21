package eofmessagetypes

type EofRingMessage struct {
	ActualAmount   uint32
	RealAmount     uint32
	ClientId       int
	Leader         uint32
	FilteredAmount uint32
}

type EofMessageCommit struct {
	ClientID       int
	Hops           int
	FilteredAmount uint32
}
