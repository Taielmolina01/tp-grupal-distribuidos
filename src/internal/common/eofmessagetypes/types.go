package eofmessagetypes

type EofRingMessage struct {
	ActualAmount   uint32
	RealAmount     uint32
	ClientId       int
	CoordinatorId  uint32
	FilteredAmount uint32
	Seq            uint64
}

type EofMessageCommit struct {
	CoordinatorId  uint32
	ClientID       int
	Hops           int
	FilteredAmount uint32
	Seq            uint64
}
