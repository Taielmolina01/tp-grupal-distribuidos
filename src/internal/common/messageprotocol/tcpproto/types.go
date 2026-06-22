package tcpproto

type MsgType uint8

const (
	AccountBatch MsgType = iota + 1
	TransBatch
	EndOfRecords
	ResultBatch
	QueryEOF
	Hello
	Welcome
)

type Phase uint8

const (
	PhaseAccounts Phase = iota + 1
	PhaseTransfers
	PhaseResults
)
