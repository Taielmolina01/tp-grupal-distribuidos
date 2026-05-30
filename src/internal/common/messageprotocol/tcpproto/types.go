package tcpproto

type MsgType uint8

const (
	AccountBatch MsgType = iota + 1
	TransBatch
	EndOfRecords
	ResultBatch
	QueryEOF
)
