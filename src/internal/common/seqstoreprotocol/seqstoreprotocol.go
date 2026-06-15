package seqstoreprotocol

import (
	"encoding/binary"
	"fmt"
)

const (
	TypeClaim  byte = 0
	TypeRemove byte = 1
)

const claimRequestSize = 13
const removeRequestSize = 5

func DecodeType(buf []byte) (byte, error) {
	if len(buf) < 1 {
		return 0, fmt.Errorf("seqstoreprotocol: empty message")
	}
	return buf[0], nil
}

func EncodeClaimRequest(clientID int, seq uint64) []byte {
	buf := make([]byte, claimRequestSize)
	buf[0] = TypeClaim
	binary.BigEndian.PutUint32(buf[1:5], uint32(clientID))
	binary.BigEndian.PutUint64(buf[5:13], seq)
	return buf
}

func DecodeClaimRequest(buf []byte) (clientID int, seq uint64, err error) {
	if len(buf) < claimRequestSize {
		return 0, 0, fmt.Errorf("seqstoreprotocol: claim request too short (%d bytes)", len(buf))
	}
	return int(binary.BigEndian.Uint32(buf[1:5])), binary.BigEndian.Uint64(buf[5:13]), nil
}

func EncodeRemoveRequest(clientID int) []byte {
	buf := make([]byte, removeRequestSize)
	buf[0] = TypeRemove
	binary.BigEndian.PutUint32(buf[1:5], uint32(clientID))
	return buf
}

func DecodeRemoveRequest(buf []byte) (clientID int, err error) {
	if len(buf) < removeRequestSize {
		return 0, fmt.Errorf("seqstoreprotocol: remove request too short (%d bytes)", len(buf))
	}
	return int(binary.BigEndian.Uint32(buf[1:5])), nil
}

func EncodeResponse(free bool) []byte {
	if free {
		return []byte{1}
	}
	return []byte{0}
}

func DecodeResponse(buf []byte) (bool, error) {
	if len(buf) < 1 {
		return false, fmt.Errorf("seqstoreprotocol: response too short")
	}
	return buf[0] != 0, nil
}
