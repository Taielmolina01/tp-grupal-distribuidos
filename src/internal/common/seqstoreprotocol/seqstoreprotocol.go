package seqstoreprotocol

import (
	"encoding/binary"
	"fmt"
)

const requestSize = 13

func EncodeRequest(clientID int, seq uint64, isEOF bool) []byte {
	buf := make([]byte, requestSize)
	binary.BigEndian.PutUint32(buf[:4], uint32(clientID))
	binary.BigEndian.PutUint64(buf[4:12], seq)
	if isEOF {
		buf[12] = 1
	}
	return buf
}

func DecodeRequest(buf []byte) (clientID int, seq uint64, isEOF bool, err error) {
	if len(buf) < requestSize {
		return 0, 0, false, fmt.Errorf("seqstoreprotocol: request too short (%d bytes)", len(buf))
	}
	return int(binary.BigEndian.Uint32(buf[:4])), binary.BigEndian.Uint64(buf[4:12]), buf[12] != 0, nil
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
