package seqstorenode

import (
	"encoding/binary"
	"fmt"
)

const requestSize = 8

func EncodeRequest(seq uint64) []byte {
	buf := make([]byte, requestSize)
	binary.BigEndian.PutUint64(buf, seq)
	return buf
}

func DecodeRequest(buf []byte) (uint64, error) {
	if len(buf) < requestSize {
		return 0, fmt.Errorf("seqstorenode: request too short (%d bytes)", len(buf))
	}
	return binary.BigEndian.Uint64(buf[:8]), nil
}

func EncodeResponse(free bool) []byte {
	if free {
		return []byte{1}
	}
	return []byte{0}
}

func DecodeResponse(buf []byte) (bool, error) {
	if len(buf) < 1 {
		return false, fmt.Errorf("seqstorenode: response too short")
	}
	return buf[0] != 0, nil
}
