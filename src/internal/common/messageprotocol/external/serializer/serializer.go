package serializer

import (
	"encoding/binary"
	"math"
)

const UINT8_SIZE uint32 = 1
const UINT16_SIZE uint32 = 2
const UINT32_SIZE uint32 = 4
const UINT64_SIZE uint32 = 8
const BOOL_SIZE uint32 = 1

func SerializeUint8(value uint8) []byte {
	return []byte{value}
}

func DeserializeUint8(bytes []byte) uint8 {
	return bytes[0]
}

func SerializeUint16(value uint16) []byte {
	data := make([]byte, UINT16_SIZE)
	binary.BigEndian.PutUint16(data, value)
	return data
}

func DeserializeUint16(bytes []byte) uint16 {
	return binary.BigEndian.Uint16(bytes)
}

func SerializeUint32(value uint32) []byte {
	data := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func DeserializeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}

func SerializeString(value string) []byte {
	data := []byte(value)
	return append(SerializeUint16(uint16(len(data))), data...)
}

func DeserializeString(bytes []byte) string {
	return string(bytes)
}

func SerializeFloat64(value float64) []byte {
	data := make([]byte, UINT64_SIZE)
	binary.BigEndian.PutUint64(data, math.Float64bits(value))
	return data
}

func DeserializeFloat64(bytes []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(bytes))
}

func SerializeBool(value bool) []byte {
	if value {
		return []byte{1}
	}
	return []byte{0}
}

func DeserializeBool(bytes []byte) bool {
	return bytes[0] == 1
}
