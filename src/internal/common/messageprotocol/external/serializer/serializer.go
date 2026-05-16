package serializer

import (
	"encoding/binary"
	"math"
)

const UINT32_SIZE uint32 = 4
const BOOL_SIZE uint32 = 1

func appendLenght(data []byte) []byte {
	length := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	return append(length, data...)
}

func SerializeString(value string) []byte {
	data := []byte(value)
	return appendLenght(data)
}

func DeserializeString(bytes []byte) string {
	return string(bytes[:])
}

func SerializeUint32(value uint32) []byte {
	data := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func DeserializeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}

func SerializeFloat32(value float32) []byte {
	data := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(data, math.Float32bits(value))
	return data
}

func DeserializeFloat32(bytes []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(bytes))
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
