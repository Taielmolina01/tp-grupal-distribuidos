package shard

import (
	"fmt"
	"hash/fnv"
)

func convertToBytes(FromBank string, clientID int) []byte {
	return []byte(fmt.Sprintf("%v%d", FromBank, clientID))
}

func CalculateIndexForShard(clientID int, FromBank string, amount int) int {
	bytes := convertToBytes(FromBank, clientID)
	hash := fnv.New64a()
	hash.Write(bytes)
	return int(hash.Sum64() % uint64(amount))
}
