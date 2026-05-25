package shard

import (
	"fmt"
	"hash/fnv"
)

type Hasher struct {
	totalShards int
}

func New(totalShards int) Hasher {
	return Hasher{totalShards: totalShards}
}

func (h Hasher) ShardFor(clientID int, keys ...string) int {
	hash := fnv.New32a()
	fmt.Fprintf(hash, "%d", clientID)
	for _, k := range keys {
		fmt.Fprintf(hash, "\x00%s", k)
	}
	return int(hash.Sum32() % uint32(h.totalShards))
}

// Esto dsps lo volamos, lo dejo para no romper las implementaciones actuales
func CalculateIndexForShard(clientID int, fromBank string, amount int) int {
	bytes := []byte(fmt.Sprintf("%v%d", fromBank, clientID))
	hash := fnv.New64a()
	hash.Write(bytes)
	return int(hash.Sum64() % uint64(amount))
}
