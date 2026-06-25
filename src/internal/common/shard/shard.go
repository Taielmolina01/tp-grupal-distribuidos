package shard

import (
	"fmt"
	"hash/fnv"
	"log/slog"
)

type Hasher struct {
	totalShards int
}

func New(totalShards int) Hasher {
	return Hasher{totalShards: totalShards}
}

func (h Hasher) ShardFor(clientID int, keys ...string) int {
	hash := fnv.New32a()
	if _, err := fmt.Fprintf(hash, "%d", clientID); err != nil {
		slog.Error("failed to write clientID to hash: %v", err)
	}
	for _, k := range keys {
		if _, err := fmt.Fprintf(hash, ",%s", k); err != nil {
			slog.Error("failed to write key to hash: %v", err)
		}
	}
	return int(hash.Sum32() % uint32(h.totalShards))
}

func (h Hasher) TotalShards() int { return h.totalShards }

// Esto dsps lo volamos, lo dejo para no romper las implementaciones actuales
func CalculateIndexForShard(clientID int, fromBank string, amount int) int {
	bytes := []byte(fmt.Sprintf("%v%d", fromBank, clientID))
	hash := fnv.New64a()
	hash.Write(bytes)
	return int(hash.Sum64() % uint64(amount))
}
