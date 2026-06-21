package newmiddleware

import "tp-grupal-distribuidos/internal/common/shard"

type ShardedCluster struct {
	Middleware Middleware
	Hasher     shard.Hasher
}
