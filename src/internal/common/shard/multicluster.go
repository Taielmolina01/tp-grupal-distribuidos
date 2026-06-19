package shard

import "fmt"

type ClusterConfig struct {
	Prefix    string
	NodeCount int
}

type MultiClusterHasher struct {
	clusters []clusterEntry
}

type clusterEntry struct {
	prefix string
	hasher Hasher
}

func NewMultiCluster(clusters []ClusterConfig) MultiClusterHasher {
	entries := make([]clusterEntry, len(clusters))
	for i, c := range clusters {
		entries[i] = clusterEntry{
			prefix: c.Prefix,
			hasher: New(c.NodeCount),
		}
	}
	return MultiClusterHasher{clusters: entries}
}

func (m MultiClusterHasher) RoutingKeysFor(clientID int, keys ...string) []string {
	rks := make([]string, len(m.clusters))
	for i, c := range m.clusters {
		rks[i] = fmt.Sprintf("%s_shard-%d", c.prefix, c.hasher.ShardFor(clientID, keys...))
	}
	return rks
}
