// Package shard routes jobs across N Postgres shards behind store.Store,
// so the HTTP contract stays unchanged. Routing is hash-modulo:
//   - jobs by hash(jobID) % N (uniform)
//   - keyed jobs by hash(idempotencyKey) % N (replay affinity)
//   - workers by hash(workerID) % N
//
// Resharding (changing N) is offline: hashes move. Poll merges per-shard
// top-1 candidates, so cross-shard priority order is approximate.
package shard

import (
	"hash/fnv"
	"os"
	"strings"

	"github.com/google/uuid"
)

// Router maps IDs and keys to shard indexes in [0, N).
type Router struct {
	n uint32
}

// NewRouter builds a Router over n shards. Panics for n < 1.
func NewRouter(n int) Router {
	if n < 1 {
		panic("shard: need at least 1 shard")
	}
	return Router{n: uint32(n)}
}

// N returns the shard count.
func (r Router) N() int { return int(r.n) }

func (r Router) shardForBytes(b []byte) int {
	h := fnv.New32a()
	_, _ = h.Write(b)
	return int(h.Sum32() % r.n)
}

// ShardForID routes a job (or any UUID-keyed row) to its home shard.
func (r Router) ShardForID(id uuid.UUID) int {
	return r.shardForBytes(id[:])
}

// ShardForKey routes an idempotency key to its shard.
func (r Router) ShardForKey(key string) int {
	return r.shardForBytes([]byte(key))
}

// ShardForWorker routes a worker row to its home shard.
func (r Router) ShardForWorker(id string) int {
	return r.shardForBytes([]byte(id))
}

// DSNs returns every shard DSN: SHARD_DATABASE_URLS (comma-separated)
// or the single DATABASE_URL. Shared by the coordinator and migration tool.
func DSNs() []string {
	return DSNsFrom(os.Getenv("SHARD_DATABASE_URLS"), os.Getenv("DATABASE_URL"))
}

// DSNsFrom splits shard DSNs, falling back to the single DSN.
func DSNsFrom(shards, single string) []string {
	if shards != "" {
		var out []string
		for _, dsn := range strings.Split(shards, ",") {
			if dsn = strings.TrimSpace(dsn); dsn != "" {
				out = append(out, dsn)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{single}
}
