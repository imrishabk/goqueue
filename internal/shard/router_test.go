package shard

import (
	"testing"

	"github.com/google/uuid"
)

func TestRouter_SingleShard(t *testing.T) {
	r := NewRouter(1)
	for i := 0; i < 50; i++ {
		if got := r.ShardForID(uuid.New()); got != 0 {
			t.Fatalf("N=1 must always route 0, got %d", got)
		}
		if got := r.ShardForKey("any-key"); got != 0 {
			t.Fatalf("N=1 key must route 0, got %d", got)
		}
	}
}

func TestRouter_Deterministic(t *testing.T) {
	r := NewRouter(4)
	id := uuid.New()
	a, b := r.ShardForID(id), r.ShardForID(id)
	if a != b || a < 0 || a >= 4 {
		t.Fatalf("not deterministic/in range: %d %d", a, b)
	}
	if a, b := r.ShardForKey("k"), r.ShardForKey("k"); a != b {
		t.Fatalf("key not deterministic: %d %d", a, b)
	}
}

func TestRouter_SpreadsLoad(t *testing.T) {
	r := NewRouter(4)
	seen := map[int]int{}
	for i := 0; i < 400; i++ {
		seen[r.ShardForID(uuid.New())]++
	}
	if len(seen) != 4 {
		t.Fatalf("expected all 4 shards hit, got %v", seen)
	}
	for s, n := range seen {
		if n < 50 || n > 150 {
			t.Fatalf("shard %d badly skewed: %d/400", s, n)
		}
	}
}

func TestRouter_InvalidN(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for N<1")
		}
	}()
	NewRouter(0)
}

func TestDSNsFrom(t *testing.T) {
	if got := DSNsFrom("", "single"); len(got) != 1 || got[0] != "single" {
		t.Fatalf("fallback: %v", got)
	}
	got := DSNsFrom("a, b,,c ", "single")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("split/trim: %v", got)
	}
	if got := DSNsFrom(" , ", "single"); len(got) != 1 || got[0] != "single" {
		t.Fatalf("blank falls back: %v", got)
	}
}
