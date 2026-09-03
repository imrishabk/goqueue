// Package backoff computes retry delays: exponential growth with equal
// jitter, capped. Equal jitter (temp/2 + uniform[0, temp/2]) keeps a
// guaranteed positive floor so retries always move forward in time while
// still decorrelating thundering herds.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy tunes retry delays.
type Policy struct {
	// Base is the delay window for the first retry.
	Base time.Duration
	// Max caps any delay.
	Max time.Duration
}

// Default returns the coordinator's default policy.
func Default() Policy {
	return Policy{Base: 5 * time.Second, Max: 10 * time.Minute}
}

// Delay returns the wait before retry number attempt (1-based; values < 1
// behave as 1). Result is in [temp/2, temp] where temp = min(Max, Base*2^(attempt-1)).
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	temp := p.Base
	for i := 1; i < attempt; i++ {
		temp *= 2
		if temp >= p.Max {
			temp = p.Max
			break
		}
	}
	if temp > p.Max {
		temp = p.Max
	}
	half := temp / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
