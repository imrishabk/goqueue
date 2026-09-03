package backoff

import (
	"testing"
	"time"
)

func TestDelay_BoundedByCap(t *testing.T) {
	p := Policy{Base: 5 * time.Second, Max: time.Minute}
	for attempt := 1; attempt <= 20; attempt++ {
		for i := 0; i < 200; i++ {
			if d := p.Delay(attempt); d < 0 || d > time.Minute {
				t.Fatalf("attempt %d: delay %s out of [0, cap]", attempt, d)
			}
		}
	}
}

func TestDelay_NonZeroFloor(t *testing.T) {
	// equal jitter guarantees at least half the exponential temp
	p := Policy{Base: 4 * time.Second, Max: time.Hour}
	for i := 0; i < 200; i++ {
		if d := p.Delay(1); d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("attempt 1: delay %s not in [2s, 4s]", d)
		}
	}
}

func TestDelay_GrowsThenCaps(t *testing.T) {
	p := Policy{Base: time.Second, Max: 5 * time.Second}
	// attempt 1: temp=1s -> [0.5s,1s]; attempt 10: temp=min(512s,5s)=5s -> [2.5s,5s]
	var lo1, lo10 time.Duration = time.Hour, time.Hour
	for i := 0; i < 500; i++ {
		if d := p.Delay(1); d < lo1 {
			lo1 = d
		}
		if d := p.Delay(10); d < lo10 {
			lo10 = d
		}
	}
	if lo1 < 500*time.Millisecond {
		t.Fatalf("attempt 1 floor too low: %s", lo1)
	}
	if lo10 < 2500*time.Millisecond {
		t.Fatalf("attempt 10 floor too low (cap not applied?): %s", lo10)
	}
}

func TestDelay_AttemptFloor(t *testing.T) {
	p := Default()
	if d := p.Delay(0); d <= 0 || d > p.Max {
		t.Fatalf("attempt 0 should behave like 1, got %s", d)
	}
	if p.Base != 5*time.Second || p.Max != 10*time.Minute {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}
