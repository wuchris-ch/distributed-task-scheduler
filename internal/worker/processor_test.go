package worker

import (
	"math/rand"
	"testing"
	"time"
)

func TestBackoffWithJitter(t *testing.T) {
	rand.Seed(1)
	base := time.Second
	max := 8 * time.Second

	b1 := backoffWithJitter(base, max, 1)
	if b1 < base/2 || b1 > max {
		t.Fatalf("backoff out of range: %s", b1)
	}

	b3 := backoffWithJitter(base, max, 3)
	if b3 < base || b3 > max {
		t.Fatalf("backoff out of range for attempt 3: %s", b3)
	}
}
