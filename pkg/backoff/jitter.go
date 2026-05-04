// Package backoff provides small utilities for randomized backoff timings
// used by reconnection and retry loops in this project.
package backoff

import (
	"math/rand/v2"
	"sync"
	"time"
)

var rngMu sync.Mutex

// Jitter adds a small random component (up to 25% of d) to the provided
// duration. For non-positive d, it returns d unchanged.
// It is safe for concurrent use.
// Uses math/rand/v2 for better seeding and unpredictability.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	half := int64(d) / 2
	if half <= 0 {
		return d
	}
	rngMu.Lock()
	n := rand.N(half)
	rngMu.Unlock()
	return d + time.Duration(n)
}
