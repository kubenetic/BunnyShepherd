// Package backoff provides small utilities for randomized backoff timings
// used by reconnection and retry loops in this project.
package backoff

import (
	"math/rand"
	"sync"
	"time"
)

var (
	rng   = rand.New(rand.NewSource(time.Now().UnixNano()))
	rngMu sync.Mutex
)

// Jitter adds a small random component (up to 25% of d) to the provided
// duration. It is safe for concurrent use.
func Jitter(d time.Duration) time.Duration {
	rngMu.Lock()
	n := rng.Int63n(int64(d/4) + 1)
	rngMu.Unlock()
	return d + time.Duration(n)
}
