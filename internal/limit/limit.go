// Package limit provides the three throttling primitives: shared/per-request
// bandwidth shaping, request-rate token buckets and concurrency gates.
// Every limiter reads its rate through a function so that policy edits and
// fleet-share changes take effect without recreating limiters.
package limit

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const burstWindow = 100 * time.Millisecond

// Bandwidth shapes a byte stream (or a set of streams sharing the limiter).
type Bandwidth struct {
	Rate func() float64 // bytes/second; <=0 means unlimited
	mu   sync.Mutex
	next time.Time
}

// ConstRate returns a rate function with a fixed value.
func ConstRate(v float64) func() float64 { return func() float64 { return v } }

// Chunk returns the largest transfer size that keeps shaping smooth.
func (b *Bandwidth) Chunk() int {
	r := b.Rate()
	if r <= 0 {
		return 64 << 10
	}
	c := int(r / 20)
	if c < 512 {
		c = 512
	}
	if c > 64<<10 {
		c = 64 << 10
	}
	return c
}

// Wait blocks until n bytes may be considered transferred.
func (b *Bandwidth) Wait(ctx context.Context, n int) error {
	r := b.Rate()
	if r <= 0 || n <= 0 {
		return nil
	}
	b.mu.Lock()
	now := time.Now()
	if floor := now.Add(-burstWindow); b.next.Before(floor) {
		b.next = floor
	}
	b.next = b.next.Add(time.Duration(float64(n) / r * float64(time.Second)))
	wake := b.next
	b.mu.Unlock()
	d := time.Until(wake)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RateLimiter is a token bucket for request starts per second.
type RateLimiter struct {
	Rate   func() float64 // requests/second
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// Allow consumes one token if available.
func (r *RateLimiter) Allow() bool {
	rate := r.Rate()
	if rate <= 0 {
		return true
	}
	capacity := math.Max(1, rate)
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if r.last.IsZero() {
		r.tokens = capacity
	} else {
		r.tokens += now.Sub(r.last).Seconds() * rate
	}
	r.last = now
	if r.tokens > capacity {
		r.tokens = capacity
	}
	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

// Concurrency caps simultaneously active requests.
type Concurrency struct {
	Max func() int64 // <=0 means unlimited
	n   atomic.Int64
}

func (c *Concurrency) TryAcquire() bool {
	max := c.Max()
	if max <= 0 {
		c.n.Add(1)
		return true
	}
	for {
		cur := c.n.Load()
		if cur >= max {
			return false
		}
		if c.n.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (c *Concurrency) Release()      { c.n.Add(-1) }
func (c *Concurrency) Active() int64 { return c.n.Load() }
