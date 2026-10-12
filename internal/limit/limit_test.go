package limit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBandwidthSharedAcrossGoroutines(t *testing.T) {
	b := &Bandwidth{Rate: ConstRate(200_000)} // 200 kB/s shared
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sent := 0; sent < 50_000; sent += 10_000 {
				_ = b.Wait(context.Background(), 10_000)
			}
		}()
	}
	wg.Wait()
	// 200 kB total at 200 kB/s ≈ 1s (minus the 100ms burst window).
	el := time.Since(start)
	if el < 800*time.Millisecond || el > 2*time.Second {
		t.Fatalf("elapsed %v, want ~0.9-1.1s", el)
	}
}

func TestBandwidthUnlimitedAndCancel(t *testing.T) {
	if err := (&Bandwidth{Rate: ConstRate(0)}).Wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	b := &Bandwidth{Rate: ConstRate(10)}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = b.Wait(ctx, 10) // consumes the burst
	if err := b.Wait(ctx, 1000); err == nil {
		t.Fatal("expected context error")
	}
}

func TestRateLimiter(t *testing.T) {
	r := &RateLimiter{Rate: func() float64 { return 5 }}
	ok := 0
	for i := 0; i < 20; i++ {
		if r.Allow() {
			ok++
		}
	}
	if ok != 5 {
		t.Fatalf("burst allowed %d, want 5", ok)
	}
	time.Sleep(450 * time.Millisecond)
	if !r.Allow() || !r.Allow() {
		t.Fatal("tokens should refill ~2 in 450ms")
	}
}

func TestConcurrency(t *testing.T) {
	c := &Concurrency{Max: func() int64 { return 2 }}
	if !c.TryAcquire() || !c.TryAcquire() || c.TryAcquire() {
		t.Fatal("expected exactly 2 slots")
	}
	c.Release()
	if !c.TryAcquire() {
		t.Fatal("slot should be free after release")
	}
}
