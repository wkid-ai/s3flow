package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"s3flow/internal/classify"
	"s3flow/internal/engine"
	"s3flow/internal/metrics"
	"s3flow/internal/proxy"
	"s3flow/internal/store"
)

// countingBackend is a plain HTTP server that counts the requests it serves.
type countingBackend struct {
	*httptest.Server
	hits    atomic.Int64
	healthy atomic.Bool
	release chan struct{} // closed to unblock requests carrying X-Block
}

func newCounting() *countingBackend {
	b := &countingBackend{release: make(chan struct{})}
	b.healthy.Store(true)
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if !b.healthy.Load() {
				http.Error(w, "down", http.StatusServiceUnavailable)
			}
			return
		}
		b.hits.Add(1)
		if r.Header.Get("X-Block") != "" {
			<-b.release
		}
		fmt.Fprint(w, "ok")
	}))
	return b
}

// lbProxy builds an endpoint with no QoS policies in front of the backends.
func lbProxy(t *testing.T, cfg proxy.EndpointConfig) (*httptest.Server, *proxy.Endpoint, *metrics.Registry) {
	t.Helper()
	reg := metrics.New()
	eng := engine.New(reg, []string{"main"}, func() int { return 1 })
	st, err := store.Open("", eng.Validate)
	if err != nil {
		t.Fatal(err)
	}
	st.Subscribe(eng.Apply)
	cfg.Name, cfg.Type = "main", "ceph"
	ep, err := proxy.NewEndpoint(cfg, &classify.Classifier{Resolver: st}, eng, reg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(ep)
	t.Cleanup(srv.Close)
	return srv, ep, reg
}

func get(t *testing.T, base, path string, hdr ...string) int {
	t.Helper()
	req, _ := http.NewRequest("GET", base+path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestRoundRobinSpreadsAcrossBackends(t *testing.T) {
	a, b, c := newCounting(), newCounting(), newCounting()
	defer a.Close()
	defer b.Close()
	defer c.Close()
	srv, _, _ := lbProxy(t, proxy.EndpointConfig{Backends: []string{a.URL, b.URL, c.URL}})
	for i := 0; i < 30; i++ {
		if code := get(t, srv.URL, "/bkt/k"); code != 200 {
			t.Fatalf("status %d", code)
		}
	}
	for n, be := range []*countingBackend{a, b, c} {
		if got := be.hits.Load(); got != 10 {
			t.Errorf("backend %d served %d, want 10", n, got)
		}
	}
}

func TestWeightedRoundRobin(t *testing.T) {
	a, b := newCounting(), newCounting()
	defer a.Close()
	defer b.Close()
	srv, _, _ := lbProxy(t, proxy.EndpointConfig{
		Backends: []string{a.URL, b.URL},
		Weights:  map[string]int{a.URL: 3, b.URL: 1},
	})
	for i := 0; i < 40; i++ {
		get(t, srv.URL, "/bkt/k")
	}
	if a.hits.Load() != 30 || b.hits.Load() != 10 {
		t.Fatalf("weights 3:1 over 40 requests gave %d:%d, want 30:10", a.hits.Load(), b.hits.Load())
	}
}

func TestLeastConnAvoidsBusyBackend(t *testing.T) {
	a, b := newCounting(), newCounting()
	defer a.Close()
	defer b.Close()
	srv, _, _ := lbProxy(t, proxy.EndpointConfig{Backends: []string{a.URL, b.URL}, Balance: proxy.LeastConn})

	// Park one request on whichever backend gets it.
	done := make(chan int, 1)
	go func() { done <- get(t, srv.URL, "/bkt/slow", "X-Block", "1") }()
	deadline := time.Now().Add(3 * time.Second)
	for a.hits.Load()+b.hits.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("blocking request never reached a backend")
		}
		time.Sleep(5 * time.Millisecond)
	}
	busy, idle := a, b
	if b.hits.Load() == 1 {
		busy, idle = b, a
	}
	for i := 0; i < 10; i++ {
		get(t, srv.URL, "/bkt/fast")
	}
	if idle.hits.Load() != 10 || busy.hits.Load() != 1 {
		t.Fatalf("leastconn sent %d to busy and %d to idle backend, want 1 and 10", busy.hits.Load(), idle.hits.Load())
	}
	close(a.release)
	close(b.release)
	if code := <-done; code != 200 {
		t.Fatalf("parked request: %d", code)
	}
}

func TestFailedBackendIsSkipped(t *testing.T) {
	a, b := newCounting(), newCounting()
	defer b.Close()
	srv, _, _ := lbProxy(t, proxy.EndpointConfig{Backends: []string{a.URL, b.URL}, DownFor: time.Minute})
	a.Close() // connection refused from now on

	bad := 0
	for i := 0; i < 12; i++ {
		if get(t, srv.URL, "/bkt/k") != 200 {
			bad++
		}
	}
	if bad > 1 {
		t.Fatalf("%d requests failed; only the one that discovers the outage may fail", bad)
	}
	if b.hits.Load() < 11 {
		t.Fatalf("healthy backend served %d of 12", b.hits.Load())
	}
}

func TestActiveHealthCheckRemovesAndRestoresBackend(t *testing.T) {
	a, b := newCounting(), newCounting()
	defer a.Close()
	defer b.Close()
	srv, ep, reg := lbProxy(t, proxy.EndpointConfig{
		Backends: []string{a.URL, b.URL}, HealthPath: "/health",
		HealthInterval: 40 * time.Millisecond, HealthTimeout: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); ep.Run(ctx) }()
	defer func() { cancel(); wg.Wait() }()

	send := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if code := get(t, srv.URL, "/bkt/k"); code != 200 {
				t.Fatalf("status %d", code)
			}
		}
	}
	waitUp := func(host string, want string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			var sb strings.Builder
			reg.Write(&sb)
			if strings.Contains(sb.String(), fmt.Sprintf(`s3flow_backend_up{endpoint="main",backend="%s"} %s`, host, want)) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("backend %s never reached up=%s\n%s", host, want, sb.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	host := func(be *countingBackend) string { return strings.TrimPrefix(be.URL, "http://") }

	a.healthy.Store(false)
	waitUp(host(a), "0")
	a0 := a.hits.Load()
	send(10)
	if a.hits.Load() != a0 {
		t.Fatalf("unhealthy backend still received %d requests", a.hits.Load()-a0)
	}

	a.healthy.Store(true)
	waitUp(host(a), "1")
	a1, b1 := a.hits.Load(), b.hits.Load()
	send(10)
	if a.hits.Load()-a1 != 5 || b.hits.Load()-b1 != 5 {
		t.Fatalf("after recovery split was %d/%d, want 5/5", a.hits.Load()-a1, b.hits.Load()-b1)
	}
}

func TestBackendPerPortAndMetrics(t *testing.T) {
	// Same host, different ports: each is its own balancing target.
	a, b := newCounting(), newCounting()
	defer a.Close()
	defer b.Close()
	srv, _, reg := lbProxy(t, proxy.EndpointConfig{
		Backends: []string{a.URL, b.URL},
		Weights:  map[string]int{b.URL: 2},
	})
	for i := 0; i < 6; i++ {
		get(t, srv.URL, "/bkt/k")
	}
	var sb strings.Builder
	reg.Write(&sb)
	for _, u := range []*countingBackend{a, b} {
		h := strings.TrimPrefix(u.URL, "http://")
		for _, name := range []string{"s3flow_backend_requests_total", "s3flow_backend_active_requests", "s3flow_backend_weight"} {
			if !strings.Contains(sb.String(), fmt.Sprintf(`%s{endpoint="main",backend="%s"}`, name, h)) {
				t.Errorf("missing %s for %s\n%s", name, h, sb.String())
			}
		}
	}
	if !strings.Contains(sb.String(), fmt.Sprintf(`s3flow_backend_weight{endpoint="main",backend="%s"} 2`, strings.TrimPrefix(b.URL, "http://"))) {
		t.Errorf("weight metric wrong\n%s", sb.String())
	}
	if a.hits.Load() != 2 || b.hits.Load() != 4 {
		t.Errorf("1:2 split over 6 requests gave %d:%d", a.hits.Load(), b.hits.Load())
	}
}

func TestEndpointConfigValidation(t *testing.T) {
	mk := func(c proxy.EndpointConfig) error {
		reg := metrics.New()
		eng := engine.New(reg, []string{"main"}, func() int { return 1 })
		st, _ := store.Open("", eng.Validate)
		c.Name = "main"
		_, err := proxy.NewEndpoint(c, &classify.Classifier{Resolver: st}, eng, reg)
		return err
	}
	for name, c := range map[string]proxy.EndpointConfig{
		"unknown balance":  {Backends: []string{"http://h:1"}, Balance: "random"},
		"zero weight":      {Backends: []string{"http://h:1"}, Weights: map[string]int{"http://h:1": 0}},
		"weight no target": {Backends: []string{"http://h:1"}, Weights: map[string]int{"http://other:1": 2}},
		"duplicate":        {Backends: []string{"http://h:1", "http://h:1"}},
		"bad scheme":       {Backends: []string{"ftp://h:1"}},
	} {
		if err := mk(c); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if err := mk(proxy.EndpointConfig{Backends: []string{"http://10.0.0.1:80", "http://10.0.0.1:8080", "http://10.0.0.2:80"}}); err != nil {
		t.Errorf("same IP on different ports must be allowed: %v", err)
	}
}
