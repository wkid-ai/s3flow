// Package proxy is the transparent S3 reverse proxy. It never rewrites the
// request line, Host header, signed headers or body, so backend SigV4/SigV2
// verification, bucket authorisation and S3 semantics are untouched.
package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"s3flow/internal/classify"
	"s3flow/internal/engine"
	"s3flow/internal/limit"
	"s3flow/internal/metrics"
)

// SlowDownDelay is how long a rejected request is held before the 503 is sent.
const SlowDownDelay = 250 * time.Millisecond

// Balancing algorithms.
const (
	RoundRobin = "roundrobin" // smooth weighted round-robin (default)
	LeastConn  = "leastconn"  // fewest in-flight requests per unit of weight
)

type Backend struct {
	URL       *url.URL
	Weight    int
	downUntil atomic.Int64 // unix nano
	active    atomic.Int64 // in-flight requests
	current   int          // smooth-WRR state, guarded by Endpoint.mu
}

func (b *Backend) healthy() bool { return time.Now().UnixNano() >= b.downUntil.Load() }
func (b *Backend) markDown(d time.Duration) {
	b.downUntil.Store(time.Now().Add(d).UnixNano())
}
func (b *Backend) markUp() { b.downUntil.Store(0) }

type EndpointConfig struct {
	Name     string
	Type     string // ceph | scale | icos | other (label only)
	Backends []string
	// Weights maps a backend URL (as listed in Backends) to its relative
	// share of traffic. Missing entries default to 1.
	Weights     map[string]int
	Balance     string // RoundRobin (default) | LeastConn
	CAFile      string
	InsecureTLS bool
	DownFor     time.Duration // how long a failed backend is skipped (default 5s)
	// Active health checking: when HealthPath is set, every HealthInterval each
	// backend is probed with GET HealthPath. Any response below 500 is "up".
	HealthPath     string
	HealthInterval time.Duration // default 5s
	HealthTimeout  time.Duration // default 2s
}

type Endpoint struct {
	cfg      EndpointConfig
	backends []*Backend
	mu       sync.Mutex // guards Backend.current and serialises pick
	rr       atomic.Uint64
	tr       *http.Transport
	rp       *httputil.ReverseProxy
	cls      *classify.Classifier
	eng      *engine.Engine
	reg      *metrics.Registry
}

func NewEndpoint(cfg EndpointConfig, cls *classify.Classifier, eng *engine.Engine, reg *metrics.Registry) (*Endpoint, error) {
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("endpoint %q has no backends", cfg.Name)
	}
	switch cfg.Balance {
	case "":
		cfg.Balance = RoundRobin
	case RoundRobin, LeastConn:
	default:
		return nil, fmt.Errorf("endpoint %q: unknown balance %q (use %s or %s)", cfg.Name, cfg.Balance, RoundRobin, LeastConn)
	}
	if cfg.DownFor <= 0 {
		cfg.DownFor = 5 * time.Second
	}
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = 5 * time.Second
	}
	if cfg.HealthTimeout <= 0 {
		cfg.HealthTimeout = 2 * time.Second
	}
	e := &Endpoint{cfg: cfg, cls: cls, eng: eng, reg: reg}
	seen := map[string]bool{}
	for _, s := range cfg.Backends {
		u, err := url.Parse(s)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: bad backend %q", cfg.Name, s)
		}
		if seen[u.Host] {
			return nil, fmt.Errorf("endpoint %q: backend %q listed twice", cfg.Name, u.Host)
		}
		seen[u.Host] = true
		w := 1
		if v, ok := cfg.Weights[s]; ok {
			w = v
		}
		if w < 1 {
			return nil, fmt.Errorf("endpoint %q: backend %q weight must be >= 1", cfg.Name, s)
		}
		e.backends = append(e.backends, &Backend{URL: u, Weight: w})
	}
	for s := range cfg.Weights {
		found := false
		for _, b := range cfg.Backends {
			found = found || b == s
		}
		if !found {
			return nil, fmt.Errorf("endpoint %q: weight given for unknown backend %q", cfg.Name, s)
		}
	}
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // never alter Accept-Encoding / payload
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureTLS} //nolint:gosec // explicit opt-in
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("endpoint %q: no certificates in %s", cfg.Name, cfg.CAFile)
		}
		tc.RootCAs = pool
	}
	tr.TLSClientConfig = tc
	e.tr = tr
	e.rp = &httputil.ReverseProxy{
		Transport:     tr,
		FlushInterval: -1,
		Director: func(r *http.Request) {
			b := r.Context().Value(backendKey{}).(*Backend)
			r.URL.Scheme = b.URL.Scheme
			r.URL.Host = b.URL.Host
			// r.Host intentionally untouched: it is part of the signature.
		},
		ModifyResponse: func(resp *http.Response) error {
			if d, _ := resp.Request.Context().Value(decisionKey{}).(*engine.Decision); d != nil {
				outBytes := e.reg.Counter("s3flow_endpoint_bytes_total", "endpoint", cfg.Name, "direction", "out")
				resp.Body = &shapedReader{rc: resp.Body, ctx: resp.Request.Context(), bw: d.Out, count: func(n int) { d.AddOut(n); outBytes.Add(uint64(n)) }}
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			if b, ok := r.Context().Value(backendKey{}).(*Backend); ok {
				var ne net.Error
				if errors.As(err, &ne) || errors.Is(err, io.EOF) {
					b.markDown(cfg.DownFor)
				}
			}
			writeS3Error(w, http.StatusBadGateway, "BadGateway", "Backend unavailable.")
		},
	}
	reg.AddCollector(func(w io.Writer) {
		for _, b := range e.backends {
			v := 1
			if !b.healthy() {
				v = 0
			}
			fmt.Fprintf(w, "%s %d\n", metrics.Label("s3flow_backend_up", "endpoint", cfg.Name, "backend", b.URL.Host), v)
			fmt.Fprintf(w, "%s %d\n", metrics.Label("s3flow_backend_active_requests", "endpoint", cfg.Name, "backend", b.URL.Host), b.active.Load())
			fmt.Fprintf(w, "%s %d\n", metrics.Label("s3flow_backend_weight", "endpoint", cfg.Name, "backend", b.URL.Host), b.Weight)
		}
	})
	return e, nil
}

type backendKey struct{}
type decisionKey struct{}

// pick chooses a backend and counts the request as in flight on it; the caller
// must decrement Backend.active when the request finishes. Unhealthy backends
// are skipped unless every backend is down, in which case all are tried.
func (e *Endpoint) pick() *Backend {
	e.mu.Lock()
	defer e.mu.Unlock()
	cands := make([]*Backend, 0, len(e.backends))
	for _, b := range e.backends {
		if b.healthy() {
			cands = append(cands, b)
		}
	}
	if len(cands) == 0 {
		cands = e.backends
	}
	var best *Backend
	if e.cfg.Balance == LeastConn {
		// Lowest active/weight wins; rotate the starting point so ties spread.
		start := int(e.rr.Add(1) % uint64(len(cands)))
		for i := range cands {
			b := cands[(start+i)%len(cands)]
			if best == nil || b.active.Load()*int64(best.Weight) < best.active.Load()*int64(b.Weight) {
				best = b
			}
		}
	} else {
		// Smooth weighted round-robin: interleaves rather than bursting.
		total := 0
		for _, b := range cands {
			b.current += b.Weight
			total += b.Weight
			if best == nil || b.current > best.current {
				best = b
			}
		}
		best.current -= total
	}
	best.active.Add(1)
	return best
}

// Run probes every backend with GET HealthPath until ctx ends. It is a no-op
// when no HealthPath is configured. Probes use the same TLS settings as
// proxied traffic and never carry client credentials.
func (e *Endpoint) Run(ctx context.Context) {
	if e.cfg.HealthPath == "" {
		return
	}
	probe := func() {
		var wg sync.WaitGroup
		for _, b := range e.backends {
			wg.Add(1)
			go func(b *Backend) {
				defer wg.Done()
				pctx, cancel := context.WithTimeout(ctx, e.cfg.HealthTimeout)
				defer cancel()
				u := *b.URL
				u.Path, u.RawQuery = e.cfg.HealthPath, ""
				req, err := http.NewRequestWithContext(pctx, http.MethodGet, u.String(), nil)
				if err != nil {
					return
				}
				resp, err := e.tr.RoundTrip(req)
				if err == nil {
					_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
					resp.Body.Close()
				}
				if ctx.Err() != nil {
					return
				}
				if err != nil || resp.StatusCode >= 500 {
					b.markDown(2 * e.cfg.HealthInterval)
				} else {
					b.markUp()
				}
			}(b)
		}
		wg.Wait()
	}
	probe()
	t := time.NewTicker(e.cfg.HealthInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			probe()
		}
	}
}

func (e *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()
	op := e.cls.Operation(r)
	class := "write"
	if op.IsRead() {
		class = "read"
	}
	attrs := e.cls.Classify(r, e.cfg.Name)
	d := e.eng.Admit(attrs, op)
	defer d.Done()

	rec := &statusWriter{ResponseWriter: w, code: 200}
	defer func() {
		code := strconv.Itoa(rec.code/100) + "xx"
		if d.Rejected {
			code = "503"
		}
		e.reg.Counter("s3flow_requests_total", "endpoint", e.cfg.Name, "type", e.cfg.Type, "class", class, "op", string(op), "code", code).Add(1)
		e.reg.Counter("s3flow_request_seconds_sum_micro", "endpoint", e.cfg.Name, "class", class).Add(uint64(time.Since(begin).Microseconds()))
	}()

	if d.Rejected {
		// Delay, then 503 SlowDown (not an unbounded queue).
		t := time.NewTimer(SlowDownDelay)
		select {
		case <-t.C:
		case <-r.Context().Done():
			t.Stop()
			return
		}
		rec.Header().Set("Retry-After", "1")
		writeS3Error(rec, http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate.")
		return
	}

	b := e.pick()
	defer b.active.Add(-1)
	e.reg.Counter("s3flow_backend_requests_total", "endpoint", e.cfg.Name, "backend", b.URL.Host).Add(1)
	ctx := context.WithValue(r.Context(), backendKey{}, b)
	ctx = context.WithValue(ctx, decisionKey{}, d)
	r = r.WithContext(ctx)
	if r.Body != nil && r.Body != http.NoBody {
		inBytes := e.reg.Counter("s3flow_endpoint_bytes_total", "endpoint", e.cfg.Name, "direction", "in")
		r.Body = &shapedReader{rc: r.Body, ctx: ctx, bw: d.In, count: func(n int) { d.AddIn(n); inBytes.Add(uint64(n)) }}
	}
	e.rp.ServeHTTP(rec, r)
}

// statusWriter records the status and keeps streaming responses flushable.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }
func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// shapedReader meters bytes and (optionally) throttles them.
type shapedReader struct {
	rc    io.ReadCloser
	ctx   context.Context
	bw    *limit.Bandwidth
	count func(int)
}

func (s *shapedReader) Read(p []byte) (int, error) {
	if s.bw != nil {
		if c := s.bw.Chunk(); len(p) > c {
			p = p[:c]
		}
	}
	n, err := s.rc.Read(p)
	if n > 0 {
		s.count(n)
		if s.bw != nil {
			if werr := s.bw.Wait(s.ctx, n); werr != nil {
				return n, werr
			}
		}
	}
	return n, err
}

func (s *shapedReader) Close() error { return s.rc.Close() }

func writeS3Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, msg)
}
