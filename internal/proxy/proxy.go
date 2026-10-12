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
	"sync/atomic"
	"time"

	"s3qos/internal/classify"
	"s3qos/internal/engine"
	"s3qos/internal/limit"
	"s3qos/internal/metrics"
)

// SlowDownDelay is how long a rejected request is held before the 503 is sent.
const SlowDownDelay = 250 * time.Millisecond

type Backend struct {
	URL       *url.URL
	downUntil atomic.Int64 // unix nano
}

func (b *Backend) healthy() bool { return time.Now().UnixNano() >= b.downUntil.Load() }
func (b *Backend) markDown(d time.Duration) {
	b.downUntil.Store(time.Now().Add(d).UnixNano())
}

type EndpointConfig struct {
	Name        string
	Type        string // ceph | scale | icos | other (label only)
	Backends    []string
	CAFile      string
	InsecureTLS bool
	DownFor     time.Duration
}

type Endpoint struct {
	cfg      EndpointConfig
	backends []*Backend
	rr       atomic.Uint64
	rp       *httputil.ReverseProxy
	cls      *classify.Classifier
	eng      *engine.Engine
	reg      *metrics.Registry
}

func NewEndpoint(cfg EndpointConfig, cls *classify.Classifier, eng *engine.Engine, reg *metrics.Registry) (*Endpoint, error) {
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("endpoint %q has no backends", cfg.Name)
	}
	if cfg.DownFor <= 0 {
		cfg.DownFor = 5 * time.Second
	}
	e := &Endpoint{cfg: cfg, cls: cls, eng: eng, reg: reg}
	for _, s := range cfg.Backends {
		u, err := url.Parse(s)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: bad backend %q", cfg.Name, s)
		}
		e.backends = append(e.backends, &Backend{URL: u})
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
				resp.Body = &shapedReader{rc: resp.Body, ctx: resp.Request.Context(), bw: d.Out, count: d.AddOut}
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
			fmt.Fprintf(w, "%s %d\n", metrics.Label("s3qos_backend_up", "endpoint", cfg.Name, "backend", b.URL.Host), v)
		}
	})
	return e, nil
}

type backendKey struct{}
type decisionKey struct{}

func (e *Endpoint) pick() *Backend {
	n := uint64(len(e.backends))
	start := e.rr.Add(1)
	for i := uint64(0); i < n; i++ {
		if b := e.backends[(start+i)%n]; b.healthy() {
			return b
		}
	}
	return e.backends[start%n] // all marked down: try anyway
}

func (e *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()
	read := classify.IsRead(r.Method)
	class := "write"
	if read {
		class = "read"
	}
	attrs := e.cls.Classify(r, e.cfg.Name)
	d := e.eng.Admit(attrs, read)
	defer d.Done()

	rec := &statusWriter{ResponseWriter: w, code: 200}
	defer func() {
		code := strconv.Itoa(rec.code/100) + "xx"
		if d.Rejected {
			code = "503"
		}
		e.reg.Counter("s3qos_requests_total", "endpoint", e.cfg.Name, "type", e.cfg.Type, "class", class, "code", code).Add(1)
		e.reg.Counter("s3qos_request_seconds_sum_micro", "endpoint", e.cfg.Name, "class", class).Add(uint64(time.Since(begin).Microseconds()))
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
	ctx := context.WithValue(r.Context(), backendKey{}, b)
	ctx = context.WithValue(ctx, decisionKey{}, d)
	r = r.WithContext(ctx)
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &shapedReader{rc: r.Body, ctx: ctx, bw: d.In, count: d.AddIn}
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
