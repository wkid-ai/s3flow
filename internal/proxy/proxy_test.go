package proxy_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"s3flow/internal/classify"
	"s3flow/internal/engine"
	"s3flow/internal/metrics"
	"s3flow/internal/policy"
	"s3flow/internal/proxy"
	"s3flow/internal/store"
)

const (
	accessKey = "AKIDTEST"
	secretKey = "secret"
	region    = "us-east-1"
)

func hm(key []byte, s string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(s))
	return h.Sum(nil)
}

// sigV4 computes the signature for the given request pieces.
func sigV4(method, path, rawQuery string, hdr http.Header, signed []string, payloadHash, amzDate string) string {
	sort.Strings(signed)
	var ch strings.Builder
	for _, h := range signed {
		ch.WriteString(h + ":" + strings.TrimSpace(hdr.Get(h)) + "\n")
	}
	canon := strings.Join([]string{method, path, rawQuery, ch.String(), strings.Join(signed, ";"), payloadHash}, "\n")
	sum := sha256.Sum256([]byte(canon))
	day := amzDate[:8]
	scope := day + "/" + region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	k := hm([]byte("AWS4"+secretKey), day)
	k = hm(k, region)
	k = hm(k, "s3")
	k = hm(k, "aws4_request")
	return hex.EncodeToString(hm(k, sts))
}

func signRequest(r *http.Request, payloadHash string) {
	amz := time.Now().UTC().Format("20060102T150405Z")
	r.Header.Set("X-Amz-Date", amz)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	hdr := r.Header.Clone()
	hdr.Set("Host", r.Host)
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sig := sigV4(r.Method, r.URL.EscapedPath(), r.URL.RawQuery, hdr, signed, payloadHash, amz)
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=%s, Signature=%s",
		accessKey, amz[:8], region, strings.Join(signed, ";"), sig))
}

// fakeS3 verifies SigV4 exactly like a real backend would, using what it received.
func fakeS3() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		i := strings.Index(auth, "SignedHeaders=")
		j := strings.Index(auth, ", Signature=")
		if i < 0 || j < 0 {
			http.Error(w, "AccessDenied", http.StatusForbidden)
			return
		}
		signed := strings.Split(auth[i+len("SignedHeaders="):j], ";")
		got := auth[j+len(", Signature="):]
		hdr := r.Header.Clone()
		hdr.Set("Host", r.Host)
		want := sigV4(r.Method, r.URL.EscapedPath(), r.URL.RawQuery, hdr, signed, r.Header.Get("X-Amz-Content-Sha256"), r.Header.Get("X-Amz-Date"))
		if got != want {
			http.Error(w, "SignatureDoesNotMatch", http.StatusForbidden)
			return
		}
		switch r.Method {
		case http.MethodPut:
			n, _ := io.Copy(io.Discard, r.Body)
			fmt.Fprintf(w, "stored %d", n)
		default:
			_, _ = w.Write(bytes.Repeat([]byte("x"), 400_000))
		}
	}))
}

type env struct {
	proxy *httptest.Server
	st    *store.Store
	eng   *engine.Engine
	reg   *metrics.Registry
	nodes int
}

func setup(t *testing.T, backend string, mutate func(*store.State)) *env {
	t.Helper()
	e := &env{reg: metrics.New(), nodes: 1}
	e.eng = engine.New(e.reg, []string{"main"}, func() int { return e.nodes })
	st, err := store.Open("", e.eng.Validate)
	if err != nil {
		t.Fatal(err)
	}
	e.st = st
	st.Subscribe(e.eng.Apply)
	if err := st.Update(func(s *store.State) error {
		s.Tenants["tenantA"] = store.Tenant{AccessKeys: []string{accessKey}}
		if mutate != nil {
			mutate(s)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cls := &classify.Classifier{Resolver: st}
	ep, err := proxy.NewEndpoint(proxy.EndpointConfig{Name: "main", Type: "ceph", Backends: []string{backend}}, cls, e.eng, e.reg)
	if err != nil {
		t.Fatal(err)
	}
	e.proxy = httptest.NewServer(ep)
	t.Cleanup(e.proxy.Close)
	return e
}

func (e *env) do(t *testing.T, method, path string, body []byte) (*http.Response, []byte, time.Duration) {
	t.Helper()
	var rd io.Reader
	hash := sha256.Sum256(nil)
	if body != nil {
		rd = bytes.NewReader(body)
		hash = sha256.Sum256(body)
	}
	req, _ := http.NewRequest(method, e.proxy.URL+path, rd)
	signRequest(req, hex.EncodeToString(hash[:]))
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return &http.Response{StatusCode: 0}, nil, 0
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b, time.Since(start)
}

func TestSignedRequestPassesThroughUnmodified(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Policies = []policy.Policy{{ID: "1", Name: "all", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}},
			Limits: []policy.Limit{{Type: policy.AggOut, Value: 50_000_000}}}}
	})
	resp, body, _ := e.do(t, "GET", "/bkt/some%20key?x=1&a=2", nil)
	if resp.StatusCode != 200 || len(body) != 400_000 {
		t.Fatalf("status=%d len=%d body=%.80s", resp.StatusCode, len(body), body)
	}
	resp, body, _ = e.do(t, "PUT", "/bkt/obj", bytes.Repeat([]byte("y"), 1000))
	if resp.StatusCode != 200 || string(body) != "stored 1000" {
		t.Fatalf("PUT status=%d body=%s", resp.StatusCode, body)
	}
}

func TestAggregateUploadShapingPerBucket(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Policies = []policy.Policy{{ID: "1", Name: "slow", Matchers: []policy.Matcher{{Type: "bucket", Members: []string{"slowbkt"}}},
			Limits: []policy.Limit{{Type: policy.AggIn, Value: 400_000}}}}
	})
	body := bytes.Repeat([]byte("z"), 400_000)
	resp, _, d := e.do(t, "PUT", "/slowbkt/o", body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if d < 700*time.Millisecond {
		t.Fatalf("400KB at 400KB/s should take ~0.9s, took %v", d)
	}
	_, _, d = e.do(t, "PUT", "/otherbkt/o", body)
	if d > 400*time.Millisecond {
		t.Fatalf("unmatched bucket must be unthrottled, took %v", d)
	}
}

func TestPerRequestDownloadMode(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.BandwidthMode = policy.ModePerRequest
		s.Policies = []policy.Policy{{ID: "1", Name: "pr", Matchers: []policy.Matcher{{Type: "tenant", Members: []string{"tenantA"}}},
			Limits: []policy.Limit{{Type: policy.ReqOut, Value: 400_000}}}}
	})
	// Two parallel downloads should each take ~1s (independent), not ~2s (shared).
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.do(t, "GET", "/b/k", nil) }()
	}
	wg.Wait()
	if el := time.Since(start); el < 700*time.Millisecond || el > 1600*time.Millisecond {
		t.Fatalf("per-request limits should be independent, took %v", el)
	}
}

func TestRequestRateSlowDown(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Policies = []policy.Policy{{ID: "1", Name: "rate", Matchers: []policy.Matcher{{Type: "tenant", Members: []string{"tenantA"}}},
			Limits: []policy.Limit{{Type: policy.WriteRate, Value: 2}}}}
	})
	codes := map[int]int{}
	var slowest time.Duration
	for i := 0; i < 4; i++ { // 4 sends: refill during the 250ms delay stays < 1 token
		resp, body, d := e.do(t, "PUT", "/b/o", []byte("hi"))
		codes[resp.StatusCode]++
		if resp.StatusCode == 503 {
			if !strings.Contains(string(body), "SlowDown") {
				t.Fatalf("body: %s", body)
			}
			slowest = d
		}
	}
	if codes[200] != 2 || codes[503] != 2 {
		t.Fatalf("codes: %v", codes)
	}
	if slowest < 240*time.Millisecond {
		t.Fatalf("503 must be delayed 250ms, got %v", slowest)
	}
	// reads are unaffected by a write limit
	if resp, _, _ := e.do(t, "GET", "/b/o", nil); resp.StatusCode != 200 {
		t.Fatalf("read status %d", resp.StatusCode)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	release := make(chan struct{})
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Policies = []policy.Policy{{ID: "1", Name: "conc", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}},
			Limits: []policy.Limit{{Type: policy.ConcRead, Value: 1}}}}
	})
	done := make(chan int, 1)
	go func() {
		resp, err := http.Get(e.proxy.URL + "/b/k")
		if err != nil {
			done <- 0
			return
		}
		resp.Body.Close()
		done <- resp.StatusCode
	}()
	time.Sleep(150 * time.Millisecond)
	resp, err := http.Get(e.proxy.URL + "/b/k2")
	if err != nil || resp.StatusCode != 503 {
		t.Fatalf("second concurrent read should be 503, got %v %v", resp, err)
	}
	resp.Body.Close()
	close(release)
	if c := <-done; c != 200 {
		t.Fatalf("first request status %d", c)
	}
}

func TestFleetEnforcementDividesRate(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Enforcement = engine.EnforceFleet
		s.Policies = []policy.Policy{{ID: "1", Name: "rate", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}},
			Limits: []policy.Limit{{Type: policy.ReadRate, Value: 8}}}}
	})
	e.nodes = 4 // 8 req/s fleet-wide => 2 req/s on this node
	ok := 0
	for i := 0; i < 3; i++ {
		if resp, _, _ := e.do(t, "GET", "/b/o", nil); resp.StatusCode == 200 {
			ok++
		}
	}
	if ok != 2 {
		t.Fatalf("fleet share should allow 2 immediate requests, got %d", ok)
	}
}

func TestMetricsExposed(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Policies = []policy.Policy{{ID: "1", Name: "m", Matchers: []policy.Matcher{{Type: "tenant", Members: []string{"tenantA"}}},
			Limits: []policy.Limit{{Type: policy.AggOut, Value: 100_000_000}}}}
	})
	e.do(t, "GET", "/b/k", nil)
	var out bytes.Buffer
	e.reg.Write(&out)
	for _, want := range []string{
		`s3flow_policy_requests_total{policy="m"} 1`,
		`s3flow_policy_bytes_total{policy="m",direction="out"} 400000`,
		`s3flow_requests_total{endpoint="main",type="ceph",class="read",op="get",code="2xx"} 1`,
		`s3flow_backend_up{endpoint="main"`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("metrics missing %q\n%s", want, out.String())
		}
	}
}

func TestPerOperationLimitsThroughProxy(t *testing.T) {
	be := fakeS3()
	defer be.Close()
	e := setup(t, be.URL, func(s *store.State) {
		s.Policies = []policy.Policy{{ID: "1", Name: "ops", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}},
			Limits: []policy.Limit{{Type: "listRequestRate", Value: 1}, {Type: "deleteRequestRate", Value: 1}}}}
	})
	status := func(method, path string) int {
		resp, _, _ := e.do(t, method, path, nil)
		return resp.StatusCode
	}
	if c := status("GET", "/b?list-type=2"); c != 200 {
		t.Fatalf("first LIST: %d", c)
	}
	if c := status("GET", "/b?list-type=2"); c != 503 {
		t.Fatalf("second LIST should be throttled, got %d", c)
	}
	// GET object is a different operation: not throttled by the list limit.
	for i := 0; i < 3; i++ {
		if c := status("GET", "/b/k"); c != 200 {
			t.Fatalf("GET object %d: %d", i, c)
		}
	}
	if c := status("DELETE", "/b/k"); c != 200 {
		t.Fatalf("first DELETE: %d", c)
	}
	if c := status("DELETE", "/b/k2"); c != 503 {
		t.Fatalf("second DELETE should be throttled, got %d", c)
	}
	// PUT is untouched by the delete limit.
	if resp, _, _ := e.do(t, "PUT", "/b/o", []byte("x")); resp.StatusCode != 200 {
		t.Fatalf("PUT: %d", resp.StatusCode)
	}
	var out bytes.Buffer
	e.reg.Write(&out)
	for _, want := range []string{
		`s3flow_policy_throttled_total{policy="ops",reason="rate",limit="listRequestRate"} 1`,
		`s3flow_policy_throttled_total{policy="ops",reason="rate",limit="deleteRequestRate"} 1`,
		`op="list",code="503"`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("metrics missing %q\n%s", want, out.String())
		}
	}
}
