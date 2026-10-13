package proxy_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"s3qos/internal/classify"
	"s3qos/internal/engine"
	"s3qos/internal/metrics"
	"s3qos/internal/policy"
	"s3qos/internal/proxy"
	"s3qos/internal/store"
)

// Two environments (test and prod) served by one node on separate listeners,
// each with its own backend pool and its own policy.
func TestSeparateEnvironmentsAreIsolated(t *testing.T) {
	testBE, prodBE := newCounting(), newCounting()
	defer testBE.Close()
	defer prodBE.Close()

	reg := metrics.New()
	eng := engine.New(reg, []string{"s3test", "s3prod"}, func() int { return 1 })
	st, err := store.Open("", eng.Validate)
	if err != nil {
		t.Fatal(err)
	}
	st.Subscribe(eng.Apply)
	if err := st.Update(func(s *store.State) error {
		s.Policies = []policy.Policy{
			{ID: "1", Name: "test-tier", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"s3test"}}},
				Limits: []policy.Limit{{Type: policy.ReadRate, Value: 2}}},
			{ID: "2", Name: "prod-tier", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"s3prod"}}},
				Limits: []policy.Limit{{Type: policy.ReadRate, Value: 1000}}},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	cls := &classify.Classifier{Resolver: st}
	listen := func(name, backend string) *httptest.Server {
		ep, err := proxy.NewEndpoint(proxy.EndpointConfig{Name: name, Type: "ceph", Backends: []string{backend}}, cls, eng, reg)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(ep)
		t.Cleanup(srv.Close)
		return srv
	}
	testSrv, prodSrv := listen("s3test", testBE.URL), listen("s3prod", prodBE.URL)

	// The test environment is capped at 2 reads/s: the third request is rejected.
	var codes []int
	for i := 0; i < 3; i++ {
		codes = append(codes, get(t, testSrv.URL, "/b/k"))
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 503 {
		t.Fatalf("test environment codes = %v, want [200 200 503]", codes)
	}

	// The test environment's limit never touches prod.
	for i := 0; i < 50; i++ {
		if c := get(t, prodSrv.URL, "/b/k"); c != 200 {
			t.Fatalf("prod request %d throttled by the test policy: %d", i, c)
		}
	}

	// Traffic stayed on its own backend pool.
	if testBE.hits.Load() != 2 || prodBE.hits.Load() != 50 {
		t.Fatalf("backend hits test=%d prod=%d, want 2 and 50", testBE.hits.Load(), prodBE.hits.Load())
	}

	// Per-policy and per-endpoint metrics are kept separately.
	var sb strings.Builder
	reg.Write(&sb)
	for _, want := range []string{
		`s3qos_policy_requests_total{policy="test-tier"} 3`,
		`s3qos_policy_requests_total{policy="prod-tier"} 50`,
		`s3qos_requests_total{endpoint="s3test",type="ceph",class="read",op="get",code="503"} 1`,
		`s3qos_requests_total{endpoint="s3prod",type="ceph",class="read",op="get",code="2xx"} 50`,
	} {
		if !strings.Contains(sb.String(), want) {
			t.Errorf("metrics missing %q\n%s", want, sb.String())
		}
	}
}
