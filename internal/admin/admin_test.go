package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"s3qos/internal/admin"
	"s3qos/internal/cluster"
	"s3qos/internal/config"
	"s3qos/internal/engine"
	"s3qos/internal/metrics"
	"s3qos/internal/store"
)

const tok = "0123456789abcdef-admin"
const ctok = "0123456789abcdef-cluster"

type node struct {
	srv *httptest.Server
	st  *store.Store
	cl  *cluster.Cluster
	eng *engine.Engine
}

func newNode(t *testing.T, id string) *node {
	reg := metrics.New()
	eng := engine.New(reg, []string{"main"}, nil)
	st, err := store.Open("", eng.Validate)
	if err != nil {
		t.Fatal(err)
	}
	st.Subscribe(eng.Apply)
	cl := cluster.New(id, nil, ctok, 50*time.Millisecond, st)
	n := &node{st: st, cl: cl, eng: eng}
	pub := config.Config{NodeID: id, AdminToken: "SECRET-ADMIN-TOKEN-xyz", ClusterToken: "SECRET-CLUSTER-TOKEN-xyz",
		AdminTLSKey: "/secret/key.pem", Endpoints: []config.Endpoint{{Name: "main", Type: "ceph", Listen: ":8080",
			Backends: []string{"http://b1"}, TLSCert: "/c.pem", TLSKey: "/secret/k.pem"}}}
	n.srv = httptest.NewServer((&admin.API{Store: st, Cluster: cl, Metrics: reg, Token: tok,
		Config: pub.Public(), Endpoints: []string{"main"}}).Handler())
	t.Cleanup(n.srv.Close)
	return n
}

func call(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, url, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestCRUDAndValidation(t *testing.T) {
	n := newNode(t, "a")
	u := n.srv.URL + "/api/v1/traffic-classes"

	if c, _ := call(t, "GET", u, "", nil); c != 401 {
		t.Fatalf("no token: %d", c)
	}
	pol := map[string]any{
		"name":     "backup-prod-qos",
		"matchers": []any{map[string]any{"type": "bucket", "members": []string{"backup-prod"}, "inverse": false}},
		"limits": []any{
			map[string]any{"type": "aggregateBandwidthIn", "value": 100000000},
			map[string]any{"type": "aggregateBandwidthOut", "value": 200000000},
			map[string]any{"type": "readRequestRate", "value": 2000},
			map[string]any{"type": "writeRequestRate", "value": 500},
		},
	}
	c, out := call(t, "POST", u, tok, pol)
	if c != 201 {
		t.Fatalf("create: %d %v", c, out)
	}
	id := out["data"].(map[string]any)["id"].(string)

	if c, _ := call(t, "POST", u, tok, pol); c != 422 {
		t.Fatalf("duplicate name should be 422, got %d", c)
	}
	bad := map[string]any{"name": "x", "matchers": []any{}, "limits": []any{map[string]any{"type": "perRequestBandwidthIn", "value": 1}}}
	if c, _ := call(t, "POST", u, tok, bad); c != 422 {
		t.Fatalf("mixed bandwidth modes should be 422, got %d", c)
	}
	if c, _ := call(t, "POST", u, tok, map[string]any{"name": "y", "bogus": 1}); c != 400 {
		t.Fatalf("unknown field should be 400, got %d", c)
	}

	pol["description"] = "updated"
	if c, _ := call(t, "PUT", u+"/"+id, tok, pol); c != 200 {
		t.Fatalf("update: %d", c)
	}
	if _, out := call(t, "GET", u+"/"+id, tok, nil); out["data"].(map[string]any)["description"] != "updated" {
		t.Fatalf("not updated: %v", out)
	}

	// Switching to per-request mode while aggregate limits exist must be refused.
	if c, _ := call(t, "PUT", n.srv.URL+"/api/v1/settings", tok, map[string]any{"bandwidthMode": "perRequest", "enforcement": "node"}); c != 422 {
		t.Fatalf("mode switch with aggregate policies should be 422, got %d", c)
	}
	if c, _ := call(t, "DELETE", u+"/"+id, tok, nil); c != 204 {
		t.Fatalf("delete: %d", c)
	}
	if c, _ := call(t, "GET", u+"/"+id, tok, nil); c != 404 {
		t.Fatalf("after delete: %d", c)
	}
	if c, _ := call(t, "PUT", n.srv.URL+"/api/v1/settings", tok, map[string]any{"bandwidthMode": "perRequest", "enforcement": "fleet"}); c != 200 {
		t.Fatalf("mode switch: %d", c)
	}
}

func TestTenantMappingConflict(t *testing.T) {
	n := newNode(t, "a")
	u := n.srv.URL + "/api/v1/tenants/"
	if c, _ := call(t, "PUT", u+"t1", tok, store.Tenant{AccessKeys: []string{"AK1"}, Buckets: []string{"b1"}}); c != 200 {
		t.Fatalf("t1: %d", c)
	}
	if c, _ := call(t, "PUT", u+"t2", tok, store.Tenant{AccessKeys: []string{"AK1"}}); c != 422 {
		t.Fatalf("same key on two tenants must be 422, got %d", c)
	}
	if tn, ok := n.st.TenantByKey("AK1"); !ok || tn != "t1" {
		t.Fatalf("resolver: %v %v", tn, ok)
	}
}

func TestClusterReplication(t *testing.T) {
	a, b := newNode(t, "a"), newNode(t, "b")
	a.cl.Peers = []string{b.srv.URL}
	b.cl.Peers = []string{a.srv.URL}

	pol := map[string]any{"name": "p", "matchers": []any{map[string]any{"type": "tenant", "members": []string{"t"}}},
		"limits": []any{map[string]any{"type": "readRequestRate", "value": 10}}}
	if c, out := call(t, "POST", a.srv.URL+"/api/v1/traffic-classes", tok, pol); c != 201 {
		t.Fatalf("%d %v", c, out)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(b.st.Snapshot().Policies) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(b.st.Snapshot().Policies) != 1 {
		t.Fatal("policy was not replicated to peer b")
	}
	if a.st.Version() != b.st.Version() {
		t.Fatalf("versions differ: %d vs %d", a.st.Version(), b.st.Version())
	}
	// Peer endpoint requires the cluster token.
	if c, _ := call(t, "GET", a.srv.URL+"/internal/state", tok, nil); c != 401 {
		t.Fatalf("admin token must not open cluster endpoint: %d", c)
	}
	// Pull path marks peers alive.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	b.cl.Run(ctx)
	if n := b.cl.LiveNodes(); n != 2 {
		t.Fatalf("live nodes = %d, want 2", n)
	}
}

func raw(t *testing.T, method, url, token string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	return resp.StatusCode, b.String(), resp.Header
}

func TestConfigEndpointNeverLeaksSecrets(t *testing.T) {
	n := newNode(t, "node-a")
	if c, _, _ := raw(t, "GET", n.srv.URL+"/api/v1/config", ""); c != 401 {
		t.Fatalf("config must need a token, got %d", c)
	}
	c, body, _ := raw(t, "GET", n.srv.URL+"/api/v1/config", tok)
	if c != 200 {
		t.Fatalf("status %d", c)
	}
	for _, secret := range []string{"SECRET-ADMIN", "SECRET-CLUSTER", "/secret/", "adminToken", "clusterToken", "tlsKey"} {
		if strings.Contains(body, secret) {
			t.Errorf("config response leaks %q: %s", secret, body)
		}
	}
	if !strings.Contains(body, `"nodeId":"node-a"`) || !strings.Contains(body, `"tls":true`) || !strings.Contains(body, "http://b1") {
		t.Errorf("config missing expected fields: %s", body)
	}
}

func TestSchemaDescribesLimitsAndOperations(t *testing.T) {
	n := newNode(t, "a")
	if c, _, _ := raw(t, "GET", n.srv.URL+"/api/v1/schema", ""); c != 401 {
		t.Fatalf("schema must need a token, got %d", c)
	}
	c, body, _ := raw(t, "GET", n.srv.URL+"/api/v1/schema", tok)
	if c != 200 {
		t.Fatalf("status %d", c)
	}
	var out struct {
		Data struct {
			Limits []struct {
				Type, Kind, Scope, Operation string
			}
			Operations []struct {
				Operation string
				Read      bool
			}
			Matchers  []string
			Endpoints []string
		}
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Data.Limits) != 22 {
		t.Errorf("limits = %d, want 8 group/bandwidth + 14 per-operation = 22", len(out.Data.Limits))
	}
	kinds := map[string]int{}
	for _, l := range out.Data.Limits {
		kinds[l.Kind]++
		if l.Type == "listRequestRate" && (l.Operation != "list" || l.Scope != "operation" || l.Kind != "rate") {
			t.Errorf("listRequestRate described wrongly: %+v", l)
		}
		if l.Type == "readRequestRate" && l.Scope != "read" {
			t.Errorf("readRequestRate scope: %+v", l)
		}
	}
	if kinds["bandwidth"] != 4 || kinds["rate"] != 9 || kinds["concurrency"] != 9 {
		t.Errorf("kinds: %v", kinds)
	}
	if len(out.Data.Operations) != 7 || len(out.Data.Matchers) != 5 || len(out.Data.Endpoints) != 1 {
		t.Errorf("schema: %+v", out.Data)
	}
}

func TestClusterReportsPeerStatusAndUIRoutes(t *testing.T) {
	a := newNode(t, "a")
	a.cl.Peers = []string{"http://127.0.0.1:1"} // unreachable
	_, body, _ := raw(t, "GET", a.srv.URL+"/api/v1/cluster", tok)
	if !strings.Contains(body, `"url":"http://127.0.0.1:1"`) || !strings.Contains(body, `"alive":false`) {
		t.Errorf("peer status: %s", body)
	}
	// The UI shell is public (it carries no data); everything it fetches is not.
	if c, _, h := raw(t, "GET", a.srv.URL+"/", ""); c != 302 || h.Get("Location") != "/ui/" {
		t.Errorf("/ should redirect to /ui/: %d %v", c, h.Get("Location"))
	}
	if c, body, _ := raw(t, "GET", a.srv.URL+"/ui/", ""); c != 200 || !strings.Contains(body, "app.js") {
		t.Errorf("/ui/: %d", c)
	}
	if c, _, _ := raw(t, "GET", a.srv.URL+"/nope", ""); c != 404 {
		t.Errorf("unknown path: %d", c)
	}
	for _, p := range []string{"/api/v1/traffic-classes", "/api/v1/tenants", "/api/v1/settings", "/api/v1/cluster", "/metrics"} {
		if c, _, _ := raw(t, "GET", a.srv.URL+p, ""); c != 401 {
			t.Errorf("%s must require the token, got %d", p, c)
		}
	}
}
