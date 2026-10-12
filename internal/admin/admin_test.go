package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"s3qos/internal/admin"
	"s3qos/internal/cluster"
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
	n.srv = httptest.NewServer((&admin.API{Store: st, Cluster: cl, Metrics: reg, Token: tok}).Handler())
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
