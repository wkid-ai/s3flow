package engine

import (
	"net"
	"testing"

	"s3qos/internal/metrics"
	"s3qos/internal/policy"
	"s3qos/internal/store"
)

func newEngine(t *testing.T, st store.State) *Engine {
	t.Helper()
	e := New(metrics.New(), []string{"main"}, nil)
	if st.BandwidthMode == "" {
		st.BandwidthMode = policy.ModeAggregate
	}
	if st.Enforcement == "" {
		st.Enforcement = EnforceNode
	}
	if err := e.Validate(st); err != nil {
		t.Fatal(err)
	}
	e.Apply(st)
	return e
}

func TestBestMatchBandwidthPrecedence(t *testing.T) {
	e := newEngine(t, store.State{Policies: []policy.Policy{
		{ID: "t", Name: "tenant", Matchers: []policy.Matcher{{Type: "tenant", Members: []string{"T"}}},
			Limits: []policy.Limit{{Type: policy.AggOut, Value: 1000}}},
		{ID: "b", Name: "bucket", Matchers: []policy.Matcher{{Type: "bucket", Members: []string{"B"}}},
			Limits: []policy.Limit{{Type: policy.AggIn, Value: 500}}},
	}})
	a := policy.Attrs{Tenant: "T", Bucket: "B", Endpoint: "main", IP: net.ParseIP("1.2.3.4")}
	d := e.Admit(a, policy.OpGet)
	defer d.Done()
	if d.BWPolicy != "bucket" {
		t.Fatalf("exact bucket must win over tenant, got %q", d.BWPolicy)
	}
	// Bandwidth comes from the single winning policy only: it limits only
	// upload, so download is NOT limited by the less specific tenant policy.
	if d.In == nil || d.Out != nil {
		t.Fatalf("in=%v out=%v; want only upload shaped", d.In, d.Out)
	}
	d2 := e.Admit(policy.Attrs{Tenant: "T", Bucket: "other", Endpoint: "main"}, policy.OpGet)
	defer d2.Done()
	if d2.BWPolicy != "tenant" || d2.Out == nil {
		t.Fatalf("tenant-only request should use tenant policy: %q", d2.BWPolicy)
	}
}

func TestRateRejectedByAnyMatchingPolicyAndConcurrencyRollback(t *testing.T) {
	e := newEngine(t, store.State{Policies: []policy.Policy{
		{ID: "1", Name: "a", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}},
			Limits: []policy.Limit{{Type: policy.ConcRead, Value: 5}}},
		{ID: "2", Name: "b", Matchers: []policy.Matcher{{Type: "tenant", Members: []string{"T"}}},
			Limits: []policy.Limit{{Type: policy.ConcRead, Value: 1}}},
	}})
	a := policy.Attrs{Tenant: "T", Endpoint: "main"}
	d1 := e.Admit(a, policy.OpGet)
	if d1.Rejected {
		t.Fatal("first must pass")
	}
	d2 := e.Admit(a, policy.OpGet)
	if !d2.Rejected || d2.Policy != "b" || d2.Reason != "concurrency" {
		t.Fatalf("second must be rejected by tightest policy: %+v", d2)
	}
	// policy "a" slot taken by d2 before failing on "b" must have been rolled back
	d1.Done()
	for i := 0; i < 5; i++ {
		d := e.Admit(policy.Attrs{Tenant: "other", Endpoint: "main"}, policy.OpGet)
		if d.Rejected {
			t.Fatalf("slot leak in policy a at iteration %d", i)
		}
		defer d.Done()
	}
}

func TestApplyPreservesStateAcrossReload(t *testing.T) {
	st := store.State{Policies: []policy.Policy{
		{ID: "1", Name: "a", Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}},
			Limits: []policy.Limit{{Type: policy.ConcWrite, Value: 1}}},
	}, BandwidthMode: policy.ModeAggregate, Enforcement: EnforceNode}
	e := newEngine(t, st)
	d := e.Admit(policy.Attrs{Endpoint: "main"}, policy.OpPut)
	e.Apply(st) // policy reload while a request is in flight
	if d2 := e.Admit(policy.Attrs{Endpoint: "main"}, policy.OpPut); !d2.Rejected {
		t.Fatal("in-flight request must still count after reload")
	}
	d.Done()
}

func lim(t string, v int64) policy.Limit { return policy.Limit{Type: t, Value: v} }

func endpointPolicy(id, name string, limits ...policy.Limit) policy.Policy {
	return policy.Policy{ID: id, Name: name, Limits: limits,
		Matchers: []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}}}
}

func count(e *Engine, op policy.Operation, n int) (ok int) {
	for i := 0; i < n; i++ {
		d := e.Admit(policy.Attrs{Endpoint: "main"}, op)
		if !d.Rejected {
			ok++
		}
	}
	return
}

func TestPerOperationRateOnlyAffectsThatOperation(t *testing.T) {
	// No group read limit: only LIST is capped, GET/HEAD stay unlimited.
	e := newEngine(t, store.State{Policies: []policy.Policy{endpointPolicy("1", "p", lim("listRequestRate", 3))}})
	if got := count(e, policy.OpList, 10); got != 3 {
		t.Fatalf("list admitted %d, want 3", got)
	}
	if got := count(e, policy.OpGet, 50); got != 50 {
		t.Fatalf("get admitted %d, want 50", got)
	}
	if got := count(e, policy.OpPut, 50); got != 50 {
		t.Fatalf("put admitted %d, want 50", got)
	}
}

func TestGroupAndOperationLimitsBothApply(t *testing.T) {
	// Group write budget 6/s, delete sub-budget 2/s: fine-tune delete inside write.
	e := newEngine(t, store.State{Policies: []policy.Policy{
		endpointPolicy("1", "p", lim(policy.WriteRate, 6), lim("deleteRequestRate", 2))}})
	if got := count(e, policy.OpDelete, 10); got != 2 {
		t.Fatalf("delete admitted %d, want 2 (op limit)", got)
	}
	// Rejected deletes must not consume group tokens: 6 - 2 = 4 puts remain.
	if got := count(e, policy.OpPut, 10); got != 4 {
		t.Fatalf("put admitted %d, want 4 (group budget minus accepted deletes only)", got)
	}
	// Reads are a separate group and unaffected.
	if got := count(e, policy.OpGet, 20); got != 20 {
		t.Fatalf("get admitted %d, want 20", got)
	}
}

func TestGroupLimitStillCapsOperationWithLooserOwnLimit(t *testing.T) {
	e := newEngine(t, store.State{Policies: []policy.Policy{
		endpointPolicy("1", "p", lim(policy.ReadRate, 2), lim("getRequestRate", 100))}})
	if got := count(e, policy.OpGet, 10); got != 2 {
		t.Fatalf("get admitted %d, want 2 (group cap wins)", got)
	}
}

func TestPerOperationConcurrency(t *testing.T) {
	e := newEngine(t, store.State{Policies: []policy.Policy{
		endpointPolicy("1", "p", lim("concurrentListRequests", 1), lim(policy.ConcWrite, 3), lim("concurrentDeleteRequests", 1))}})
	a := policy.Attrs{Endpoint: "main"}
	l1 := e.Admit(a, policy.OpList)
	if l1.Rejected {
		t.Fatal("first list must pass")
	}
	if l2 := e.Admit(a, policy.OpList); !l2.Rejected || l2.Limit != "concurrentListRequests" || l2.Reason != "concurrency" {
		t.Fatalf("second list must hit concurrentListRequests: %+v", l2)
	}
	if g := e.Admit(a, policy.OpGet); g.Rejected {
		t.Fatal("get must not be limited by list concurrency")
	}
	d1 := e.Admit(a, policy.OpDelete)
	if d1.Rejected {
		t.Fatal("first delete must pass")
	}
	if d2 := e.Admit(a, policy.OpDelete); !d2.Rejected || d2.Limit != "concurrentDeleteRequests" {
		t.Fatalf("second delete must hit its own limit: %+v", d2)
	}
	// The failed delete must have released the group write slot it briefly held.
	p1, p2 := e.Admit(a, policy.OpPut), e.Admit(a, policy.OpPut)
	if p1.Rejected || p2.Rejected {
		t.Fatal("group write slots leaked by rejected delete (3 total, 1 in use by d1)")
	}
	if p3 := e.Admit(a, policy.OpPut); !p3.Rejected || p3.Limit != policy.ConcWrite {
		t.Fatalf("third put should hit group write cap: %+v", p3)
	}
	l1.Done()
	d1.Done()
	if l3 := e.Admit(a, policy.OpList); l3.Rejected {
		t.Fatal("list slot should be free after Done")
	}
}
