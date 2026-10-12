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
	d := e.Admit(a, true)
	defer d.Done()
	if d.BWPolicy != "bucket" {
		t.Fatalf("exact bucket must win over tenant, got %q", d.BWPolicy)
	}
	// Bandwidth comes from the single winning policy only: it limits only
	// upload, so download is NOT limited by the less specific tenant policy.
	if d.In == nil || d.Out != nil {
		t.Fatalf("in=%v out=%v; want only upload shaped", d.In, d.Out)
	}
	d2 := e.Admit(policy.Attrs{Tenant: "T", Bucket: "other", Endpoint: "main"}, true)
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
	d1 := e.Admit(a, true)
	if d1.Rejected {
		t.Fatal("first must pass")
	}
	d2 := e.Admit(a, true)
	if !d2.Rejected || d2.Policy != "b" || d2.Reason != "concurrency" {
		t.Fatalf("second must be rejected by tightest policy: %+v", d2)
	}
	// policy "a" slot taken by d2 before failing on "b" must have been rolled back
	d1.Done()
	for i := 0; i < 5; i++ {
		d := e.Admit(policy.Attrs{Tenant: "other", Endpoint: "main"}, true)
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
	d := e.Admit(policy.Attrs{Endpoint: "main"}, false)
	e.Apply(st) // policy reload while a request is in flight
	if d2 := e.Admit(policy.Attrs{Endpoint: "main"}, false); !d2.Rejected {
		t.Fatal("in-flight request must still count after reload")
	}
	d.Done()
}
