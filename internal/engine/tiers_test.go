package engine

import (
	"testing"

	"s3flow/internal/policy"
	"s3flow/internal/store"
)

// The built-in tiers must pass the engine's own validation, match nothing
// while unattached, and enforce their request rate once attached.
func TestDefaultTiersInEngine(t *testing.T) {
	st := store.State{
		Policies:      policy.DefaultTiers(),
		BandwidthMode: policy.ModeAggregate,
		Enforcement:   EnforceNode,
	}
	e := newEngine(t, st)

	d := e.Admit(policy.Attrs{Endpoint: "main"}, policy.OpGet)
	if d.BWPolicy != "" || d.Rejected {
		t.Fatalf("unattached tiers must not affect traffic: %+v", d)
	}
	d.Done()

	// Attach Bronze (10 K read req/s + 1.25 GB/s) to the endpoint.
	for i := range st.Policies {
		if st.Policies[i].ID == policy.IDBronze {
			st.Policies[i].Matchers = []policy.Matcher{{Type: "endpoint", Members: []string{"main"}}}
		}
	}
	e.Apply(st)
	d = e.Admit(policy.Attrs{Endpoint: "main"}, policy.OpGet)
	defer d.Done()
	if d.BWPolicy != "Bronze" || d.In == nil || d.Out == nil {
		t.Fatalf("Bronze should shape both directions: policy=%q in=%v out=%v", d.BWPolicy, d.In, d.Out)
	}
	rejected := 0
	for i := 0; i < 12_000; i++ {
		x := e.Admit(policy.Attrs{Endpoint: "main"}, policy.OpGet)
		if x.Rejected {
			rejected++
		}
		x.Done()
	}
	// With a 10 K/s capacity and 12 K instant requests the rate limiter rejects
	// roughly 2 000, but exact timing varies; accept anything in a wide window.
	if rejected < 1_000 || rejected > 3_000 {
		t.Fatalf("a 10 K/s limit should reject roughly 2 000 of 12 000 instant requests, rejected %d", rejected)
	}
}
