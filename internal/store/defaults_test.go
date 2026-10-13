package store

import (
	"path/filepath"
	"testing"

	"s3flow/internal/policy"
)

func names(st State) map[string]policy.Policy {
	m := map[string]policy.Policy{}
	for _, p := range st.Policies {
		m[p.Name] = p
	}
	return m
}

func limitOf(p policy.Policy, typ string) int64 {
	for _, l := range p.Limits {
		if l.Type == typ {
			return l.Value
		}
	}
	return -1
}

func TestDefaultTiersMatchTheSLATable(t *testing.T) {
	want := map[string]struct{ rate, bw int64 }{
		"Gold":   {50_000, 10_000_000_000},
		"Silver": {20_000, 5_000_000_000},
		"Bronze": {10_000, 1_250_000_000},
	}
	got := names(State{Policies: policy.DefaultTiers()})
	if len(got) != 3 {
		t.Fatalf("want 3 tiers, got %d", len(got))
	}
	for n, w := range want {
		p, ok := got[n]
		if !ok {
			t.Fatalf("missing tier %s", n)
		}
		for _, typ := range []string{policy.ReadRate, policy.WriteRate} {
			if v := limitOf(p, typ); v != w.rate {
				t.Errorf("%s %s = %d, want %d", n, typ, v, w.rate)
			}
		}
		for _, typ := range []string{policy.AggIn, policy.AggOut} {
			if v := limitOf(p, typ); v != w.bw {
				t.Errorf("%s %s = %d, want %d", n, typ, v, w.bw)
			}
		}
		if len(p.Matchers) != 0 {
			t.Errorf("%s must not match anything until attached", n)
		}
	}
}

func TestSeedDefaultsOnceAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.SeedDefaults(policy.DefaultTiers()); err != nil || !ok {
		t.Fatalf("first seed: ok=%v err=%v", ok, err)
	}
	if ok, _ := s.SeedDefaults(policy.DefaultTiers()); ok {
		t.Fatal("second seed in the same process must be a no-op")
	}
	// Restart: still three, not six.
	s2, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := s2.SeedDefaults(policy.DefaultTiers()); ok {
		t.Fatal("seeding after restart must be a no-op")
	}
	if n := len(s2.Snapshot().Policies); n != 3 {
		t.Fatalf("after restart have %d policies, want 3", n)
	}
}

func TestEditsAndDeletionsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, _ := Open(path, nil)
	if _, err := s.SeedDefaults(policy.DefaultTiers()); err != nil {
		t.Fatal(err)
	}
	err := s.Update(func(st *State) error {
		var keep []policy.Policy
		for _, p := range st.Policies {
			switch p.ID {
			case policy.IDBronze: // delete
				continue
			case policy.IDGold: // edit every kind of limit
				p.Limits = []policy.Limit{{Type: policy.ReadRate, Value: 7}, {Type: "listRequestRate", Value: 3}}
				p.Matchers = []policy.Matcher{{Type: "endpoint", Members: []string{"prod"}}}
			}
			keep = append(keep, p)
		}
		st.Policies = keep
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := Open(path, nil)
	if ok, _ := s2.SeedDefaults(policy.DefaultTiers()); ok {
		t.Fatal("deleted defaults must not come back")
	}
	got := names(s2.Snapshot())
	if _, ok := got["Bronze"]; ok || len(got) != 2 {
		t.Fatalf("policies after restart: %v", got)
	}
	if g := got["Gold"]; limitOf(g, policy.ReadRate) != 7 || limitOf(g, "listRequestRate") != 3 || len(g.Matchers) != 1 {
		t.Fatalf("edit was lost: %+v", g)
	}
}

func TestSeedSkippedWhenPoliciesExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, _ := Open(path, nil)
	if err := s.Update(func(st *State) error {
		st.Policies = []policy.Policy{{ID: "mine", Name: "mine"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.SeedDefaults(policy.DefaultTiers()); ok {
		t.Fatal("existing policies must not be mixed with defaults")
	}
	if n := len(s.Snapshot().Policies); n != 1 {
		t.Fatalf("policies = %d, want 1", n)
	}
}

// A node joining a running cluster seeds its own defaults at version 0 and
// must then take the cluster's state rather than overwrite it.
func TestPeerStateReplacesSeededDefaults(t *testing.T) {
	s, _ := Open("", nil)
	if _, err := s.SeedDefaults(policy.DefaultTiers()); err != nil {
		t.Fatal(err)
	}
	if v := s.Version(); v != 0 {
		t.Fatalf("seeded version = %d, want 0 so any peer state is newer", v)
	}
	peer := State{Version: 5, BandwidthMode: policy.ModeAggregate, Enforcement: "node",
		Policies: []policy.Policy{{ID: "x", Name: "Platinum"}}, DefaultsSeeded: true}
	if ok, err := s.Replace(peer); err != nil || !ok {
		t.Fatalf("replace: ok=%v err=%v", ok, err)
	}
	got := names(s.Snapshot())
	if _, ok := got["Platinum"]; !ok || len(got) != 1 {
		t.Fatalf("cluster state should win, got %v", got)
	}
}
