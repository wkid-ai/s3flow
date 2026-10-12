// Package store holds the replicated control-plane state (policies, bandwidth
// mode, tenant identity mapping) with atomic file persistence and a
// last-writer-wins version used for multi-node synchronisation.
package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"s3qos/internal/policy"
)

// Tenant maps S3 access keys and buckets (ownership) to a tenant identity.
type Tenant struct {
	AccessKeys []string `json:"accessKeys"`
	Buckets    []string `json:"buckets"`
}

type State struct {
	Version       int64             `json:"version"`
	BandwidthMode string            `json:"bandwidthMode"`
	Enforcement   string            `json:"enforcement"` // "node" (each proxy enforces the full limit) or "fleet"
	Policies      []policy.Policy   `json:"policies"`
	Tenants       map[string]Tenant `json:"tenants"`
}

type index struct {
	byKey    map[string]string
	byBucket map[string]string
}

type Store struct {
	mu       sync.RWMutex
	path     string
	st       State
	idx      atomic.Pointer[index]
	subs     []func(State)
	validate func(State) error
}

// NewID returns a random policy identifier.
func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Open loads (or initialises) the state file. validate is called before any
// state is accepted; it may be nil.
func Open(path string, validate func(State) error) (*Store, error) {
	s := &Store{path: path, validate: validate}
	s.st = State{BandwidthMode: policy.ModeAggregate, Enforcement: "node", Tenants: map[string]Tenant{}}
	if path != "" {
		b, err := os.ReadFile(path)
		switch {
		case err == nil:
			if err := json.Unmarshal(b, &s.st); err != nil {
				return nil, fmt.Errorf("parse %s: %w", path, err)
			}
		case !os.IsNotExist(err):
			return nil, err
		}
	}
	if s.st.BandwidthMode == "" {
		s.st.BandwidthMode = policy.ModeAggregate
	}
	if s.st.Enforcement == "" {
		s.st.Enforcement = "node"
	}
	if s.st.Tenants == nil {
		s.st.Tenants = map[string]Tenant{}
	}
	if validate != nil {
		if err := validate(s.st); err != nil {
			return nil, fmt.Errorf("state %s invalid: %w", path, err)
		}
	}
	s.reindex()
	return s, nil
}

func clone(st State) State {
	b, _ := json.Marshal(st)
	var out State
	_ = json.Unmarshal(b, &out)
	if out.Tenants == nil {
		out.Tenants = map[string]Tenant{}
	}
	return out
}

// Snapshot returns a deep copy of the current state.
func (s *Store) Snapshot() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return clone(s.st)
}

func (s *Store) Version() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.Version
}

// Subscribe registers a callback invoked after every accepted change.
func (s *Store) Subscribe(f func(State)) {
	s.mu.Lock()
	s.subs = append(s.subs, f)
	s.mu.Unlock()
}

// Notify invokes subscribers with the current state (used at start-up).
func (s *Store) Notify() {
	s.mu.RLock()
	st := clone(s.st)
	subs := append([]func(State){}, s.subs...)
	s.mu.RUnlock()
	for _, f := range subs {
		f(st)
	}
}

// Update applies fn to a copy of the state, validates, bumps the version,
// persists and notifies subscribers.
func (s *Store) Update(fn func(*State) error) error {
	s.mu.Lock()
	next := clone(s.st)
	if err := fn(&next); err != nil {
		s.mu.Unlock()
		return err
	}
	if err := s.accept(next, false); err != nil {
		s.mu.Unlock()
		return err
	}
	subs := append([]func(State){}, s.subs...)
	out := clone(s.st)
	s.mu.Unlock()
	for _, f := range subs {
		f(out)
	}
	return nil
}

// Replace installs a state received from a peer if it is newer.
func (s *Store) Replace(next State) (bool, error) {
	s.mu.Lock()
	if next.Version <= s.st.Version {
		s.mu.Unlock()
		return false, nil
	}
	next = clone(next)
	if err := s.accept(next, true); err != nil {
		s.mu.Unlock()
		return false, err
	}
	subs := append([]func(State){}, s.subs...)
	out := clone(s.st)
	s.mu.Unlock()
	for _, f := range subs {
		f(out)
	}
	return true, nil
}

// accept must be called with s.mu held.
func (s *Store) accept(next State, keepVersion bool) error {
	if err := check(next); err != nil {
		return err
	}
	if s.validate != nil {
		if err := s.validate(next); err != nil {
			return err
		}
	}
	if !keepVersion {
		v := time.Now().UnixNano()
		if v <= s.st.Version {
			v = s.st.Version + 1
		}
		next.Version = v
	}
	if err := s.persist(next); err != nil {
		return err
	}
	s.st = next
	s.reindex()
	return nil
}

func check(st State) error {
	names := map[string]bool{}
	ids := map[string]bool{}
	for _, p := range st.Policies {
		if p.ID == "" {
			return fmt.Errorf("policy %q has no id", p.Name)
		}
		if names[p.Name] {
			return fmt.Errorf("duplicate policy name %q", p.Name)
		}
		if ids[p.ID] {
			return fmt.Errorf("duplicate policy id %q", p.ID)
		}
		names[p.Name], ids[p.ID] = true, true
	}
	keys, buckets := map[string]string{}, map[string]string{}
	for t, v := range st.Tenants {
		if t == "" {
			return fmt.Errorf("tenant id must not be empty")
		}
		for _, k := range v.AccessKeys {
			if o, ok := keys[k]; ok && o != t {
				return fmt.Errorf("access key %q mapped to both %q and %q", k, o, t)
			}
			keys[k] = t
		}
		for _, b := range v.Buckets {
			if o, ok := buckets[b]; ok && o != t {
				return fmt.Errorf("bucket %q owned by both %q and %q", b, o, t)
			}
			buckets[b] = t
		}
	}
	return nil
}

func (s *Store) persist(st State) error {
	if s.path == "" {
		return nil
	}
	sort.Slice(st.Policies, func(i, j int) bool { return st.Policies[i].Name < st.Policies[j].Name })
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Store) reindex() {
	ix := &index{byKey: map[string]string{}, byBucket: map[string]string{}}
	for t, v := range s.st.Tenants {
		for _, k := range v.AccessKeys {
			ix.byKey[k] = t
		}
		for _, b := range v.Buckets {
			ix.byBucket[b] = t
		}
	}
	s.idx.Store(ix)
}

// TenantByKey implements classify.Resolver.
func (s *Store) TenantByKey(ak string) (string, bool) {
	t, ok := s.idx.Load().byKey[ak]
	return t, ok
}

// TenantByBucket implements classify.Resolver.
func (s *Store) TenantByBucket(b string) (string, bool) {
	t, ok := s.idx.Load().byBucket[b]
	return t, ok
}
