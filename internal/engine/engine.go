// Package engine evaluates classified requests against policies and applies
// admission control (request rate, concurrency) and returns bandwidth shapers.
package engine

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"s3qos/internal/limit"
	"s3qos/internal/metrics"
	"s3qos/internal/policy"
	"s3qos/internal/store"
)

const (
	EnforceNode  = "node"  // each proxy enforces the full limit
	EnforceFleet = "fleet" // each proxy enforces limit / live-node-count
)

type runtime struct {
	c    atomic.Pointer[policy.Compiled]
	val  [8]atomic.Int64
	agg  [2]*limit.Bandwidth
	rate [2]*limit.RateLimiter
	conc [2]*limit.Concurrency

	requests, rejRate, rejConc, bytesIn, bytesOut *metrics.Counter
}

type Engine struct {
	reg       *metrics.Registry
	endpoints map[string]bool
	nodes     func() int

	mu       sync.RWMutex
	rts      map[string]*runtime // by policy ID
	list     []*runtime
	mode     string
	enforce  string
	fleetDiv atomic.Bool
}

func New(reg *metrics.Registry, endpoints []string, nodes func() int) *Engine {
	e := &Engine{reg: reg, endpoints: map[string]bool{}, nodes: nodes, rts: map[string]*runtime{}}
	for _, n := range endpoints {
		e.endpoints[n] = true
	}
	if nodes == nil {
		e.nodes = func() int { return 1 }
	}
	reg.AddCollector(e.collect)
	return e
}

// Validate checks that every policy in st compiles. Used by the store.
func (e *Engine) Validate(st store.State) error {
	if st.BandwidthMode != policy.ModeAggregate && st.BandwidthMode != policy.ModePerRequest {
		return fmt.Errorf("bandwidthMode must be %q or %q", policy.ModeAggregate, policy.ModePerRequest)
	}
	if st.Enforcement != EnforceNode && st.Enforcement != EnforceFleet {
		return fmt.Errorf("enforcement must be %q or %q", EnforceNode, EnforceFleet)
	}
	for _, p := range st.Policies {
		if _, err := policy.Compile(p, st.BandwidthMode, func(n string) bool { return e.endpoints[n] }); err != nil {
			return fmt.Errorf("policy %q: %w", p.Name, err)
		}
	}
	return nil
}

// Apply installs the policies of st, preserving limiter state of unchanged IDs.
func (e *Engine) Apply(st store.State) {
	e.mu.Lock()
	defer e.mu.Unlock()
	next := make(map[string]*runtime, len(st.Policies))
	list := make([]*runtime, 0, len(st.Policies))
	for _, p := range st.Policies {
		c, err := policy.Compile(p, st.BandwidthMode, func(n string) bool { return e.endpoints[n] })
		if err != nil {
			continue // already rejected by Validate; skip defensively
		}
		rt := e.rts[p.ID]
		if rt == nil {
			rt = e.newRuntime(p.Name)
		}
		rt.c.Store(c)
		for i := range rt.val {
			rt.val[i].Store(0)
		}
		for _, l := range p.Limits {
			rt.val[policy.LimitIndex(l.Type)].Store(l.Value)
		}
		next[p.ID] = rt
		list = append(list, rt)
	}
	e.rts, e.list, e.mode, e.enforce = next, list, st.BandwidthMode, st.Enforcement
	e.fleetDiv.Store(st.Enforcement == EnforceFleet)
}

func (e *Engine) share() float64 {
	if !e.fleetDiv.Load() {
		return 1
	}
	if n := e.nodes(); n > 1 {
		return float64(n)
	}
	return 1
}

func (e *Engine) newRuntime(name string) *runtime {
	rt := &runtime{}
	idx := policy.LimitIndex
	bw := func(slot int) *limit.Bandwidth {
		return &limit.Bandwidth{Rate: func() float64 { return float64(rt.val[slot].Load()) / e.share() }}
	}
	rt.agg[0], rt.agg[1] = bw(idx(policy.AggIn)), bw(idx(policy.AggOut))
	for i, t := range []string{policy.ReadRate, policy.WriteRate} {
		slot := idx(t)
		rt.rate[i] = &limit.RateLimiter{Rate: func() float64 { return float64(rt.val[slot].Load()) / e.share() }}
	}
	for i, t := range []string{policy.ConcRead, policy.ConcWrite} {
		slot := idx(t)
		rt.conc[i] = &limit.Concurrency{Max: func() int64 {
			v := rt.val[slot].Load()
			if v <= 0 {
				return 0
			}
			return int64(math.Max(1, math.Ceil(float64(v)/e.share())))
		}}
	}
	rt.requests = e.reg.Counter("s3qos_policy_requests_total", "policy", name)
	rt.rejRate = e.reg.Counter("s3qos_policy_throttled_total", "policy", name, "reason", "rate")
	rt.rejConc = e.reg.Counter("s3qos_policy_throttled_total", "policy", name, "reason", "concurrency")
	rt.bytesIn = e.reg.Counter("s3qos_policy_bytes_total", "policy", name, "direction", "in")
	rt.bytesOut = e.reg.Counter("s3qos_policy_bytes_total", "policy", name, "direction", "out")
	return rt
}

// Decision is the outcome of Admit for one request.
type Decision struct {
	Rejected  bool
	Reason    string // "rate" or "concurrency"
	Policy    string // policy that rejected
	Matched   []string
	In, Out   *limit.Bandwidth // nil = unshaped
	BWPolicy  string           // policy selecting the bandwidth limits
	rts       []*runtime
	releases  []func()
	releaseMu sync.Once
}

// Done releases concurrency slots. Safe to call more than once.
func (d *Decision) Done() {
	d.releaseMu.Do(func() {
		for _, f := range d.releases {
			f()
		}
	})
}

func (d *Decision) AddIn(n int) {
	for _, rt := range d.rts {
		rt.bytesIn.Add(uint64(n))
	}
}

func (d *Decision) AddOut(n int) {
	for _, rt := range d.rts {
		rt.bytesOut.Add(uint64(n))
	}
}

// Admit classifies against all policies and enforces rate and concurrency
// (any matching policy may reject) and selects bandwidth shapers.
func (e *Engine) Admit(a policy.Attrs, read bool) *Decision {
	e.mu.RLock()
	list, mode := e.list, e.mode
	e.mu.RUnlock()

	d := &Decision{}
	type match struct {
		rt   *runtime
		c    *policy.Compiled
		rank int
	}
	var ms []match
	for _, rt := range list {
		c := rt.c.Load()
		if rank, ok := c.Match(a); ok {
			ms = append(ms, match{rt, c, rank})
			d.rts = append(d.rts, rt)
			d.Matched = append(d.Matched, c.Name)
			rt.requests.Add(1)
		}
	}
	dir := 1
	if read {
		dir = 0
	}
	// Request rate: every matching policy must allow.
	for _, m := range ms {
		if m.rt.val[policy.LimitIndex([]string{policy.ReadRate, policy.WriteRate}[dir])].Load() > 0 && !m.rt.rate[dir].Allow() {
			m.rt.rejRate.Add(1)
			d.Rejected, d.Reason, d.Policy = true, "rate", m.c.Name
			return d
		}
	}
	// Concurrency: acquire from every matching policy, roll back on failure.
	for _, m := range ms {
		if m.rt.val[policy.LimitIndex([]string{policy.ConcRead, policy.ConcWrite}[dir])].Load() <= 0 {
			continue
		}
		cg := m.rt.conc[dir]
		if !cg.TryAcquire() {
			m.rt.rejConc.Add(1)
			for _, f := range d.releases {
				f()
			}
			d.releases = nil
			d.Rejected, d.Reason, d.Policy = true, "concurrency", m.c.Name
			return d
		}
		d.releases = append(d.releases, cg.Release)
	}
	// Bandwidth: single best-match policy among those with a bandwidth limit.
	var best *match
	for i := range ms {
		m := &ms[i]
		has := false
		for _, t := range []string{policy.AggIn, policy.AggOut, policy.ReqIn, policy.ReqOut} {
			if m.rt.val[policy.LimitIndex(t)].Load() > 0 {
				has = true
			}
		}
		if !has {
			continue
		}
		if best == nil || m.rank < best.rank || (m.rank == best.rank && m.c.Name < best.c.Name) {
			best = m
		}
	}
	if best != nil {
		d.BWPolicy = best.c.Name
		rt := best.rt
		if mode == policy.ModeAggregate {
			if rt.val[policy.LimitIndex(policy.AggIn)].Load() > 0 {
				d.In = rt.agg[0]
			}
			if rt.val[policy.LimitIndex(policy.AggOut)].Load() > 0 {
				d.Out = rt.agg[1]
			}
		} else {
			if v := rt.val[policy.LimitIndex(policy.ReqIn)].Load(); v > 0 {
				d.In = &limit.Bandwidth{Rate: limit.ConstRate(float64(v))}
			}
			if v := rt.val[policy.LimitIndex(policy.ReqOut)].Load(); v > 0 {
				d.Out = &limit.Bandwidth{Rate: limit.ConstRate(float64(v))}
			}
		}
	}
	return d
}

func (e *Engine) collect(w io.Writer) {
	e.mu.RLock()
	list := append([]*runtime{}, e.list...)
	e.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool { return list[i].c.Load().Name < list[j].c.Load().Name })
	for _, rt := range list {
		n := rt.c.Load().Name
		fmt.Fprintf(w, "%s %d\n", metrics.Label("s3qos_policy_active_requests", "policy", n, "class", "read"), rt.conc[0].Active())
		fmt.Fprintf(w, "%s %d\n", metrics.Label("s3qos_policy_active_requests", "policy", n, "class", "write"), rt.conc[1].Active())
	}
	fmt.Fprintf(w, "s3qos_fleet_share_divisor %g\n", e.share())
}
