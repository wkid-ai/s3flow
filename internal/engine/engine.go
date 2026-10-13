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

	"s3flow/internal/limit"
	"s3flow/internal/metrics"
	"s3flow/internal/policy"
	"s3flow/internal/store"
)

const (
	EnforceNode  = "node"  // each proxy enforces the full limit
	EnforceFleet = "fleet" // each proxy enforces limit / live-node-count
)

type runtime struct {
	c    atomic.Pointer[policy.Compiled]
	val  []atomic.Int64
	agg  [2]*limit.Bandwidth
	rate []*limit.RateLimiter // indexed by limit slot; nil for non-rate limits
	conc []*limit.Concurrency // indexed by limit slot; nil for non-concurrency limits

	requests, bytesIn, bytesOut *metrics.Counter
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
	rt := &runtime{val: make([]atomic.Int64, len(policy.LimitTypes)),
		rate: make([]*limit.RateLimiter, len(policy.LimitTypes)),
		conc: make([]*limit.Concurrency, len(policy.LimitTypes))}
	bw := func(slot int) *limit.Bandwidth {
		return &limit.Bandwidth{Rate: func() float64 { return float64(rt.val[slot].Load()) / e.share() }}
	}
	rt.agg[0], rt.agg[1] = bw(policy.LimitIndex(policy.AggIn)), bw(policy.LimitIndex(policy.AggOut))
	for slot, t := range policy.LimitTypes {
		slot := slot
		switch {
		case policy.IsRateLimit(t):
			rt.rate[slot] = &limit.RateLimiter{Rate: func() float64 { return float64(rt.val[slot].Load()) / e.share() }}
		case policy.IsConcLimit(t):
			rt.conc[slot] = &limit.Concurrency{Max: func() int64 {
				v := rt.val[slot].Load()
				if v <= 0 {
					return 0
				}
				return int64(math.Max(1, math.Ceil(float64(v)/e.share())))
			}}
		}
	}
	rt.requests = e.reg.Counter("s3flow_policy_requests_total", "policy", name)
	rt.bytesIn = e.reg.Counter("s3flow_policy_bytes_total", "policy", name, "direction", "in")
	rt.bytesOut = e.reg.Counter("s3flow_policy_bytes_total", "policy", name, "direction", "out")
	return rt
}

// Decision is the outcome of Admit for one request.
type Decision struct {
	Rejected  bool
	Reason    string // "rate" or "concurrency"
	Policy    string // policy that rejected
	Limit     string // limit type that rejected, e.g. "listRequestRate"
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
// (any matching policy may reject) and selects bandwidth shapers. A request is
// checked against its group limit (read/write) and, if set, its operation
// limit (get, list, put, delete, ...); every one of them must allow it.
func (e *Engine) Admit(a policy.Attrs, op policy.Operation) *Decision {
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
	reject := func(rt *runtime, reason, lim string) *Decision {
		name := rt.c.Load().Name
		d.Rejected, d.Reason, d.Policy, d.Limit = true, reason, name, lim
		e.reg.Counter("s3flow_policy_throttled_total", "policy", name, "reason", reason, "limit", lim).Add(1)
		return d
	}
	// Request rate: every matching policy must allow. Tokens already taken are
	// refunded if a later limit rejects, since the request never starts.
	var taken []*limit.RateLimiter
	for _, m := range ms {
		for _, t := range policy.RateLimits(op) {
			slot := policy.LimitIndex(t)
			if m.rt.val[slot].Load() <= 0 {
				continue
			}
			if !m.rt.rate[slot].Allow() {
				for _, rl := range taken {
					rl.Refund()
				}
				return reject(m.rt, "rate", t)
			}
			taken = append(taken, m.rt.rate[slot])
		}
	}
	// Concurrency: acquire from every matching policy, roll back on failure.
	for _, m := range ms {
		for _, t := range policy.ConcLimits(op) {
			slot := policy.LimitIndex(t)
			if m.rt.val[slot].Load() <= 0 {
				continue
			}
			cg := m.rt.conc[slot]
			if !cg.TryAcquire() {
				for _, f := range d.releases {
					f()
				}
				d.releases = nil
				for _, rl := range taken {
					rl.Refund()
				}
				return reject(m.rt, "concurrency", t)
			}
			d.releases = append(d.releases, cg.Release)
		}
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
		for slot, t := range policy.LimitTypes {
			cg := rt.conc[slot]
			if cg == nil || (rt.val[slot].Load() <= 0 && cg.Active() == 0) {
				continue
			}
			fmt.Fprintf(w, "%s %d\n", metrics.Label("s3flow_policy_active_requests", "policy", rt.c.Load().Name, "limit", t), cg.Active())
		}
	}
	fmt.Fprintf(w, "s3flow_fleet_share_divisor %g\n", e.share())
}
