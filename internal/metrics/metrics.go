// Package metrics is a minimal Prometheus text-format registry.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Counter struct{ v atomic.Uint64 }

func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	collectors []func(w io.Writer)
}

func New() *Registry { return &Registry{counters: map[string]*Counter{}} }

var escaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// Label formats name{k="v",...} from alternating key/value arguments.
func Label(name string, kv ...string) string {
	if len(kv) == 0 {
		return name
	}
	var b strings.Builder
	b.WriteString(name + "{")
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, kv[i], escaper.Replace(kv[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

// Counter returns (creating if needed) the counter for name and labels.
func (r *Registry) Counter(name string, kv ...string) *Counter {
	key := Label(name, kv...)
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[key]
	if !ok {
		c = &Counter{}
		r.counters[key] = c
	}
	return c
}

// AddCollector registers a function that writes extra (gauge) lines.
func (r *Registry) AddCollector(f func(w io.Writer)) {
	r.mu.Lock()
	r.collectors = append(r.collectors, f)
	r.mu.Unlock()
}

func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	keys := make([]string, 0, len(r.counters))
	for k := range r.counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := make([]uint64, len(keys))
	for i, k := range keys {
		vals[i] = r.counters[k].Value()
	}
	cols := append([]func(io.Writer){}, r.collectors...)
	r.mu.Unlock()
	for i, k := range keys {
		fmt.Fprintf(w, "%s %d\n", k, vals[i])
	}
	for _, c := range cols {
		c(w)
	}
}
