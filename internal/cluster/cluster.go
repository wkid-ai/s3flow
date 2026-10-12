// Package cluster keeps the control-plane state identical on all proxy nodes
// (pull + push, last-writer-wins) and tracks how many nodes are alive so that
// fleet-wide limits can be divided between them.
package cluster

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"s3qos/internal/store"
)

type Wire struct {
	Node  string      `json:"node"`
	State store.State `json:"state"`
}

type Cluster struct {
	NodeID   string
	Peers    []string
	Token    string
	Interval time.Duration
	Store    *store.Store
	Client   *http.Client

	mu   sync.Mutex
	seen map[string]time.Time
}

func New(id string, peers []string, token string, interval time.Duration, st *store.Store) *Cluster {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Cluster{
		NodeID: id, Peers: peers, Token: token, Interval: interval, Store: st,
		Client: &http.Client{Timeout: interval * 2},
		seen:   map[string]time.Time{},
	}
}

// LiveNodes counts this node plus peers heard from within 3 intervals.
func (c *Cluster) LiveNodes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 1
	for _, t := range c.seen {
		if time.Since(t) <= 3*c.Interval {
			n++
		}
	}
	return n
}

// Run polls peers until ctx is cancelled.
func (c *Cluster) Run(ctx context.Context) {
	if len(c.Peers) == 0 {
		return
	}
	t := time.NewTicker(c.Interval)
	defer t.Stop()
	for {
		c.pullAll()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Cluster) pullAll() {
	var wg sync.WaitGroup
	for _, p := range c.Peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, strings.TrimRight(p, "/")+"/internal/state", nil)
			req.Header.Set("Authorization", "Bearer "+c.Token)
			resp, err := c.Client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return
			}
			var w Wire
			if json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&w) != nil {
				return
			}
			c.mu.Lock()
			c.seen[p] = time.Now()
			c.mu.Unlock()
			if ok, err := c.Store.Replace(w.State); err != nil {
				log.Printf("cluster: rejected state v%d from %s: %v", w.State.Version, p, err)
			} else if ok {
				log.Printf("cluster: adopted state v%d from %s", w.State.Version, w.Node)
			}
		}(p)
	}
	wg.Wait()
}

// Push sends the current state to every peer (best effort; pulls repair misses).
func (c *Cluster) Push() {
	body, _ := json.Marshal(Wire{Node: c.NodeID, State: c.Store.Snapshot()})
	for _, p := range c.Peers {
		go func(p string) {
			req, _ := http.NewRequest(http.MethodPut, strings.TrimRight(p, "/")+"/internal/state", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+c.Token)
			req.Header.Set("Content-Type", "application/json")
			if resp, err := c.Client.Do(req); err == nil {
				resp.Body.Close()
			}
		}(p)
	}
}

func (c *Cluster) authorised(r *http.Request) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return c.Token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(c.Token)) == 1
}

// Handler serves /internal/state (GET: export, PUT: import).
func (c *Cluster) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !c.authorised(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(Wire{Node: c.NodeID, State: c.Store.Snapshot()})
		case http.MethodPut:
			var in Wire
			if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&in); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if _, err := c.Store.Replace(in.State); err != nil {
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}
