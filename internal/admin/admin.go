// Package admin exposes the management API
// (/api/v1/traffic-classes, settings, tenants), metrics and cluster sync.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"s3qos/internal/cluster"
	"s3qos/internal/metrics"
	"s3qos/internal/policy"
	"s3qos/internal/store"
	"s3qos/internal/ui"
)

type API struct {
	Store   *store.Store
	Cluster *cluster.Cluster
	Metrics *metrics.Registry
	Token   string // bearer token required for /api/*

	Config    any      // sanitised configuration shown by /api/v1/config
	Endpoints []string // endpoint names, offered by /api/v1/schema
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/internal/state", a.Cluster.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !a.authed(r) {
			unauthorized(w)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		a.Metrics.Write(w)
	})
	mux.HandleFunc("/api/v1/traffic-classes", a.auth(a.collection))
	mux.HandleFunc("/api/v1/traffic-classes/", a.auth(a.item))
	mux.HandleFunc("/api/v1/settings", a.auth(a.settings))
	mux.HandleFunc("/api/v1/tenants", a.auth(a.tenants))
	mux.HandleFunc("/api/v1/tenants/", a.auth(a.tenant))
	mux.HandleFunc("/api/v1/cluster", a.auth(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{
			"node": a.Cluster.NodeID, "liveNodes": a.Cluster.LiveNodes(),
			"peers": a.Cluster.PeerStatus(), "version": a.Store.Version(),
		})
	}))
	mux.HandleFunc("/api/v1/config", a.auth(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "success", "data": a.Config})
	}))
	mux.HandleFunc("/api/v1/schema", a.auth(a.schema))
	mux.Handle("/ui/", ui.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/ui/", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})
	return mux
}

func (a *API) authed(r *http.Request) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return a.Token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(a.Token)) == 1
}

func (a *API) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.authed(r) {
			unauthorized(w)
			return
		}
		h(w, r)
	}
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="s3qos"`)
	writeErr(w, 401, "unauthorized")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"status": "error", "code": code, "message": msg})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (a *API) commit(w http.ResponseWriter, okCode int, fn func(*store.State) error, result func() any) {
	err := a.Store.Update(fn)
	var nf notFound
	switch {
	case errors.As(err, &nf):
		writeErr(w, 404, err.Error())
		return
	case err != nil:
		writeErr(w, 422, err.Error())
		return
	}
	a.Cluster.Push()
	if result == nil {
		w.WriteHeader(okCode)
		return
	}
	writeJSON(w, okCode, map[string]any{"status": "success", "data": result()})
}

type notFound struct{ what string }

func (n notFound) Error() string { return n.what + " not found" }

// ---- traffic classes ------------------------------------------------------

func (a *API) collection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, map[string]any{"status": "success", "data": a.Store.Snapshot().Policies})
	case http.MethodPost:
		var p policy.Policy
		if err := decode(r, &p); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		p.ID = store.NewID()
		a.commit(w, 201, func(s *store.State) error {
			s.Policies = append(s.Policies, p)
			return nil
		}, func() any { return p })
	default:
		writeErr(w, 405, "method not allowed")
	}
}

func (a *API) item(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/traffic-classes/")
	snap := a.Store.Snapshot()
	idx := -1
	for i, p := range snap.Policies {
		if p.ID == id {
			idx = i
		}
	}
	if idx < 0 {
		writeErr(w, 404, "traffic class not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, map[string]any{"status": "success", "data": snap.Policies[idx]})
	case http.MethodPut: // full replacement
		var p policy.Policy
		if err := decode(r, &p); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		p.ID = id
		a.commit(w, 200, func(s *store.State) error {
			for i := range s.Policies {
				if s.Policies[i].ID == id {
					s.Policies[i] = p
					return nil
				}
			}
			return notFound{"traffic class"}
		}, func() any { return p })
	case http.MethodDelete:
		a.commit(w, 204, func(s *store.State) error {
			for i := range s.Policies {
				if s.Policies[i].ID == id {
					s.Policies = append(s.Policies[:i], s.Policies[i+1:]...)
					return nil
				}
			}
			return notFound{"traffic class"}
		}, nil)
	default:
		writeErr(w, 405, "method not allowed")
	}
}

// ---- settings -------------------------------------------------------------

type settings struct {
	BandwidthMode string `json:"bandwidthMode"`
	Enforcement   string `json:"enforcement"`
}

func (a *API) settings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s := a.Store.Snapshot()
		writeJSON(w, 200, map[string]any{"status": "success", "data": settings{s.BandwidthMode, s.Enforcement}})
	case http.MethodPut:
		var in settings
		if err := decode(r, &in); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		a.commit(w, 200, func(s *store.State) error {
			s.BandwidthMode, s.Enforcement = in.BandwidthMode, in.Enforcement
			return nil
		}, func() any { return in })
	default:
		writeErr(w, 405, "method not allowed")
	}
}

// ---- tenants --------------------------------------------------------------

func (a *API) tenants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, 405, "method not allowed")
		return
	}
	writeJSON(w, 200, map[string]any{"status": "success", "data": a.Store.Snapshot().Tenants})
}

func (a *API) tenant(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/tenants/")
	if id == "" {
		writeErr(w, 404, "tenant id required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		t, ok := a.Store.Snapshot().Tenants[id]
		if !ok {
			writeErr(w, 404, "tenant not found")
			return
		}
		writeJSON(w, 200, map[string]any{"status": "success", "data": t})
	case http.MethodPut:
		var t store.Tenant
		if err := decode(r, &t); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		a.commit(w, 200, func(s *store.State) error { s.Tenants[id] = t; return nil }, func() any { return t })
	case http.MethodDelete:
		a.commit(w, 204, func(s *store.State) error {
			if _, ok := s.Tenants[id]; !ok {
				return notFound{"tenant"}
			}
			delete(s.Tenants, id)
			return nil
		}, nil)
	default:
		writeErr(w, 405, "method not allowed")
	}
}

// ---- schema ---------------------------------------------------------------

type limitInfo struct {
	Type      string `json:"type"`
	Kind      string `json:"kind"`      // bandwidth | rate | concurrency
	Scope     string `json:"scope"`     // group name or operation name
	Operation string `json:"operation"` // empty for group/bandwidth limits
}

// schema tells the UI which limit types, matcher types and endpoints exist so
// it never has to hard-code them.
func (a *API) schema(w http.ResponseWriter, _ *http.Request) {
	var limits []limitInfo
	for _, t := range policy.LimitTypes {
		li := limitInfo{Type: t}
		switch {
		case policy.IsRateLimit(t):
			li.Kind = "rate"
		case policy.IsConcLimit(t):
			li.Kind = "concurrency"
		default:
			li.Kind = "bandwidth"
		}
		li.Scope = "bandwidth"
		switch t {
		case policy.ReadRate, policy.ConcRead:
			li.Scope = "read"
		case policy.WriteRate, policy.ConcWrite:
			li.Scope = "write"
		}
		for _, s := range policy.OpSpecs {
			if t == s.Rate || t == s.Conc {
				li.Scope, li.Operation = "operation", string(s.Op)
			}
		}
		limits = append(limits, li)
	}
	var ops []map[string]any
	for _, s := range policy.OpSpecs {
		ops = append(ops, map[string]any{"operation": s.Op, "read": s.Op.IsRead()})
	}
	writeJSON(w, 200, map[string]any{"status": "success", "data": map[string]any{
		"limits":         limits,
		"operations":     ops,
		"matchers":       []string{policy.MatchBucket, policy.MatchBucketRegex, policy.MatchTenant, policy.MatchCIDR, policy.MatchEndpoint},
		"bandwidthModes": []string{policy.ModeAggregate, policy.ModePerRequest},
		"enforcement":    []string{"node", "fleet"},
		"endpoints":      a.Endpoints,
	}})
}
