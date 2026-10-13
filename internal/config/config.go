// Package config loads the static (per-node) configuration. Dynamic QoS
// policy lives in the replicated store and is managed through the API.
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type Endpoint struct {
	Name string `json:"name"`
	// Listen is the primary bind address (host:port). Listeners adds more
	// addresses, each served identically, so one endpoint can accept clients on
	// several IPs and ports. At least one of the two is required.
	Listen    string   `json:"listen"`
	Listeners []string `json:"listeners,omitempty"`
	Type      string   `json:"type"` // ceph | scale | icos
	// Backends are the servers traffic is balanced across, each as
	// scheme://host:port. Different IPs and ports are separate targets.
	Backends []string `json:"backends"`
	// BackendWeights gives a backend (exactly as written in Backends) a relative
	// share of traffic; unlisted backends have weight 1.
	BackendWeights map[string]int `json:"backendWeights,omitempty"`
	Balance        string         `json:"balance,omitempty"`        // roundrobin (default) | leastconn
	DownFor        string         `json:"backendDownFor,omitempty"` // duration, default 5s
	HealthPath     string         `json:"healthPath,omitempty"`     // enables active probes, e.g. "/"
	HealthInterval string         `json:"healthInterval,omitempty"` // default 5s
	HealthTimeout  string         `json:"healthTimeout,omitempty"`  // default 2s
	TLSCert        string         `json:"tlsCert,omitempty"`
	TLSKey         string         `json:"tlsKey,omitempty"`
	BackendCA      string         `json:"backendCA,omitempty"`
	InsecureTLS    bool           `json:"backendInsecureSkipVerify,omitempty"`
}

// Addrs returns every address this endpoint binds, primary first.
func (e Endpoint) Addrs() []string {
	var out []string
	if e.Listen != "" {
		out = append(out, e.Listen)
	}
	return append(out, e.Listeners...)
}

// Durations parses the optional duration fields (zero means "use default").
func (e Endpoint) Durations() (down, interval, timeout time.Duration, err error) {
	parse := func(name, s string) (time.Duration, error) {
		if s == "" {
			return 0, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("endpoint %q: bad %s %q", e.Name, name, s)
		}
		return d, nil
	}
	if down, err = parse("backendDownFor", e.DownFor); err != nil {
		return
	}
	if interval, err = parse("healthInterval", e.HealthInterval); err != nil {
		return
	}
	timeout, err = parse("healthTimeout", e.HealthTimeout)
	return
}

type Config struct {
	NodeID          string     `json:"nodeId"`
	AdminListen     string     `json:"adminListen"`
	AdminToken      string     `json:"adminToken"`
	AdminTokenEnv   string     `json:"adminTokenEnv,omitempty"`
	ClusterToken    string     `json:"clusterToken"`
	ClusterTokenEnv string     `json:"clusterTokenEnv,omitempty"`
	AdminTLSCert    string     `json:"adminTlsCert,omitempty"`
	AdminTLSKey     string     `json:"adminTlsKey,omitempty"`
	Peers           []string   `json:"peers"`
	SyncInterval    string     `json:"syncInterval"`
	StatePath       string     `json:"statePath"`
	BaseDomains     []string   `json:"baseDomains"`
	TrustedProxies  []string   `json:"trustedProxies"`
	Endpoints       []Endpoint `json:"endpoints"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &c, c.Normalize()
}

func (c *Config) Normalize() error {
	if c.NodeID == "" {
		h, _ := os.Hostname()
		c.NodeID = h
	}
	if c.AdminListen == "" {
		c.AdminListen = "127.0.0.1:9090"
	}
	if c.StatePath == "" {
		c.StatePath = "state.json"
	}
	if c.AdminTokenEnv != "" {
		c.AdminToken = os.Getenv(c.AdminTokenEnv)
	}
	if c.ClusterTokenEnv != "" {
		c.ClusterToken = os.Getenv(c.ClusterTokenEnv)
	}
	if len(c.AdminToken) < 16 {
		return fmt.Errorf("adminToken (or adminTokenEnv) must be at least 16 characters")
	}
	if len(c.Peers) > 0 && len(c.ClusterToken) < 16 {
		return fmt.Errorf("clusterToken (or clusterTokenEnv) must be at least 16 characters when peers are configured")
	}
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("at least one endpoint is required")
	}
	names, listens := map[string]bool{}, map[string]bool{c.AdminListen: true}
	for i := range c.Endpoints {
		e := &c.Endpoints[i]
		if e.Name == "" || len(e.Addrs()) == 0 || len(e.Backends) == 0 {
			return fmt.Errorf("endpoint %d: name, listen (or listeners) and backends are required", i)
		}
		if names[e.Name] {
			return fmt.Errorf("duplicate endpoint name %q", e.Name)
		}
		for _, a := range e.Addrs() {
			if _, _, err := net.SplitHostPort(a); err != nil {
				return fmt.Errorf("endpoint %q: bad listen address %q (want host:port)", e.Name, a)
			}
			if listens[a] {
				return fmt.Errorf("listen address %q used more than once", a)
			}
			listens[a] = true
		}
		if (e.TLSCert == "") != (e.TLSKey == "") {
			return fmt.Errorf("endpoint %q: tlsCert and tlsKey must be set together", e.Name)
		}
		if _, _, _, err := e.Durations(); err != nil {
			return err
		}
		if e.HealthPath != "" && !strings.HasPrefix(e.HealthPath, "/") {
			return fmt.Errorf("endpoint %q: healthPath must start with /", e.Name)
		}
		names[e.Name] = true
	}
	if _, err := c.Interval(); err != nil {
		return err
	}
	if _, err := c.TrustedNets(); err != nil {
		return err
	}
	return nil
}

func (c *Config) Interval() (time.Duration, error) {
	if c.SyncInterval == "" {
		return 2 * time.Second, nil
	}
	d, err := time.ParseDuration(c.SyncInterval)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("bad syncInterval %q", c.SyncInterval)
	}
	return d, nil
}

func (c *Config) TrustedNets() ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, s := range c.TrustedProxies {
		if !strings.Contains(s, "/") {
			s += "/32"
		}
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			return nil, fmt.Errorf("bad trustedProxies entry %q", s)
		}
		out = append(out, n)
	}
	return out, nil
}

func (c *Config) EndpointNames() []string {
	var n []string
	for _, e := range c.Endpoints {
		n = append(n, e.Name)
	}
	return n
}

// PublicEndpoint is the non-secret view of an endpoint shown in the UI/API.
type PublicEndpoint struct {
	Name            string         `json:"name"`
	Type            string         `json:"type"`
	Listen          string         `json:"listen"` // all bind addresses, comma separated
	Listeners       []string       `json:"listeners"`
	Backends        []string       `json:"backends"`
	Weights         map[string]int `json:"weights"`
	Balance         string         `json:"balance"`
	HealthPath      string         `json:"healthPath"`
	TLS             bool           `json:"tls"`
	BackendCA       bool           `json:"backendCA"`
	BackendInsecure bool           `json:"backendInsecureSkipVerify"`
}

// Public is the configuration view exposed by the management API. It never
// contains tokens, key material or file paths of secrets.
type Public struct {
	NodeID         string           `json:"nodeId"`
	AdminListen    string           `json:"adminListen"`
	AdminTLS       bool             `json:"adminTls"`
	Peers          []string         `json:"peers"`
	SyncInterval   string           `json:"syncInterval"`
	StatePath      string           `json:"statePath"`
	BaseDomains    []string         `json:"baseDomains"`
	TrustedProxies []string         `json:"trustedProxies"`
	Endpoints      []PublicEndpoint `json:"endpoints"`
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// Public returns the sanitised configuration.
func (c *Config) Public() Public {
	p := Public{
		NodeID: c.NodeID, AdminListen: c.AdminListen, AdminTLS: c.AdminTLSCert != "",
		Peers: nonNil(c.Peers), SyncInterval: c.SyncInterval, StatePath: c.StatePath,
		BaseDomains: nonNil(c.BaseDomains), TrustedProxies: nonNil(c.TrustedProxies),
	}
	if p.SyncInterval == "" {
		p.SyncInterval = "2s"
	}
	for _, e := range c.Endpoints {
		bal := e.Balance
		if bal == "" {
			bal = "roundrobin"
		}
		w := map[string]int{}
		for _, b := range e.Backends {
			w[b] = 1
			if v, ok := e.BackendWeights[b]; ok {
				w[b] = v
			}
		}
		p.Endpoints = append(p.Endpoints, PublicEndpoint{
			Name: e.Name, Type: e.Type, Listen: strings.Join(e.Addrs(), ", "), Listeners: e.Addrs(),
			Backends: e.Backends, Weights: w, Balance: bal, HealthPath: e.HealthPath,
			TLS: e.TLSCert != "", BackendCA: e.BackendCA != "", BackendInsecure: e.InsecureTLS,
		})
	}
	return p
}
