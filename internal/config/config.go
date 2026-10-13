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
	Name        string   `json:"name"`
	Listen      string   `json:"listen"`
	Type        string   `json:"type"` // ceph | scale | icos
	Backends    []string `json:"backends"`
	TLSCert     string   `json:"tlsCert,omitempty"`
	TLSKey      string   `json:"tlsKey,omitempty"`
	BackendCA   string   `json:"backendCA,omitempty"`
	InsecureTLS bool     `json:"backendInsecureSkipVerify,omitempty"`
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
		if e.Name == "" || e.Listen == "" || len(e.Backends) == 0 {
			return fmt.Errorf("endpoint %d: name, listen and backends are required", i)
		}
		if names[e.Name] {
			return fmt.Errorf("duplicate endpoint name %q", e.Name)
		}
		if listens[e.Listen] {
			return fmt.Errorf("listen address %q used more than once", e.Listen)
		}
		if (e.TLSCert == "") != (e.TLSKey == "") {
			return fmt.Errorf("endpoint %q: tlsCert and tlsKey must be set together", e.Name)
		}
		names[e.Name], listens[e.Listen] = true, true
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
	Name            string   `json:"name"`
	Type            string   `json:"type"`
	Listen          string   `json:"listen"`
	Backends        []string `json:"backends"`
	TLS             bool     `json:"tls"`
	BackendCA       bool     `json:"backendCA"`
	BackendInsecure bool     `json:"backendInsecureSkipVerify"`
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
		p.Endpoints = append(p.Endpoints, PublicEndpoint{
			Name: e.Name, Type: e.Type, Listen: e.Listen, Backends: e.Backends,
			TLS: e.TLSCert != "", BackendCA: e.BackendCA != "", BackendInsecure: e.InsecureTLS,
		})
	}
	return p
}
