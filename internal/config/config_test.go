package config

import (
	"strings"
	"testing"
)

func base() *Config {
	return &Config{
		AdminToken: "0123456789abcdef",
		Endpoints:  []Endpoint{{Name: "a", Listen: ":8443", Backends: []string{"http://10.0.0.1:80"}}},
	}
}

func TestMultipleListenAddresses(t *testing.T) {
	c := base()
	c.Endpoints[0].Listeners = []string{"10.1.0.5:443", "10.1.0.6:443", "10.1.0.5:9443"}
	if err := c.Normalize(); err != nil {
		t.Fatal(err)
	}
	if got := c.Endpoints[0].Addrs(); len(got) != 4 || got[0] != ":8443" {
		t.Fatalf("addrs = %v", got)
	}
	p := c.Public().Endpoints[0]
	if len(p.Listeners) != 4 || !strings.Contains(p.Listen, "10.1.0.6:443") {
		t.Fatalf("public view = %+v", p)
	}
}

func TestListenersOnlyIsEnough(t *testing.T) {
	c := base()
	c.Endpoints[0].Listen = ""
	c.Endpoints[0].Listeners = []string{"10.1.0.5:443"}
	if err := c.Normalize(); err != nil {
		t.Fatal(err)
	}
}

func TestListenValidation(t *testing.T) {
	for name, mut := range map[string]func(*Config){
		"no address":         func(c *Config) { c.Endpoints[0].Listen = "" },
		"missing port":       func(c *Config) { c.Endpoints[0].Listeners = []string{"10.1.0.5"} },
		"duplicate in list":  func(c *Config) { c.Endpoints[0].Listeners = []string{":8443"} },
		"clashes with admin": func(c *Config) { c.AdminListen = "10.1.0.5:443"; c.Endpoints[0].Listeners = []string{"10.1.0.5:443"} },
		"clashes with other endpoint": func(c *Config) {
			c.Endpoints = append(c.Endpoints, Endpoint{Name: "b", Listen: ":1", Listeners: []string{":8443"}, Backends: []string{"http://h:1"}})
		},
		"bad duration":    func(c *Config) { c.Endpoints[0].HealthInterval = "soon" },
		"bad health path": func(c *Config) { c.Endpoints[0].HealthPath = "health" },
	} {
		c := base()
		mut(c)
		if err := c.Normalize(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestPublicViewReportsBalancing(t *testing.T) {
	c := base()
	c.Endpoints[0].Backends = []string{"http://10.0.0.1:80", "http://10.0.0.1:8080"}
	c.Endpoints[0].BackendWeights = map[string]int{"http://10.0.0.1:8080": 3}
	c.Endpoints[0].HealthPath = "/"
	if err := c.Normalize(); err != nil {
		t.Fatal(err)
	}
	p := c.Public().Endpoints[0]
	if p.Balance != "roundrobin" || p.Weights["http://10.0.0.1:80"] != 1 || p.Weights["http://10.0.0.1:8080"] != 3 || p.HealthPath != "/" {
		t.Fatalf("public view = %+v", p)
	}
}
