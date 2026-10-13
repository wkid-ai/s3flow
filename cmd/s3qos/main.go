// Command s3qos is a transparent S3 front-end proxy with QoS
// for IBM Storage Ceph, Storage Scale CES S3 and IBM Cloud Object Storage.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"s3qos/internal/admin"
	"s3qos/internal/classify"
	"s3qos/internal/cluster"
	"s3qos/internal/config"
	"s3qos/internal/engine"
	"s3qos/internal/metrics"
	"s3qos/internal/proxy"
	"s3qos/internal/store"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to node configuration")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	interval, _ := cfg.Interval()
	trusted, _ := cfg.TrustedNets()

	reg := metrics.New()
	var cl *cluster.Cluster
	eng := engine.New(reg, cfg.EndpointNames(), func() int {
		if cl == nil {
			return 1
		}
		return cl.LiveNodes()
	})
	st, err := store.Open(cfg.StatePath, eng.Validate)
	if err != nil {
		log.Fatalf("state: %v", err)
	}
	cl = cluster.New(cfg.NodeID, cfg.Peers, cfg.ClusterToken, interval, st)
	st.Subscribe(eng.Apply)
	st.Notify()

	cls := &classify.Classifier{BaseDomains: cfg.BaseDomains, Trusted: trusted, Resolver: st}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go cl.Run(ctx)

	var servers []*http.Server
	errc := make(chan error, len(cfg.Endpoints)+1)
	serve := func(s *http.Server, cert, key string) {
		servers = append(servers, s)
		go func() {
			var err error
			if cert != "" {
				err = s.ListenAndServeTLS(cert, key)
			} else {
				err = s.ListenAndServe()
			}
			errc <- err
		}()
	}

	for _, e := range cfg.Endpoints {
		ep, err := proxy.NewEndpoint(proxy.EndpointConfig{
			Name: e.Name, Type: e.Type, Backends: e.Backends, CAFile: e.BackendCA, InsecureTLS: e.InsecureTLS,
		}, cls, eng, reg)
		if err != nil {
			log.Fatalf("endpoint: %v", err)
		}
		// No Read/WriteTimeout: they would cut off large streaming transfers.
		serve(&http.Server{Addr: e.Listen, Handler: ep, ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}, e.TLSCert, e.TLSKey)
		log.Printf("endpoint %q (%s) listening on %s -> %v", e.Name, e.Type, e.Listen, e.Backends)
	}

	api := &admin.API{Store: st, Cluster: cl, Metrics: reg, Token: cfg.AdminToken,
		Config: cfg.Public(), Endpoints: cfg.EndpointNames()}
	serve(&http.Server{Addr: cfg.AdminListen, Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}, cfg.AdminTLSCert, cfg.AdminTLSKey)
	log.Printf("node %q admin API on %s", cfg.NodeID, cfg.AdminListen)

	select {
	case <-ctx.Done():
	case err := <-errc:
		log.Printf("server stopped: %v", err)
	}
	sd, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(sd)
	}
}
