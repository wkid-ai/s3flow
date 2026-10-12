# s3qos – transparent S3 front-end proxy with QoS

A single static Go binary (standard library only) that sits in front of
**IBM Storage Ceph (RGW)**, **Storage Scale CES S3** and **IBM Cloud Object
Storage (ICOS)** and applies traffic-classification policies (bandwidth,
request-rate and concurrency limits) to S3 traffic.

It is a *transparent reverse proxy*, not a gateway: requests and responses are
streamed. The request line, `Host`, signed headers and body are never altered,
so SigV4/SigV2 verification, bucket authorisation and S3 semantics stay with
the backend. This is covered by a test (`TestSignedRequestPassesThroughUnmodified`)
that runs a backend performing real SigV4 verification behind the proxy.

## Feature mapping

| Capability | s3qos |
|---|---|
| Matchers `bucket`, `bucket-regex`, `tenant`, `cidr` (IPv4), `endpoint`, `inverse` | same names/JSON; OR semantics; regex unanchored |
| Best-match bandwidth precedence (/32 IP, bucket, regex, tenant, endpoint, CIDR, inverse) | implemented; bandwidth taken from the single winning policy only |
| `aggregateBandwidthIn/Out` | shared limiter per policy |
| `perRequestBandwidthIn/Out` | independent limiter per request |
| Aggregate and per-request cannot be mixed | enforced via `bandwidthMode` setting (policies violating it are rejected with 422) |
| `read/writeRequestRate` | token bucket; GET/HEAD = read, others = write |
| `concurrentRead/WriteRequests` | counting gate; slots rolled back on partial failure |
| Over-limit behaviour | 250 ms delay then `503 SlowDown` (S3 XML error) – no queue |
| Request/concurrency limits apply from *any* matching policy | implemented |
| Tenant = access key ID, else bucket owner | `/api/v1/tenants` mapping (access keys and owned buckets); handles SigV4, SigV2 and presigned URLs |
| Management API | `/api/v1/traffic-classes` CRUD (PUT is a full replacement) |
| Observability | Prometheus `/metrics` |
| Multi-node | state replication + optional fleet-wide division (below) |

Not provided: a GUI, and any *guarantee* of backend performance – these are
ceilings, not reservations.

## Build / run

```sh
make test build
export S3QOS_ADMIN_TOKEN=$(openssl rand -hex 24)
export S3QOS_CLUSTER_TOKEN=$(openssl rand -hex 24)
./bin/s3qos -config config.example.json
```

Each `endpoint` is a listener (own port, optional TLS) with a list of backends
(round-robin with passive health: a backend that fails to connect is skipped for
5 s). Endpoint names are what the `endpoint` matcher refers to. Use one endpoint
per backend system (Ceph / Scale / ICOS) and/or per client-facing VIP.

`baseDomains` enables virtual-hosted-style bucket extraction
(`bucket.s3.example.com`); path-style always works. `trustedProxies` lists
upstream load balancers whose `X-Forwarded-For` is honoured for CIDR matching –
leave empty if clients connect directly.

## Management API

All `/api/*` and `/metrics` calls need `Authorization: Bearer $S3QOS_ADMIN_TOKEN`.

```sh
A="Authorization: Bearer $S3QOS_ADMIN_TOKEN"; API=http://localhost:9090/api/v1

# tenant identity mapping (access key -> tenant; bucket ownership for anonymous requests)
curl -XPUT -H "$A" $API/tenants/acme -d '{"accessKeys":["AKIAEXAMPLE"],"buckets":["backup-prod"]}'

# example policy (bandwidth values are bytes/second)
curl -XPOST -H "$A" $API/traffic-classes -d '{
  "name": "backup-prod-qos",
  "matchers": [{"type":"bucket","members":["backup-prod"],"inverse":false}],
  "limits": [
    {"type":"aggregateBandwidthIn","value":100000000},
    {"type":"aggregateBandwidthOut","value":200000000},
    {"type":"readRequestRate","value":2000},
    {"type":"writeRequestRate","value":500}]}'

curl -H "$A" $API/traffic-classes                 # list
curl -XPUT -H "$A" $API/traffic-classes/<id> -d @full-body.json   # full replacement
curl -XDELETE -H "$A" $API/traffic-classes/<id>

# global settings
curl -XPUT -H "$A" $API/settings -d '{"bandwidthMode":"aggregate","enforcement":"node"}'
```

Changes are validated, persisted atomically to `statePath`, applied immediately
(limiter state of unchanged policies is preserved) and pushed to peers.

Recommended rollout: create a policy with no limits first to collect baseline
metrics, then add limits.

## Multi-node behaviour (`enforcement`)

* **Policy replication** – every node holds the full state; writes go to any
  node, are pushed to peers and peers also poll every `syncInterval`
  (last-writer-wins on a nanosecond version). Peers authenticate with
  `clusterToken`; `/internal/state` is separate from the admin token.
* **`"enforcement": "node"`** (default) – every proxy enforces the full limit
  independently, so *N* nodes allow up to *N ×* the configured value.
* **`"enforcement": "fleet"`** – each node enforces `limit ÷ live-nodes`
  (liveness = peer answered a poll within 3 intervals). This approximates a
  fleet-wide ceiling with **no per-request network hop** and degrades safely
  (a dead peer's share is re-distributed after ~3 intervals). It assumes the
  front-end spreads clients evenly (DNS RR / L4 LB); skewed distribution leaves
  headroom unused. For exact global request counting a shared rate-limit
  service (e.g. Envoy RLS/Redis) would be needed; that is intentionally out of
  scope here.

## Operational notes and limitations

* Put an L4 load balancer or DNS round-robin in front of several s3qos nodes;
  clients must not bypass the proxy (restrict backend access by firewall).
* TLS is terminated at the proxy and re-originated to the backend with the
  original `Host` header preserved (it is part of the SigV4 signature). The
  backend must therefore accept the client-facing hostname.
* Rate is a ceiling on streaming throughput measured at the proxy; bandwidth
  shaping paces reads of the body in ≤64 KiB chunks with a 100 ms burst window.
* Only IPv4 CIDR matching. The regex dialect is Go RE2
  (a subset of PCRE; no look-around/back-references).
* Request-rate "started requests" are counted when headers arrive; bandwidth
  limits do not count requests rejected with 503.
* Not implemented: GUI, historical charts (use Prometheus/Grafana on
  `s3qos_policy_*`), STS/IAM integration, mutual-TLS for the admin API
  (terminate it with `adminTlsCert/adminTlsKey` and a network ACL instead).

## Metrics

`s3qos_requests_total{endpoint,type,class,code}`,
`s3qos_policy_requests_total{policy}`,
`s3qos_policy_throttled_total{policy,reason=rate|concurrency}`,
`s3qos_policy_bytes_total{policy,direction=in|out}`,
`s3qos_policy_active_requests{policy,class}`,
`s3qos_backend_up{endpoint,backend}`, `s3qos_fleet_share_divisor`,
`s3qos_request_seconds_sum_micro{endpoint,class}`.

## Layout

```
cmd/s3qos            main
internal/policy      matchers, limits, precedence, validation
internal/classify    access key / bucket / client IP / tenant extraction
internal/limit       bandwidth shaper, token bucket, concurrency gate
internal/engine      admission control + best-match bandwidth selection
internal/store       replicated state, persistence, tenant index
internal/cluster     peer sync, liveness
internal/proxy       streaming reverse proxy, backend pool, shaped bodies
internal/admin       REST API + /metrics
internal/config      node configuration
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
