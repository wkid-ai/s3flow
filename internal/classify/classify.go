// Package classify extracts the S3 attributes needed for QoS (access key,
// bucket, operation class, client address) from a request WITHOUT modifying
// it, so the signed request reaches the backend byte-for-byte unchanged.
package classify

import (
	"net"
	"net/http"
	"net/url"
	"strings"

	"s3qos/internal/policy"
)

// Resolver maps identities to tenants.
type Resolver interface {
	TenantByKey(accessKey string) (string, bool)
	TenantByBucket(bucket string) (string, bool)
}

type Classifier struct {
	BaseDomains []string     // virtual-hosted-style suffixes, e.g. "s3.example.com"
	Trusted     []*net.IPNet // proxies whose X-Forwarded-For is honoured
	Resolver    Resolver
}

// AccessKey extracts the access key ID from SigV4/SigV2 headers or presigned
// query parameters. Returns "" for anonymous requests.
func AccessKey(r *http.Request) string {
	if a := r.Header.Get("Authorization"); a != "" {
		switch {
		case strings.HasPrefix(a, "AWS4-"):
			// AWS4-HMAC-SHA256 Credential=AKID/20240101/region/s3/aws4_request, ...
			if i := strings.Index(a, "Credential="); i >= 0 {
				c := a[i+len("Credential="):]
				if j := strings.IndexAny(c, "/,"); j >= 0 {
					return c[:j]
				}
				return c
			}
		case strings.HasPrefix(a, "AWS "):
			// AWS AKID:signature
			c := strings.TrimPrefix(a, "AWS ")
			if j := strings.IndexByte(c, ':'); j >= 0 {
				return c[:j]
			}
		}
		return ""
	}
	q := r.URL.Query()
	if c := q.Get("X-Amz-Credential"); c != "" {
		if j := strings.IndexByte(c, '/'); j >= 0 {
			return c[:j]
		}
		return c
	}
	return q.Get("AWSAccessKeyId")
}

// Bucket extracts the bucket from the Host (virtual-hosted) or the path.
func (c *Classifier) Bucket(r *http.Request) string {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, d := range c.BaseDomains {
		d = strings.ToLower(d)
		if host != d && strings.HasSuffix(host, "."+d) {
			return strings.TrimSuffix(host, "."+d)
		}
	}
	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if p == "" {
		return ""
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	if u, err := url.PathUnescape(p); err == nil {
		return u
	}
	return p
}

// ClientIP returns the originating address, honouring X-Forwarded-For only
// when the direct peer is a trusted proxy.
func (c *Classifier) ClientIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer == nil || !c.trusted(peer) {
		return peer
	}
	xff := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(xff) - 1; i >= 0; i-- {
		ip := net.ParseIP(strings.TrimSpace(xff[i]))
		if ip == nil {
			continue
		}
		if !c.trusted(ip) {
			return ip
		}
	}
	return peer
}

func (c *Classifier) trusted(ip net.IP) bool {
	for _, n := range c.Trusted {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// Classify returns the QoS attributes of a request.
func (c *Classifier) Classify(r *http.Request, endpoint string) policy.Attrs {
	a := policy.Attrs{Endpoint: endpoint, IP: c.ClientIP(r), Bucket: c.Bucket(r)}
	if c.Resolver == nil {
		return a
	}
	if ak := AccessKey(r); ak != "" {
		if t, ok := c.Resolver.TenantByKey(ak); ok {
			a.Tenant = t
		}
	} else if a.Bucket != "" {
		if t, ok := c.Resolver.TenantByBucket(a.Bucket); ok {
			a.Tenant = t
		}
	}
	return a
}

// IsRead reports whether the method is a read (GET/HEAD); everything else is a write.
func IsRead(method string) bool { return method == http.MethodGet || method == http.MethodHead }
