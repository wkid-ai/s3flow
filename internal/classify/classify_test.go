package classify

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func req(method, target, host string, hdr map[string]string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	if host != "" {
		r.Host = host
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	return r
}

func TestAccessKey(t *testing.T) {
	cases := []struct {
		name string
		r    *http.Request
		want string
	}{
		{"sigv4", req("GET", "/b/k", "", map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc"}), "AKIDEXAMPLE"},
		{"sigv2", req("GET", "/b/k", "", map[string]string{"Authorization": "AWS AKIDV2:c2lnbmF0dXJl"}), "AKIDV2"},
		{"presigned v4", req("GET", "/b/k?X-Amz-Credential=AKPRE%2F20240101%2Fus%2Fs3%2Faws4_request&X-Amz-Signature=x", "", nil), "AKPRE"},
		{"presigned v2", req("GET", "/b/k?AWSAccessKeyId=AKP2&Signature=x", "", nil), "AKP2"},
		{"anonymous", req("GET", "/b/k", "", nil), ""},
	}
	for _, tc := range cases {
		if got := AccessKey(tc.r); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

func TestBucket(t *testing.T) {
	c := &Classifier{BaseDomains: []string{"s3.example.com"}}
	cases := []struct {
		target, host, want string
	}{
		{"/mybucket/key", "s3.example.com", "mybucket"},
		{"/mybucket", "s3.example.com:443", "mybucket"},
		{"/key/with/slash", "vh.s3.example.com", "vh"},
		{"/", "s3.example.com", ""},
		{"/b%20x/k", "s3.example.com", "b x"},
	}
	for _, tc := range cases {
		if got := c.Bucket(req("GET", tc.target, tc.host, nil)); got != tc.want {
			t.Errorf("%s %s: got %q want %q", tc.host, tc.target, got, tc.want)
		}
	}
}

type fakeRes struct{ keys, buckets map[string]string }

func (f fakeRes) TenantByKey(k string) (string, bool)    { t, ok := f.keys[k]; return t, ok }
func (f fakeRes) TenantByBucket(b string) (string, bool) { t, ok := f.buckets[b]; return t, ok }

func TestTenantResolution(t *testing.T) {
	c := &Classifier{Resolver: fakeRes{keys: map[string]string{"AK": "T-key"}, buckets: map[string]string{"anonbkt": "T-owner", "b": "T-owner"}}}
	signed := req("GET", "/b/k", "", map[string]string{"Authorization": "AWS AK:sig"})
	if a := c.Classify(signed, "e"); a.Tenant != "T-key" || a.Bucket != "b" || a.Endpoint != "e" {
		t.Errorf("signed: %+v", a)
	}
	if a := c.Classify(req("GET", "/anonbkt/k", "", nil), "e"); a.Tenant != "T-owner" {
		t.Errorf("anonymous should use bucket owner: %+v", a)
	}
	unknown := req("GET", "/b/k", "", map[string]string{"Authorization": "AWS ZZ:sig"})
	if a := c.Classify(unknown, "e"); a.Tenant != "" {
		t.Errorf("unknown key must not fall back to bucket owner: %+v", a)
	}
}

func TestClientIPTrust(t *testing.T) {
	_, n, _ := net.ParseCIDR("10.0.0.0/8")
	c := &Classifier{Trusted: []*net.IPNet{n}}
	r := req("GET", "/", "", map[string]string{"X-Forwarded-For": "203.0.113.9, 10.0.0.5"})
	r.RemoteAddr = "10.0.0.1:1234"
	if ip := c.ClientIP(r); ip.String() != "203.0.113.9" {
		t.Errorf("trusted hop: got %v", ip)
	}
	r.RemoteAddr = "198.51.100.7:1234" // untrusted peer spoofing XFF
	if ip := c.ClientIP(r); ip.String() != "198.51.100.7" {
		t.Errorf("untrusted peer must ignore XFF: got %v", ip)
	}
}
