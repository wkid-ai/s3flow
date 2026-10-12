package classify

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"s3qos/internal/policy"
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

func TestOperationClassification(t *testing.T) {
	c := &Classifier{BaseDomains: []string{"s3.example.com"}}
	cases := []struct {
		name, method, target, host string
		hdr                        map[string]string
		want                       policy.Operation
	}{
		{"GetObject", "GET", "/b/k", "s3.example.com", nil, policy.OpGet},
		{"GetObject vhost", "GET", "/dir/k", "b.s3.example.com", nil, policy.OpGet},
		{"GetObject range", "GET", "/b/k?versionId=1", "s3.example.com", nil, policy.OpGet},
		{"HeadObject", "HEAD", "/b/k", "s3.example.com", nil, policy.OpHead},
		{"HeadBucket", "HEAD", "/b", "s3.example.com", nil, policy.OpHead},
		{"ListBuckets", "GET", "/", "s3.example.com", nil, policy.OpList},
		{"ListObjectsV2", "GET", "/b?list-type=2&prefix=x", "s3.example.com", nil, policy.OpList},
		{"ListObjectsV2 vhost", "GET", "/?list-type=2", "b.s3.example.com", nil, policy.OpList},
		{"ListObjectsV2 trailing slash", "GET", "/b/?list-type=2", "s3.example.com", nil, policy.OpList},
		{"ListVersions", "GET", "/b?versions", "s3.example.com", nil, policy.OpList},
		{"ListMultipartUploads", "GET", "/b?uploads", "s3.example.com", nil, policy.OpList},
		{"ListParts", "GET", "/b/k?uploadId=u", "s3.example.com", nil, policy.OpList},
		{"GetBucketAcl is a get", "GET", "/b?acl", "s3.example.com", nil, policy.OpGet},
		{"GetObjectTagging", "GET", "/b/k?tagging", "s3.example.com", nil, policy.OpGet},
		{"PutObject", "PUT", "/b/k", "s3.example.com", nil, policy.OpPut},
		{"CreateBucket", "PUT", "/b", "s3.example.com", nil, policy.OpPut},
		{"CopyObject", "PUT", "/b/k", "s3.example.com", map[string]string{"X-Amz-Copy-Source": "/b/src"}, policy.OpCopy},
		{"UploadPart", "PUT", "/b/k?partNumber=1&uploadId=u", "s3.example.com", nil, policy.OpMultipart},
		{"UploadPartCopy", "PUT", "/b/k?partNumber=1&uploadId=u", "s3.example.com", map[string]string{"X-Amz-Copy-Source": "/b/s"}, policy.OpMultipart},
		{"InitiateMultipart", "POST", "/b/k?uploads", "s3.example.com", nil, policy.OpMultipart},
		{"CompleteMultipart", "POST", "/b/k?uploadId=u", "s3.example.com", nil, policy.OpMultipart},
		{"AbortMultipart", "DELETE", "/b/k?uploadId=u", "s3.example.com", nil, policy.OpMultipart},
		{"DeleteObject", "DELETE", "/b/k", "s3.example.com", nil, policy.OpDelete},
		{"DeleteBucket", "DELETE", "/b", "s3.example.com", nil, policy.OpDelete},
		{"DeleteObjects", "POST", "/b?delete", "s3.example.com", nil, policy.OpDelete},
		{"POST select", "POST", "/b/k?select&select-type=2", "s3.example.com", nil, policy.OpPut},
		{"OPTIONS", "OPTIONS", "/b/k", "s3.example.com", nil, policy.OpOther},
	}
	for _, tc := range cases {
		if got := c.Operation(req(tc.method, tc.target, tc.host, tc.hdr)); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
	for _, op := range []policy.Operation{policy.OpGet, policy.OpHead, policy.OpList} {
		if !op.IsRead() {
			t.Errorf("%s should be in the read group", op)
		}
	}
	for _, op := range []policy.Operation{policy.OpPut, policy.OpCopy, policy.OpMultipart, policy.OpDelete, policy.OpOther} {
		if op.IsRead() {
			t.Errorf("%s should be in the write group", op)
		}
	}
}
