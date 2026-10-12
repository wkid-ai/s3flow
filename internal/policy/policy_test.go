package policy

import (
	"net"
	"strings"
	"testing"
)

func mustCompile(t *testing.T, p Policy, mode string) *Compiled {
	t.Helper()
	c, err := Compile(p, mode, func(string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestORSemantics(t *testing.T) {
	c := mustCompile(t, Policy{Name: "p", Matchers: []Matcher{
		{Type: MatchTenant, Members: []string{"t1"}},
		{Type: MatchBucket, Members: []string{"b1"}},
	}}, ModeAggregate)
	if _, ok := c.Match(Attrs{Tenant: "t1", Bucket: "other"}); !ok {
		t.Fatal("tenant alone must match (OR)")
	}
	if _, ok := c.Match(Attrs{Tenant: "x", Bucket: "b1"}); !ok {
		t.Fatal("bucket alone must match (OR)")
	}
	if _, ok := c.Match(Attrs{Tenant: "x", Bucket: "y"}); ok {
		t.Fatal("neither should not match")
	}
}

func TestPrecedenceRanks(t *testing.T) {
	ip := net.ParseIP("10.1.2.3")
	cases := []struct {
		m    Matcher
		want int
	}{
		{Matcher{Type: MatchCIDR, Members: []string{"10.1.2.3/32"}}, rankExactIP},
		{Matcher{Type: MatchBucket, Members: []string{"b"}}, rankBucket},
		{Matcher{Type: MatchBucketRegex, Members: []string{"^b"}}, rankRegex},
		{Matcher{Type: MatchTenant, Members: []string{"t"}}, rankTenant},
		{Matcher{Type: MatchEndpoint, Members: []string{"e"}}, rankEndpoint},
		{Matcher{Type: MatchCIDR, Members: []string{"10.0.0.0/8"}}, rankCIDR},
		{Matcher{Type: MatchBucket, Members: []string{"zzz"}, Inverse: true}, rankInverse},
	}
	prev := -1
	for _, tc := range cases {
		c := mustCompile(t, Policy{Name: "p", Matchers: []Matcher{tc.m}}, ModeAggregate)
		r, ok := c.Match(Attrs{Tenant: "t", Bucket: "b", Endpoint: "e", IP: ip})
		if !ok || r != tc.want {
			t.Fatalf("%v: rank=%d ok=%v want %d", tc.m, r, ok, tc.want)
		}
		if r <= prev {
			t.Fatalf("ranks must be strictly increasing, got %d after %d", r, prev)
		}
		prev = r
	}
}

func TestRegexUnanchored(t *testing.T) {
	c := mustCompile(t, Policy{Name: "p", Matchers: []Matcher{{Type: MatchBucketRegex, Members: []string{"prod"}}}}, ModeAggregate)
	if _, ok := c.Match(Attrs{Bucket: "my-prod-data"}); !ok {
		t.Fatal("unanchored regex should match substring")
	}
	c = mustCompile(t, Policy{Name: "p", Matchers: []Matcher{{Type: MatchBucketRegex, Members: []string{"^prod$"}}}}, ModeAggregate)
	if _, ok := c.Match(Attrs{Bucket: "my-prod-data"}); ok {
		t.Fatal("anchored regex must not match substring")
	}
}

func TestInverseCanMatchEverything(t *testing.T) {
	c := mustCompile(t, Policy{Name: "p", Matchers: []Matcher{{Type: MatchBucket, Members: []string{"b"}, Inverse: true}}}, ModeAggregate)
	if _, ok := c.Match(Attrs{Bucket: "anything"}); !ok {
		t.Fatal("inverse should match non-members")
	}
	if _, ok := c.Match(Attrs{Bucket: "b"}); ok {
		t.Fatal("inverse should not match member")
	}
}

func TestValidation(t *testing.T) {
	ok := func(string) bool { return true }
	bad := []struct {
		name string
		p    Policy
		mode string
		want string
	}{
		{"noname", Policy{}, ModeAggregate, "name"},
		{"dup limit", Policy{Name: "a", Limits: []Limit{{AggIn, 1}, {AggIn, 2}}}, ModeAggregate, "more than once"},
		{"unknown limit", Policy{Name: "a", Limits: []Limit{{"nope", 1}}}, ModeAggregate, "unknown limit"},
		{"zero", Policy{Name: "a", Limits: []Limit{{AggIn, 0}}}, ModeAggregate, "positive"},
		{"req in agg mode", Policy{Name: "a", Limits: []Limit{{ReqIn, 1}}}, ModeAggregate, "cannot be mixed"},
		{"agg in req mode", Policy{Name: "a", Limits: []Limit{{AggOut, 1}}}, ModePerRequest, "cannot be mixed"},
		{"bad regex", Policy{Name: "a", Matchers: []Matcher{{Type: MatchBucketRegex, Members: []string{"("}}}}, ModeAggregate, "regex"},
		{"ipv6", Policy{Name: "a", Matchers: []Matcher{{Type: MatchCIDR, Members: []string{"::1/128"}}}}, ModeAggregate, "IPv4"},
		{"no members", Policy{Name: "a", Matchers: []Matcher{{Type: MatchBucket}}}, ModeAggregate, "no members"},
		{"bad type", Policy{Name: "a", Matchers: []Matcher{{Type: "x", Members: []string{"a"}}}}, ModeAggregate, "unknown matcher"},
	}
	for _, tc := range bad {
		_, err := Compile(tc.p, tc.mode, ok)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err=%v want contains %q", tc.name, err, tc.want)
		}
	}
	if _, err := Compile(Policy{Name: "a", Matchers: []Matcher{{Type: MatchEndpoint, Members: []string{"zz"}}}}, ModeAggregate, func(string) bool { return false }); err == nil {
		t.Error("unknown endpoint should be rejected")
	}
}

func TestOperationLimitTypes(t *testing.T) {
	ok := func(string) bool { return true }
	all := []Limit{{"getRequestRate", 1}, {"headRequestRate", 1}, {"listRequestRate", 1}, {"putRequestRate", 1},
		{"copyRequestRate", 1}, {"multipartRequestRate", 1}, {"deleteRequestRate", 1},
		{"concurrentGetRequests", 1}, {"concurrentHeadRequests", 1}, {"concurrentListRequests", 1}, {"concurrentPutRequests", 1},
		{"concurrentCopyRequests", 1}, {"concurrentMultipartRequests", 1}, {"concurrentDeleteRequests", 1}}
	if _, err := Compile(Policy{Name: "a", Limits: all}, ModeAggregate, ok); err != nil {
		t.Fatalf("all per-operation limits must be accepted: %v", err)
	}
	if _, err := Compile(Policy{Name: "a", Limits: []Limit{{"listRequestRate", 1}, {"listRequestRate", 2}}}, ModeAggregate, ok); err == nil {
		t.Fatal("duplicate per-operation limit must be rejected")
	}
	if got := RateLimits(OpDelete); len(got) != 2 || got[0] != WriteRate || got[1] != "deleteRequestRate" {
		t.Fatalf("delete rate limits: %v", got)
	}
	if got := ConcLimits(OpList); len(got) != 2 || got[0] != ConcRead || got[1] != "concurrentListRequests" {
		t.Fatalf("list conc limits: %v", got)
	}
	if got := RateLimits(OpOther); len(got) != 1 || got[0] != WriteRate {
		t.Fatalf("other: %v", got)
	}
}
