// Package policy defines traffic classification policies:
// matchers (OR semantics), limits, best-match precedence and validation.
package policy

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

// Limit type names (as used in the management API JSON).
const (
	AggIn     = "aggregateBandwidthIn"
	AggOut    = "aggregateBandwidthOut"
	ReqIn     = "perRequestBandwidthIn"
	ReqOut    = "perRequestBandwidthOut"
	ReadRate  = "readRequestRate"
	WriteRate = "writeRequestRate"
	ConcRead  = "concurrentReadRequests"
	ConcWrite = "concurrentWriteRequests"
)

// LimitTypes fixes the index used for per-policy limit storage.
var LimitTypes = []string{AggIn, AggOut, ReqIn, ReqOut, ReadRate, WriteRate, ConcRead, ConcWrite}

// LimitIndex returns the slot of a limit type or -1.
func LimitIndex(t string) int {
	for i, n := range LimitTypes {
		if n == t {
			return i
		}
	}
	return -1
}

// Bandwidth modes (a configuration-wide decision).
const (
	ModeAggregate  = "aggregate"
	ModePerRequest = "perRequest"
)

// Matcher types.
const (
	MatchBucket      = "bucket"
	MatchBucketRegex = "bucket-regex"
	MatchTenant      = "tenant"
	MatchCIDR        = "cidr"
	MatchEndpoint    = "endpoint"
)

// Precedence ranks for bandwidth best-match (lower wins).
const (
	rankExactIP = iota
	rankBucket
	rankRegex
	rankTenant
	rankEndpoint
	rankCIDR
	rankInverse
)

type Matcher struct {
	Type    string   `json:"type"`
	Members []string `json:"members"`
	Inverse bool     `json:"inverse"`
}

type Limit struct {
	Type  string `json:"type"`
	Value int64  `json:"value"`
}

type Policy struct {
	ID          string    `json:"id,omitempty"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Matchers    []Matcher `json:"matchers"`
	Limits      []Limit   `json:"limits"`
}

// Attrs are the classified attributes of one request.
type Attrs struct {
	Tenant   string
	Bucket   string
	Endpoint string
	IP       net.IP
}

type compiledMatcher struct {
	typ     string
	inverse bool
	strs    map[string]struct{}
	res     []*regexp.Regexp
	nets    []*net.IPNet
}

// Compiled is a validated policy ready for matching.
type Compiled struct {
	Policy
	matchers []compiledMatcher
}

// Compile validates p and prepares it for matching. endpointOK may be nil.
func Compile(p Policy, mode string, endpointOK func(string) bool) (*Compiled, error) {
	if strings.TrimSpace(p.Name) == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	seen := map[string]bool{}
	for _, l := range p.Limits {
		if LimitIndex(l.Type) < 0 {
			return nil, fmt.Errorf("unknown limit type %q", l.Type)
		}
		if seen[l.Type] {
			return nil, fmt.Errorf("limit type %q specified more than once", l.Type)
		}
		seen[l.Type] = true
		if l.Value <= 0 {
			return nil, fmt.Errorf("limit %q must be a positive integer", l.Type)
		}
		isReq := l.Type == ReqIn || l.Type == ReqOut
		isAgg := l.Type == AggIn || l.Type == AggOut
		if mode == ModeAggregate && isReq {
			return nil, fmt.Errorf("%s not allowed while bandwidthMode is %q (aggregate and per-request limiting cannot be mixed)", l.Type, mode)
		}
		if mode == ModePerRequest && isAgg {
			return nil, fmt.Errorf("%s not allowed while bandwidthMode is %q (aggregate and per-request limiting cannot be mixed)", l.Type, mode)
		}
	}
	c := &Compiled{Policy: p}
	for i, m := range p.Matchers {
		if len(m.Members) == 0 {
			return nil, fmt.Errorf("matcher %d (%s) has no members", i, m.Type)
		}
		cm := compiledMatcher{typ: m.Type, inverse: m.Inverse, strs: map[string]struct{}{}}
		for _, mem := range m.Members {
			if strings.TrimSpace(mem) == "" {
				return nil, fmt.Errorf("matcher %d (%s) has an empty member", i, m.Type)
			}
			switch m.Type {
			case MatchBucket, MatchTenant:
				cm.strs[mem] = struct{}{}
			case MatchEndpoint:
				if endpointOK != nil && !endpointOK(mem) {
					return nil, fmt.Errorf("unknown endpoint %q", mem)
				}
				cm.strs[mem] = struct{}{}
			case MatchBucketRegex:
				re, err := regexp.Compile(mem)
				if err != nil {
					return nil, fmt.Errorf("bad bucket-regex %q: %v", mem, err)
				}
				cm.res = append(cm.res, re)
			case MatchCIDR:
				s := mem
				if !strings.Contains(s, "/") {
					s += "/32"
				}
				_, n, err := net.ParseCIDR(s)
				if err != nil || n.IP.To4() == nil {
					return nil, fmt.Errorf("cidr %q must be a valid IPv4 CIDR", mem)
				}
				cm.nets = append(cm.nets, n)
			default:
				return nil, fmt.Errorf("unknown matcher type %q", m.Type)
			}
		}
		c.matchers = append(c.matchers, cm)
	}
	return c, nil
}

// Match reports whether any matcher matches (OR semantics) and the best
// precedence rank among the matchers that matched.
func (c *Compiled) Match(a Attrs) (rank int, ok bool) {
	rank = -1
	for _, m := range c.matchers {
		hit, r := m.eval(a)
		if hit && (rank < 0 || r < rank) {
			rank = r
		}
	}
	return rank, rank >= 0
}

func (m compiledMatcher) eval(a Attrs) (bool, int) {
	matched, rank := false, 0
	switch m.typ {
	case MatchBucket:
		_, matched = m.strs[a.Bucket]
		rank = rankBucket
	case MatchTenant:
		_, matched = m.strs[a.Tenant]
		rank = rankTenant
	case MatchEndpoint:
		_, matched = m.strs[a.Endpoint]
		rank = rankEndpoint
	case MatchBucketRegex:
		rank = rankRegex
		if a.Bucket != "" {
			for _, re := range m.res {
				if re.MatchString(a.Bucket) {
					matched = true
					break
				}
			}
		}
	case MatchCIDR:
		rank = rankCIDR
		if ip := a.IP.To4(); ip != nil {
			for _, n := range m.nets {
				if n.Contains(ip) {
					matched = true
					if ones, _ := n.Mask.Size(); ones == 32 {
						rank = rankExactIP
					}
					break
				}
			}
		}
	}
	if m.inverse {
		return !matched, rankInverse
	}
	return matched, rank
}
