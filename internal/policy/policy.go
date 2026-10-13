// Package policy defines traffic classification policies:
// matchers (OR semantics), limits, best-match precedence and validation.
package policy

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

// The Gold, Silver and Bronze tier IDs are well-known so nodes can identify
// and update them without creating duplicates after a restart or a re-seed.
const (
	IDGold   = "tier-gold-000000"
	IDSilver = "tier-silver-000000"
	IDBronze = "tier-bronze-000000"
)

// DefaultTiers returns the three built-in SLA tiers. They carry no matchers
// so they match nothing until an operator attaches an endpoint/tenant/CIDR
// matcher via the API or the web console.
func DefaultTiers() []Policy {
	return []Policy{
		{
			ID:          IDGold,
			Name:        "Gold",
			Description: "50 K req/s, 10 GB/s — attach via endpoint, tenant or CIDR matcher",
			Limits: []Limit{
				{Type: AggIn, Value: 10_000_000_000},
				{Type: AggOut, Value: 10_000_000_000},
				{Type: ReadRate, Value: 50_000},
				{Type: WriteRate, Value: 50_000},
			},
		},
		{
			ID:          IDSilver,
			Name:        "Silver",
			Description: "20 K req/s, 5 GB/s — attach via endpoint, tenant or CIDR matcher",
			Limits: []Limit{
				{Type: AggIn, Value: 5_000_000_000},
				{Type: AggOut, Value: 5_000_000_000},
				{Type: ReadRate, Value: 20_000},
				{Type: WriteRate, Value: 20_000},
			},
		},
		{
			ID:          IDBronze,
			Name:        "Bronze",
			Description: "10 K req/s, 1.25 GB/s — attach via endpoint, tenant or CIDR matcher",
			Limits: []Limit{
				{Type: AggIn, Value: 1_250_000_000},
				{Type: AggOut, Value: 1_250_000_000},
				{Type: ReadRate, Value: 10_000},
				{Type: WriteRate, Value: 10_000},
			},
		},
	}
}

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

// Operation is the S3 operation class of a request. Every operation belongs to
// either the read group (GET/HEAD/LIST) or the write group (everything else),
// so a request is governed by its group limit AND its own operation limit.
type Operation string

const (
	OpGet       Operation = "get"       // GetObject and object/bucket sub-resource reads
	OpHead      Operation = "head"      // HeadObject / HeadBucket
	OpList      Operation = "list"      // ListBuckets, ListObjects(V2), ListObjectVersions, ListMultipartUploads, ListParts
	OpPut       Operation = "put"       // PutObject, CreateBucket, sub-resource writes, POST uploads/select
	OpCopy      Operation = "copy"      // CopyObject (PUT with x-amz-copy-source)
	OpMultipart Operation = "multipart" // Initiate / UploadPart / UploadPartCopy / Complete / Abort
	OpDelete    Operation = "delete"    // DeleteObject, DeleteObjects (POST ?delete), DeleteBucket
	OpOther     Operation = "other"     // any other method; only group limits apply
)

// IsRead reports whether the operation belongs to the read group.
func (o Operation) IsRead() bool { return o == OpGet || o == OpHead || o == OpList }

// OpSpec names the optional per-operation limits.
type OpSpec struct {
	Op         Operation
	Rate, Conc string
}

// OpSpecs lists every operation that has dedicated limits.
var OpSpecs = []OpSpec{
	{OpGet, "getRequestRate", "concurrentGetRequests"},
	{OpHead, "headRequestRate", "concurrentHeadRequests"},
	{OpList, "listRequestRate", "concurrentListRequests"},
	{OpPut, "putRequestRate", "concurrentPutRequests"},
	{OpCopy, "copyRequestRate", "concurrentCopyRequests"},
	{OpMultipart, "multipartRequestRate", "concurrentMultipartRequests"},
	{OpDelete, "deleteRequestRate", "concurrentDeleteRequests"},
}

var (
	// LimitTypes fixes the slot used for per-policy limit storage.
	LimitTypes []string
	limitIdx   = map[string]int{}
	rateSet    = map[string]bool{}
	concSet    = map[string]bool{}
	rateByOp   = map[Operation][]string{}
	concByOp   = map[Operation][]string{}
)

func init() {
	LimitTypes = []string{AggIn, AggOut, ReqIn, ReqOut, ReadRate, WriteRate, ConcRead, ConcWrite}
	rateSet[ReadRate], rateSet[WriteRate] = true, true
	concSet[ConcRead], concSet[ConcWrite] = true, true
	for _, s := range OpSpecs {
		LimitTypes = append(LimitTypes, s.Rate, s.Conc)
		rateSet[s.Rate], concSet[s.Conc] = true, true
	}
	for i, t := range LimitTypes {
		limitIdx[t] = i
	}
	ops := []Operation{OpOther}
	for _, s := range OpSpecs {
		ops = append(ops, s.Op)
	}
	for _, op := range ops {
		rate, conc := WriteRate, ConcWrite
		if op.IsRead() {
			rate, conc = ReadRate, ConcRead
		}
		rateByOp[op], concByOp[op] = []string{rate}, []string{conc}
	}
	for _, s := range OpSpecs {
		rateByOp[s.Op] = append(rateByOp[s.Op], s.Rate)
		concByOp[s.Op] = append(concByOp[s.Op], s.Conc)
	}
}

// LimitIndex returns the slot of a limit type or -1.
func LimitIndex(t string) int {
	if i, ok := limitIdx[t]; ok {
		return i
	}
	return -1
}

// IsRateLimit / IsConcLimit report the kind of a limit type.
func IsRateLimit(t string) bool { return rateSet[t] }
func IsConcLimit(t string) bool { return concSet[t] }

// RateLimits returns the request-rate limit types governing op: the group
// limit first, then the operation's own limit.
func RateLimits(op Operation) []string { return rateByOp[op] }

// ConcLimits returns the concurrency limit types governing op.
func ConcLimits(op Operation) []string { return concByOp[op] }

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
