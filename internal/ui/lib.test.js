const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('./static/lib.js');

test('parseMetrics handles labels, escapes, and skips comments/garbage', () => {
  const s = L.parseMetrics([
    '# HELP x',
    's3flow_requests_total{endpoint="main",type="ceph",class="read",op="get",code="2xx"} 12',
    's3flow_fleet_share_divisor 4',
    's3flow_policy_throttled_total{policy="a \\"q\\" b",reason="rate",limit="listRequestRate"} 3',
    'bad line',
    's3flow_x{a="}"} 5',
    's3flow_nan NaN',
  ].join('\n'));
  assert.equal(s.length, 4);
  assert.equal(s[0].labels.op, 'get');
  assert.equal(s[0].value, 12);
  assert.equal(s[1].value, 4);
  assert.equal(s[2].labels.policy, 'a "q" b');
  assert.equal(s[3].labels.a, '}');
});

test('sum / groupSum filter by labels and by predicate', () => {
  const s = L.parseMetrics('m{c="2xx",o="get"} 5\nm{c="503",o="get"} 2\nm{c="503",o="list"} 3\nn 9');
  assert.equal(L.sum(s, 'm'), 10);
  assert.equal(L.sum(s, 'm', { c: '503' }), 5);
  assert.equal(L.sum(s, 'm', (l) => l.c !== '2xx'), 5);
  const g = L.groupSum(s, 'm', 'o', { c: '503' });
  assert.equal(g.get('get'), 2);
  assert.equal(g.get('list'), 3);
});

test('withRates computes per-second deltas and treats resets as zero', () => {
  const a = L.parseMetrics('m{k="1"} 100\nm{k="2"} 50');
  const prev = L.withRates(a, null, 0);
  assert.equal(a[0].rate, 0);
  const b = L.parseMetrics('m{k="1"} 160\nm{k="2"} 10\nm{k="3"} 7');
  L.withRates(b, prev, 5);
  assert.equal(b[0].rate, 12);
  assert.equal(b[1].rate, 0, 'counter reset');
  assert.equal(b[2].rate, 0, 'new series');
  assert.equal(L.sum(b, 'm', null, 'rate'), 12);
});

test('parseSize accepts decimal/binary units and rejects ambiguous input', () => {
  assert.equal(L.parseSize('100MB'), 100000000);
  assert.equal(L.parseSize('100 MB/s'), 100000000);
  assert.equal(L.parseSize('1.5 GB'), 1500000000);
  assert.equal(L.parseSize('1 MiB'), 1048576);
  assert.equal(L.parseSize('250000'), 250000);
  assert.equal(L.parseSize('2 kB'), 2000);
  for (const bad of ['', 'abc', '-5', '0', '10 Mbps', '10 furlongs', '1e3', '5 M B']) {
    assert.ok(Number.isNaN(L.parseSize(bad)), 'should reject ' + JSON.stringify(bad));
  }
});

test('sizeToInput round-trips exactly', () => {
  for (const n of [1, 999, 1000, 1500000, 12500000, 100000000, 200000000, 123456789, 1234567, 5000000000000, 7]) {
    assert.equal(L.parseSize(L.sizeToInput(n)), n);
  }
  assert.equal(L.sizeToInput(100000000), '100 MB');
  assert.equal(L.sizeToInput(12500000), '12.5 MB');
  assert.equal(L.sizeToInput(1500000), '1.5 MB');
  assert.equal(L.sizeToInput(1234567), '1234.567 kB');
  assert.equal(L.sizeToInput(999), '999');
});

test('parseCount only accepts positive whole numbers', () => {
  assert.equal(L.parseCount('2000'), 2000);
  assert.equal(L.parseCount(' 7 '), 7);
  for (const bad of ['', '0', '-1', '1.5', '1e3', 'ten']) assert.ok(Number.isNaN(L.parseCount(bad)));
});

test('splitList splits on commas, spaces and newlines', () => {
  assert.deepEqual(L.splitList('a, b\nc  d,,'), ['a', 'b', 'c', 'd']);
  assert.deepEqual(L.splitList(''), []);
});

test('formatting', () => {
  assert.equal(L.fmtBytes(0), '0 B');
  assert.equal(L.fmtBytes(999), '999 B');
  assert.equal(L.fmtBytes(1500), '1.50 kB');
  assert.equal(L.fmtBytes(200000000), '200 MB');
  assert.equal(L.fmtBytesPerSec(100000000), '100 MB/s');
  assert.equal(L.fmtNum(12), '12');
  assert.equal(L.fmtNum(1500000), '1.50M');
  assert.equal(L.ago(null), 'never');
  assert.equal(L.ago('2026-01-01T00:00:00Z', Date.parse('2026-01-01T00:00:30Z')), '30s ago');
  assert.equal(L.ago('2026-01-01T00:00:00Z', Date.parse('2026-01-01T00:05:00Z')), '5m ago');
});

test('limit names and values read naturally', () => {
  assert.equal(L.limitName('aggregateBandwidthIn'), 'agg in');
  assert.equal(L.limitName('readRequestRate'), 'read/s');
  assert.equal(L.limitName('listRequestRate'), 'list/s');
  assert.equal(L.limitName('concurrentDeleteRequests'), 'delete conc');
  assert.equal(L.formatLimit({ type: 'aggregateBandwidthOut', value: 200000000 }), '200 MB/s');
  assert.equal(L.formatLimit({ type: 'writeRequestRate', value: 500 }), '500');
});

test('matcherSummary truncates long member lists and marks inverse', () => {
  assert.equal(L.matcherSummary({ type: 'bucket', members: ['a', 'b'], inverse: false }), 'bucket: a, b');
  assert.equal(L.matcherSummary({ type: 'tenant', members: ['1', '2', '3', '4', '5'], inverse: true }), 'NOT tenant: 1, 2, 3 +2');
});

test('validatePolicy mirrors the server rules', () => {
  const types = ['aggregateBandwidthIn', 'perRequestBandwidthIn', 'readRequestRate'];
  const ok = { name: 'p', matchers: [{ type: 'bucket', members: ['b'] }], limits: [{ type: 'readRequestRate', value: 5 }] };
  assert.deepEqual(L.validatePolicy(ok, 'aggregate', types), []);
  const e = (p, m) => L.validatePolicy(p, m || 'aggregate', types).join('|');
  assert.match(e({ name: '', matchers: ok.matchers, limits: [] }), /Name is required/);
  assert.match(e({ name: 'p', matchers: ok.matchers, limits: [{ type: 'readRequestRate', value: 1 }, { type: 'readRequestRate', value: 2 }] }), /more than once/);
  assert.match(e({ name: 'p', matchers: ok.matchers, limits: [{ type: 'perRequestBandwidthIn', value: 1 }] }), /not allowed in aggregate/);
  assert.match(e({ name: 'p', matchers: ok.matchers, limits: [{ type: 'aggregateBandwidthIn', value: 1 }] }, 'perRequest'), /not allowed in per-request/);
  assert.match(e({ name: 'p', matchers: ok.matchers, limits: [{ type: 'readRequestRate', value: 0 }] }), /positive integer/);
  assert.match(e({ name: 'p', matchers: [{ type: 'cidr', members: ['999.1'] }], limits: [] }), /not an IPv4/);
  assert.match(e({ name: 'p', matchers: [{ type: 'bucket', members: [] }], limits: [] }), /no members/);
  assert.match(e({ name: 'p', matchers: [], limits: [] }), /No matchers/);
});

test('pushHistory caps length', () => {
  const a = [];
  for (let i = 0; i < 10; i++) L.pushHistory(a, i, 4);
  assert.deepEqual(a, [6, 7, 8, 9]);
});
