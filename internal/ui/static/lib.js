/* s3qos UI – pure helpers (no DOM). Loaded by the browser as window.S3QOS and
 * by `node --test` as a CommonJS module. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.S3QOS = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /* ---------- Prometheus text format ---------- */

  function parseLabels(s) {
    const labels = {};
    const re = /([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"/g;
    let m;
    while ((m = re.exec(s)) !== null) {
      labels[m[1]] = m[2].replace(/\\(["\\n])/g, function (_, c) { return c === 'n' ? '\n' : c; });
    }
    return labels;
  }

  function parseMetrics(text) {
    const out = [];
    const lines = String(text).split('\n');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line || line[0] === '#') continue;
      let name, labels = {}, rest;
      const br = line.indexOf('{');
      if (br >= 0) {
        let end = -1, inQ = false;
        for (let j = br + 1; j < line.length; j++) {
          const c = line[j];
          if (c === '\\' && inQ) { j++; continue; }
          if (c === '"') inQ = !inQ;
          else if (c === '}' && !inQ) { end = j; break; }
        }
        if (end < 0) continue;
        name = line.slice(0, br);
        labels = parseLabels(line.slice(br + 1, end));
        rest = line.slice(end + 1);
      } else {
        const sp = line.search(/\s/);
        if (sp < 0) continue;
        name = line.slice(0, sp);
        rest = line.slice(sp);
      }
      const value = parseFloat(rest.trim().split(/\s+/)[0]);
      if (!isFinite(value)) continue;
      out.push({ name: name, labels: labels, value: value, key: name + JSON.stringify(labels) });
    }
    return out;
  }

  function matches(s, name, want) {
    if (s.name !== name) return false;
    if (!want) return true;
    if (typeof want === 'function') return want(s.labels);
    for (const k in want) if (s.labels[k] !== want[k]) return false;
    return true;
  }

  function sum(samples, name, want, field) {
    let t = 0;
    for (const s of samples) if (matches(s, name, want)) t += (field ? (s[field] || 0) : s.value);
    return t;
  }

  function groupSum(samples, name, labelKey, want, field) {
    const m = new Map();
    for (const s of samples) {
      if (!matches(s, name, want)) continue;
      const k = s.labels[labelKey] === undefined ? '' : s.labels[labelKey];
      m.set(k, (m.get(k) || 0) + (field ? (s[field] || 0) : s.value));
    }
    return m;
  }

  /* Attach per-second rates (counter delta / dt) to samples using the previous
   * poll. Counter resets yield 0. prev is a Map key->value or null. */
  function withRates(samples, prev, dtSeconds) {
    for (const s of samples) {
      s.rate = 0;
      if (prev && dtSeconds > 0 && prev.has(s.key)) {
        const d = s.value - prev.get(s.key);
        s.rate = d > 0 ? d / dtSeconds : 0;
      }
    }
    const next = new Map();
    for (const s of samples) next.set(s.key, s.value);
    return next;
  }

  /* ---------- number formatting / parsing ---------- */

  const DEC = [['TB', 1e12], ['GB', 1e9], ['MB', 1e6], ['kB', 1e3]];

  function fmtBytes(n) {
    n = Number(n) || 0;
    for (const u of DEC) if (Math.abs(n) >= u[1]) return trim(n / u[1]) + ' ' + u[0];
    return Math.round(n) + ' B';
  }
  function fmtBytesPerSec(n) { return fmtBytes(n) + '/s'; }
  function trim(x) { return x >= 100 ? x.toFixed(0) : x >= 10 ? x.toFixed(1) : x.toFixed(2); }
  function fmtNum(n) {
    n = Number(n) || 0;
    if (Math.abs(n) >= 1e6) return trim(n / 1e6) + 'M';
    if (Math.abs(n) >= 1e4) return trim(n / 1e3) + 'k';
    return Number.isInteger(n) ? String(n) : n.toFixed(n < 10 ? 2 : 1);
  }

  const SIZE_UNITS = {
    '': 1, b: 1, kb: 1e3, mb: 1e6, gb: 1e9, tb: 1e12,
    kib: 1024, mib: 1048576, gib: 1073741824, tib: 1099511627776,
  };

  /* "100MB/s", "1.5 GiB", "250000" -> integer bytes; NaN if invalid.
   * Decimal units (kB, MB, GB, TB) and binary units (KiB..TiB) are accepted;
   * bit units such as "Mbps" are rejected as ambiguous. */
  function parseSize(str) {
    const m = /^\s*(\d*\.?\d+)\s*([A-Za-z]*)\s*(?:\/\s*s|ps)?\s*$/.exec(String(str));
    if (!m) return NaN;
    const unit = m[2].toLowerCase();
    if (!Object.prototype.hasOwnProperty.call(SIZE_UNITS, unit)) return NaN;
    const v = Math.round(parseFloat(m[1]) * SIZE_UNITS[unit]);
    return v > 0 ? v : NaN;
  }

  /* Exact, lossless text for an input field: the largest decimal unit that
   * represents n with at most three decimals (12500000 -> "12.5 MB"). */
  function sizeToInput(n) {
    for (const u of DEC) {
      if (n >= u[1] && (n * 1000) % u[1] === 0) return String(n / u[1]) + ' ' + u[0];
    }
    return String(n);
  }

  function parseCount(str) {
    const s = String(str).trim();
    if (!/^\d+$/.test(s)) return NaN;
    const v = parseInt(s, 10);
    return v > 0 ? v : NaN;
  }

  function splitList(str) {
    return String(str).split(/[\s,]+/).map(function (x) { return x.trim(); }).filter(Boolean);
  }

  function ago(iso, now) {
    if (!iso) return 'never';
    const s = Math.max(0, Math.round(((now || Date.now()) - Date.parse(iso)) / 1000));
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    return Math.floor(s / 3600) + 'h ago';
  }

  /* ---------- policy helpers ---------- */

  const BW_AGG = ['aggregateBandwidthIn', 'aggregateBandwidthOut'];
  const BW_REQ = ['perRequestBandwidthIn', 'perRequestBandwidthOut'];

  function isBandwidth(t) { return BW_AGG.indexOf(t) >= 0 || BW_REQ.indexOf(t) >= 0; }

  function formatLimit(l) {
    return isBandwidth(l.type) ? fmtBytesPerSec(l.value) : fmtNum(l.value);
  }

  const SHORT = {
    aggregateBandwidthIn: 'agg in', aggregateBandwidthOut: 'agg out',
    perRequestBandwidthIn: 'req in', perRequestBandwidthOut: 'req out',
    readRequestRate: 'read/s', writeRequestRate: 'write/s',
    concurrentReadRequests: 'read conc', concurrentWriteRequests: 'write conc',
  };

  function limitName(t) {
    if (SHORT[t]) return SHORT[t];
    let m = /^(\w+?)RequestRate$/.exec(t);
    if (m) return m[1] + '/s';
    m = /^concurrent(\w+?)Requests$/.exec(t);
    if (m) return m[1].toLowerCase() + ' conc';
    return t;
  }

  function matcherSummary(m) {
    const mem = m.members.length > 3 ? m.members.slice(0, 3).join(', ') + ' +' + (m.members.length - 3) : m.members.join(', ');
    return (m.inverse ? 'NOT ' : '') + m.type + ': ' + mem;
  }

  /* Client-side hints; the server remains the authority. */
  function validatePolicy(p, mode, schemaTypes) {
    const errs = [];
    if (!p.name || !p.name.trim()) errs.push('Name is required.');
    const seen = {};
    for (const l of p.limits || []) {
      if (schemaTypes && schemaTypes.indexOf(l.type) < 0) errs.push('Unknown limit ' + l.type + '.');
      if (seen[l.type]) errs.push('Limit ' + l.type + ' is set more than once.');
      seen[l.type] = true;
      if (!(l.value > 0) || !Number.isInteger(l.value)) errs.push(l.type + ' must be a positive integer.');
      if (mode === 'aggregate' && BW_REQ.indexOf(l.type) >= 0) errs.push(l.type + ' is not allowed in aggregate bandwidth mode.');
      if (mode === 'perRequest' && BW_AGG.indexOf(l.type) >= 0) errs.push(l.type + ' is not allowed in per-request bandwidth mode.');
    }
    (p.matchers || []).forEach(function (m, i) {
      if (!m.members || m.members.length === 0) errs.push('Matcher ' + (i + 1) + ' (' + m.type + ') has no members.');
      if (m.type === 'cidr') {
        for (const c of m.members || []) if (!/^\d{1,3}(\.\d{1,3}){3}(\/\d{1,2})?$/.test(c)) errs.push('"' + c + '" is not an IPv4 address or CIDR.');
      }
    });
    if (!(p.matchers || []).length) errs.push('No matchers: this policy will not match any request (add a matcher, or use an inverse matcher to match everything).');
    return errs;
  }

  function pushHistory(arr, v, max) {
    arr.push(v);
    while (arr.length > max) arr.shift();
    return arr;
  }

  return {
    parseMetrics: parseMetrics, sum: sum, groupSum: groupSum, withRates: withRates,
    fmtBytes: fmtBytes, fmtBytesPerSec: fmtBytesPerSec, fmtNum: fmtNum,
    parseSize: parseSize, sizeToInput: sizeToInput, parseCount: parseCount, splitList: splitList,
    ago: ago, isBandwidth: isBandwidth, formatLimit: formatLimit, limitName: limitName,
    matcherSummary: matcherSummary, validatePolicy: validatePolicy, pushHistory: pushHistory,
    BW_AGG: BW_AGG, BW_REQ: BW_REQ,
  };
});
