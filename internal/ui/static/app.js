/* s3qos management console. Talks only to the authenticated /api/v1 endpoints
 * and /metrics of the node serving it. DOM is built with createElement (never
 * innerHTML), so data from the API can never inject markup. */
(function () {
  'use strict';
  const L = window.S3QOS;
  const POLL_MS = 5000, HISTORY = 60;
  const TOKEN_KEY = 's3qos_token';

  /* ---------- DOM helpers ---------- */
  const SVGNS = 'http://www.w3.org/2000/svg';
  function build(el, attrs, kids) {
    for (const k in attrs || {}) {
      const v = attrs[k];
      if (v === null || v === undefined || v === false) continue;
      if (k === 'class') el.setAttribute('class', v);
      else if (k.slice(0, 2) === 'on') el.addEventListener(k.slice(2), v);
      else if (k === 'value' || k === 'checked' || k === 'disabled' || k === 'selected') el[k] = v;
      else el.setAttribute(k, v === true ? '' : String(v));
    }
    (function add(list) {
      for (const c of list) {
        if (c === null || c === undefined || c === false) continue;
        if (Array.isArray(c)) add(c);
        else el.appendChild(c.nodeType ? c : document.createTextNode(String(c)));
      }
    })(kids);
    return el;
  }
  const h = (tag, attrs, ...kids) => build(document.createElement(tag), attrs, kids);
  const sv = (tag, attrs, ...kids) => build(document.createElementNS(SVGNS, tag), attrs, kids);
  const $ = (id) => document.getElementById(id);

  /* ---------- state ---------- */
  const S = {
    token: sessionStorage.getItem(TOKEN_KEY) || '',
    page: 'dashboard',
    policies: [], tenants: {}, settings: null, cluster: null, config: null, schema: null,
    samples: [], prev: null, prevT: 0,
    hist: { req: [], thr: [], err: [], inb: [], outb: [] },
    filter: '', error: '', lastOk: 0, loaded: false, timer: null, firstPollDone: false,
  };

  /* ---------- API ---------- */
  class ApiError extends Error { constructor(status, msg) { super(msg); this.status = status; } }

  async function api(method, path, body) {
    const opt = { method: method, headers: { Authorization: 'Bearer ' + S.token }, cache: 'no-store' };
    if (body !== undefined) { opt.body = JSON.stringify(body); opt.headers['Content-Type'] = 'application/json'; }
    const r = await fetch(path, opt);
    if (r.status === 401) { logout(); throw new ApiError(401, 'Session expired. Sign in again.'); }
    const text = await r.text();
    let j = null;
    try { j = text ? JSON.parse(text) : null; } catch (e) { /* non-JSON body */ }
    if (!r.ok) throw new ApiError(r.status, (j && j.message) || r.statusText || ('HTTP ' + r.status));
    return j;
  }
  const data = (j) => (j && j.data !== undefined ? j.data : j);

  function logout() {
    sessionStorage.removeItem(TOKEN_KEY);
    S.token = ''; S.loaded = false; S.firstPollDone = false; S.prev = null;
    if (S.timer) { clearInterval(S.timer); S.timer = null; }
    render();
  }

  /* ---------- polling ---------- */
  async function refresh() {
    try {
      const reqs = [
        fetch('/metrics', { headers: { Authorization: 'Bearer ' + S.token }, cache: 'no-store' }),
        api('GET', '/api/v1/traffic-classes'), api('GET', '/api/v1/tenants'),
        api('GET', '/api/v1/settings'), api('GET', '/api/v1/cluster'),
      ];
      if (!S.config) reqs.push(api('GET', '/api/v1/config'));
      if (!S.schema) reqs.push(api('GET', '/api/v1/schema'));
      const res = await Promise.all(reqs);
      const mres = res[0];
      if (mres.status === 401) { logout(); return; }
      if (!mres.ok) throw new Error('metrics: HTTP ' + mres.status);
      const text = await mres.text();
      S.policies = data(res[1]) || []; S.tenants = data(res[2]) || {};
      S.settings = data(res[3]); S.cluster = res[4];
      let i = 5;
      if (!S.config) S.config = data(res[i++]);
      if (!S.schema) S.schema = data(res[i++]);
      ingestMetrics(text);
      S.error = ''; S.lastOk = Date.now(); S.loaded = true;
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) return;
      S.error = 'Cannot reach the management API: ' + e.message;
    }
    render();
  }

  function ingestMetrics(text) {
    const now = Date.now();
    const samples = L.parseMetrics(text);
    const dt = S.prevT ? (now - S.prevT) / 1000 : 0;
    S.prev = L.withRates(samples, S.prev, dt);
    S.prevT = now;
    S.samples = samples;
    if (dt > 0) {
      const R = (name, want) => L.sum(samples, name, want, 'rate');
      L.pushHistory(S.hist.req, R('s3qos_requests_total'), HISTORY);
      L.pushHistory(S.hist.thr, R('s3qos_requests_total', { code: '503' }), HISTORY);
      L.pushHistory(S.hist.err, R('s3qos_requests_total', (l) => l.code === '4xx' || l.code === '5xx'), HISTORY);
      L.pushHistory(S.hist.inb, R('s3qos_endpoint_bytes_total', { direction: 'in' }), HISTORY);
      L.pushHistory(S.hist.outb, R('s3qos_endpoint_bytes_total', { direction: 'out' }), HISTORY);
    }
  }

  /* ---------- small UI pieces ---------- */
  function toast(msg) {
    const t = h('div', { class: 'toast', role: 'status' }, msg);
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 3500);
  }

  function chip(text, cls) { return h('span', { class: 'chip' + (cls ? ' ' + cls : '') }, text); }
  function badge(text, kind) { return h('span', { class: 'badge ' + kind }, text); }

  function stat(label, value, note, cls) {
    return h('div', { class: 'card stat ' + (cls || '') },
      h('div', { class: 'label' }, label), h('div', { class: 'value' }, value), note ? h('div', { class: 'note' }, note) : null);
  }

  function table(headers, rows, emptyText) {
    if (!rows.length) return h('div', { class: 'empty' }, emptyText || 'Nothing to show.');
    return h('div', { class: 'tablewrap' }, h('table', null,
      h('thead', null, h('tr', null, headers.map((x) => h('th', { class: x.num ? 'num' : '' }, x.text || x)))),
      h('tbody', null, rows)));
  }
  const td = (c, cls) => h('td', { class: cls || '' }, c);

  function lineChart(title, unit, series, fmt) {
    const n = Math.max.apply(null, series.map((s) => s.data.length).concat([0]));
    const card = h('div', { class: 'card chart' }, h('h3', null, title, h('span', { class: 'sub' }, unit)));
    card.appendChild(h('div', { class: 'legend' }, series.map((s, i) => h('span', null, h('i', { class: 'bg' + i }), s.name))));
    if (n < 2) { card.appendChild(h('div', { class: 'collecting' }, 'Collecting data…')); return card; }
    let max = 0;
    for (const s of series) for (const v of s.data) if (v > max) max = v;
    const label = max > 0 ? fmt(max) : fmt(0);
    if (max <= 0) max = 1;
    const W = 300, Hh = 100;
    const svg = sv('svg', { viewBox: '0 0 ' + W + ' ' + Hh, preserveAspectRatio: 'none', role: 'img', 'aria-label': title });
    for (let g = 0; g <= 4; g++) svg.appendChild(sv('line', { class: 'grid-line', x1: 0, x2: W, y1: (Hh * g) / 4, y2: (Hh * g) / 4 }));
    series.forEach((s, i) => {
      const off = HISTORY - s.data.length;
      const pts = s.data.map((v, k) => ((off + k) * W / (HISTORY - 1)).toFixed(2) + ',' + (Hh - (v / max) * (Hh - 3) - 1.5).toFixed(2));
      if (pts.length < 2) return;
      const firstX = pts[0].split(',')[0], lastX = pts[pts.length - 1].split(',')[0];
      svg.appendChild(sv('polygon', { class: 'area s' + i, points: firstX + ',' + Hh + ' ' + pts.join(' ') + ' ' + lastX + ',' + Hh }));
      svg.appendChild(sv('polyline', { class: 'line s' + i, points: pts.join(' ') }));
    });
    card.appendChild(h('div', { class: 'wrap' }, h('span', { class: 'ymax' }, label), svg));
    return card;
  }

  /* ---------- modal ---------- */
  function openModal(title, bodyEl, buttons, opts) {
    const prevFocus = document.activeElement;
    const root = $('modal');
    const box = h('div', { class: 'modal' + (opts && opts.narrow ? ' narrow' : ''), role: 'dialog', 'aria-modal': 'true', 'aria-label': title },
      h('header', null, h('h2', null, title)), h('div', { class: 'body' }, bodyEl), h('footer', null, buttons));
    const overlay = h('div', { class: 'overlay' }, box);
    const close = () => {
      document.removeEventListener('keydown', onKey);
      overlay.remove();
      if (prevFocus && prevFocus.focus) prevFocus.focus();
    };
    const onKey = (e) => { if (e.key === 'Escape') close(); };
    document.addEventListener('keydown', onKey);
    root.appendChild(overlay);
    const first = box.querySelector('input,select,textarea,button');
    if (first) first.focus();
    return close;
  }

  function confirmDialog(title, message, actionLabel, onConfirm) {
    const err = h('div', { class: 'formerr', hidden: true });
    err.hidden = true;
    let close;
    const go = h('button', { class: 'btn danger', onclick: async () => {
      go.disabled = true;
      try { await onConfirm(); close(); } catch (e) { err.hidden = false; err.textContent = e.message; go.disabled = false; }
    } }, actionLabel);
    close = openModal(title, h('div', null, err, h('p', null, message)),
      [h('button', { class: 'btn', onclick: () => close() }, 'Cancel'), go], { narrow: true });
  }

  /* ---------- pages ---------- */
  function pageDashboard() {
    const sm = S.samples, R = (n, w) => L.sum(sm, n, w, 'rate'), T = (n, w) => L.sum(sm, n, w);
    const up = L.sum(sm, 's3qos_backend_up'), total = sm.filter((x) => x.name === 's3qos_backend_up').length;
    const thrTotal = T('s3qos_requests_total', { code: '503' });
    const cl = S.cluster || {};
    const liveNodes = cl.liveNodes || 1, nodes = (cl.peers ? cl.peers.length : 0) + 1;
    const view = h('div', null,
      h('div', { class: 'grid stats' },
        stat('Requests / s', L.fmtNum(R('s3qos_requests_total')), L.fmtNum(T('s3qos_requests_total')) + ' since start'),
        stat('Throttled / s', L.fmtNum(R('s3qos_requests_total', { code: '503' })), L.fmtNum(thrTotal) + ' rejected (503 SlowDown)', thrTotal > 0 ? 'bad' : ''),
        stat('Upload', L.fmtBytesPerSec(R('s3qos_endpoint_bytes_total', { direction: 'in' })), L.fmtBytes(T('s3qos_endpoint_bytes_total', { direction: 'in' })) + ' total'),
        stat('Download', L.fmtBytesPerSec(R('s3qos_endpoint_bytes_total', { direction: 'out' })), L.fmtBytes(T('s3qos_endpoint_bytes_total', { direction: 'out' })) + ' total'),
        stat('Backends up', up + ' / ' + total, total && up < total ? 'some backends are marked down' : 'all healthy', total && up < total ? 'bad' : 'ok'),
        stat('Proxy nodes', liveNodes + ' / ' + nodes, 'live / configured', liveNodes < nodes ? 'bad' : 'ok')),
      h('div', { class: 'grid two' },
        lineChart('Requests', 'per second', [{ name: 'All', data: S.hist.req }, { name: 'Throttled (503)', data: S.hist.thr }, { name: 'Client/backend errors', data: S.hist.err }], L.fmtNum),
        lineChart('Throughput', 'bytes per second', [{ name: 'Upload', data: S.hist.inb }, { name: 'Download', data: S.hist.outb }], L.fmtBytesPerSec)),
      h('div', { class: 'grid two' }, endpointsCard(), operationsCard()),
      throttleCard());
    return view;
  }

  function endpointsCard() {
    const sm = S.samples;
    const eps = (S.config && S.config.endpoints) || [];
    const rows = eps.map((e) => {
      const w = { endpoint: e.name };
      const ups = sm.filter((x) => x.name === 's3qos_backend_up' && x.labels.endpoint === e.name);
      const upN = ups.filter((x) => x.value === 1).length;
      return h('tr', null,
        td([h('strong', null, e.name), ' ', chip(e.type, 'mute')]),
        td(e.listen, 'mono'),
        td(badge(upN + '/' + ups.length + ' up', ups.length && upN === ups.length ? 'ok' : 'bad')),
        td(L.fmtNum(L.sum(sm, 's3qos_requests_total', w, 'rate')) + '/s', 'num'),
        td(L.fmtBytesPerSec(L.sum(sm, 's3qos_endpoint_bytes_total', { endpoint: e.name, direction: 'in' }, 'rate')), 'num'),
        td(L.fmtBytesPerSec(L.sum(sm, 's3qos_endpoint_bytes_total', { endpoint: e.name, direction: 'out' }, 'rate')), 'num'));
    });
    return h('div', { class: 'card' }, h('h3', null, 'Endpoints'),
      table(['Endpoint', 'Listen', 'Backends', { text: 'Requests', num: 1 }, { text: 'In', num: 1 }, { text: 'Out', num: 1 }], rows, 'No endpoints.'));
  }

  function operationsCard() {
    const sm = S.samples;
    const ops = ((S.schema && S.schema.operations) || []).map((o) => ({ op: o.operation, read: o.read }));
    ops.push({ op: 'other', read: false });
    const rows = [];
    for (const grp of [true, false]) {
      rows.push(h('tr', { class: 'grouprow' }, h('td', { colspan: 4 }, grp ? 'Read group' : 'Write group')));
      for (const o of ops.filter((x) => x.read === grp)) {
        const w = { op: o.op };
        rows.push(h('tr', null, td(o.op, 'opcell mono'),
          td(L.fmtNum(L.sum(sm, 's3qos_requests_total', w)), 'num'),
          td(L.fmtNum(L.sum(sm, 's3qos_requests_total', w, 'rate')) + '/s', 'num'),
          td(L.fmtNum(L.sum(sm, 's3qos_requests_total', { op: o.op, code: '503' })), 'num')));
      }
    }
    return h('div', { class: 'card' }, h('h3', null, 'Operations', h('span', { class: 'sub' }, 'classified from method, path and query')),
      table(['Operation', { text: 'Requests', num: 1 }, { text: 'Rate', num: 1 }, { text: 'Throttled', num: 1 }], rows));
  }

  function throttleCard() {
    const sm = S.samples;
    const rows = sm.filter((x) => x.name === 's3qos_policy_throttled_total' && x.value > 0)
      .sort((a, b) => b.value - a.value).slice(0, 12).map((x) =>
        h('tr', null, td(x.labels.policy), td(x.labels.limit || '-', 'mono'), td(x.labels.reason), td(L.fmtNum(x.value), 'num'), td(L.fmtNum(x.rate) + '/s', 'num')));
    return h('div', { class: 'card' }, h('h3', null, 'Throttling by policy and limit', h('span', { class: 'sub' }, 'which limit is firing')),
      table(['Policy', 'Limit', 'Reason', { text: 'Rejected', num: 1 }, { text: 'Rate', num: 1 }], rows, 'No requests have been throttled.'));
  }

  /* --- policies --- */
  function limitChips(p) {
    if (!(p.limits || []).length) return chip('no limits (monitor only)', 'mute');
    return h('div', { class: 'chips' }, p.limits.map((l) => chip(L.limitName(l.type) + ' ' + L.formatLimit(l))));
  }

  function pagePolicies() {
    const sm = S.samples;
    const rows = S.policies.slice().sort((a, b) => a.name.localeCompare(b.name)).map((p) => {
      const w = { policy: p.name };
      const active = L.sum(sm, 's3qos_policy_active_requests', w);
      const thr = L.sum(sm, 's3qos_policy_throttled_total', w);
      return h('tr', null,
        td([h('strong', null, p.name), p.description ? h('div', { class: 'hint' }, p.description) : null]),
        td(h('div', { class: 'chips' }, (p.matchers || []).length ? (p.matchers || []).map((m) => chip(L.matcherSummary(m), m.inverse ? 'neg' : '')) : chip('matches nothing', 'mute'))),
        td(limitChips(p)),
        td(L.fmtNum(L.sum(sm, 's3qos_policy_requests_total', w)), 'num'),
        td(thr > 0 ? badge(L.fmtNum(thr), 'bad') : '0', 'num'),
        td(String(active), 'num'),
        td(L.fmtBytes(L.sum(sm, 's3qos_policy_bytes_total', { policy: p.name, direction: 'in' })) + ' / ' + L.fmtBytes(L.sum(sm, 's3qos_policy_bytes_total', { policy: p.name, direction: 'out' })), 'num'),
        td(h('div', { class: 'actions' },
          h('button', { class: 'btn small', onclick: () => policyEditor(p) }, 'Edit'),
          h('button', { class: 'btn small', onclick: () => policyEditor(Object.assign({}, p, { id: '', name: p.name + '-copy' })) }, 'Clone'),
          h('button', { class: 'btn small danger', onclick: () => confirmDialog('Delete policy', 'Delete policy "' + p.name + '"? Its limits stop applying immediately on all nodes.', 'Delete', async () => {
            await api('DELETE', '/api/v1/traffic-classes/' + encodeURIComponent(p.id)); toast('Policy deleted'); await refresh();
          }) }, 'Delete'))));
    });
    const mode = S.settings ? S.settings.bandwidthMode : '';
    return h('div', null,
      h('div', { class: 'pagehead' }, h('h2', null, 'Traffic classification policies'), h('div', { class: 'spacer' }),
        h('button', { class: 'btn primary', onclick: () => policyEditor(null) }, 'Create policy')),
      h('div', { class: 'card' },
        h('p', { class: 'hint' }, 'Matchers use OR semantics. Bandwidth comes from the single best-matching policy; request-rate and concurrency limits apply from every matching policy. Bandwidth mode: ',
          h('strong', null, mode), '.'),
        table(['Policy', 'Matchers', 'Limits', { text: 'Matched', num: 1 }, { text: 'Throttled', num: 1 }, { text: 'Active', num: 1 }, { text: 'In / out', num: 1 }, ''], rows,
          'No policies yet. Create one with no limits first to collect baseline metrics.')));
  }

  const MATCHER_HELP = {
    bucket: 'Exact bucket names', 'bucket-regex': 'Regular expressions (unanchored; use ^ and $)', tenant: 'Tenant IDs (see Tenants)',
    cidr: 'IPv4 addresses or CIDRs, e.g. 10.0.0.0/8 or 192.0.2.7/32', endpoint: 'Endpoint names',
  };

  function policyEditor(existing) {
    if (!S.schema || !S.settings) return;
    const mode = S.settings.bandwidthMode;
    const isEdit = !!(existing && existing.id);
    const orig = existing || { name: '', description: '', matchers: [], limits: [] };
    const errBox = h('div', { class: 'formerr', hidden: true });
    const name = h('input', { type: 'text', id: 'pe-name', value: orig.name, autocomplete: 'off' });
    const desc = h('input', { type: 'text', value: orig.description || '', autocomplete: 'off' });

    /* matchers */
    const matcherRows = [];
    const matcherBox = h('div');
    function addMatcher(m) {
      m = m || { type: S.schema.matchers[0], members: [], inverse: false };
      const type = h('select', { 'aria-label': 'Matcher type' }, S.schema.matchers.map((t) => h('option', { value: t, selected: t === m.type }, t)));
      const members = h('textarea', { rows: 2, 'aria-label': 'Matcher members', placeholder: MATCHER_HELP[m.type] }, m.members.join('\n'));
      members.value = m.members.join('\n');
      const inv = h('input', { type: 'checkbox', checked: !!m.inverse });
      const hint = h('p', { class: 'hint' }, MATCHER_HELP[m.type]);
      type.addEventListener('change', () => { hint.textContent = MATCHER_HELP[type.value]; members.placeholder = MATCHER_HELP[type.value]; });
      const box = h('div', { class: 'matcher' },
        h('div', { class: 'row' }, type, h('div', { class: 'grow' }, members),
          h('label', { class: 'inline' }, inv, 'Inverse'),
          h('button', { class: 'btn small', type: 'button', onclick: () => { box.remove(); matcherRows.splice(matcherRows.indexOf(rec), 1); } }, 'Remove')), hint);
      const rec = { type, members, inv };
      matcherRows.push(rec); matcherBox.appendChild(box);
    }
    (orig.matchers || []).forEach(addMatcher);
    if (!(orig.matchers || []).length) addMatcher();

    /* limits */
    const fields = {};   // type -> input
    const cur = {}; (orig.limits || []).forEach((l) => { cur[l.type] = l.value; });
    const info = {}; S.schema.limits.forEach((l) => { info[l.type] = l; });
    function limitInput(type) {
      const bw = L.isBandwidth(type);
      const v = cur[type];
      const inp = h('input', { type: 'text', value: v === undefined ? '' : (bw ? L.sizeToInput(v) : String(v)), placeholder: bw ? 'e.g. 100 MB' : 'unlimited', autocomplete: 'off', 'aria-label': type });
      fields[type] = inp; return inp;
    }
    const field = (label, type, hint) => h('label', { class: 'field' }, h('span', null, label), limitInput(type), hint ? h('p', { class: 'hint' }, hint) : null);
    const bwTypes = mode === 'aggregate' ? L.BW_AGG : L.BW_REQ;
    const bwSet = h('fieldset', null, h('legend', null, mode === 'aggregate' ? 'Bandwidth: shared ceiling per proxy node' : 'Bandwidth: ceiling per request'),
      h('div', { class: 'bwgrid' },
        field('Upload (client to storage)', bwTypes[0], 'Bytes per second. Units: kB, MB, GB, TB (decimal) or KiB, MiB, GiB (binary).'),
        field('Download (storage to client)', bwTypes[1])));
    const groupSet = h('fieldset', null, h('legend', null, 'Groups: all operations in the group together'),
      h('div', { class: 'bwgrid' },
        field('Read requests / second (get, head, list)', 'readRequestRate'), field('Read concurrent requests', 'concurrentReadRequests'),
        field('Write requests / second (put, copy, multipart, delete)', 'writeRequestRate'), field('Write concurrent requests', 'concurrentWriteRequests')));
    const opRows = S.schema.operations.map((o) => {
      const rate = S.schema.limits.find((l) => l.operation === o.operation && l.kind === 'rate');
      const conc = S.schema.limits.find((l) => l.operation === o.operation && l.kind === 'concurrency');
      return h('tr', null, td(h('strong', null, o.operation), 'mono'), td(o.read ? 'read' : 'write'),
        td(limitInput(rate.type)), td(limitInput(conc.type)));
    });
    const opSet = h('fieldset', { class: 'limits' }, h('legend', null, 'Per-operation: fine-tune a single operation (applied in addition to its group)'),
      h('div', { class: 'tablewrap' }, h('table', null, h('thead', null, h('tr', null, ['Operation', 'Group', 'Requests / second', 'Concurrent'].map((t) => h('th', null, t)))), h('tbody', null, opRows))),
      h('p', { class: 'hint' }, 'A request must pass its group limit and its own operation limit. Leave a field empty for no limit.'));

    const body = h('div', null, errBox,
      h('label', { class: 'field' }, h('span', null, 'Name'), name),
      h('label', { class: 'field' }, h('span', null, 'Description (optional)'), desc),
      h('fieldset', null, h('legend', null, 'Matchers (any matcher matching selects the policy)'), matcherBox,
        h('button', { class: 'btn small', type: 'button', onclick: () => addMatcher() }, 'Add matcher')),
      bwSet, groupSet, opSet);

    function collect() {
      const errs = [];
      const limits = [];
      for (const t in fields) {
        const raw = fields[t].value.trim();
        fields[t].classList.remove('invalid');
        if (!raw) continue;
        const v = L.isBandwidth(t) ? L.parseSize(raw) : L.parseCount(raw);
        if (!(v > 0)) { errs.push(t + ': "' + raw + '" is not a valid ' + (L.isBandwidth(t) ? 'size such as 100 MB' : 'positive whole number') + '.'); fields[t].classList.add('invalid'); continue; }
        limits.push({ type: t, value: v });
      }
      // Blank matcher rows are ignored, so a policy (such as a default tier) can be saved before it is attached.
      const matchers = matcherRows.map((r) => ({ type: r.type.value, members: L.splitList(r.members.value.replace(/\n/g, ',')), inverse: r.inv.checked }))
        .filter((m) => m.members.length > 0);
      const p = { name: name.value.trim(), description: desc.value.trim(), matchers: matchers, limits: limits };
      return { p: p, errs: errs.concat(L.validatePolicy(p, mode, S.schema.limits.map((l) => l.type))) };
    }

    let close;
    const save = h('button', { class: 'btn primary', onclick: async () => {
      const r = collect();
      // An empty matcher list is only advisory (the server accepts it); everything else blocks saving.
      const seen = {};
      const errs = r.errs.filter((e) => !/^No matchers/.test(e) && !seen[e] && (seen[e] = true));
      if (errs.length) { errBox.hidden = false; errBox.textContent = errs.join('\n'); return; }
      save.disabled = true;
      try {
        if (isEdit) await api('PUT', '/api/v1/traffic-classes/' + encodeURIComponent(existing.id), r.p);
        else await api('POST', '/api/v1/traffic-classes', r.p);
        close(); toast(isEdit ? 'Policy updated' : 'Policy created'); await refresh();
      } catch (e) { errBox.hidden = false; errBox.textContent = e.message; save.disabled = false; }
    } }, isEdit ? 'Save changes' : 'Create policy');
    close = openModal(isEdit ? 'Edit policy' : 'Create policy', body, [h('button', { class: 'btn', onclick: () => close() }, 'Cancel'), save]);
  }

  /* --- tenants --- */
  function pageTenants() {
    const ids = Object.keys(S.tenants).sort();
    const rows = ids.map((id) => {
      const t = S.tenants[id];
      return h('tr', null, td(h('strong', null, id)),
        td(h('div', { class: 'chips' }, (t.accessKeys || []).map((k) => chip(k)))),
        td(h('div', { class: 'chips' }, (t.buckets || []).map((b) => chip(b)))),
        td(h('div', { class: 'actions' }, h('button', { class: 'btn small', onclick: () => tenantEditor(id) }, 'Edit'),
          h('button', { class: 'btn small danger', onclick: () => confirmDialog('Delete tenant', 'Delete tenant "' + id + '"? Policies that match this tenant will no longer apply to its requests.', 'Delete', async () => {
            await api('DELETE', '/api/v1/tenants/' + encodeURIComponent(id)); toast('Tenant deleted'); await refresh();
          }) }, 'Delete'))));
    });
    return h('div', null,
      h('div', { class: 'pagehead' }, h('h2', null, 'Tenant identity mapping'), h('div', { class: 'spacer' }),
        h('button', { class: 'btn primary', onclick: () => tenantEditor(null) }, 'Add tenant')),
      h('div', { class: 'card' },
        h('p', { class: 'hint' }, 'A request is attributed to a tenant by its access key ID. Requests without an access key (anonymous) use the owner of the accessed bucket.'),
        table(['Tenant', 'Access key IDs', 'Owned buckets', ''], rows, 'No tenants defined. Tenant matchers in policies need a mapping here.')));
  }

  function tenantEditor(id) {
    const t = id ? S.tenants[id] : { accessKeys: [], buckets: [] };
    const errBox = h('div', { class: 'formerr', hidden: true });
    const idIn = h('input', { type: 'text', value: id || '', disabled: !!id, autocomplete: 'off' });
    const keys = h('textarea', { rows: 3 }, ''); keys.value = (t.accessKeys || []).join('\n');
    const buckets = h('textarea', { rows: 3 }, ''); buckets.value = (t.buckets || []).join('\n');
    const body = h('div', null, errBox,
      h('label', { class: 'field' }, h('span', null, 'Tenant ID'), idIn),
      h('label', { class: 'field' }, h('span', null, 'Access key IDs (one per line)'), keys),
      h('label', { class: 'field' }, h('span', null, 'Owned buckets (one per line)'), buckets, h('p', { class: 'hint' }, 'Used to identify anonymous requests.')));
    let close;
    const save = h('button', { class: 'btn primary', onclick: async () => {
      const tid = idIn.value.trim();
      if (!tid) { errBox.hidden = false; errBox.textContent = 'Tenant ID is required.'; return; }
      if (!id && S.tenants[tid]) { errBox.hidden = false; errBox.textContent = 'Tenant "' + tid + '" already exists. Edit it instead.'; return; }
      save.disabled = true;
      try {
        await api('PUT', '/api/v1/tenants/' + encodeURIComponent(tid), { accessKeys: L.splitList(keys.value), buckets: L.splitList(buckets.value) });
        close(); toast('Tenant saved'); await refresh();
      } catch (e) { errBox.hidden = false; errBox.textContent = e.message; save.disabled = false; }
    } }, 'Save');
    close = openModal(id ? 'Edit tenant' : 'Add tenant', body, [h('button', { class: 'btn', onclick: () => close() }, 'Cancel'), save]);
  }

  /* --- configuration --- */
  function pageConfig() {
    const c = S.config || {}, st = S.settings || {}, cl = S.cluster || {};
    const eps = (c.endpoints || []).map((e) => h('tr', null,
      td([h('strong', null, e.name), ' ', chip(e.type, 'mute')]),
      td(h('div', { class: 'chips' }, (e.listeners || []).map((a) => chip(a)))),
      td(h('div', { class: 'chips' }, e.backends.map((b) => chip(b + ((e.weights && e.weights[b] > 1) ? '  ×' + e.weights[b] : ''))))),
      td([chip(e.balance === 'leastconn' ? 'least connections' : 'round-robin', 'mute'), ' ',
        e.healthPath ? badge('probe ' + e.healthPath, 'ok') : badge('passive health only', 'warn')]),
      td([e.tls ? badge('TLS', 'ok') : badge('plain HTTP', 'warn'), ' ', e.backendInsecureSkipVerify ? badge('backend cert unchecked', 'bad') : null])));
    const peers = (cl.peers || []).map((p) => h('tr', null, td(p.url, 'mono'), td(badge(p.alive ? 'alive' : 'unreachable', p.alive ? 'ok' : 'bad')), td(L.ago(p.lastSeen))));
    const fleet = L.sum(S.samples, 's3qos_fleet_share_divisor');
    return h('div', null,
      h('div', { class: 'pagehead' }, h('h2', null, 'Configuration')),
      h('div', { class: 'grid two' },
        h('div', { class: 'card' }, h('h3', null, 'Global settings', h('span', { class: 'sub' }, 'replicated to all nodes')),
          h('dl', { class: 'kv' },
            h('dt', null, 'Bandwidth mode'), h('dd', null, st.bandwidthMode),
            h('dt', null, 'Enforcement'), h('dd', null, st.enforcement + (st.enforcement === 'fleet' ? ' (each node enforces limit ÷ ' + fleet + ')' : ' (each node enforces the full limit)'))),
          h('p', null, h('button', { class: 'btn', onclick: settingsEditor }, 'Change settings'))),
        h('div', { class: 'card' }, h('h3', null, 'This node'),
          h('dl', { class: 'kv' },
            h('dt', null, 'Node ID'), h('dd', null, c.nodeId), h('dt', null, 'Admin listen'), h('dd', { class: 'mono' }, c.adminListen + (c.adminTls ? ' (TLS)' : ' (plain HTTP)')),
            h('dt', null, 'State file'), h('dd', { class: 'mono' }, c.statePath), h('dt', null, 'State version'), h('dd', { class: 'mono' }, String(cl.version || '')),
            h('dt', null, 'Sync interval'), h('dd', null, c.syncInterval), h('dt', null, 'Base domains'), h('dd', null, (c.baseDomains || []).join(', ') || 'none (path-style only)'),
            h('dt', null, 'Trusted proxies'), h('dd', null, (c.trustedProxies || []).join(', ') || 'none')),
          h('p', { class: 'hint' }, 'Node settings are read from the configuration file at start-up; restart the node to change them.'))),
      h('div', { class: 'card' }, h('h3', null, 'Endpoints'), table(['Endpoint', 'Listen on', 'Backends (weight)', 'Balancing', 'Security'], eps, 'No endpoints.')),
      h('div', { class: 'card' }, h('h3', null, 'Cluster peers'), table(['Peer', 'State', 'Last contact'], peers, 'Standalone node: no peers configured.')));
  }

  function settingsEditor() {
    const st = S.settings;
    const errBox = h('div', { class: 'formerr', hidden: true });
    const mode = h('select', null, S.schema.bandwidthModes.map((m) => h('option', { value: m, selected: m === st.bandwidthMode }, m === 'aggregate' ? 'aggregate (shared ceiling)' : 'perRequest (per-request ceiling)')));
    const enf = h('select', null, S.schema.enforcement.map((m) => h('option', { value: m, selected: m === st.enforcement }, m === 'node' ? 'node (every proxy enforces the full limit)' : 'fleet (limit divided by live nodes)')));
    const body = h('div', null, errBox,
      h('div', { class: 'warnbox' }, 'Aggregate and per-request bandwidth limits cannot be mixed. Switching mode is refused while policies still contain the other kind.'),
      h('label', { class: 'field' }, h('span', null, 'Bandwidth mode'), mode),
      h('label', { class: 'field' }, h('span', null, 'Multi-node enforcement'), enf,
        h('p', { class: 'hint' }, 'fleet assumes clients are spread evenly across proxy nodes and shifts a dead node\'s share to the survivors after about three sync intervals.')));
    let close;
    const save = h('button', { class: 'btn primary', onclick: async () => {
      save.disabled = true;
      try { await api('PUT', '/api/v1/settings', { bandwidthMode: mode.value, enforcement: enf.value }); close(); toast('Settings saved'); await refresh(); }
      catch (e) { errBox.hidden = false; errBox.textContent = e.message; save.disabled = false; }
    } }, 'Save');
    close = openModal('Global settings', body, [h('button', { class: 'btn', onclick: () => close() }, 'Cancel'), save]);
  }

  /* --- raw metrics --- */
  function pageMetrics() {
    const f = S.filter.trim().toLowerCase();
    const rows = S.samples.filter((x) => !f || x.key.toLowerCase().indexOf(f) >= 0).slice(0, 500).map((x) =>
      h('tr', null, td(x.name, 'mono'),
        td(h('div', { class: 'chips' }, Object.keys(x.labels).map((k) => chip(k + '=' + x.labels[k], 'mute')))),
        td(String(x.value), 'num'), td(/_total$|_micro$/.test(x.name) ? L.fmtNum(x.rate) + '/s' : '', 'num')));
    return h('div', null,
      h('div', { class: 'pagehead' }, h('h2', null, 'Raw metrics'), h('div', { class: 'spacer' }),
        h('input', { id: 'filter', type: 'text', placeholder: 'Filter name or label…', value: S.filter, 'aria-label': 'Filter metrics', oninput: (e) => { S.filter = e.target.value; render(); } })),
      h('div', { class: 'card' }, h('p', { class: 'hint' }, S.samples.length + ' series. The same data is available to Prometheus at /metrics (bearer token required).'),
        table(['Metric', 'Labels', { text: 'Value', num: 1 }, { text: 'Rate', num: 1 }], rows, 'No metrics match.')));
  }

  /* ---------- shell ---------- */
  const PAGES = [['dashboard', 'Dashboard', pageDashboard], ['policies', 'Policies', pagePolicies], ['tenants', 'Tenants', pageTenants],
    ['config', 'Configuration', pageConfig], ['metrics', 'Metrics', pageMetrics]];

  function route() {
    const p = (location.hash || '').replace(/^#\/?/, '');
    S.page = PAGES.some((x) => x[0] === p) ? p : 'dashboard';
  }

  function loginView() {
    const err = h('div', { class: 'formerr', hidden: true });
    const tok = h('input', { type: 'password', id: 'tok', autocomplete: 'off', 'aria-label': 'Admin token' });
    const go = async (ev) => {
      ev.preventDefault();
      const candidate = tok.value.trim();
      err.hidden = true;
      try {
        const r = await fetch('/api/v1/cluster', { headers: { Authorization: 'Bearer ' + candidate }, cache: 'no-store' });
        if (r.status === 401) { err.hidden = false; err.textContent = 'That token was not accepted.'; return; }
        if (!r.ok) { err.hidden = false; err.textContent = 'Unexpected response from the node: HTTP ' + r.status; return; }
      } catch (e) { err.hidden = false; err.textContent = 'Cannot reach the node: ' + e.message; return; }
      S.token = candidate;
      sessionStorage.setItem(TOKEN_KEY, S.token);
      S.config = null; S.schema = null; S.prevT = 0; S.prev = null;
      for (const k in S.hist) S.hist[k] = [];
      await refresh(); startPolling();
    };
    return h('div', { class: 'login' }, h('form', { class: 'card', onsubmit: go },
      h('h1', null, 's3qos'), h('p', null, 'Sign in with the admin token configured for this node.'), err,
      h('label', { class: 'field' }, h('span', null, 'Admin token'), tok),
      h('button', { class: 'btn primary', type: 'submit' }, 'Sign in')));
  }

  function shell(content) {
    const ok = !S.error;
    return h('div', null,
      h('div', { class: 'topbar' }, h('div', { class: 'brand' }, 's3qos', h('small', null, S.config ? S.config.nodeId : '')),
        h('nav', { class: 'tabs' }, PAGES.map((p) => h('a', { class: 'tab' + (S.page === p[0] ? ' active' : ''), href: '#/' + p[0], 'aria-current': S.page === p[0] ? 'page' : null }, p[1]))),
        h('div', { class: 'spacer' }),
        h('div', { class: 'status' }, h('span', { class: 'dot' + (ok ? '' : ' bad') }), ok ? 'live, updated ' + new Date(S.lastOk).toLocaleTimeString() : 'disconnected'),
        h('button', { class: 'btn small', onclick: logout }, 'Sign out')),
      h('main', null, S.error ? h('div', { class: 'banner', role: 'alert' }, S.error) : null, content));
  }

  function render() {
    const app = $('app');
    const active = document.activeElement;
    const keep = active && active.id === 'filter' ? { id: 'filter', s: active.selectionStart, e: active.selectionEnd } : null;
    const scroll = window.scrollY;
    route();
    app.replaceChildren(!S.token ? loginView() : !S.loaded ? shell(h('div', { class: 'empty' }, S.error || 'Loading…')) : shell(PAGES.find((x) => x[0] === S.page)[2]()));
    if (keep && $('filter')) { $('filter').focus(); $('filter').setSelectionRange(keep.s, keep.e); }
    window.scrollTo(0, scroll);
  }

  function startPolling() {
    if (S.timer) clearInterval(S.timer);
    S.timer = setInterval(() => { if (!document.hidden && S.token) refresh(); }, POLL_MS);
  }

  function init() {
    document.body.appendChild(h('div', { id: 'modal' }));
    window.addEventListener('hashchange', render);
    render();
    if (S.token) { refresh().then(startPolling); }
  }
  init();
})();
