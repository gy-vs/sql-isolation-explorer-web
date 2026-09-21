'use strict';

// ------------------------------------------------------------ demo templates
const TEMPLATES = {
  '写偏斜 (SI 下发生 / serializable 下中止)': [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'acct_a', value: { balance: 100 } },
    { type: 'insert', txn: 'T0', key: 'acct_b', value: { balance: 100 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1' },
    { type: 'begin', txn: 'T2' },
    { type: 'read', txn: 'T1', key: 'acct_a' },
    { type: 'read', txn: 'T1', key: 'acct_b' },
    { type: 'read', txn: 'T2', key: 'acct_a' },
    { type: 'read', txn: 'T2', key: 'acct_b' },
    { type: 'update', txn: 'T1', key: 'acct_a', value: { balance: -50 } },
    { type: 'update', txn: 'T2', key: 'acct_b', value: { balance: -50 } },
    { type: 'commit', txn: 'T1' },
    { type: 'commit', txn: 'T2' },
  ],
  '死锁 (互相等待成环)': [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'a', value: { v: 1 } },
    { type: 'insert', txn: 'T0', key: 'b', value: { v: 1 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1' },
    { type: 'begin', txn: 'T2' },
    { type: 'update', txn: 'T1', key: 'a', value: { v: 10 } },
    { type: 'update', txn: 'T2', key: 'b', value: { v: 20 } },
    { type: 'update', txn: 'T1', key: 'b', value: { v: 11 } },
    { type: 'update', txn: 'T2', key: 'a', value: { v: 21 } },
    { type: 'commit', txn: 'T1' },
    { type: 'commit', txn: 'T2' },
  ],
  '不可重复读 (read committed)': [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'a', value: { v: 1 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1' },
    { type: 'begin', txn: 'T2' },
    { type: 'read', txn: 'T1', key: 'a' },
    { type: 'update', txn: 'T2', key: 'a', value: { v: 2 } },
    { type: 'commit', txn: 'T2' },
    { type: 'read', txn: 'T1', key: 'a' },
    { type: 'commit', txn: 'T1' },
  ],
  '幻读 (谓词范围, read committed)': [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'e1', value: { balance: 120 } },
    { type: 'insert', txn: 'T0', key: 'e2', value: { balance: 80 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1' },
    { type: 'begin', txn: 'T2' },
    { type: 'select', txn: 'T1', pred: { kind: 'field', field: 'balance', op: '>=', value: 100 } },
    { type: 'insert', txn: 'T2', key: 'e3', value: { balance: 150 } },
    { type: 'commit', txn: 'T2' },
    { type: 'select', txn: 'T1', pred: { kind: 'field', field: 'balance', op: '>=', value: 100 } },
    { type: 'commit', txn: 'T1' },
  ],
  '并发更新同一行 (SI 首提交者胜)': [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'a', value: { n: 0 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1' },
    { type: 'begin', txn: 'T2' },
    { type: 'update', txn: 'T1', key: 'a', value: { n: 1 } },
    { type: 'update', txn: 'T2', key: 'a', value: { n: 2 } },
    { type: 'commit', txn: 'T1' },
    { type: 'commit', txn: 'T2' },
  ],
  '保存点与部分回滚': [
    { type: 'begin', txn: 'T1' },
    { type: 'insert', txn: 'T1', key: 'a', value: { v: 1 } },
    { type: 'savepoint', txn: 'T1', name: 'sp1' },
    { type: 'insert', txn: 'T1', key: 'b', value: { v: 2 } },
    { type: 'rollback_to', txn: 'T1', name: 'sp1' },
    { type: 'insert', txn: 'T1', key: 'c', value: { v: 3 } },
    { type: 'commit', txn: 'T1' },
    { type: 'begin', txn: 'T2', readOnly: true },
    { type: 'read', txn: 'T2', key: 'a' },
    { type: 'read', txn: 'T2', key: 'b' },
    { type: 'read', txn: 'T2', key: 'c' },
    { type: 'commit', txn: 'T2' },
  ],
  '只读事务 (写被拒绝)': [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'a', value: { v: 1 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1', readOnly: true },
    { type: 'read', txn: 'T1', key: 'a' },
    { type: 'update', txn: 'T1', key: 'a', value: { v: 9 } },
    { type: 'commit', txn: 'T1' },
  ],
};

// ------------------------------------------------------------------- state
const S = { schedules: [], current: null, selAnomaly: null, timer: null, viewpoint: '' };

async function api(path, method = 'GET', body) {
  const r = await fetch(path, {
    method,
    headers: { 'content-type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  return r.json();
}

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const j = (v) => esc(JSON.stringify(v));

// ------------------------------------------------------------------ render
function render() {
  const st = S.current;
  $('clockbar').innerHTML = st
    ? `逻辑时钟 <b>${st.engine.clock}</b> · 提交序号 <b>${st.engine.commitSeq}</b> · 隔离级别 <b>${st.isolation}</b> · ${st.finished ? '调度已完成' : '调度进行中'}`
    : '未选择调度';

  // schedule list
  $('schedule-list').innerHTML =
    S.schedules
      .map(
        (s) => `<div class="sched-item ${st && s.id === st.id ? 'active' : ''}" data-id="${s.id}">
      <div>${esc(s.name)}</div><div class="meta">${s.isolation} · ${s.steps}/${s.ops} 步 · 历史运行 ${s.runs}</div>
    </div>`
      )
      .join('') || '<div class="muted">（空）</div>';
  document.querySelectorAll('.sched-item').forEach((el) => (el.onclick = () => load(el.dataset.id)));
  if (!st) return;

  // ops queue
  const cursor = st.scheduler.cursor;
  $('ops-list').innerHTML = st.scheduler.ops
    .map((o, i) => {
      const cls = ['op', o.status, i === cursor && o.status === 'pending' ? 'next' : ''].join(' ');
      return `<div class="${cls}">#${i} ${esc(fmtOp(o))}${o.result ? ` <span class="muted">→ ${esc(o.result)}</span>` : ''}</div>`;
    })
    .join('');

  // timeline
  const hl = highlightSet();
  $('timeline').innerHTML = st.engine.events
    .map((e) => `<div class="ev ${e.type} ${hl.events.has(e.seq) ? 'hl' : ''}" data-seq="${e.seq}">
      <span class="clk">t=${e.clock}</span>${esc(fmtEvent(e))}</div>`)
    .join('');
  const tl = $('timeline');
  tl.scrollTop = tl.scrollHeight;

  // txns
  $('txns').innerHTML =
    st.engine.txns
      .map(([id, t]) => {
        const snap = t.snapshot == null ? '—' : t.snapshot;
        return `<div class="txn"><b>${esc(id)}</b>
        <span class="badge ${t.state}">${t.state}</span>${t.readOnly ? '<span class="badge ro">read-only</span>' : ''}
        <div class="snap">快照@${snap} · 开始t=${t.startClock}${t.commitSeq ? ` · 提交#${t.commitSeq}` : ''}${t.abortReason ? ` · ${esc(t.abortReason)}` : ''}${t.waitingOn ? ` · 等待 ${esc(t.waitingOn)}` : ''}</div>
        ${t.state === 'blocked' ? `<button data-abort="${esc(id)}">强制回滚</button>` : ''}
      </div>`;
      })
      .join('') || '<div class="muted">（无事务）</div>';
  document.querySelectorAll('[data-abort]').forEach(
    (b) => (b.onclick = async () => { await api(`/api/schedules/${st.id}/abort`, 'POST', { txn: b.dataset.abort }); await refresh(); })
  );

  // viewpoint selector
  const vp = $('viewpoint');
  const prevVp = S.viewpoint;
  vp.innerHTML = '<option value="">（上帝视角）</option>' + st.engine.txns.map(([id]) => `<option ${id === prevVp ? 'selected' : ''}>${esc(id)}</option>`).join('');
  vp.onchange = () => { S.viewpoint = vp.value; render(); };

  // rows & versions
  const viewTxn = S.viewpoint ? st.engine.txns.find(([id]) => id === S.viewpoint)?.[1] : null;
  $('rows').innerHTML =
    st.engine.rows
      .map(([key, vs]) => {
        const visibleVid = viewTxn ? visibleVersion(st.engine, viewTxn, key)?.vid : null;
        const vers = vs
          .map((v) => {
            const val = v.value == null ? '<span class="tomb">∅ 墓碑</span>' : j(v.value);
            const meta =
              v.status === 'committed' ? `committed #${v.createdSeq}` : v.status === 'inflight' ? `inflight` : 'aborted';
            return `<div class="ver ${v.status} ${v.vid === visibleVid ? 'visible-now' : ''}">v${v.vid} ${val} <span class="muted">by ${esc(v.createdBy)} · ${meta}</span></div>`;
          })
          .join('');
        return `<div class="rowkey"><span class="k">${esc(key)}</span>${vers}</div>`;
      })
      .join('') || '<div class="muted">（无数据）</div>';

  // locks
  $('locks').innerHTML =
    st.engine.locks
      .map(([key, l]) => `<div class="lock">🔒 <b>${esc(key)}</b> 持有 ${esc(l.holder)}
        ${l.queue.map((q) => `<div class="q">⏳ ${esc(q.txnId)} 等待</div>`).join('')}</div>`)
      .join('') || '<div class="muted">（无锁）</div>';

  // conflict edges
  $('edges').innerHTML =
    st.engine.edges
      .map((e, i) => `<div class="edge ${e.type} ${hl.edges.has(i) ? 'hl' : ''}" data-edge="${i}">
        ${esc(e.from)} —<b>${e.type}</b>(${esc(e.key)}${e.predicate ? ', 谓词' : ''})→ ${esc(e.to)} <span class="muted">t=${e.clock}</span></div>`)
      .join('') || '<div class="muted">（无边）</div>';

  // anomalies
  $('anomalies').innerHTML =
    st.engine.anomalies
      .map((a) => `<div class="anom ${S.selAnomaly === a.id ? 'sel' : ''}" data-anom="${a.id}">
        <span class="kind">${esc(ANOM_NAMES[a.kind] || a.kind)}</span> <span class="muted">t=${a.clock}</span>
        <div class="detail">${esc(fmtAnomaly(a))}</div>
        <div class="detail">点击查看形成冲突的读写边 →</div>
      </div>`)
      .join('') || '<div class="muted">（无异常。脏读在本模型三种级别下均被阻止。）</div>';
  document.querySelectorAll('[data-anom]').forEach((el) => {
    el.onclick = () => {
      S.selAnomaly = S.selAnomaly === +el.dataset.anom ? null : +el.dataset.anom;
      render();
      const first = document.querySelector('.edge.hl');
      if (first) first.scrollIntoView({ block: 'center', behavior: 'smooth' });
    };
  });
}

const ANOM_NAMES = {
  dirty_read: '脏读', non_repeatable_read: '不可重复读', phantom: '幻读',
  write_skew: '写偏斜', serialization_failure: '序列化失败', deadlock: '死锁',
};

function fmtAnomaly(a) {
  switch (a.kind) {
    case 'non_repeatable_read': return `${a.txn} 重读 ${a.key}: ${JSON.stringify(a.before)} → ${JSON.stringify(a.after)}`;
    case 'phantom': return `${a.txn} 谓词重查: +${JSON.stringify(a.added)} -${JSON.stringify(a.removed)}`;
    case 'write_skew': return `rw 环: ${(a.cycle || []).join(' → ')}`;
    case 'serialization_failure': return `${a.txn} 被中止 (${a.reason || ''}) ${a.cycle ? '环: ' + a.cycle.join(' → ') : a.key || ''}`;
    case 'deadlock': return `等待环: ${(a.cycle || []).join(' → ')}`;
    default: return JSON.stringify(a);
  }
}

// anomaly -> involved txns -> edges & events to highlight
function highlightSet() {
  const out = { edges: new Set(), events: new Set() };
  const st = S.current;
  if (!st || S.selAnomaly == null) return out;
  const a = st.engine.anomalies[S.selAnomaly];
  if (!a) return out;
  const txns = new Set(a.cycle || [a.txn].filter(Boolean));
  if (a.kind === 'non_repeatable_read' || a.kind === 'phantom') txns.add(a.txn);
  st.engine.edges.forEach((e, i) => {
    if (txns.has(e.from) && txns.has(e.to)) out.edges.add(i);
    if (a.key && e.key === a.key && (txns.has(e.from) || txns.has(e.to))) out.edges.add(i);
  });
  st.engine.events.forEach((e) => {
    if (e.txn && txns.has(e.txn) && Math.abs(e.clock - a.clock) <= 12) out.events.add(e.seq);
  });
  return out;
}

// client-side visibility for the "viewpoint" selector (mirrors engine rules)
function visibleVersion(engine, txn, key) {
  const row = engine.rows.find(([k]) => k === key);
  if (!row) return null;
  const snap = engine.isolation === 'read_committed' || txn.snapshot == null ? engine.commitSeq : txn.snapshot;
  for (const v of row[1]) {
    if (v.status === 'aborted') continue;
    if (v.createdBy === txn.id) return v;
    if (v.status !== 'committed') continue;
    if (v.createdSeq > snap) continue;
    return v;
  }
  return null;
}

function fmtOp(o) {
  const parts = [o.type, o.txn, o.key, o.value != null ? JSON.stringify(o.value) : '', o.pred ? JSON.stringify(o.pred) : '', o.name || '', o.readOnly ? 'read-only' : ''];
  return parts.filter(Boolean).join(' ');
}

function fmtEvent(e) {
  switch (e.type) {
    case 'begin': return `${e.txn} BEGIN${e.readOnly ? ' (read-only)' : ''}`;
    case 'read': return `${e.txn} READ ${e.key} → ${JSON.stringify(e.value)} (快照@${e.snapshot})`;
    case 'select': return `${e.txn} SELECT ${JSON.stringify(e.pred)} → [${(e.keys || []).join(', ')}] (快照@${e.snapshot})`;
    case 'write': return `${e.txn} ${e.op.toUpperCase()} ${e.key} = ${JSON.stringify(e.value)}`;
    case 'commit': return `${e.txn} COMMIT (#${e.commitSeq})`;
    case 'rollback': return `${e.txn} ROLLBACK`;
    case 'abort': return `${e.txn} ABORTED (${e.reason})`;
    case 'savepoint': return `${e.txn} SAVEPOINT ${e.name}`;
    case 'rollback_to': return `${e.txn} ROLLBACK TO ${e.name}`;
    case 'lock_wait': return `${e.txn} 等待 ${e.key} 上的锁 (持有者 ${e.holder})`;
    case 'lock_grant': return `${e.txn} 获得 ${e.key} 上的锁`;
    case 'lock_release': return `${e.txn} 释放 ${e.key} 上的锁`;
    case 'anomaly': return `⚠ ${ANOM_NAMES[e.kind] || e.kind}`;
    case 'error': return `✗ ${e.message}`;
    default: return JSON.stringify(e);
  }
}

// ------------------------------------------------------------------ actions
async function refresh() {
  const list = await api('/api/schedules');
  S.schedules = list.schedules;
  if (S.schedules.length && !S.current) {
    const last = localStorage.getItem('mvcc.current');
    await load(S.schedules.find((s) => s.id === last)?.id || S.schedules[0].id);
    return;
  }
  if (S.current) {
    const cur = await api(`/api/schedules/${S.current.id}`);
    if (!cur.error) S.current = cur;
  }
  render();
}

async function load(id) {
  const st = await api(`/api/schedules/${id}`);
  if (st.error) return;
  S.current = st;
  S.selAnomaly = null;
  localStorage.setItem('mvcc.current', id);
  render();
}

async function createSchedule() {
  $('create-err').textContent = '';
  let ops;
  try { ops = JSON.parse($('ops-editor').value); } catch (e) { $('create-err').textContent = 'JSON 解析失败: ' + e.message; return; }
  const r = await api('/api/schedules', 'POST', { name: $('sched-name').value, isolation: $('sched-isolation').value, ops });
  if (r.error) { $('create-err').textContent = r.error; return; }
  S.current = r;
  localStorage.setItem('mvcc.current', r.id);
  await refresh();
}

function stopAuto() {
  clearInterval(S.timer);
  S.timer = null;
  $('pause-btn').disabled = true;
  $('auto-btn').disabled = false;
}

async function boot() {
  const tpl = $('template');
  Object.keys(TEMPLATES).forEach((name) => {
    const o = document.createElement('option');
    o.textContent = name;
    tpl.appendChild(o);
  });
  tpl.onchange = () => {
    if (!tpl.value) return;
    $('ops-editor').value = JSON.stringify(TEMPLATES[tpl.value], null, 2);
    $('sched-name').value = tpl.value;
    if (tpl.value.includes('read committed')) $('sched-isolation').value = 'read_committed';
  };

  $('create-btn').onclick = createSchedule;
  $('step-btn').onclick = async () => {
    if (!S.current) return;
    await api(`/api/schedules/${S.current.id}/step`, 'POST');
    await refresh();
  };
  $('run-btn').onclick = async () => {
    if (!S.current) return;
    await api(`/api/schedules/${S.current.id}/run`, 'POST');
    await refresh();
  };
  $('auto-btn').onclick = () => {
    if (!S.current || S.timer) return;
    $('auto-btn').disabled = true;
    $('pause-btn').disabled = false;
    S.timer = setInterval(async () => {
      const r = await api(`/api/schedules/${S.current.id}/step`, 'POST');
      await refresh();
      if (r.result && r.result.done) stopAuto();
    }, 700);
  };
  $('pause-btn').onclick = stopAuto;
  $('reset-btn').onclick = async () => {
    if (!S.current) return;
    await api(`/api/schedules/${S.current.id}/reset`, 'POST');
    S.selAnomaly = null;
    await refresh();
  };
  $('delete-btn').onclick = async () => {
    if (!S.current) return;
    await api(`/api/schedules/${S.current.id}`, 'DELETE');
    S.current = null;
    localStorage.removeItem('mvcc.current');
    await refresh();
  };

  await refresh();
}

boot();
