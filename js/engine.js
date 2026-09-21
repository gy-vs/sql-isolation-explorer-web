/*
 * In-memory MVCC engine for the isolation-level workbench.
 *
 * No real database: each key carries a version chain; every transaction gets a
 * logical snapshot; locks are plain exclusive key locks.  All state is plain
 * JSON-serializable so a running schedule can survive a page reload.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.MVCC = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const ISO = {
    RU: 'RU', // read uncommitted (educational extra: needed to *see* dirty reads)
    RC: 'RC', // read committed
    SI: 'SI', // snapshot isolation
    SE: 'SE'  // serializable (SSI-style rw-cycle detection)
  };

  const ISO_LABEL = {
    RU: '读未提交 RU',
    RC: '读已提交 RC',
    SI: '快照隔离 SI',
    SE: '可序列化 SE'
  };

  class EngineError extends Error {
    constructor(code, message, extra) {
      super(message);
      this.code = code;
      Object.assign(this, extra || {});
    }
  }

  /* ---------- small helpers ---------- */

  function latestCommitted(row) {
    if (!row) return null;
    let best = null;
    for (const v of row.versions) {
      if (v.aborted || v.ts == null) continue;
      if (!best || v.ts > best.ts) best = v;
    }
    return best;
  }

  // Most recent version visible under a snapshot descriptor.
  // reader: the reading transaction (own uncommitted writes are always visible)
  function visibleVersion(row, snap, reader) {
    if (!row) return null;
    let best = null;
    for (const v of row.versions) {
      if (v.aborted) continue;
      let visible;
      if (v.ts == null) {
        visible = v.txId === reader.id || snap.iso === ISO.RU;
      } else {
        visible = snap.iso === ISO.RU || v.ts <= snap.seq;
      }
      // versions are ordered by creation (ord); a dirty version is the newest
      if (visible && (!best || v.ord > best.ord)) best = v;
    }
    return best;
  }

  function predMatch(v, p) {
    if (!v || v.deleted) return false;
    const x = typeof v.val === 'number' ? v.val : parseFloat(v.val);
    const y = typeof p.value === 'number' ? p.value : parseFloat(p.value);
    switch (p.op) {
      case '>': return x > y;
      case '<': return x < y;
      case '>=': return x >= y;
      case '<=': return x <= y;
      case '<>': return x !== y;
      case '=': default: return x === y;
    }
  }

  function predKey(p) { return p.op + ':' + p.value; }

  /* ---------- directed graph of ordering dependencies ---------- */

  class Graph {
    constructor() { this.adj = {}; }
    addNode(n) { if (!(n in this.adj)) this.adj[n] = []; }
    edge(a, b) {
      this.addNode(a); this.addNode(b);
      if (!this.adj[a].includes(b)) this.adj[a].push(b);
    }
    // cycle containing `mustInclude` (node id), ignoring excluded nodes
    findCycle(mustInclude, exclude) {
      const excludeSet = new Set(exclude || []);
      const stack = [];
      const onStack = new Set();
      let found = null;
      const dfs = (u) => {
        if (found) return;
        stack.push(u); onStack.add(u);
        for (const w of (this.adj[u] || [])) {
          if (excludeSet.has(w)) continue;
          if (onStack.has(w)) {
            const i = stack.indexOf(w);
            const cyc = stack.slice(i).concat(w);
            if (!mustInclude || cyc.includes(mustInclude)) { found = cyc; return; }
          } else dfs(w);
          if (found) return;
        }
        stack.pop(); onStack.delete(u);
      };
      if (mustInclude) { if (!excludeSet.has(mustInclude)) dfs(mustInclude); }
      else { for (const n of Object.keys(this.adj)) { dfs(n); if (found) break; } }
      return found;
    }
  }

  /* ---------- engine ---------- */

  class Engine {
    constructor() {
      this.rows = {};        // key -> {key, versions:[{id,txId,ts,val,deleted,aborted,stepId}]}
      this.txns = {};        // id -> txn state
      this.edges = [];       // {id, from, to, kind: 'rw'|'ww'|'wr', rowId, stepId, label, stale}
      this.events = [];      // append-only execution / anomaly events
      this.locks = {};       // key -> txId  (exclusive key locks)
      this.clock = 0;        // logical event tick
      this.commitSeq = 0;    // commit timestamp counter
      this.verOrd = 0;       // global version creation order
      this._id = 0;
    }

    _nid(prefix) { return prefix + '_' + (++this._id); }

    /* serialization */
    toJSON() {
      return {
        rows: this.rows, txns: this.txns, edges: this.edges, events: this.events,
        locks: this.locks, clock: this.clock, commitSeq: this.commitSeq,
        verOrd: this.verOrd, _id: this._id
      };
    }
    static fromJSON(s) {
      const e = new Engine();
      Object.assign(e, s);
      return e;
    }

    emit(type, severity, data) {
      const ev = Object.assign({
        id: this._nid('ev'), tick: ++this.clock, type, severity: severity || 'info'
      }, data);
      this.events.push(ev);
      return ev;
    }

    seed(entries) {
      for (const [key, val] of Object.entries(entries)) {
        const v = {
          id: this._nid('v'), txId: 'SYS', ts: 0, ord: 0,
          val, deleted: false, aborted: false, stepId: 'seed'
        };
        this.rows[key] = { key, versions: [v] };
      }
      this.txns.SYS = {
        id: 'SYS', iso: 'SYS', status: 'committed', beginSeq: 0, commitSeq: 0,
        readOnly: false, writes: {}, locks: [], readsByKey: {}, predicateReads: [],
        savepoints: [], label: '初始数据'
      };
    }

    _tx(id) {
      const t = this.txns[id];
      if (!t) throw new EngineError('NO_TXN', `事务 ${id} 不存在`);
      return t;
    }
    _active(id) {
      const t = this._tx(id);
      if (t.status !== 'active') {
        throw new EngineError(
          t.status === 'committed' ? 'TXN_DONE' : 'TXN_ABORTED',
          `事务 ${id} 已${t.status === 'committed' ? '提交' : '中止'}，不能再执行语句（终态只发生一次）`
        );
      }
      return t;
    }

    snapshot(t) {
      return { iso: t.iso, seq: t.iso === ISO.SI || t.iso === ISO.SE ? t.beginSeq : this.commitSeq };
    }

    /* ---- transaction lifecycle ---- */

    begin({ id, iso, readOnly, label }) {
      if (this.txns[id]) {
        throw new EngineError('DUP_TXN', `事务 ${id} 已经存在（${this.txns[id].status}）`);
      }
      const t = {
        id, iso: iso || ISO.RC, readOnly: !!readOnly,
        status: 'active',
        beginSeq: this.commitSeq,
        commitSeq: null,
        label: label || id,
        writes: {},          // key -> versionId (its pending version)
        locks: [],           // keys it holds x-locks on
        readsByKey: {},      // key -> {versionId, val, deleted, stepId}
        predicateReads: [],  // {op,value,firstCount,ids,stepId}
        savepoints: [],
        startedAt: ++this.clock
      };
      this.txns[id] = t;
      this.emit('begin', 'info', {
        txId: id,
        message: `${id} 开始 · ${ISO_LABEL[t.iso]}${readOnly ? ' · 只读' : ''} · 快照序号 ${t.beginSeq}`
      });
      return t;
    }

    /* ---- locking ---- */

    tryLockKey(txId, key, stepId) {
      const t = this._active(txId);
      const holder = this.locks[key];
      if (holder === txId) return { granted: true };
      if (!holder) {
        this.locks[key] = txId;
        t.locks.push(key);
        return { granted: true };
      }
      if (this.txns[holder] && this.txns[holder].status !== 'active') {
        // stale holder (shouldn't happen, but stay consistent)
        delete this.locks[key];
        this.locks[key] = txId;
        t.locks.push(key);
        return { granted: true };
      }
      this.emit('lock_wait', 'wait', {
        txId, holder, key, stepId,
        message: `${txId} 等待 ${key} 上的写锁（持有者 ${holder}）`
      });
      return { granted: false, holder };
    }

    _releaseLocks(t) {
      const released = [];
      for (const k of t.locks) {
        if (this.locks[k] === t.id) { delete this.locks[k]; released.push(k); }
      }
      t.locks = [];
      return released;
    }

    /* ---- reads ---- */

    read({ txId, key, stepId }) {
      const t = this._active(txId);
      const snap = this.snapshot(t);
      const row = this.rows[key];
      const v = visibleVersion(row, snap, t);

      // wr edge: reading someone else's committed (or dirty, RU) version
      if (v && v.txId !== txId) {
        this._addEdge({
          from: v.txId, to: txId, kind: 'wr', rowId: key, stepId,
          label: v.ts == null ? `${v.txId} 脏写 → ${txId} 脏读 ${key}` : `${v.txId} 写 ${key} → ${txId} 读`,
          dirty: v.ts == null
        });
      }

      const value = v && !v.deleted ? v.val : undefined;
      const deleted = v ? v.deleted : true;
      const dirty = !!(v && v.ts == null && v.txId !== txId);

      const prev = t.readsByKey[key];
      let anomaly = null;
      if (prev) {
        const changed = prev.versionId !== (v && v.id) || prev.deleted !== deleted ||
          JSON.stringify(prev.val) !== JSON.stringify(value);
        if (changed) {
          if (dirty) {
            anomaly = this._anomaly('dirty_read', {
              txId, key, stepId, prev, now: v,
              message: `脏读：${txId} 在 ${key} 上读到 ${v.txId} 尚未提交的值 ${fmt(value)}`,
              relatedStepIds: [stepId, v.stepId].filter(Boolean)
            });
          } else if (t.iso === ISO.RC) {
            anomaly = this._anomaly('non_repeatable_read', {
              txId, key, stepId, prev, now: v,
              message: `不可重复读：${txId} 重读 ${key}，值从 ${fmt(prev.val)} 变为 ${fmt(value)}`,
              relatedStepIds: [stepId, prev.stepId, v && v.stepId].filter(Boolean)
            });
          }
        } else if (dirty) {
          anomaly = this._anomaly('dirty_read', {
            txId, key, stepId, prev, now: v,
            message: `脏读：${txId} 读到 ${v.txId} 未提交的 ${key}=${fmt(value)}`,
            relatedStepIds: [stepId, v.stepId].filter(Boolean)
          });
        }
      } else if (dirty) {
        anomaly = this._anomaly('dirty_read', {
          txId, key, stepId, now: v,
          message: `脏读：${txId} 读到 ${v.txId} 未提交的 ${key}=${fmt(value)}`,
          relatedStepIds: [stepId, v.stepId].filter(Boolean)
        });
      }

      t.readsByKey[key] = {
        versionId: v ? v.id : null, val: value, deleted,
        stepId, dirty
      };

      this.emit('read', dirty ? 'warn' : 'info', {
        txId, key, value, deleted, dirty, stepId,
        versionId: v ? v.id : null,
        anomalyId: anomaly ? anomaly.id : null,
        visibleVersionTs: v ? v.ts : null,
        message: `${txId} 读 ${key} = ${deleted ? '∅ (不存在/已删除)' : fmt(value)}` +
          (dirty ? '（未提交版本！）' : '') +
          `  [快照≤${snap.seq}]`
      });
      return { value, deleted, dirty, version: v };
    }

    predicateRead({ txId, op, value, stepId }) {
      const t = this._active(txId);
      const snap = this.snapshot(t);
      const p = { op, value };
      const matched = [];
      for (const row of Object.values(this.rows)) {
        const v = visibleVersion(row, snap, t);
        if (predMatch(v, p)) matched.push({ key: row.key, val: v.val, versionId: v.id, ts: v.ts });
      }
      matched.sort((a, b) => a.key.localeCompare(b.key));
      const ids = matched.map(m => m.key + '@' + m.versionId).join(',');

      const sig = predKey(p);
      const prev = t.predicateReads.find(x => predKey(x) === sig);
      let anomaly = null;
      if (prev) {
        const oldSet = new Set(prev.ids.split(',').filter(Boolean));
        const newSet = new Set(ids.split(',').filter(Boolean));
        const added = [...newSet].filter(x => !oldSet.has(x));
        const removed = [...oldSet].filter(x => !newSet.has(x));
        if (added.length || removed.length) {
          if (t.iso === ISO.RC) {
            anomaly = this._anomaly('phantom', {
              txId, stepId, op, value, prev: prev.stepId,
              message: `幻读：${txId} 重复范围读 val ${op} ${value}，新增/消失行 [${added.concat(removed).join(', ')}]`,
              relatedStepIds: [stepId, prev.stepId]
            });
          }
        }
      }
      if (!prev) t.predicateReads.push({ op, value, firstCount: matched.length, ids, stepId });

      this.emit('predicate_read', anomaly ? 'warn' : 'info', {
        txId, op, value, rows: matched, stepId,
        anomalyId: anomaly ? anomaly.id : null,
        message: `${txId} 范围读 val ${op} ${value} → ${matched.length} 行 {${matched.map(m => m.key + '=' + m.val).join(', ')}}  [快照≤${snap.seq}]`
      });
      return { rows: matched };
    }

    /* ---- writes (lock must already be granted by scheduler) ---- */

    mutate({ txId, key, op, val, stepId }) {
      const t = this._active(txId);
      if (t.readOnly) {
        throw new EngineError('READ_ONLY', `只读事务 ${txId} 不能执行写操作`, { txId });
      }
      if (this.locks[key] && this.locks[key] !== txId) {
        throw new EngineError('NOT_LOCKED', `内部错误：${key} 未获得锁`);
      }
      const row = this.rows[key];
      const latest = latestCommitted(row);
      const live = !!(latest && !latest.deleted);

      if (op === 'insert' && live) {
        throw new EngineError('UNIQUE_VIOLATION', `${key} 已存在（值 ${fmt(latest.val)}），插入冲突`, { txId });
      }
      if ((op === 'update' || op === 'delete') && !live) {
        this.emit('write', 'muted', {
          txId, key, op, stepId, noop: true,
          message: `${txId} ${op === 'delete' ? '删除' : '更新'} ${key}：没有符合条件的已提交行，语句无效果`
        });
        return { noop: true };
      }

      // First-committer-wins for snapshot-based isolation: a committed version
      // newer than the txn's snapshot appeared -> it must abort.
      if ((t.iso === ISO.SI || t.iso === ISO.SE) && latest && latest.ts > t.beginSeq) {
        const rel = new Set([stepId, latest.stepId].filter(Boolean));
        const edgeIds = this.edges
          .filter(e => !e.stale && (e.rowId === key) &&
            (e.from === txId || e.to === txId || e.from === latest.txId || e.to === latest.txId))
          .map(e => e.id);
        this._anomaly('write_conflict', {
          txId, key, stepId, conflictTxId: latest.txId, conflictVersionId: latest.id,
          edgeIds, relatedStepIds: [...rel],
          message: `写冲突：${key} 在 ${txId} 的快照（序号 ${t.beginSeq}）之后已被 ${latest.txId} 提交（序号 ${latest.ts}），先提交者获胜，${txId} 中止`
        });
        throw new EngineError('WRITE_CONFLICT',
          `写冲突：${key} 在 ${txId} 的快照（序号 ${t.beginSeq}）之后已被提交新版本（序号 ${latest.ts}），先提交者获胜`,
          { txId, key, conflictVersionId: latest.id, conflictTxId: latest.txId });
      }

      let v;
      if (t.writes[key]) {
        v = row.versions.find(x => x.id === t.writes[key]);
      } else {
        v = {
          id: this._nid('v'), txId: txId, ts: null, ord: ++this.verOrd,
          val: latest ? latest.val : null,
          deleted: false, aborted: false, stepId
        };
        if (!row) this.rows[key] = { key, versions: [v] };
        else row.versions.push(v);
        t.writes[key] = v.id;
      }
      if (op === 'delete') { v.deleted = true; }
      else if (op === 'insert' || op === 'update' || op === 'upsert') {
        v.deleted = false;
        if (val !== undefined) v.val = val;
      }
      v.stepId = stepId;

      const edgeIds = this._antiEdges(t, key, latest, v, stepId);

      this.emit('write', 'info', {
        txId, key, op, value: v.val, deleted: v.deleted, stepId, versionId: v.id,
        edgeIds,
        waited: arguments[0].waited || false,
        message: `${txId} ${opLabel(op)} ${key}${v.deleted ? '（删除标记）' : ' = ' + fmt(v.val)}` +
          `  [未提交版本 ${v.id}]`
      });
      return { version: v, edgeIds };
    }

    // create rw edges: other active SI/SE readers saw the pre-write state of key
    _antiEdges(writer, key, oldV, newV, stepId) {
      const ids = [];
      const oldMatch = !!(oldV && !oldV.deleted);
      const newMatch = !newV.deleted;
      for (const t2 of Object.values(this.txns)) {
        if (t2.id === writer.id || t2.id === 'SYS') continue;
        if (t2.status !== 'active') continue;
        if (t2.iso !== ISO.SI && t2.iso !== ISO.SE) continue;

        // point reads
        const r = t2.readsByKey[key];
        if (r) {
          const oldVisible = oldV && oldV.ts != null && oldV.ts <= t2.beginSeq;
          const valueChanged = r.versionId !== newV.id &&
            (JSON.stringify(r.val) !== JSON.stringify(newV.deleted ? undefined : newV.val) ||
              !!r.deleted !== newV.deleted);
          if (valueChanged && (r.deleted || oldVisible || r.versionId == null)) {
            const e = this._addEdge({
              from: t2.id, to: writer.id, kind: 'rw', rowId: key, stepId,
              readStepId: r.stepId,
              label: `${t2.id} 读过 ${key} 旧值 → ${writer.id} 改写（读写依赖）`
            });
            ids.push(e.id);
          }
        }
        // predicate reads
        for (const pr of t2.predicateReads) {
          const p = { op: pr.op, value: pr.value };
          const wasIn = oldV && oldV.ts != null && oldV.ts <= t2.beginSeq && predMatch(oldV, p);
          const nowIn = predMatch(newV, p);
          if (wasIn !== nowIn) {
            const e = this._addEdge({
              from: t2.id, to: writer.id, kind: 'rw', rowId: key, stepId,
              predicate: pr.op + ' ' + pr.value, readStepId: pr.stepId,
              label: `${t2.id} 的谓词读(${pr.op} ${pr.value})${wasIn ? '不再包含' : '将包含'} ${key} → ${writer.id} 的写`
            });
            ids.push(e.id);
          }
        }
      }
      return ids;
    }

    _addEdge(d) {
      const sig = [d.from, d.to, d.kind, d.rowId || '', d.predicate || '', d.readStepId || ''].join('|');
      const found = this.edges.find(e => e._sig === sig);
      if (found) return found;
      const e = Object.assign({ id: this._nid('e'), stale: false, _sig: sig }, d);
      this.edges.push(e);
      return e;
    }

    _anomaly(kind, data) {
      const ev = this.emit(kind, 'anomaly', Object.assign({ kind }, data));
      return ev;
    }

    /* ---- commit / abort ---- */

    commit(txId, stepId) {
      const t = this._active(txId);

      // first-committer-wins recheck for all updated keys
      if (t.iso === ISO.SI || t.iso === ISO.SE) {
        for (const key of Object.keys(t.writes)) {
          const vId = t.writes[key];
          const row = this.rows[key];
          const latest = latestCommitted(row);
          if (latest && latest.ts > t.beginSeq && latest.txId !== txId) {
            this._fail(t, 'WRITE_CONFLICT',
              `提交失败：${key} 已被 ${latest.txId} 先提交（快照序号 ${t.beginSeq} < ${latest.ts}），先提交者获胜`,
              stepId, latest);
            throw new EngineError('WRITE_CONFLICT', '提交时写冲突', { txId });
          }
        }
      }

      // commit: freeze pending versions, add ww edges against versions overwritten
      this.commitSeq += 1;
      t.commitSeq = this.commitSeq;
      const wwEdgeIds = [];
      for (const key of Object.keys(t.writes)) {
        const row = this.rows[key];
        const v = row.versions.find(x => x.id === t.writes[key]);
        const base = this._baseCommitted(row, v);
        v.ts = t.commitSeq;
        if (base && base.txId !== txId) {
          const e = this._addEdge({
            from: base.txId, to: txId, kind: 'ww', rowId: key, stepId,
            label: `${base.txId} 先写 ${key} → ${txId} 覆盖（写写顺序）`
          });
          wwEdgeIds.push(e.id);
        }
      }
      t.status = 'committed';
      const released = this._releaseLocks(t);

      // SSI validation *after* this txn becomes committed: an rw cycle is only
      // dangerous once every node on it is committed (the cycle has actually
      // closed).  At the earlier commit, peers were still active so it passed;
      // this "last committer" is therefore the one that must abort.
      if (t.iso === ISO.SE) {
        const cyc = this._committedCycle(t.id);
        if (cyc) {
          const cycleEdges = this._cycleEdges(cyc);
          // undo the just-assigned commit before reporting failure
          this._rollbackCommit(t, 'SERIALIZATION');
          this._fail(t, 'SERIALIZATION',
            `序列化失败：${t.id} 提交后读写依赖环闭合 ${cyc.slice(0, -1).join(' → ')} → ${cyc[0]}，环上事务均已提交，不存在串行执行顺序`,
            stepId, null, { cycle: cyc, cycleEdges });
          throw new EngineError('SERIALIZATION', '序列化失败（依赖环）', { txId, cycle: cyc, cycleEdges });
        }
      }

      this.emit('commit', 'success', {
        txId, stepId, commitSeq: t.commitSeq, wwEdgeIds, releasedKeys: released,
        message: `${txId} 提交 ✓ · 提交序号 #${t.commitSeq} · ${Object.keys(t.writes).length} 个版本生效`
      });

      // SI: if this commit closes an rw cycle, the anomaly *happens* (write skew)
      if (t.iso === ISO.SI) this._detectWriteSkew(t, stepId);

      return { releasedKeys: released, commitSeq: t.commitSeq };
    }

    // cycle whose nodes are all committed (SYS excluded) and include txId
    _committedCycle(txId, kind) {
      const g = new Graph();
      for (const x of Object.values(this.txns)) {
        if (x.id === 'SYS' || x.status !== 'committed') continue;
        g.addNode(x.id);
      }
      for (const e of this.edges) {
        if (e.stale) continue;
        if (kind && e.kind !== kind) continue;
        if (!(e.from in g.adj) || !(e.to in g.adj)) continue;
        g.edge(e.from, e.to);
      }
      return g.findCycle(txId);
    }

    // undo a commit that failed post-commit SSI validation
    _rollbackCommit(t, reason) {
      for (const vid of Object.values(t.writes)) {
        for (const row of Object.values(this.rows)) {
          const v = row.versions.find(x => x.id === vid);
          if (v) { v.aborted = true; v.ts = null; }
        }
      }
      t.status = 'aborted';
      t.abortReason = reason;
      t.commitSeq = null;
    }

    _baseCommitted(row, v) {
      let best = null;
      for (const x of row.versions) {
        if (x === v || x.aborted || x.ts == null) continue;
        if (x.ts > v.ts) continue; // v.ts just assigned current; all others older
        if (!best || x.ts > best.ts) best = x;
      }
      return best;
    }

    _cycleEdges(cycleNodes, kind) {
      return this.edges.filter(e => {
        if (e.stale) return false;
        if (kind && e.kind !== kind) return false;
        const i = cycleNodes.indexOf(e.from);
        return i >= 0 && cycleNodes[i + 1] === e.to;
      }).map(e => e.id);
    }

    _detectWriteSkew(t, stepId) {
      // SI admits the cycle; it is observed exactly when it closes (this commit
      // made every node of the rw cycle committed).
      const cyc = this._committedCycle(t.id, 'rw');
      if (!cyc) return;
      const cycEdgeObjs = this._cycleEdges(cyc, 'rw');
      const related = new Set();
      cyc.slice(0, -1).forEach(id => {
        const tx = this.txns[id];
        Object.values(tx.readsByKey).forEach(r => r.stepId && related.add(r.stepId));
        tx.predicateReads.forEach(r => r.stepId && related.add(r.stepId));
      });
      // steps that produced the cycle versions / reads
      for (const eid of cycEdgeObjs) {
        const e = this.edges.find(x => x.id === eid);
        if (e.stepId) related.add(e.stepId);
        if (e.readStepId) related.add(e.readStepId);
      }
      if (stepId) related.add(stepId);
      this._anomaly('write_skew', {
        txId: t.id, stepId,
        cycle: cyc, edgeIds: cycEdgeObjs,
        relatedStepIds: [...related],
        message: `写偏斜：SI 提交后形成读写依赖环 ${cyc.slice(0, -1).join(' → ')} → ${cyc[0]}，各事务基于对方即将推翻的前提提交`
      });
    }

    _fail(t, code, message, stepId, latest, extra) {
      const cycleEdges = extra && extra.cycleEdges;
      const related = new Set();
      if (cycleEdges) {
        for (const eid of cycleEdges) {
          const e = this.edges.find(x => x.id === eid);
          if (e) { if (e.stepId) related.add(e.stepId); if (e.readStepId) related.add(e.readStepId); }
        }
      }
      if (latest && latest.stepId) related.add(latest.stepId);
      if (stepId) related.add(stepId);
      this._anomaly(code === 'SERIALIZATION' ? 'serialization_failure' : 'write_conflict', Object.assign({
        txId: t.id, stepId, code,
        edgeIds: cycleEdges || (latest ? this._edgesForConflict(t, latest) : []),
        relatedStepIds: [...related],
        message
      }, extra || {}));
      this._abort(t.id, { reason: code, silent: true });
    }

    _edgesForConflict(t, latest) {
      return this.edges
        .filter(e => !e.stale && (e.from === t.id || e.to === t.id) &&
          (e.from === latest.txId || e.to === latest.txId))
        .map(e => e.id);
    }

    abort(txId, stepId, name) {
      const t = this._tx(txId);
      if (t.status !== 'active') {
        throw new EngineError(
          t.status === 'committed' ? 'TXN_DONE' : 'TXN_ABORTED',
          `事务 ${txId} 已${t.status === 'committed' ? '提交，无法回滚' : '中止'}`
        );
      }
      const released = this._abort(txId, { reason: name || 'user_rollback' });
      this.emit('rollback', 'muted', {
        txId, stepId, releasedKeys: released,
        message: `${txId} 回滚 ✗ · ${Object.keys(t.writes).length} 个未提交版本作废，锁释放`
      });
      return { releasedKeys: released };
    }

    _abort(txId, opts) {
      const t = this.txns[txId];
      t.status = 'aborted';
      t.abortReason = (opts && opts.reason) || 'aborted';
      for (const vid of Object.values(t.writes)) {
        for (const row of Object.values(this.rows)) {
          const v = row.versions.find(x => x.id === vid);
          if (v) v.aborted = true;
        }
      }
      // roll back savepoint bookkeeping too
      t.savepoints = [];
      if (!opts || !opts.silent) { /* event emitted by caller */ }
      return this._releaseLocks(t);
    }

    /* ---- savepoints ---- */

    savepoint(txId, name, stepId) {
      const t = this._active(txId);
      const sp = {
        name,
        stepId,
        writes: Object.keys(t.writes),
        locks: t.locks.slice(),
        readKeys: Object.keys(t.readsByKey),
        predLen: t.predicateReads.length,
        edgeCount: this.edges.length
      };
      t.savepoints.push(sp);
      this.emit('savepoint', 'info', { txId, name, stepId, message: `${txId} 建立保存点 ${name}` });
      return sp;
    }

    rollbackTo(txId, name, stepId) {
      const t = this._active(txId);
      const i = t.savepoints.findIndex(s => s.name === name);
      if (i < 0) throw new EngineError('NO_SAVEPOINT', `保存点 ${name} 不存在`);
      const sp = t.savepoints[i];

      const newWrites = Object.keys(t.writes).filter(k => !sp.writes.includes(k));
      for (const k of newWrites) {
        const vid = t.writes[k];
        const row = this.rows[k];
        const v = row && row.versions.find(x => x.id === vid);
        if (v) v.aborted = true;
        delete t.writes[k];
        if (this.locks[k] === txId) { delete this.locks[k]; }
      }
      t.locks = t.locks.filter(k => sp.locks.includes(k));

      for (const k of Object.keys(t.readsByKey)) {
        if (!sp.readKeys.includes(k)) delete t.readsByKey[k];
      }
      t.predicateReads = t.predicateReads.slice(0, sp.predLen);

      // edges created afterwards that touch this txn's rolled-back reads are stale
      for (const e of this.edges.slice(sp.edgeCount)) {
        if (e.from === txId || e.to === txId) e.stale = true;
      }
      t.savepoints = t.savepoints.slice(0, i + 1);

      this.emit('rollback_to', 'muted', {
        txId, name, stepId,
        undoneWrites: newWrites,
        message: `${txId} 回滚到保存点 ${name}：撤销 [${newWrites.join(', ') || '无'}] 上的写，释放其锁，读集合回退`
      });
      return { releasedKeys: newWrites };
    }

    releaseSavepoint(txId, name, stepId) {
      const t = this._active(txId);
      const i = t.savepoints.findIndex(s => s.name === name);
      if (i < 0) throw new EngineError('NO_SAVEPOINT', `保存点 ${name} 不存在`);
      t.savepoints.splice(i, 1);
      this.emit('release_savepoint', 'info', { txId, name, stepId, message: `${txId} 释放保存点 ${name}` });
    }

    /* ---- inspection helpers used by the UI ---- */

    liveData() {
      const out = [];
      for (const row of Object.values(this.rows)) {
        const v = latestCommitted(row);
        if (v && !v.deleted) out.push({ key: row.key, val: v.val, ts: v.ts, txId: v.txId });
      }
      return out.sort((a, b) => a.key.localeCompare(b.key));
    }

    waiterGraph() {
      // placeholder: scheduler builds the wait-for graph itself
      return null;
    }
  }

  function fmt(v) {
    if (v === undefined || v === null) return '∅';
    return String(v);
  }
  function opLabel(op) {
    return { insert: '插入', update: '更新', upsert: '写入', delete: '删除' }[op] || op;
  }

  return { Engine, EngineError, Graph, ISO, ISO_LABEL, latestCommitted, visibleVersion: visibleVersion, predMatch };
});
