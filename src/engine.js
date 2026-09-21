'use strict';

/**
 * In-memory MVCC engine for exploring SQL isolation levels.
 *
 * Levels: read_committed | snapshot_isolation | serializable
 *
 * - Versions are append-only; a version is never mutated after creation except
 *   for its status transition (inflight -> committed|aborted) exactly once.
 * - Deletes are tombstone versions (value === null).
 * - Logical clock `clock` ticks on every emitted event; `commitSeq` orders
 *   commits and defines snapshots.
 * - Conflict edges: wr (read dependency), ww (write dependency),
 *   rw (anti-dependency, the dangerous one for write skew / SSI).
 */

const ISOLATION_LEVELS = ['read_committed', 'snapshot_isolation', 'serializable'];

function clone(v) {
  return v === undefined ? v : JSON.parse(JSON.stringify(v));
}

function matchPred(pred, key, value) {
  if (!pred) return false;
  if (pred.kind === 'keyRange') {
    if (pred.from != null && key < pred.from) return false;
    if (pred.to != null && key > pred.to) return false;
    return true;
  }
  if (pred.kind === 'field') {
    if (value == null || typeof value !== 'object') return false;
    const x = value[pred.field];
    switch (pred.op) {
      case '=':
      case '==':
        return x === pred.value;
      case '!=':
        return x !== pred.value;
      case '>':
        return x > pred.value;
      case '>=':
        return x >= pred.value;
      case '<':
        return x < pred.value;
      case '<=':
        return x <= pred.value;
      default:
        return false;
    }
  }
  return false;
}

class Engine {
  constructor(isolation = 'snapshot_isolation') {
    if (!ISOLATION_LEVELS.includes(isolation)) {
      throw new Error(`unknown isolation level: ${isolation}`);
    }
    this.isolation = isolation;
    this.clock = 0; // logical clock: ticks on every event
    this.hseq = 0; // history sequence: monotonic, used by savepoints
    this.commitSeq = 0; // commit counter: defines snapshots
    this.rows = new Map(); // key -> [versions, newest first]
    this.txns = new Map(); // id -> txn
    this.locks = new Map(); // key -> { holder, queue: [{txnId, opId, op}] }
    this.events = []; // immutable, append-only
    this.anomalies = []; // immutable, append-only
    this.edges = []; // conflict edges {from,to,type,key,clock}
    this.predicateReads = []; // {txnId, pred, keys, clock}
    this.keyReaders = new Map(); // key -> [{txnId, clock}]
    this._reportedSkew = new Set();
    this._vid = 1;
    this.hooks = { onUnblock: null }; // scheduler attaches here
  }

  // ---------------------------------------------------------------- events

  _emit(type, details) {
    this.clock++;
    this.hseq++;
    const ev = { seq: this.events.length, clock: this.clock, type, ...details };
    this.events.push(ev);
    return ev;
  }

  _anomaly(kind, details) {
    const a = { id: this.anomalies.length, clock: this.clock, kind, ...details };
    this.anomalies.push(a);
    this._emit('anomaly', { kind, anomalyId: a.id, ...details });
    return a;
  }

  _edge(from, to, type, key, extra) {
    if (from === to) return;
    if (this.edges.some((e) => e.from === from && e.to === to && e.type === type && e.key === key)) return;
    this.edges.push({ from, to, type, key, clock: this.clock, ...extra });
  }

  // ------------------------------------------------------------- txn state

  _requireTxn(txnId) {
    const txn = this.txns.get(txnId);
    if (!txn) throw new Error(`txn ${txnId} does not exist`);
    return txn;
  }

  _requireActive(txnId) {
    const txn = this._requireTxn(txnId);
    if (txn.state === 'blocked') throw new Error(`txn ${txnId} is blocked waiting for a lock`);
    if (txn.state !== 'active') throw new Error(`txn ${txnId} is already ${txn.state}`);
    return txn;
  }

  _snapshotFor(txn) {
    if (this.isolation === 'read_committed') {
      txn.snapshot = this.commitSeq; // statement-level snapshot
    } else if (txn.snapshot == null) {
      txn.snapshot = this.commitSeq; // transaction-level snapshot at first statement
    }
    return txn.snapshot;
  }

  _ownInflight(txnId) {
    const out = [];
    for (const vs of this.rows.values()) {
      for (const v of vs) {
        if (v.createdBy === txnId && v.status === 'inflight') out.push(v);
      }
    }
    return out;
  }

  _hasInflight(txnId, key) {
    const vs = this.rows.get(key) || [];
    return vs.some((v) => v.createdBy === txnId && v.status === 'inflight');
  }

  _latestCommitted(key) {
    for (const v of this.rows.get(key) || []) {
      if (v.status === 'committed') return v;
    }
    return null;
  }

  _latestAny(key) {
    for (const v of this.rows.get(key) || []) {
      if (v.status !== 'aborted') return v;
    }
    return null;
  }

  _visibleVersion(txn, key, snapshot) {
    for (const v of this.rows.get(key) || []) {
      if (v.status === 'aborted') continue;
      if (v.createdBy === txn.id) return v; // own newest write (incl. tombstone)
      if (v.status !== 'committed') continue;
      if (v.createdSeq > snapshot) continue;
      return v;
    }
    return null;
  }

  // -------------------------------------------------------------- lifecycle

  begin(txnId, opts = {}) {
    return this._safe(() => {
      if (this.txns.has(txnId)) throw new Error(`txn ${txnId} already exists`);
      const txn = {
        id: txnId,
        readOnly: !!opts.readOnly,
        state: 'active',
        snapshot: null,
        startSeq: this.commitSeq,
        startClock: this.clock,
        commitSeq: null,
        commitClock: null,
        abortReason: null,
        waitingOn: null,
        readHistory: new Map(), // key -> [{value, clock}]
        predicates: [], // this txn's own predicate reads
        savepoints: [], // {name, hseq}
      };
      this.txns.set(txnId, txn);
      this._emit('begin', { txn: txnId, readOnly: txn.readOnly });
      return { ok: true };
    });
  }

  commit(txnId) {
    return this._safe(() => this._commit(txnId));
  }

  _commit(txnId) {
    const txn = this._requireTxn(txnId);
    if (txn.state === 'blocked') throw new Error(`txn ${txnId} is blocked waiting for a lock`);
    if (txn.state !== 'active') throw new Error(`txn ${txnId} is already ${txn.state}`);

    if (this.isolation === 'serializable') {
      // Serialization graph test: abort if committing would close a cycle.
      const ids = this._committedIds();
      ids.add(txnId);
      const cyc = this._findCycle(txnId, ids, null);
      if (cyc) {
        this._abortTxn(txn, 'serialization_failure');
        this._anomaly('serialization_failure', {
          txn: txnId,
          reason: 'cycle',
          cycle: cyc.txns,
          edges: cyc.edges.map(fmtEdge),
        });
        return { aborted: true, cycle: cyc.txns };
      }
    }

    txn.state = 'committed';
    txn.commitSeq = ++this.commitSeq;
    this._emit('commit', { txn: txnId, commitSeq: txn.commitSeq, readOnly: txn.readOnly });
    txn.commitClock = this.clock;
    for (const v of this._ownInflight(txnId)) {
      v.status = 'committed';
      v.createdSeq = txn.commitSeq;
      v.createdClock = txn.commitClock;
    }
    this._releaseLocks(txnId);

    if (this.isolation !== 'serializable') {
      // Under RC/SI a pure rw-antidependency cycle is allowed: that is write skew.
      const cyc = this._findCycle(txnId, this._committedIds(), ['rw']);
      if (cyc) {
        const sig = cyc.txns.slice().sort().join('|');
        if (!this._reportedSkew.has(sig)) {
          this._reportedSkew.add(sig);
          this._anomaly('write_skew', { cycle: cyc.txns, edges: cyc.edges.map(fmtEdge) });
        }
      }
    }
    return { ok: true, commitSeq: txn.commitSeq };
  }

  rollback(txnId) {
    return this._safe(() => {
      const txn = this._requireTxn(txnId);
      if (txn.state !== 'active' && txn.state !== 'blocked') {
        throw new Error(`txn ${txnId} is already ${txn.state}`);
      }
      this._abortTxn(txn, 'rollback');
      return { ok: true };
    });
  }

  _abortTxn(txn, reason) {
    txn.state = 'aborted';
    txn.abortReason = reason;
    txn.waitingOn = null;
    for (const v of this._ownInflight(txn.id)) v.status = 'aborted';
    this._emit(reason === 'rollback' ? 'rollback' : 'abort', { txn: txn.id, reason });
    this._releaseLocks(txn.id);
  }

  savepoint(txnId, name) {
    return this._safe(() => {
      const txn = this._requireActive(txnId);
      txn.savepoints = txn.savepoints.filter((s) => s.name !== name);
      this._emit('savepoint', { txn: txnId, name });
      txn.savepoints.push({ name, hseq: this.hseq });
      return { ok: true };
    });
  }

  rollbackTo(txnId, name) {
    return this._safe(() => {
      const txn = this._requireActive(txnId);
      const idx = txn.savepoints.findIndex((s) => s.name === name);
      if (idx < 0) throw new Error(`txn ${txnId} has no savepoint ${name}`);
      const sp = txn.savepoints[idx];
      for (const vs of this.rows.values()) {
        for (const v of vs) {
          if (v.createdBy === txnId && v.status === 'inflight' && v.hseq >= sp.hseq) {
            v.status = 'aborted';
          }
        }
      }
      txn.savepoints.length = idx + 1; // later savepoints are destroyed
      this._emit('rollback_to', { txn: txnId, name });
      // release locks this txn no longer needs
      for (const [key, lock] of [...this.locks]) {
        if (lock.holder === txnId && !this._hasInflight(txnId, key)) {
          this._emit('lock_release', { txn: txnId, key });
          this.locks.delete(key);
          this._grantNext(key, lock.queue);
        }
      }
      return { ok: true };
    });
  }

  // ------------------------------------------------------------------ reads

  read(txnId, key) {
    return this._safe(() => {
      const txn = this._requireActive(txnId);
      const snap = this._snapshotFor(txn);
      const v = this._visibleVersion(txn, key, snap);
      const value = v && v.value != null ? clone(v.value) : null;

      const hist = txn.readHistory.get(key) || [];
      if (hist.length) {
        const prev = hist[hist.length - 1];
        if (JSON.stringify(prev.value) !== JSON.stringify(value)) {
          this._anomaly('non_repeatable_read', { txn: txnId, key, before: prev.value, after: value });
        }
      }
      hist.push({ value: clone(value), clock: this.clock });
      txn.readHistory.set(key, hist);

      if (v && v.createdBy !== txnId) this._edge(v.createdBy, txnId, 'wr', key);
      this._trackReader(key, txnId);
      this._emit('read', { txn: txnId, key, value, snapshot: snap });
      return { ok: true, value };
    });
  }

  select(txnId, pred) {
    return this._safe(() => {
      const txn = this._requireActive(txnId);
      const snap = this._snapshotFor(txn);
      const result = [];
      for (const key of [...this.rows.keys()].sort()) {
        const v = this._visibleVersion(txn, key, snap);
        if (v && v.value != null && matchPred(pred, key, v.value)) {
          result.push({ key, value: clone(v.value) });
          if (v.createdBy !== txnId) this._edge(v.createdBy, txnId, 'wr', key);
        }
        this._trackReader(key, txnId);
      }
      const keys = result.map((r) => r.key);

      const prevRuns = txn.predicates.filter((p) => JSON.stringify(p.pred) === JSON.stringify(pred));
      if (prevRuns.length) {
        const last = prevRuns[prevRuns.length - 1];
        const added = keys.filter((k) => !last.keys.includes(k));
        const removed = last.keys.filter((k) => !keys.includes(k));
        if (added.length || removed.length) {
          this._anomaly('phantom', { txn: txnId, pred, added, removed, before: last.keys, after: keys });
        }
      }
      txn.predicates.push({ pred: clone(pred), keys, clock: this.clock });
      this.predicateReads.push({ txnId, pred: clone(pred), keys, clock: this.clock });
      this._emit('select', { txn: txnId, pred, keys, snapshot: snap });
      return { ok: true, keys };
    });
  }

  _trackReader(key, txnId) {
    if (!this.keyReaders.has(key)) this.keyReaders.set(key, []);
    this.keyReaders.get(key).push({ txnId, clock: this.clock });
  }

  // ----------------------------------------------------------------- writes

  write(txnId, key, value, op = 'update', opId = null) {
    return this._safe(() => this._write(txnId, key, value, op, opId));
  }

  _write(txnId, key, value, op, opId) {
    const txn = this._requireActive(txnId);
    if (txn.readOnly) throw new Error(`txn ${txnId} is read-only`);

    // The snapshot is established when the statement is first attempted,
    // even if it then blocks on a lock — otherwise a retried write would
    // see a newer snapshot and first-committer-wins would never fire.
    const snap = this._snapshotFor(txn);

    let lock = this.locks.get(key);
    if (lock && lock.holder !== txnId) {
      const holder = this.txns.get(lock.holder);
      if (holder && (holder.state === 'active' || holder.state === 'blocked')) {
        lock.queue.push({ txnId, opId, op: { key, value: clone(value), op } });
        txn.state = 'blocked';
        txn.waitingOn = lock.holder;
        this._emit('lock_wait', { txn: txnId, key, holder: lock.holder });
        const cycle = this._detectWaitCycle(txnId);
        if (cycle) this._anomaly('deadlock', { cycle });
        return { blocked: true, holder: lock.holder, cycle: cycle || null };
      }
      this.locks.delete(key); // stale lock
      lock = null;
    }
    if (!lock) this.locks.set(key, { holder: txnId, queue: [] });

    if (this.isolation !== 'read_committed') {
      // first-committer-wins
      const latest = this._latestCommitted(key);
      if (latest && latest.createdSeq > snap) {
        this._abortTxn(txn, 'write_conflict');
        this._anomaly('serialization_failure', {
          txn: txnId,
          reason: 'write_conflict',
          key,
          message: `first-committer-wins: "${key}" changed after snapshot`,
        });
        return { aborted: true };
      }
    }

    this._recordWriteEdges(txn, key, value);
    const prev = this._latestAny(key);
    if (prev && prev.createdBy !== txnId) this._edge(prev.createdBy, txnId, 'ww', key);

    const v = {
      vid: this._vid++,
      key,
      value: op === 'delete' ? null : clone(value),
      createdBy: txnId,
      createdSeq: null,
      createdClock: null,
      hseq: this.hseq,
      status: 'inflight',
    };
    if (!this.rows.has(key)) this.rows.set(key, []);
    this.rows.get(key).unshift(v);
    this._emit('write', { txn: txnId, key, value: v.value, op, snapshot: snap });
    return { ok: true };
  }

  _recordWriteEdges(txn, key, value) {
    const concurrent = (readerTxn) =>
      readerTxn &&
      readerTxn.state !== 'aborted' &&
      (readerTxn.commitClock == null || readerTxn.commitClock > txn.startClock);

    for (const r of this.keyReaders.get(key) || []) {
      if (r.txnId === txn.id) continue;
      if (concurrent(this.txns.get(r.txnId))) this._edge(r.txnId, txn.id, 'rw', key);
    }
    const inResults = new Set();
    for (const pr of this.predicateReads) {
      if (pr.txnId === txn.id) continue;
      const rt = this.txns.get(pr.txnId);
      if (!concurrent(rt)) continue;
      const matchesNow = value != null && matchPred(pr.pred, key, value);
      if (pr.keys.includes(key) || matchesNow) {
        if (!inResults.has(pr.txnId + key)) {
          inResults.add(pr.txnId + key);
          this._edge(pr.txnId, txn.id, 'rw', key, { predicate: true });
        }
      }
    }
  }

  // ------------------------------------------------------------------ locks

  _releaseLocks(txnId) {
    for (const [key, lock] of [...this.locks]) {
      lock.queue = lock.queue.filter((w) => w.txnId !== txnId);
      if (lock.holder === txnId) {
        this._emit('lock_release', { txn: txnId, key });
        this.locks.delete(key);
        this._grantNext(key, lock.queue);
      }
    }
  }

  _grantNext(key, queue) {
    while (queue.length) {
      const next = queue.shift();
      const t = this.txns.get(next.txnId);
      if (!t || t.state !== 'blocked') continue;
      this.locks.set(key, { holder: next.txnId, queue });
      t.state = 'active';
      t.waitingOn = null;
      this._emit('lock_grant', { txn: next.txnId, key });
      const r = this._write(next.txnId, key, next.op.value, next.op.op, next.opId);
      if (this.hooks.onUnblock) this.hooks.onUnblock(next.opId, r);
      return;
    }
  }

  _detectWaitCycle(startId) {
    const adj = new Map();
    for (const [key, lock] of this.locks) {
      for (const w of lock.queue) {
        if (!adj.has(w.txnId)) adj.set(w.txnId, []);
        adj.get(w.txnId).push(lock.holder);
      }
    }
    const path = [];
    const onPath = new Set([startId]);
    const dfs = (node) => {
      for (const next of adj.get(node) || []) {
        if (next === startId) return [...path, node];
        if (onPath.has(next)) continue;
        onPath.add(next);
        path.push(node);
        const r = dfs(next);
        if (r) return r;
        path.pop();
        onPath.delete(next);
      }
      return null;
    };
    return dfs(startId);
  }

  // ---------------------------------------------------- serialization graph

  _committedIds() {
    const s = new Set();
    for (const [id, t] of this.txns) if (t.state === 'committed') s.add(id);
    return s;
  }

  _findCycle(startId, ids, edgeTypes) {
    const adj = new Map();
    for (const e of this.edges) {
      if (edgeTypes && !edgeTypes.includes(e.type)) continue;
      if (!ids.has(e.from) || !ids.has(e.to)) continue;
      if (!adj.has(e.from)) adj.set(e.from, []);
      adj.get(e.from).push(e);
    }
    let found = null;
    const visit = (node, txnsPath, edgesPath, onPath) => {
      if (found) return;
      for (const e of adj.get(node) || []) {
        if (e.to === startId) {
          found = { txns: [...txnsPath], edges: [...edgesPath, e] };
          return;
        }
        if (onPath.has(e.to)) continue;
        onPath.add(e.to);
        txnsPath.push(e.to);
        edgesPath.push(e);
        visit(e.to, txnsPath, edgesPath, onPath);
        txnsPath.pop();
        edgesPath.pop();
        onPath.delete(e.to);
        if (found) return;
      }
    };
    visit(startId, [startId], [], new Set([startId]));
    return found;
  }

  // ------------------------------------------------------------------- misc

  _safe(fn) {
    try {
      return fn();
    } catch (err) {
      this._emit('error', { message: err.message });
      return { error: err.message };
    }
  }

  // ---------------------------------------------------------- serialization

  toJSON() {
    return {
      isolation: this.isolation,
      clock: this.clock,
      hseq: this.hseq,
      commitSeq: this.commitSeq,
      vid: this._vid,
      rows: [...this.rows.entries()],
      txns: [...this.txns.entries()].map(([id, t]) => [
        id,
        { ...t, readHistory: [...t.readHistory.entries()] },
      ]),
      locks: [...this.locks.entries()],
      events: this.events,
      anomalies: this.anomalies,
      edges: this.edges,
      predicateReads: this.predicateReads,
      keyReaders: [...this.keyReaders.entries()],
      reportedSkew: [...this._reportedSkew],
    };
  }

  static fromJSON(j) {
    const e = new Engine(j.isolation);
    e.clock = j.clock;
    e.hseq = j.hseq;
    e.commitSeq = j.commitSeq;
    e._vid = j.vid;
    e.rows = new Map(j.rows);
    e.txns = new Map(
      j.txns.map(([id, t]) => [id, { ...t, readHistory: new Map(t.readHistory) }])
    );
    e.locks = new Map(j.locks);
    e.events = j.events;
    e.anomalies = j.anomalies;
    e.edges = j.edges;
    e.predicateReads = j.predicateReads;
    e.keyReaders = new Map(j.keyReaders);
    e._reportedSkew = new Set(j.reportedSkew);
    return e;
  }
}

function fmtEdge(e) {
  return `${e.from} -${e.type}(${e.key})-> ${e.to}`;
}

module.exports = { Engine, ISOLATION_LEVELS, matchPred };
