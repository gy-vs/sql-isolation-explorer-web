/*
 * Interleaved step scheduler on top of the MVCC engine.
 *
 * Steps execute strictly in user-defined order.  A write that cannot get its
 * key lock becomes a *waiting* step (cursor still advances); it is resumed in
 * FIFO order when locks are released.  Every state transition is persisted so
 * a refresh resumes an unfinished schedule without re-executing any step.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('./engine'));
  } else root.MVCCScheduler = factory(root.MVCC);
})(typeof self !== 'undefined' ? self : this, function (MVCC) {
  'use strict';

  const { Engine, EngineError, Graph, ISO } = MVCC;

  const ACTIONS = {
    begin: { tx: true, label: 'BEGIN' },
    read: { tx: true, label: '读键' },
    predicate: { tx: true, label: '谓词读' },
    insert: { tx: true, write: true, label: '插入' },
    update: { tx: true, write: true, label: '更新' },
    upsert: { tx: true, write: true, label: '写入' },
    delete: { tx: true, write: true, label: '删除' },
    commit: { tx: true, label: '提交' },
    rollback: { tx: true, label: '回滚' },
    savepoint: { tx: true, label: '保存点' },
    rollbackTo: { tx: true, label: '回滚到保存点' },
    releaseSavepoint: { tx: true, label: '释放保存点' }
  };

  const memoryStorage = {
    _d: {},
    getItem(k) { return Object.prototype.hasOwnProperty.call(this._d, k) ? this._d[k] : null; },
    setItem(k, v) { this._d[k] = String(v); },
    removeItem(k) { delete this._d[k]; }
  };

  function getStorage() {
    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem('__mvcc_probe__', '1');
        localStorage.removeItem('__mvcc_probe__');
        return localStorage;
      }
    } catch (e) { /* private mode etc. */ }
    return memoryStorage;
  }

  class Scheduler {
    constructor(storageKey, storage) {
      this.storageKey = storageKey || 'mvcc-workbench-v1';
      this.storage = storage || getStorage();
      this.steps = [];
      this.cursor = 0;
      this.engine = new Engine();
      this.waits = {}; // stepId -> {stepId, txId, key, holder}
      this.title = '未命名调度';
    }

    /* ---------- persistence ---------- */

    toJSON() {
      return {
        title: this.title,
        steps: this.steps,
        cursor: this.cursor,
        waits: this.waits,
        engine: this.engine.toJSON()
      };
    }
    persist() {
      try { this.storage.setItem(this.storageKey, JSON.stringify(this.toJSON())); }
      catch (e) { /* quota / serialization: keep running in memory */ }
    }
    static restore(storageKey, storage) {
      const s = new Scheduler(storageKey, storage);
      let raw = null;
      try { raw = s.storage.getItem(s.storageKey); } catch (e) { raw = null; }
      if (!raw) return null;
      try {
        const d = JSON.parse(raw);
        s.title = d.title || '未命名调度';
        s.steps = d.steps || [];
        s.cursor = d.cursor || 0;
        s.waits = d.waits || {};
        s.engine = Engine.fromJSON(d.engine || new Engine().toJSON());
        return s;
      } catch (e) {
        return null;
      }
    }
    clearSave() { try { this.storage.removeItem(this.storageKey); } catch (e) {} }

    /* ---------- plan editing (only un-executed steps are mutable) ---------- */

    addStep(s) {
      const step = Object.assign({
        id: this.engine._nid('s'),
        txId: '', action: 'read',
        key: '', val: '', op: '>', value: '', name: '',
        iso: 'RC', readOnly: false, label: '',
        breakpoint: false,
        status: 'pending'
      }, s);
      this.steps.push(step);
      this.persist();
      return step;
    }
    updateStep(id, patch) {
      const i = this.steps.findIndex(s => s.id === id);
      if (i < 0) return;
      if (i < this.executedBoundary()) {
        throw new Error('已执行的步骤属于不可改写的历史，不能编辑/删除');
      }
      Object.assign(this.steps[i], patch);
      this.persist();
    }
    removeStep(id) {
      const i = this.steps.findIndex(s => s.id === id);
      if (i < 0) return;
      if (i < this.executedBoundary()) {
        throw new Error('已执行的步骤属于不可改写的历史，不能编辑/删除');
      }
      this.steps.splice(i, 1);
      this.persist();
    }
    moveStep(id, dir) {
      const i = this.steps.findIndex(s => s.id === id);
      const j = i + dir;
      if (i < 0 || j < 0 || j >= this.steps.length) return;
      if (i < this.executedBoundary() || j < this.executedBoundary()) {
        throw new Error('已执行步骤的顺序不可改变');
      }
      const [x] = this.steps.splice(i, 1);
      this.steps.splice(j, 0, x);
      this.persist();
    }
    executedBoundary() {
      // first index that isn't in a terminal executed state
      for (let i = 0; i < this.steps.length; i++) {
        const st = this.steps[i].status;
        if (st === 'pending' || st === 'waiting') return i;
      }
      return this.steps.length;
    }
    isDone() {
      return this.steps.length > 0 &&
        this.steps.every(s => ['done', 'error', 'skipped'].includes(s.status));
    }

    /* ---------- execution ---------- */

    nextRunnable() {
      // waiting steps are parked (they resume automatically on lock release);
      // steps after them stay schedulable so transactions can interleave
      return this.steps.find(s => s.status === 'pending');
    }
    canRun() { return !!this.nextRunnable() || Object.keys(this.waits).length > 0; }

    runNext() {
      const step = this.nextRunnable();
      if (!step) {
        // nothing schedulable: report the current blocking waits instead of
        // pretending the schedule advanced
        return { blocked: true, waits: Object.values(this.waits) };
      }
      return this._execute(step);
    }

    runToBreakpointOrEnd() {
      let ran = 0;
      for (;;) {
        const step = this.nextRunnable();
        if (!step) break;
        const stoppedAtBreakpoint = step.breakpoint;
        this._execute(step);
        ran++;
        if (stoppedAtBreakpoint && (step.status === 'done' || step.status === 'error')) break;
      }
      return ran;
    }

    _execute(step) {
      this.cursor = this.steps.indexOf(step);
      const t = this.engine.txns[step.txId];
      if (step.action !== 'begin' && t && t.status !== 'active') {
        // terminal state reached earlier: every later statement of this tx is skipped
        return this._finish(step, 'skipped', {
          code: 'TXN_TERMINAL',
          message: `事务 ${step.txId} 已${t.status === 'committed' ? '提交' : '中止'}，语句跳过（终态只发生一次）`
        });
      }
      try {
        this._dispatch(step, t);
      } catch (err) {
        this._failStep(step, err);
      }
      this.persist();
      return step;
    }

    _dispatch(step, t) {
      const e = this.engine;
      switch (step.action) {
        case 'begin':
          e.begin({
            id: step.txId, iso: step.iso || 'RC',
            readOnly: !!step.readOnly, label: step.label || step.txId
          });
          this._finish(step, 'done');
          break;

        case 'read':
          e.read({ txId: step.txId, key: step.key, stepId: step.id });
          this._finish(step, 'done');
          break;

        case 'predicate':
          e.predicateRead({
            txId: step.txId, op: step.op || '>',
            value: parseMaybe(step.value), stepId: step.id
          });
          this._finish(step, 'done');
          break;

        case 'insert':
        case 'update':
        case 'upsert':
        case 'delete':
          this._write(step);
          break;

        case 'commit': {
          const res = e.commit(step.txId, step.id);
          this._finish(step, 'done');
          this._resumeWaiters(res.releasedKeys);
          break;
        }
        case 'rollback': {
          const res = e.abort(step.txId, step.id, 'user_rollback');
          this._finish(step, 'done');
          this._resumeWaiters(res.releasedKeys);
          break;
        }
        case 'savepoint':
          e.savepoint(step.txId, step.name || 'sp', step.id);
          this._finish(step, 'done');
          break;
        case 'rollbackTo': {
          const res = e.rollbackTo(step.txId, step.name || 'sp', step.id);
          this._finish(step, 'done');
          this._resumeWaiters(res.releasedKeys);
          break;
        }
        case 'releaseSavepoint':
          e.releaseSavepoint(step.txId, step.name || 'sp', step.id);
          this._finish(step, 'done');
          break;
        default:
          throw new EngineError('BAD_ACTION', '未知动作: ' + step.action);
      }
    }

    /* ---------- writes + locking ---------- */

    _write(step) {
      const e = this.engine;
      const attempt = e.tryLockKey(step.txId, step.key, step.id);
      if (attempt.granted) {
        this._doMutate(step, false);
        return;
      }
      // block as a waiting step
      step.status = 'waiting';
      step.result = { code: 'LOCK_WAIT', holder: attempt.holder, message: `等待 ${step.key} 锁（持有者 ${attempt.holder}）` };
      this.waits[step.id] = { stepId: step.id, txId: step.txId, key: step.key, holder: attempt.holder };
      this.persist();

      const victim = this._deadlockVictim(step.txId);
      if (victim) this._killDeadlockVictim(victim);
    }

    _doMutate(step, afterWait) {
      const e = this.engine;
      const r = e.mutate({
        txId: step.txId, key: step.key, op: step.action,
        val: parseMaybe(step.val), stepId: step.id, waited: afterWait
      });
      if (afterWait) delete this.waits[step.id];
      this._finish(step, r.noop ? 'done' : 'done', Object.assign({ afterWait }, r.noop ? { noop: true } : {}));
    }

    _resumeWaiters(releasedKeys) {
      if (!releasedKeys || !releasedKeys.length) return;
      // FIFO by program order among waiters on the released keys
      const ordered = this.steps.filter(s => s.status === 'waiting' && releasedKeys.includes(s.key));
      for (const wstep of ordered) {
        if (wstep.status !== 'waiting') continue;
        const t = this.engine.txns[wstep.txId];
        if (!t || t.status !== 'active') {
          // waiter died meanwhile (deadlock) — clean up
          delete this.waits[wstep.id];
          wstep.status = 'skipped';
          wstep.result = { code: 'TXN_TERMINAL', message: `事务 ${wstep.txId} 已中止，等待取消` };
          this.persist();
          continue;
        }
        const attempt = this.engine.tryLockKey(wstep.txId, wstep.key, wstep.id);
        if (!attempt.granted) continue; // earlier FIFO waiter got it
        delete this.waits[wstep.id];
        this.cursor = this.steps.indexOf(wstep);
        try {
          this._doMutate(wstep, true);
          this.engine.emit('lock_granted', 'info', {
            txId: wstep.txId, key: wstep.key, stepId: wstep.id,
            message: `${wstep.txId} 在锁释放后获得 ${wstep.key}，语句继续执行`
          });
        } catch (err) {
          this._failStep(wstep, err);
        }
        this.persist();
      }
    }

    /* ---------- deadlock detection ---------- */

    _waitGraph() {
      const g = new Graph();
      for (const w of Object.values(this.waits)) {
        if (this.engine.txns[w.txId] && this.engine.txns[w.txId].status === 'active') g.edge(w.txId, w.holder);
      }
      return g;
    }

    _deadlockVictim(newWaiter) {
      const g = this._waitGraph();
      const cyc = g.findCycle(newWaiter) || g.findCycle(null);
      if (!cyc) return null;
      const nodes = cyc.slice(0, -1);
      // victim = cycle member whose wait appears latest in program order
      let victim = null, victimIdx = -1;
      for (const id of nodes) {
        const ws = this.steps.find(s => s.status === 'waiting' && s.txId === id);
        const idx = ws ? this.steps.indexOf(ws) : -1;
        if (idx > victimIdx) { victim = id; victimIdx = idx; }
      }
      return { txId: victim, cycle: nodes };
    }

    _killDeadlockVictim(victim) {
      const e = this.engine;
      const t = e.txns[victim.txId];
      const waitingStep = this.steps.find(s => s.status === 'waiting' && s.txId === victim.txId);
      const released = e._abort(victim.txId, { reason: 'deadlock' });

      const related = new Set();
      for (const w of Object.values(this.waits)) {
        if (victim.cycle.includes(w.txId)) related.add(w.stepId);
      }
      if (waitingStep) related.add(waitingStep.id);

      e.emit('deadlock', 'anomaly', {
        txId: victim.txId,
        cycle: victim.cycle.concat(victim.cycle[0]),
        stepId: waitingStep ? waitingStep.id : null,
        relatedStepIds: [...related],
        message: `死锁：等待环 ${victim.cycle.join(' → ')} → ${victim.cycle[0]}，选择 ${victim.txId} 作为牺牲者中止`
      });

      if (waitingStep) {
        delete this.waits[waitingStep.id];
        waitingStep.status = 'error';
        waitingStep.result = { code: 'DEADLOCK', message: '被选为死锁牺牲者，事务中止', cycle: victim.cycle };
      }
      this.persist();
      this._resumeWaiters(released);
    }

    /* ---------- step outcomes ---------- */

    _finish(step, status, extra) {
      step.status = status;
      step.result = Object.assign({
        executedAt: this.engine.clock
      }, extra || {});
    }

    _failStep(step, err) {
      const e = this.engine;
      step.status = 'error';
      step.result = { code: err.code || 'ERROR', message: err.message };
      if (step.action !== 'begin' && err.code !== 'READ_ONLY' &&
          ['NO_TXN', 'DUP_TXN', 'NO_SAVEPOINT', 'BAD_ACTION'].indexOf(err.code) < 0) {
        const t = this.engine.txns[step.txId];
        if (t && t.status === 'active') {
          // all write-path failures kill the transaction (MVCC abort semantics)
          const kill = ['WRITE_CONFLICT', 'SERIALIZATION'].includes(err.code) ||
            ACTIONS[step.action] && ACTIONS[step.action].write;
          if (kill) {
            e._abort(step.txId, { reason: err.code });
            e.emit('txn_aborted', 'warn', {
              txId: step.txId, stepId: step.id,
              message: `${step.txId} 因 ${err.code === 'WRITE_CONFLICT' ? '写冲突' : err.code === 'SERIALIZATION' ? '序列化失败' : '语句错误'} 中止`
            });
          }
        }
      }
      this.persist();
    }

    /* ---------- load plan as a fresh schedule ---------- */

    loadPlan(title, seed, steps) {
      this.clearSave();
      this.title = title || '未命名调度';
      this.steps = [];
      this.cursor = 0;
      this.waits = {};
      this.engine = new Engine();
      if (seed) this.engine.seed(seed);
      for (const s of steps) this.addStep(s);
      this.persist();
    }
  }

  function parseMaybe(x) {
    if (x === undefined || x === null || x === '') return undefined;
    if (typeof x === 'number') return x;
    const n = Number(x);
    return (x.trim() !== '' && !isNaN(n)) ? n : x;
  }

  return { Scheduler, ACTIONS, parseMaybe, memoryStorage };
});
