'use strict';

const { Engine } = require('./engine');

/**
 * Executes a user-authored schedule of operations against the MVCC engine.
 * One step = one scheduled op. An op that blocks on a lock is marked
 * 'blocked' and is completed automatically by the engine when the lock is
 * released (via the onUnblock hook), so it is never executed twice.
 */
class Scheduler {
  constructor(engine, ops) {
    this.engine = engine;
    this.ops = ops.map((o, i) => ({ id: i, status: 'pending', ...o }));
    this.cursor = 0;
    engine.hooks.onUnblock = (opId) => {
      const op = this.ops.find((o) => o.id === opId);
      if (op && op.status === 'blocked') op.status = 'done';
    };
  }

  static restore(engine, json) {
    const s = new Scheduler(engine, []);
    s.ops = json.ops;
    s.cursor = json.cursor;
    return s;
  }

  nextPending() {
    while (this.cursor < this.ops.length && this.ops[this.cursor].status !== 'pending') {
      this.cursor++;
    }
    return this.cursor < this.ops.length ? this.ops[this.cursor] : null;
  }

  step() {
    const op = this.nextPending();
    if (!op) return { done: true };
    const before = this.engine.events.length;
    const res = this._exec(op);
    op.status = res && res.blocked ? 'blocked' : 'done';
    op.result = res && res.error ? `error: ${res.error}` : res && res.blocked ? 'blocked' : 'ok';
    this.cursor++;
    return { done: false, op, newEvents: this.engine.events.slice(before) };
  }

  run(limit = 1000) {
    const out = [];
    let r;
    while (out.length < limit && !(r = this.step()).done) out.push(r);
    return out;
  }

  get finished() {
    return this.ops.every((o) => o.status !== 'pending');
  }

  _exec(op) {
    const e = this.engine;
    switch (op.type) {
      case 'begin':
        return e.begin(op.txn, { readOnly: !!op.readOnly });
      case 'read':
        return e.read(op.txn, op.key);
      case 'select':
        return e.select(op.txn, op.pred);
      case 'insert':
        return e.write(op.txn, op.key, op.value, 'insert', op.id);
      case 'update':
        return e.write(op.txn, op.key, op.value, 'update', op.id);
      case 'delete':
        return e.write(op.txn, op.key, null, 'delete', op.id);
      case 'savepoint':
        return e.savepoint(op.txn, op.name);
      case 'rollback_to':
        return e.rollbackTo(op.txn, op.name);
      case 'commit':
        return e.commit(op.txn);
      case 'rollback':
        return e.rollback(op.txn);
      default:
        return { error: `unknown op type: ${op.type}` };
    }
  }

  toJSON() {
    return { cursor: this.cursor, ops: this.ops };
  }
}

module.exports = { Scheduler };
