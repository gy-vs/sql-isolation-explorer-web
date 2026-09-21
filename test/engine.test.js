'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Engine } = require('../src/engine');
const { Scheduler } = require('../src/scheduler');

function seed(e, rows) {
  e.begin('T0');
  for (const [k, v] of Object.entries(rows)) e.write('T0', k, v, 'insert');
  e.commit('T0');
}

// ---------------------------------------------------------- concurrent update
test('SI: concurrent update on same row -> first-committer-wins abort', () => {
  const e = new Engine('snapshot_isolation');
  seed(e, { a: { n: 0 } });
  e.begin('T1'); e.begin('T2');
  e.write('T1', 'a', { n: 1 }, 'update');
  e.write('T2', 'a', { n: 2 }, 'update'); // blocks on T1's lock
  assert.equal(e.txns.get('T2').state, 'blocked');
  e.commit('T1'); // releases lock, T2's write retries -> write conflict -> abort
  assert.equal(e.txns.get('T2').state, 'aborted');
  assert.equal(e.txns.get('T2').abortReason, 'write_conflict');
  assert.ok(e.anomalies.some((a) => a.kind === 'serialization_failure' && a.reason === 'write_conflict'));
});

test('RC: concurrent update allowed (lost update possible), no abort', () => {
  const e = new Engine('read_committed');
  seed(e, { a: { n: 0 } });
  e.begin('T1'); e.begin('T2');
  e.write('T1', 'a', { n: 1 }, 'update');
  e.write('T2', 'a', { n: 2 }, 'update');
  e.commit('T1');
  assert.equal(e.txns.get('T2').state, 'active'); // unblocked, not aborted
  e.commit('T2');
  assert.equal(e.txns.get('T2').state, 'committed');
});

// ------------------------------------------------------- non-repeatable read
test('RC: non-repeatable read detected; SI: snapshot prevents it', () => {
  const rc = new Engine('read_committed');
  seed(rc, { a: { v: 1 } });
  rc.begin('T1'); rc.begin('T2');
  rc.read('T1', 'a');
  rc.write('T2', 'a', { v: 2 }, 'update'); rc.commit('T2');
  rc.read('T1', 'a');
  assert.ok(rc.anomalies.some((a) => a.kind === 'non_repeatable_read' && a.txn === 'T1'));

  const si = new Engine('snapshot_isolation');
  seed(si, { a: { v: 1 } });
  si.begin('T1'); si.begin('T2');
  si.read('T1', 'a');
  si.write('T2', 'a', { v: 2 }, 'update'); si.commit('T2');
  const r = si.read('T1', 'a');
  assert.deepEqual(r.value, { v: 1 }); // still sees snapshot
  assert.ok(!si.anomalies.some((a) => a.kind === 'non_repeatable_read'));
});

// ------------------------------------------------------------------- phantom
test('RC: predicate range re-read sees phantom; SI: does not', () => {
  const pred = { kind: 'field', field: 'balance', op: '>=', value: 100 };
  const rc = new Engine('read_committed');
  seed(rc, { e1: { balance: 120 }, e2: { balance: 80 } });
  rc.begin('T1'); rc.begin('T2');
  assert.deepEqual(rc.select('T1', pred).keys, ['e1']);
  rc.write('T2', 'e3', { balance: 150 }, 'insert'); rc.commit('T2');
  assert.deepEqual(rc.select('T1', pred).keys, ['e1', 'e3']);
  assert.ok(rc.anomalies.some((a) => a.kind === 'phantom' && a.added.includes('e3')));

  const si = new Engine('snapshot_isolation');
  seed(si, { e1: { balance: 120 }, e2: { balance: 80 } });
  si.begin('T1'); si.begin('T2');
  si.select('T1', pred);
  si.write('T2', 'e3', { balance: 150 }, 'insert'); si.commit('T2');
  assert.deepEqual(si.select('T1', pred).keys, ['e1']);
  assert.ok(!si.anomalies.some((a) => a.kind === 'phantom'));
});

test('key-range predicate works', () => {
  const e = new Engine('read_committed');
  seed(e, { a: { v: 1 }, b: { v: 2 }, z: { v: 3 } });
  e.begin('T1');
  const r = e.select('T1', { kind: 'keyRange', from: 'a', to: 'c' });
  assert.deepEqual(r.keys, ['a', 'b']);
});

// ------------------------------------------------------------------ rollback
test('rollback discards versions and releases locks', () => {
  const e = new Engine('snapshot_isolation');
  e.begin('T1');
  e.write('T1', 'a', { v: 1 }, 'insert');
  e.rollback('T1');
  assert.equal(e.txns.get('T1').state, 'aborted');
  e.begin('T2');
  assert.equal(e.read('T2', 'a').value, null);
  assert.equal(e.locks.size, 0);
});

// ----------------------------------------------------------------- savepoint
test('savepoint: partial rollback keeps earlier writes, releases unneeded locks', () => {
  const e = new Engine('snapshot_isolation');
  e.begin('T1');
  e.write('T1', 'a', { v: 1 }, 'insert');
  e.savepoint('T1', 'sp1');
  e.write('T1', 'b', { v: 2 }, 'insert');
  e.rollbackTo('T1', 'sp1');
  e.commit('T1');
  e.begin('T2');
  assert.deepEqual(e.read('T2', 'a').value, { v: 1 });
  assert.equal(e.read('T2', 'b').value, null);
  assert.equal(e.locks.size, 0);
});

// ------------------------------------------------------------------ deadlock
test('deadlock: wait cycle reported; aborting a victim unblocks the other', () => {
  const e = new Engine('snapshot_isolation');
  seed(e, { a: { v: 1 }, b: { v: 1 } });
  e.begin('T1'); e.begin('T2');
  e.write('T1', 'a', { v: 10 }, 'update');
  e.write('T2', 'b', { v: 20 }, 'update');
  e.write('T1', 'b', { v: 11 }, 'update'); // waits on T2
  const r = e.write('T2', 'a', { v: 21 }, 'update'); // waits on T1 -> cycle
  assert.ok(r.blocked);
  assert.deepEqual([...r.cycle].sort(), ['T1', 'T2']);
  const dl = e.anomalies.find((a) => a.kind === 'deadlock');
  assert.ok(dl, 'deadlock anomaly emitted');
  e.rollback('T2'); // break the cycle
  assert.equal(e.txns.get('T1').state, 'active'); // T1 got the lock on b
  e.commit('T1');
  assert.equal(e.txns.get('T1').state, 'committed');
});

// ---------------------------------------------------------------- write skew
test('write skew: reported under SI, prevented under serializable', () => {
  const run = (isolation) => {
    const e = new Engine(isolation);
    seed(e, { a: { balance: 100 }, b: { balance: 100 } });
    e.begin('T1'); e.begin('T2');
    e.read('T1', 'a'); e.read('T1', 'b');
    e.read('T2', 'a'); e.read('T2', 'b');
    e.write('T1', 'a', { balance: -50 }, 'update');
    e.write('T2', 'b', { balance: -50 }, 'update');
    e.commit('T1');
    e.commit('T2');
    return e;
  };
  const si = run('snapshot_isolation');
  assert.equal(si.txns.get('T2').state, 'committed'); // SI allows it
  const skew = si.anomalies.find((a) => a.kind === 'write_skew');
  assert.ok(skew, 'write skew anomaly reported');
  assert.deepEqual([...skew.cycle].sort(), ['T1', 'T2']);

  const ser = run('serializable');
  const states = [ser.txns.get('T1').state, ser.txns.get('T2').state];
  assert.ok(states.includes('aborted'), 'one txn aborted under serializable');
  assert.ok(ser.anomalies.some((a) => a.kind === 'serialization_failure'));
});

// -------------------------------------------------------------- read-only txn
test('read-only txn: reads fine, writes rejected, commits', () => {
  const e = new Engine('serializable');
  seed(e, { a: { v: 1 } });
  e.begin('T1', { readOnly: true });
  assert.deepEqual(e.read('T1', 'a').value, { v: 1 });
  const w = e.write('T1', 'a', { v: 2 }, 'update');
  assert.ok(w.error);
  assert.ok(e.events.some((ev) => ev.type === 'error'));
  assert.ok(e.commit('T1').ok);
});

// --------------------------------------------------------------- logical time
test('logical clock ticks per event; snapshots stay stable', () => {
  const e = new Engine('snapshot_isolation');
  seed(e, { a: { v: 1 } });
  const c0 = e.clock;
  e.begin('T1');
  e.read('T1', 'a');
  const snap = e.txns.get('T1').snapshot;
  e.begin('T2');
  e.write('T2', 'a', { v: 2 }, 'update');
  e.commit('T2');
  assert.ok(e.clock > c0);
  assert.ok(e.txns.get('T2').commitSeq > snap);
  assert.deepEqual(e.read('T1', 'a').value, { v: 1 }); // snapshot unchanged by later commits
  const clocks = e.events.map((ev) => ev.clock);
  assert.deepEqual(clocks, [...clocks].sort((x, y) => x - y));
  assert.equal(new Set(clocks).size, clocks.length); // strictly increasing
});

// --------------------------------------------------------------- persistence
test('serialize -> restore -> continue, without re-executing steps', () => {
  const e = new Engine('snapshot_isolation');
  seed(e, { a: { v: 1 } });
  e.begin('T1');
  e.read('T1', 'a');
  const eventCount = e.events.length;

  const restored = Engine.fromJSON(JSON.parse(JSON.stringify(e.toJSON())));
  assert.equal(restored.events.length, eventCount); // history carried over, not replayed
  assert.equal(restored.clock, e.clock);
  restored.write('T1', 'b', { v: 2 }, 'insert');
  restored.commit('T1');
  restored.begin('T2');
  assert.deepEqual(restored.read('T2', 'b').value, { v: 2 });
  assert.deepEqual(restored.read('T2', 'a').value, { v: 1 });
  // original untouched (immutability of history snapshots)
  assert.equal(e.events.length, eventCount);
});

// ------------------------------------------------------------------ scheduler
test('scheduler: step/run, blocked op auto-completes exactly once', () => {
  const ops = [
    { type: 'begin', txn: 'T0' },
    { type: 'insert', txn: 'T0', key: 'a', value: { n: 0 } },
    { type: 'commit', txn: 'T0' },
    { type: 'begin', txn: 'T1' },
    { type: 'begin', txn: 'T2' },
    { type: 'update', txn: 'T1', key: 'a', value: { n: 1 } },
    { type: 'update', txn: 'T2', key: 'a', value: { n: 2 } }, // will block
    { type: 'commit', txn: 'T1' }, // releases lock -> T2 write retries -> RC: ok
    { type: 'commit', txn: 'T2' },
  ];
  const e = new Engine('read_committed');
  const s = new Scheduler(e, ops);
  while (!s.step().done) {}
  const writes = e.events.filter((ev) => ev.type === 'write' && ev.txn === 'T2');
  assert.equal(writes.length, 1, 'blocked write executed exactly once after unblock');
  assert.equal(e.txns.get('T2').state, 'committed');
  assert.ok(s.finished);
});

test('scheduler: state survives JSON round-trip mid-schedule', () => {
  const ops = [
    { type: 'begin', txn: 'T1' },
    { type: 'insert', txn: 'T1', key: 'a', value: { v: 1 } },
    { type: 'commit', txn: 'T1' },
  ];
  const e = new Engine('snapshot_isolation');
  const s = new Scheduler(e, ops);
  s.step();
  const e2 = Engine.fromJSON(JSON.parse(JSON.stringify(e.toJSON())));
  const s2 = Scheduler.restore(e2, JSON.parse(JSON.stringify(s.toJSON())));
  s2.run();
  assert.equal(e2.txns.get('T1').state, 'committed');
  assert.equal(e2.events.filter((ev) => ev.type === 'begin').length, 1);
});
