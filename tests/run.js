/* Semantic tests for the MVCC engine + scheduler. Run: node tests/run.js */
const assert = require('assert');
const MVCC = require('../js/engine');
const { Scheduler } = require('../js/scheduler');
const { PRESETS } = require('../js/presets');
const mem = {};
const store = {
  getItem: k => (k in mem ? mem[k] : null),
  setItem: (k, v) => { mem[k] = String(v); },
  removeItem: k => { delete mem[k]; }
};

let passed = 0;
function test(name, fn) {
  for (const k of Object.keys(mem)) delete mem[k];
  fn();
  passed++;
  console.log('  ✓ ' + name);
}

let runCounter = 0;
function run(title, seed, steps) {
  const s = new Scheduler('t' + (++runCounter), store);
  s.loadPlan(title, seed, steps.map(x => Object.assign({ breakpoint: false }, x)));
  s.runToBreakpointOrEnd();
  return s;
}
const anomalyTypes = s => s.engine.events.filter(e => e.severity === 'anomaly').map(e => e.type);/* ---- dirty read ---- */
test('RU dirty read is observed', () => {
  const s = run('dr', { x: 10 }, [
    { txId: 'T1', action: 'begin', iso: 'RU' },
    { txId: 'T2', action: 'begin', iso: 'RU' },
    { txId: 'T1', action: 'update', key: 'x', val: 99 },
    { txId: 'T2', action: 'read', key: 'x' },
    { txId: 'T1', action: 'rollback' }
  ]);
  assert(anomalyTypes(s).includes('dirty_read'), 'dirty_read anomaly');
});

test('RC does not see dirty data', () => {
  const s = run('rcdr', { x: 10 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 99 },
    { txId: 'T2', action: 'read', key: 'x' },
    { txId: 'T1', action: 'rollback' }
  ]);
  assert.deepStrictEqual(anomalyTypes(s), []);
  const ev = s.engine.events.filter(e => e.type === 'read' && e.txId === 'T2')[0];
  assert.strictEqual(ev.value, 10);
});

/* ---- non-repeatable read ---- */
test('RC non-repeatable read; SI repeatable', () => {
  const mk = iso => run('nrr', { x: 10 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'begin', iso },
    { txId: 'T2', action: 'read', key: 'x' },
    { txId: 'T1', action: 'update', key: 'x', val: 20 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'read', key: 'x' },
    { txId: 'T2', action: 'commit' }
  ]);
  assert(anomalyTypes(mk('RC')).includes('non_repeatable_read'));
  assert.deepStrictEqual(anomalyTypes(mk('SI')), []);
});

/* ---- phantoms ---- */
test('RC phantom; SI stable predicate', () => {
  const mk = iso => run('ph', { x: 10, y: 30 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'begin', iso },
    { txId: 'T2', action: 'predicate', op: '>', value: 15 },
    { txId: 'T1', action: 'insert', key: 'z', val: 42 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'predicate', op: '>', value: 15 },
    { txId: 'T2', action: 'commit' }
  ]);
  assert(anomalyTypes(mk('RC')).includes('phantom'));
  assert.deepStrictEqual(anomalyTypes(mk('SI')), []);
});

/* ---- locking + RC concurrent update ---- */
test('RC concurrent update waits and resumes after release', () => {
  const s = run('cu', { x: 0 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 10 },
    { txId: 'T2', action: 'update', key: 'x', val: 20 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'read', key: 'x' },
    { txId: 'T2', action: 'commit' }
  ]);
  assert.deepStrictEqual(anomalyTypes(s), []);
  const waits = s.engine.events.filter(e => e.type === 'lock_wait');
  assert.strictEqual(waits.length, 1);
  // T2's own read sees its pending update (20 over committed 10)
  const t2read = s.engine.events.filter(e => e.type === 'read' && e.txId === 'T2').pop();
  assert.strictEqual(t2read.value, 20);
  const live = Object.fromEntries(s.engine.liveData().map(r => [r.key, r.val]));
  assert.strictEqual(live.x, 20);
});

/* ---- SI write conflict ---- */
test('SI first-committer-wins aborts second writer', () => {
  const s = run('siw', { x: 0 }, [
    { txId: 'T1', action: 'begin', iso: 'SI' },
    { txId: 'T2', action: 'begin', iso: 'SI' },
    { txId: 'T1', action: 'update', key: 'x', val: 10 },
    { txId: 'T2', action: 'update', key: 'x', val: 20 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'commit' }
  ]);
  const types = anomalyTypes(s);
  assert(types.includes('write_conflict'), 'write_conflict anomaly, got ' + types);
  assert.strictEqual(s.engine.txns.T2.status, 'aborted');
  assert.strictEqual(s.engine.txns.T1.status, 'committed');
  const live = Object.fromEntries(s.engine.liveData().map(r => [r.key, r.val]));
  assert.strictEqual(live.x, 10);
});

/* ---- write skew SI vs SE ---- */
test('SI permits write skew; SE aborts with serialization failure', () => {
  const mkPlan = iso => [
    { txId: 'T1', action: 'begin', iso },
    { txId: 'T2', action: 'begin', iso },
    { txId: 'T1', action: 'read', key: 'a' },
    { txId: 'T1', action: 'read', key: 'b' },
    { txId: 'T2', action: 'read', key: 'a' },
    { txId: 'T2', action: 'read', key: 'b' },
    { txId: 'T1', action: 'delete', key: 'a' },
    { txId: 'T2', action: 'delete', key: 'b' },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'commit' }
  ];
  const si = run('ws', { a: 1, b: 1 }, mkPlan('SI'));
  assert(anomalyTypes(si).includes('write_skew'), 'SI write skew');
  assert.strictEqual(si.engine.txns.T1.status, 'committed');
  assert.strictEqual(si.engine.txns.T2.status, 'committed');
  assert.strictEqual(si.engine.liveData().length, 0);

  const se = run('ws2', { a: 1, b: 1 }, mkPlan('SE'));
  const types = anomalyTypes(se);
  assert(types.includes('serialization_failure'), 'SE serialization failure, got ' + types);
  assert.strictEqual(se.engine.txns.T1.status, 'committed');
  assert.strictEqual(se.engine.txns.T2.status, 'aborted');
  assert.strictEqual(se.engine.liveData().length, 1); // only b deleted
});

/* ---- SE predicate conflict ---- */
test('SE predicate inserts form rw cycle -> abort', () => {
  const s = run('phse', { x: 10 }, [
    { txId: 'T1', action: 'begin', iso: 'SE' },
    { txId: 'T2', action: 'begin', iso: 'SE' },
    { txId: 'T1', action: 'predicate', op: '>', value: 15 },
    { txId: 'T2', action: 'predicate', op: '>', value: 15 },
    { txId: 'T1', action: 'insert', key: 'a', val: 16 },
    { txId: 'T2', action: 'insert', key: 'b', val: 17 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'commit' }
  ]);
  assert(anomalyTypes(s).includes('serialization_failure'));
  assert.strictEqual(s.engine.txns.T2.status, 'aborted');
});

/* ---- deadlock ---- */
test('deadlock cycle detected, one victim aborted', () => {
  const s = run('dl', { x: 1, y: 2 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 10 },
    { txId: 'T2', action: 'update', key: 'y', val: 20 },
    { txId: 'T1', action: 'update', key: 'y', val: 11 },
    { txId: 'T2', action: 'update', key: 'x', val: 21 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'commit' }
  ]);
  const dl = s.engine.events.find(e => e.type === 'deadlock');
  assert(dl, 'deadlock event');
  assert.deepStrictEqual(new Set(dl.cycle.slice().sort()), new Set(['T1', 'T2']));
  const statuses = [s.engine.txns.T1.status, s.engine.txns.T2.status];
  assert(statuses.includes('aborted') && statuses.includes('committed'));
  // survivor committed its own key; no dangling locks
  assert.deepStrictEqual(s.engine.locks, {});
});

/* ---- savepoint ---- */
test('savepoint rollback undoes only later writes', () => {
  const s = run('sp', { x: 1, y: 2 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 100 },
    { txId: 'T1', action: 'savepoint', name: 'sp1' },
    { txId: 'T1', action: 'update', key: 'y', val: 200 },
    { txId: 'T1', action: 'rollbackTo', name: 'sp1' },
    { txId: 'T1', action: 'read', key: 'y' },
    { txId: 'T1', action: 'read', key: 'x' },
    { txId: 'T1', action: 'commit' }
  ]);
  const reads = s.engine.events.filter(e => e.type === 'read');
  assert.strictEqual(reads[0].value, 2);
  assert.strictEqual(reads[1].value, 100);
  const live = Object.fromEntries(s.engine.liveData().map(r => [r.key, r.val]));
  assert.deepStrictEqual(live, { x: 100, y: 2 });
});

/* ---- read-only snapshot ---- */
test('read-only SI keeps snapshot; rejected writes', () => {
  const s = run('ro', { balance: 500 }, [
    { txId: 'RPT', action: 'begin', iso: 'SI', readOnly: true },
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'RPT', action: 'read', key: 'balance' },
    { txId: 'T1', action: 'update', key: 'balance', val: 900 },
    { txId: 'T1', action: 'commit' },
    { txId: 'RPT', action: 'read', key: 'balance' },
    { txId: 'RPT', action: 'commit' }
  ]);
  const reads = s.engine.events.filter(e => e.type === 'read');
  assert(reads.every(e => e.value === 500));

  const s2 = run('ro2', { balance: 500 }, [
    { txId: 'R', action: 'begin', iso: 'SI', readOnly: true },
    { txId: 'R', action: 'update', key: 'balance', val: 1 }
  ]);
  assert.strictEqual(s2.steps[1].status, 'error');
  assert.strictEqual(s2.steps[1].result.code, 'READ_ONLY');
});

/* ---- terminal state is one-shot ---- */
test('commit/rollback only once; later steps skipped', () => {
  const s = run('term', { x: 1 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'commit' },
    { txId: 'T1', action: 'read', key: 'x' },
    { txId: 'T1', action: 'rollback' }
  ]);
  assert.strictEqual(s.steps[2].status, 'skipped');
  assert.strictEqual(s.steps[3].status, 'skipped');
  assert.strictEqual(s.engine.txns.T1.status, 'committed');
});

/* ---- persistence / refresh ---- */
test('refresh resumes without re-executing steps', () => {
  const s = new Scheduler('persist', store);
  s.loadPlan('p', { x: 0 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 10 },
    { txId: 'T1', action: 'commit' }
  ]);
  s.runNext();
  s.runNext();
  const clockMid = s.engine.clock;
  const doneIds = s.steps.slice(0, 2).map(x => x.id);
  const execTimes = s.steps.slice(0, 2).map(x => x.result.executedAt);

  const restored = Scheduler.restore('persist', store);
  assert(restored, 'restored');
  assert.strictEqual(restored.steps[0].status, 'done');
  assert.strictEqual(restored.steps[1].status, 'done');
  assert.strictEqual(restored.steps[2].status, 'pending');
  restored.runToBreakpointOrEnd();
  assert.strictEqual(restored.engine.txns.T1.status, 'committed');
  // executed timestamps of earlier steps unchanged -> not re-executed
  assert.strictEqual(restored.steps[0].result.executedAt, execTimes[0]);
  assert.strictEqual(restored.steps[1].result.executedAt, execTimes[1]);
  assert.strictEqual(restored.engine.events.filter(e => e.type === 'begin').length, 1);
});

test('waiting step survives refresh and resumes after release', () => {
  const s = new Scheduler('w', store);
  s.loadPlan('w', { x: 0 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 10 },
    { txId: 'T2', action: 'update', key: 'x', val: 20 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'commit' }
  ]);
  for (let i = 0; i < 4; i++) s.runNext();
  assert.strictEqual(s.steps[3].status, 'waiting');

  const r = Scheduler.restore('w', store);
  assert.strictEqual(r.steps[3].status, 'waiting');
  r.runNext(); // T1 commit -> T2's parked write is resumed automatically
  assert.strictEqual(r.steps[3].status, 'done');
  r.runNext(); // T2 commit
  assert.strictEqual(r.engine.txns.T2.status, 'committed');
  const live = Object.fromEntries(r.engine.liveData().map(z => [z.key, z.val]));
  assert.strictEqual(live.x, 20);
});

/* ---- history immutability ---- */
test('executed steps cannot be edited or removed', () => {
  const s = new Scheduler('h', store);
  s.loadPlan('h', { x: 1 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'read', key: 'x' }
  ]);
  s.runNext();
  assert.throws(() => s.updateStep(s.steps[0].id, { iso: 'SI' }));
  assert.throws(() => s.removeStep(s.steps[0].id));
  // pending step still editable
  s.updateStep(s.steps[1].id, { key: 'y' });
  assert.strictEqual(s.steps[1].key, 'y');
});

/* ---- logical time monotonicity ---- */
test('commit sequence numbers and event ticks monotonic', () => {
  const s = run('lt', { x: 0 }, [
    { txId: 'T1', action: 'begin', iso: 'RC' },
    { txId: 'T1', action: 'update', key: 'x', val: 1 },
    { txId: 'T1', action: 'commit' },
    { txId: 'T2', action: 'begin', iso: 'RC' },
    { txId: 'T2', action: 'update', key: 'x', val: 2 },
    { txId: 'T2', action: 'commit' }
  ]);
  assert.strictEqual(s.engine.txns.T1.commitSeq, 1);
  assert.strictEqual(s.engine.txns.T2.commitSeq, 2);
  const ticks = s.engine.events.map(e => e.tick);
  ticks.forEach((t, i) => { if (i) assert(t > ticks[i - 1]); });
});

/* ---- all presets run end to end ---- */
test('every preset executes to a terminal schedule', () => {
  for (const p of PRESETS) {
    const s = run(p.title, p.seed, p.steps);
    assert(s.isDone(), p.id + ' finishes');
    assert.deepStrictEqual(s.engine.locks, {}, p.id + ' releases all locks');
  }
});

console.log(`\n${passed} tests passed`);
