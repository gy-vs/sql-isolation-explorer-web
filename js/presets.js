/*
 * Preset schedules for the workbench.  Each preset is:
 *   { id, title, blurb, isoNote, seed, steps:[...] }
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.MVCCPresets = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const PRESETS = [
    {
      id: 'dirty-read',
      title: '① 脏读 (RU)',
      blurb: 'T1 未提交的修改被 RU 的 T2 读到；T1 回滚后，T2 依据了一条从未存在过的数据。',
      seed: { x: 10 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'RU' },
        { txId: 'T2', action: 'begin', iso: 'RU' },
        { txId: 'T1', action: 'update', key: 'x', val: 99 },
        { txId: 'T2', action: 'read', key: 'x' },
        { txId: 'T1', action: 'rollback' }
      ]
    },
    {
      id: 'nrr',
      title: '② 不可重复读 (RC vs SI)',
      blurb: 'T2 两次读取同一行，期间 T1 提交了更新。RC 下值改变（不可重复读）；同样编排换成 SI 则可重复。',
      seed: { x: 10 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'RC' },
        { txId: 'T2', action: 'begin', iso: 'RC' },
        { txId: 'T2', action: 'read', key: 'x' },
        { txId: 'T1', action: 'update', key: 'x', val: 20 },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'read', key: 'x' },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'phantom',
      title: '③ 幻读 / 谓词范围 (RC vs SI)',
      blurb: 'T2 两次谓词读取 val > 15，期间 T1 插入新行并提交。RC 下范围多出“幻影行”；SI 下范围固定。',
      seed: { x: 10, y: 30 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'RC' },
        { txId: 'T2', action: 'begin', iso: 'RC' },
        { txId: 'T2', action: 'predicate', op: '>', value: 15 },
        { txId: 'T1', action: 'insert', key: 'z', val: 42 },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'predicate', op: '>', value: 15 },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'deadlock',
      title: '④ 死锁与等待环',
      blurb: 'T1、T2 以相反顺序持有 x/y 的写锁，调度器检测等待环并选择牺牲者中止，另一个事务继续。',
      seed: { x: 1, y: 2 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'RC' },
        { txId: 'T2', action: 'begin', iso: 'RC' },
        { txId: 'T1', action: 'update', key: 'x', val: 10 },
        { txId: 'T2', action: 'update', key: 'y', val: 20 },
        { txId: 'T1', action: 'update', key: 'y', val: 11 },
        { txId: 'T2', action: 'update', key: 'x', val: 21 },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'write-skew-si',
      title: '⑤ 写偏斜：SI 允许',
      blurb: '经典黑白球/值班医生：两事务都读到“还剩 2 个”，各自删掉一个并提交。SI 下双双成功，实际剩 0——写偏斜。',
      seed: { a: 1, b: 1 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'SI' },
        { txId: 'T2', action: 'begin', iso: 'SI' },
        { txId: 'T1', action: 'read', key: 'a' },
        { txId: 'T1', action: 'read', key: 'b' },
        { txId: 'T2', action: 'read', key: 'a' },
        { txId: 'T2', action: 'read', key: 'b' },
        { txId: 'T1', action: 'delete', key: 'a' },
        { txId: 'T2', action: 'delete', key: 'b' },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'write-skew-se',
      title: '⑥ 写偏斜：SE 拒绝',
      blurb: '与⑤相同编排，但隔离级别为可序列化。后提交者因读写依赖环收到 SERIALIZATION 失败。',
      seed: { a: 1, b: 1 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'SE' },
        { txId: 'T2', action: 'begin', iso: 'SE' },
        { txId: 'T1', action: 'read', key: 'a' },
        { txId: 'T1', action: 'read', key: 'b' },
        { txId: 'T2', action: 'read', key: 'a' },
        { txId: 'T2', action: 'read', key: 'b' },
        { txId: 'T1', action: 'delete', key: 'a' },
        { txId: 'T2', action: 'delete', key: 'b' },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'savepoint',
      title: '⑦ 回滚与保存点',
      blurb: 'T1 修改 x、建立保存点、再修改 y；回滚到保存点只撤销 y，x 的修改保留并提交。',
      seed: { x: 1, y: 2 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'RC' },
        { txId: 'T1', action: 'update', key: 'x', val: 100 },
        { txId: 'T1', action: 'savepoint', name: 'sp1' },
        { txId: 'T1', action: 'update', key: 'y', val: 200 },
        { txId: 'T1', action: 'rollbackTo', name: 'sp1' },
        { txId: 'T1', action: 'read', key: 'y' },
        { txId: 'T1', action: 'read', key: 'x' },
        { txId: 'T1', action: 'commit' }
      ]
    },
    {
      id: 'read-only',
      title: '⑧ 只读事务的一致性快照',
      blurb: '只读事务在 SI 下拿到开始时刻快照：即使 T1 提交了新值，报表事务反复读取始终看到旧值。',
      seed: { balance: 500 },
      steps: [
        { txId: 'RPT', action: 'begin', iso: 'SI', readOnly: true },
        { txId: 'T1', action: 'begin', iso: 'RC' },
        { txId: 'RPT', action: 'read', key: 'balance' },
        { txId: 'T1', action: 'update', key: 'balance', val: 900 },
        { txId: 'T1', action: 'commit' },
        { txId: 'RPT', action: 'read', key: 'balance' },
        { txId: 'RPT', action: 'commit' }
      ]
    },
    {
      id: 'phantom-se',
      title: '⑨ 谓词范围的序列化失败',
      blurb: '两事务都执行“val > 15”范围检查，各自插入满足条件的行。SE 用谓词读写边识别幻写，后提交者中止。',
      seed: { x: 10 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'SE' },
        { txId: 'T2', action: 'begin', iso: 'SE' },
        { txId: 'T1', action: 'predicate', op: '>', value: 15 },
        { txId: 'T2', action: 'predicate', op: '>', value: 15 },
        { txId: 'T1', action: 'insert', key: 'a', val: 16 },
        { txId: 'T2', action: 'insert', key: 'b', val: 17 },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'concurrent-update',
      title: '⑩ 并发更新：RC 等待 vs SI 中止',
      blurb: '同一行的两个并发更新：RC 下后者等待，取锁后读到最新提交值再更新；SI 下快照过期，提交时先提交者获胜而中止。',
      seed: { x: 0 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'RC' },
        { txId: 'T2', action: 'begin', iso: 'RC' },
        { txId: 'T1', action: 'update', key: 'x', val: 10 },
        { txId: 'T2', action: 'update', key: 'x', val: 20 },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'read', key: 'x' },
        { txId: 'T2', action: 'commit' }
      ]
    },
    {
      id: 'concurrent-update-si',
      title: '⑪ SI 并发更新冲突',
      blurb: 'SI 下两事务并发写同一行：先提交者获胜，后提交者在写/提交时收到 WRITE_CONFLICT 并中止。',
      seed: { x: 0 },
      steps: [
        { txId: 'T1', action: 'begin', iso: 'SI' },
        { txId: 'T2', action: 'begin', iso: 'SI' },
        { txId: 'T1', action: 'update', key: 'x', val: 10 },
        { txId: 'T2', action: 'update', key: 'x', val: 20 },
        { txId: 'T1', action: 'commit' },
        { txId: 'T2', action: 'commit' }
      ]
    }
  ];

  return { PRESETS };
});
