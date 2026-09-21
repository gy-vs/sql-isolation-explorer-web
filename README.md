# MVCC 隔离级别探索工作台

在内存 MVCC 模型上编排多事务调度，逐步观察可见版本、锁等待与并发异常。不连接真实数据库。

## 运行

```bash
npm start          # http://localhost:3000
npm test           # node:test 引擎与调度器测试
```

## 模型

- **隔离级别**：`read_committed`（语句级快照）、`snapshot_isolation`（事务级快照 + 首提交者胜）、`serializable`（序列化图环检测，提交时中止成环事务）。
- **版本**：追加式版本链，删除即墓碑；版本仅在事务终态时一次性转换状态，历史不可改写。
- **逻辑时间**：`clock` 每事件递增，`commitSeq` 定义快照。
- **冲突边**：`wr`（读依赖）、`ww`（写依赖）、`rw`（反依赖，写偏斜/SSI 的来源）。
- **异常**：脏读（三种级别均被阻止）、不可重复读、幻读、写偏斜（SI/RC 下报告）、序列化失败（serializable 中止 / SI 写冲突）、死锁（等待环）。

## 操作类型

`begin`（可 `readOnly`）、`read`、`select`（`pred` 谓词：字段比较或 key 范围）、`insert` / `update` / `delete`、`savepoint` / `rollback_to`、`commit`、`rollback`。

## API

`GET/POST /api/schedules`，`GET/DELETE /api/schedules/:id`，`POST .../step | run | reset | abort | ops`。

每步执行后引擎完整状态落盘 `data/state.json`，重启/刷新直接恢复现场，不重放步骤；`reset` 将本次运行归档进不可变的 `history`。
