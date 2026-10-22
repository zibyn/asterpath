# 存储用单 SQLite 文件、全同步 WAL 与进程独占写者

**Status**: accepted (2026-10-22)
**来源**: [#5 存储抽象与 SQLite 首实现](https://github.com/zibyn/asterpath/issues/5)
**前置**: [ADR-0002](0002-log-topology-and-commit-semantics.md)、[ADR-0003](0003-single-session-stream-topology.md)、[ADR-0004](0004-log-schema-version-and-upcasting.md)

## Context

日志语义（#3）已经定死：按 session 分片、终态提交、模型可见即已记录、日志层无 ack、检查点恢复。剩下的是物理存放：日志和调度状态放在哪、一次写入何时算持久、谁有资格写、哪些结构是真相源。

两个参考实现给出了相反的取舍。Pi Durable 用 WAL + `synchronous=NORMAL`，接受「最新一次提交可能丢」；Duroxide 用 peek-lock 租约与多 worker 队列，为多进程调度服务。Asterpath v1 是嵌入式单进程库，多进程调度已出界，但「意图先提交后执行」要求持久性强于 Pi 的默认值。

## Decision

- **一个 SQLite 文件装全部 session。** `EventLog` 与 `Storage` 是两个 trait，由同一模块实现。跨 session 的调度队列与 timer 索引使按文件分片没有收益。
- **信封强类型列 + `payload` JSON。** 事件体不拆 typed columns，upcast 继续在读取时对 `Value` 做（ADR-0004），DDL 不随 `LOG_SCHEMA_VERSION` 变化。
- **日志信封归 core。** `NewEntry` / `LogEntry` 是 core 的唯一信封形状，payload 是 core 不解释的 `serde_json::Value`；类型化解码与 upcast 归 protocol 的 `decode_entry(&LogEntry)`（[ADR-0020](0020-log-envelope-core-typed-decoding-protocol.md)）。
- **`journal_mode=WAL` + `synchronous=FULL` 默认。** append 返回即 fsync；`NORMAL` 是显式选项，只用于测试/开发。
- **进程独占写者。** `<db>.lock` 上非阻塞 `flock`，生命周期持有，不续约；第二个进程 `WriterBusy`。不另设租约表。
- **一条记录一个事务**；`seq` 用 `MAX(seq)+1` 在事务内分配。timer 触发 / signal 到达与派生行同事务；父子会话跨流不原子。
- **派生索引在 `append` 内维护**：`sessions`、`open_intents`、`timers`、`pending_inputs` 可由日志重建，撤销了单独 enqueue API 的必要；会话 `idle` 由未闭合意图推导，不缓存状态列（#23）。
- **幂等键按事件类型隔离。** `log` 表存 `event_kind`（payload 顶层变体名），唯一约束与查重 SQL 都用 `(session_id, event_kind, dedup_key)`。意图与终态刻意共用原始 key（如 `tool_call_id`）作为关联，不能互相抑制；因此解码提前到查重之前，以便同时得到 `event_kind`。
- **没有 torn-tail 修复**：SQLite 提交原子，解码失败是硬错误而不是可截断的尾巴。
- **v1 不做投影快照**；恢复从检查点折叠。

## Consequences

- **2026-10-22 #32 修订**：v1 是单进程显式驱动，没有并发消费者，删除 `Storage::claim` / `ack`、租约字段与 `writer_lease` 表；唤醒改为只读 `Storage::wakeable`，待处理输入由 `pending_inputs` 派生。存储不再有调度队列。
- **2026-10-22 #18 修订**：日志仍不支持原地删除、裁剪或改写；整会话退役（日志 + 墓碑）是可选管理能力，见 [ADR-0016](0016-retention-and-archive-seam.md)。
- 掉电也不丢已返回的 append，代价是每 step 约 4~6 次 fsync；在 LLM 延迟尺度下可接受。
- 写入路径只有一条：日志行与派生行同事务，不存在索引与日志分叉。
- `event_kind` 是持久判别式：带 dedup key 的事件变体改名时，映射必须保持旧值（[ADR-0004](0004-log-schema-version-and-upcasting.md)）。
- 单写者让恢复与调度不需要分布式租约；多进程/多 worker 只能作为未来的新后端或新执行模型重新设计。
- 日志不支持原地删除、裁剪或改写；整会话退役是可选管理能力（#18，见 [ADR-0016](0016-retention-and-archive-seam.md)），内核不内置保留策略。
- 事件体是 JSON，按字段查询要走 SQLite JSON 函数或全扫；v1 的查询模式（按 session + seq）不需要。
- `synchronous=NORMAL` 的掉电丢最新提交不可用于生产，这一点必须写进配置文档，而不是留给使用者猜。

## Considered alternatives

- **每 session 一个文件**：隔离与可移植性好，但调度队列、timer、子会话唤醒都跨 session，需要目录级协调器；单进程 v1 不值得。
- **事件拆 typed columns**：查询快，但每次 `LOG_SCHEMA_VERSION` bump 都变成 DDL 迁移，把两个版本轴耦合。
- **默认 `synchronous=NORMAL`**：快，但掉电丢 `ToolCallRequested` 会让已发生的副作用在恢复后不可见，直接破坏重放安全性。
- **心跳续约租约**：多 worker 才需要；单写者下只是额外故障面。
- **投影快照表**：省折叠，但引入失效逻辑；恢复本来就要扫描检查点之后的事件，先测量再优化。
- **writer 线程 + 读连接池**：并发更好，但 v1 的写频率由 LLM 决定，`Mutex<Connection>` 足够；模块内可随时升级，不动契约。
