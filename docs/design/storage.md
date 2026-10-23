# 存储抽象与 SQLite 首实现

> 来源：[#5](https://github.com/zibyn/asterpath/issues/5)，2026-10-22 已定。
> 相关 ADR：[0005 存储契约与 SQLite 首实现](../adr/0005-storage-contract-and-sqlite.md)。
> 前置：[#3 事件日志与检查点恢复](event-log-recovery.md)、[#4 schema 版本](../adr/0004-log-schema-version-and-upcasting.md)。
> 范围：v1 单进程、单写者、崩溃安全。Postgres 只在 seam 层面被考虑，不实现。
> **2026-10-22 #31 修订**：`InputRecorded` 也进入唤醒发现，使 `submit_input` 后崩溃仍可唤醒。
> **2026-10-22 #32 修订**：v1 是单进程显式驱动，没有并发消费者；删除 `claim` / `ack` / 租约 / `writer_lease`，唤醒改为只读 `Storage::wakeable`，待处理输入由 `pending_inputs` 派生。

## 1. 决策摘要

- **一个 SQLite 文件装全部 session。** `EventLog` 与 `Storage` 仍是两个 trait，由同一个模块 `asterpath-storage-sqlite` 实现；不合并、不互相调用。
- **信封强类型列 + `payload` JSON。** `SessionEvent` 不拆成 typed columns；upcast 继续在读取时对 `Value` 做（ADR-0004）。
- **`journal_mode=WAL` + `synchronous=FULL` 为默认。** `NORMAL` 只作为显式配置项，供测试/开发用。
- **写者是进程独占的。** `flock` 写者锁在打开时非阻塞获取，进程死亡由 OS 释放；不续约、不过期。第二个进程打开即 `WriterBusy`。
- **一条日志记录 = 一个事务。** 不做多记录批量事务；`seq` 在事务内用 `MAX(seq)+1` 分配。
- **派生表在 `append` 内同一事务维护**：`sessions`、`open_intents`、`pending_inputs` 都是日志的派生索引，不是真相源。（`timers` 表在 #65 删除通用 timer 后已无写入路径，表本身由 #73 的迁移删除。）
- **`idle` 是未闭合意图的谓词，不是缓存列。** `sessions` 没有 `state`；存在任一非 compaction 的 open intent 即非 `idle`。storage 与恢复共用同一个折叠函数（[event-log-recovery.md](event-log-recovery.md) §6）。
- **没有 torn tail 修复。** SQLite 提交原子；解码失败一律硬错误。
- **v1 不做投影快照。** 恢复从检查点折叠日志。

## 2. 职责拆分

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| `EventLog`（core） | append、按游标 scan、frontier | 调度 |
| `Storage`（core） | 唤醒发现 | 追加日志、事件语义 |
| `asterpath-storage-sqlite`（模块） | 同时实现两者；派生索引；写者锁；WAL | 事件解码语义（归 protocol） |

同一模块实现两个 trait，是因为派生索引必须在 `append` 的同一事务里写；这是实现细节，不是 trait 契约。

## 3. 合约签名

```rust
// core

pub type Timestamp = i64; // Unix epoch 毫秒；唯一允许的时间表示

pub struct NewEntry {
    pub session: SessionId,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: serde_json::Value, // protocol 已序列化的 SessionEvent
}

pub struct LogEntry {
    pub log_id: LogId,
    pub session: SessionId,
    pub seq: u64,
    pub recorded_at: Timestamp,   // 存储层写入；仅 trace/审计可读
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: serde_json::Value,
}

pub struct LogQuery {
    pub session: SessionId,
    pub after: Option<LogId>, // 排他游标
    pub limit: Option<usize>,
}

pub trait EventLog: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId)
        -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;
}

pub trait Storage: Send + Sync {
    /// 需要被驱动的 session：存在非 compaction 未闭合意图或待处理输入。
    fn wakeable(&self, now: Timestamp)
        -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>>;
}

#[non_exhaustive]
pub enum StorageError {
    WriterBusy,
    SchemaTooNew { found: i64, supported: i64 },
    InvalidEntry(String),
    Corrupt { session: Option<SessionId>, detail: String },
    Backend(String),
}
```

这套信封是 core 的唯一形状（[ADR-0020](../adr/0020-log-envelope-core-typed-decoding-protocol.md)）：`append` 收 `NewEntry`，因为 `seq`、`log_id`、`recorded_at` 由存储分配（#3 §4）；#2 的 `append(LogEntry)` 与 `{ kind, schema_version, payload }` 已删除。类型化解码与 upcast 不在存储，由 protocol 的 `decode_entry(&LogEntry)` 负责（[ADR-0004](../adr/0004-log-schema-version-and-upcasting.md)）；`StorageError` 不带 `source`，符合「库契约不暴露 `Box<dyn Error>`」。

`Storage` 不提供公开 `enqueue`，也没有租约或 ack：唤醒是只读查询，由 `open_intents` 与 `pending_inputs` 派生。同一 session 的并发驱动由 facade 的 `drive()` 单飞拒绝；重复驱动不产生重复记录（幂等 append）。`WakeCause` 属于引擎的持久等待（module-contracts.md §3.4），存储不需要它。

## 4. SQLite schema

```sql
PRAGMA user_version = 1;

CREATE TABLE sessions (
    session_id  BLOB    PRIMARY KEY,           -- SessionId 的 16 字节
    parent_id   BLOB,                          -- 子会话指向父会话
    created_at  INTEGER NOT NULL                -- 诊断用，不参与任何逻辑
);

-- 未闭合意图；idle = 没有非 compaction 行。由日志折叠重建。
CREATE TABLE open_intents (
    session_id BLOB NOT NULL,
    kind       TEXT NOT NULL,   -- turn | step | tool | child | compaction
    ref_id     BLOB NOT NULL,   -- 各事件 id；compaction 用 frontier LogId（8 字节大端）
    PRIMARY KEY (session_id, kind, ref_id)
);

CREATE TABLE log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      BLOB    NOT NULL,
    seq             INTEGER NOT NULL,
    recorded_at     INTEGER NOT NULL,           -- 诊断时间，仅 trace/审计可读
    schema_version  INTEGER NOT NULL,
    event_kind      TEXT    NOT NULL,           -- payload 顶层变体名，幂等键判别式
    dedup_key       TEXT,                       -- NULL = 无幂等键
    payload         TEXT    NOT NULL,           -- JSON
    UNIQUE (session_id, seq),
    UNIQUE (session_id, event_kind, dedup_key)  -- 不同事件类型互不抑制；NULL 互不冲突
);

-- 遗留：#65 后无写入路径，由 #73 的迁移删除。
CREATE TABLE timers (
    timer_id    BLOB    PRIMARY KEY,
    session_id  BLOB    NOT NULL,
    deadline    INTEGER NOT NULL,
    fired_at    INTEGER                         -- NULL = 未触发
);
CREATE INDEX timers_due ON timers (deadline) WHERE fired_at IS NULL;

-- 已记录、尚未被 step 消费的输入；StepStarted 清空。
CREATE TABLE pending_inputs (
    session_id BLOB NOT NULL,
    input_id   BLOB NOT NULL,
    PRIMARY KEY (session_id, input_id)
);

-- 退役会话的身份记录；退役接缝使用，见 retention.md
CREATE TABLE retired_sessions (
    session_id   BLOB    PRIMARY KEY,
    parent_id    BLOB,
    last_log_id  INTEGER NOT NULL,
    retired_at   INTEGER NOT NULL               -- 仅诊断
);
```

- `retired_sessions` **不是派生索引**：日志删除后无法重建，它是管理的真相来源。G7 只覆盖 `sessions` / `open_intents` / `pending_inputs`。

- `open_intents` 是派生索引：`idle` 谓词为「不存在 `kind != 'compaction'` 的行」。compaction 行用于恢复重做，不影响 `idle`；其 `ref_id` 取 dedup key 里的 frontier `LogId`。

- `UNIQUE(session_id, seq)` 同时承担 scan 顺序索引，不另建索引。
- `event_kind` 由 `append` 从 payload 顶层键派生，不进 `NewEntry` / `LogEntry`，只参与幂等查重；扫描仍按 `(session, seq)`。判别式是持久值：变体改名时映射保持旧值（[ADR-0004](../adr/0004-log-schema-version-and-upcasting.md)）。
- `log_id` 用 `AUTOINCREMENT`：id 不复用，保持单调。
- `recorded_at` 由 `append` 分配，不接受调用方传入；只供 trace/审计读取，不参与派生索引或任何逻辑。
- 无外键：单写者 + 派生索引，外键只会增加写放大。
- 物理 DDL 迁移用 `PRAGMA user_version`，前向逐级；打开时发现更高版本 → `SchemaTooNew`。与 `LOG_SCHEMA_VERSION` 完全独立（ADR-0004）。

## 5. 写路径与事务边界

### append

```
BEGIN IMMEDIATE
  event = decode(payload)                        -- 失败 -> InvalidEntry；顶层键给出 event_kind
  kind = event_kind(event)
  if dedup_key 非空:
      SELECT log_id FROM log WHERE session_id=? AND event_kind=? AND dedup_key=?
      if 命中: ROLLBACK; return 原 log_id        -- 不写、不派生
  seq = COALESCE((SELECT MAX(seq) FROM log WHERE session_id=?), 0) + 1
  recorded_at = now_ms()                        -- 存储层本地时钟
  INSERT INTO log (..., event_kind, recorded_at, ...)
  apply_derived(event, dedup_key)                -- open_intents/pending_inputs，同一事务
COMMIT                                           -- FULL 下返回时已 fsync
```

- 解码在查重之前，同时给出 `event_kind` 与派生输入；未知事件变体在追加时就报 `InvalidEntry`（与 ADR-0004 的硬错误一致）。
- 派生规则（`open_intents` 的折叠与恢复的分类器共享 protocol 的 `IntentDelta::of` / `SessionFacts`，不各写一套）：

| 事件 | `open_intents` 动作 | 其他派生动作 |
| --- | --- | --- |
| `SessionCreated` | — | 插入 sessions 行 |
| `InputRecorded` | 不变 | 插入 `pending_inputs` |
| `TurnStarted` | 插入 `(turn, turn_id)` | — |
| `TurnCommitted` | 删除 `(turn, turn_id)` | — |
| `StepStarted` | 插入 `(step, step_id)` | 删除该 session 的 `pending_inputs` |
| `StepCommitted` | 删除 `(step, step_id)` | — |
| `ToolCallRequested` | 插入 `(tool, tool_call_id)` | — |
| `ToolResult` | 删除 `(tool, tool_call_id)` | — |
| `SignalReceived` | 不变（审批意图由 `ToolResult` 关闭） | — |
| `ChildSessionSpawned` | 插入 `(child, child_session_id)` | — |
| `ChildSessionCompleted` | 删除 `(child, child_session_id)` | — |
| `CompactionStarted` | 插入 `(compaction, frontier LogId)` | — |
| `CompactionApplied` / `CompactionAborted` | 删除 `(compaction, frontier LogId)` | — |
| `AssistantMessage` / `ModelCallAborted` / `StreamResumed` / `ContextForked` | 不变 | — |

- 删除按键**精确匹配**。审批的 `SignalReceived` 不删除任何行；steering 的 `InputRecorded` 不改变 `open_intents`，但插入 `pending_inputs`，因此 `submit_input` 后崩溃时仍能被 `wakeable` 发现（S18）。
- **`idle` = `open_intents` 中没有 `kind != 'compaction'` 的行。** compaction 行只供恢复重做（#24/#27），不影响退役与唤醒发现。
- **唤醒不落独立队列**：`wakeable(now)` 直接读 `open_intents` 与 `pending_inputs`，按 `session_id` 去重。不存 generation、不记 reason、没有 ack；重复驱动由 `drive()` 单飞与幂等 append 吸收。

### 事务边界清单

| 写入 | 事务 |
| --- | --- |
| 任意一条日志记录 + 其派生行（sessions / open_intents / pending_inputs） | 一个事务，提交后返回 |
| 子会话终态（子流）与 `ChildSessionCompleted`（父流） | 两个事务，不跨流；靠幂等键修复 |
| 取消 | 就是一条终态记录（`TurnCommitted{Cancelled}` 或 `ToolResult`），无额外事务 |
| `ToolCallRequested` → 调用工具 | append 提交返回后才调用 |

## 6. 持久性、并发与连接模型

- `PRAGMA journal_mode=WAL`；`synchronous=FULL`（默认）或 `NORMAL`（显式配置）。
- **FULL 的承诺**：append 返回后，条目能扛进程崩溃**与掉电**。**NORMAL 的承诺**：能扛进程崩溃，掉电可能丢最近提交（Pi Durable 的取舍），只用于测试/开发。
- 写者锁：`<db>.lock` 上非阻塞 `flock(2)`，进程生命周期持有；拿不到 → `WriterBusy`。不另设租约表。
- 不允许多进程共享同一 DB；只读外部连接不在契约内。
- `busy_timeout=0`：任何 `SQLITE_BUSY` 立即以 `Backend` 暴露，不静默等待。
- 连接模型：一个 `Mutex<Connection>`，所有操作经 `spawn_blocking`。ops 串行化；扫描长到阻塞写路径时，再升级为 writer 线程 + WAL 只读连接（模块内优化，不动 trait）。
- 文件创建时权限 `0600`。
- 框架不做 WAL checkpoint、`VACUUM` 或 `journal_size_limit` 调参；日志保留/归档的 seam 见 [retention.md](retention.md)。删除释放页供复用，不保证文件缩小。

## 7. 唤醒发现

- **没有持久队列、租约或 ack。** `wakeable(now)` 是只读查询，返回存在非 compaction 未闭合意图或待处理输入（`pending_inputs`）的 session，按 `session_id` 去重。宿主的恢复循环是 `wakeable → open_session → drive`；并发驱动由 `drive()` 单飞拒绝（`AgentError::Busy`），重复记录由幂等 append 吸收。
- spawn 超时不经存储：截止时间在 `ChildSessionSpawned.deadline` 上，open 的子会话意图已让父会话出现在 `wakeable`，到期由 spawn 重放处理（[subagents.md](subagents.md) §9）。
- 没有 `recover_at_startup`：派生索引在同一事务维护，恢复是 per-session 的（`open_session` 折叠日志）。启动顺序：打开连接 → 设 PRAGMA → 取写者锁 → 读 `user_version`（必要时前向迁移）→ 宿主对 `wakeable` 结果逐个驱动。只有 `CompactionStarted` 未闭合的 session 仍视为 idle（丢弃一次摘要调用是安全的）。**不**预验证日志末条，不跑 `integrity_check`；解码失败在 scan 时暴露。

## 8. 保证清单

后端必须满足（Postgres 也照此实现）：

- **G1 前缀持久**：`append` 返回后，该条记录排在崩溃可见的前缀里；FULL 下扛掉电。
- **G2 幂等**：`(session, event_kind, dedup_key)` 只有一条记录；重复 append 返回原 `LogId`，不写派生行；同 key 不同事件类型互不抑制。
- **G3 全序**：`seq` 在 session 内严格单调、无洞，只由存储分配。
- **G4 游标扫描**：`scan(after)` 只返回 `log_id > after` 的已提交记录，按 `seq` 升序，`limit` 有界。扫描结果在读取期间构成稳定前缀：append 只会分配更大的 `log_id`，不会改变已返回的条目。
- **G5 唤醒发现完备**：任何存在非 compaction 未闭合意图或待处理输入的 session 都出现在 `wakeable`；同一 session 每次调用只返回一次；重复驱动不产生重复记录（幂等 append + `drive()` 单飞）。
- **G6 无跨流原子性**：任意两个 session 的写入不构成原子组。
- **G7 索引可重建**：`sessions` / `open_intents` / `pending_inputs` 可完全由日志重建，不是真相源。`open_intents` 折叠出的集合必须与恢复的 `SessionFacts` 折叠一致。
- **G8 退役原子性（可选能力）**：`SessionRetirement::retire` 在同一事务里写墓碑、删日志与全部派生行；要么全部生效，要么全部不生效；退役后的会话对 `EventLog` / `Storage` / `SessionCatalog` 不可见。契约见 [retention.md](retention.md)。

trait 不暴露：SQL、WAL、fsync、`log_id` 的 rowid 性质、文件路径、`flock`、JSON 文本编码。SQLite 特有实现（`MAX(seq)`、partial index、`AUTOINCREMENT`）留在模块内。

## 9. 故障注入与测试

`asterpath-testing` 提供五个注入点（装饰器实现，不进 core/protocol）；API 形状、动作与 CI 编排见 [testing.md](testing.md) 与 [ADR-0017](../adr/0017-fault-injection-and-test-tooling.md)。前四个点是 trait 装饰器，`clock:advance` 由 `ManualClock::advance` 提供。

| 注入点 | 模拟 |
| --- | --- |
| `append:before_commit` | 意图尚未持久就崩溃 |
| `append:after_commit` | 已持久、调用方未收到结果 |
| `tool:before_effect` | 副作用前崩溃（配合 `replay` 语义） |
| `tool:after_effect` | 副作用已发生、终态未写 |
| `clock:advance` | 可控时钟推进（spawn 截止时间、退避） |

| 场景 | 归属 |
| --- | --- |
| S4 坏 payload → `Corrupt`；S5 幂等；S8 `seq` 单调、`recorded_at` 不参与逻辑；`wakeable` 完备且不重复；`pending_inputs` 保留到 `StepStarted` | 存储 crate 单测 |
| S13 `open_intents` 派生与日志折叠一致；steering / 审批 / 子会话完成在 open step 中到达后仍非 idle；仅 compaction 未闭合时视为 idle | 存储 crate 单测（与 #6 / #8 的分类器共享用例） |
| S1 `Safe` 工具中 kill、S2 `Unsafe` 工具中 kill、S3 模型流中 kill、S6 父等子时 kill | #6 / #17 的进程级验收；本轮只固定注入点契约 |
| S9 spawn 超时在停机期间到期 | scheduler 测试（#65） |
| S7 `CompactionApplied` 投影 | #9 / #6 |

## 10. 留给下游

- **#6**：turn/step 的终止条件、取消宽限期，决定哪些意图在何时闭合；`idle` 由未闭合意图推导。
- **#8**：已定。父流何时 `ChildSessionCompleted`、启动时子会话缺失（R2 物化）与孤儿收尾（R4）见 [subagents.md](subagents.md) §9。
- **#9**：压缩触发条件；`CompactionApplied` 只影响投影，不删日志。
- **#17**：审批 signal 投递与子会话等待；唤醒发现见 §7。
- **#18**：已定。只保留 `SessionRetirement` 可选能力（整会话退役、墓碑）；导出由宿主用 `frontier` + `scan` 完成，导入不在 v1。见 [retention.md](retention.md)；本文件只承载墓碑 DDL 与 G8。
- **#19**：五个注入点的工具链与 CI 场景。
