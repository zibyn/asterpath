# 日志保留与退役

> 来源：[#18](https://github.com/zibyn/asterpath/issues/18)，2026-10-22 已定；同日按参考实现收敛范围。
> 相关 ADR：[0016 只保留安全退役接缝](../adr/0016-retention-and-archive-seam.md)。
> 前置：[#3 事件日志与恢复](event-log-recovery.md)、[#5 存储契约](storage.md)、[#4 schema 版本](../adr/0004-log-schema-version-and-upcasting.md)、[#9 压缩](compaction.md)、[#13 用量](usage.md)。
> 范围：**只定义安全退役的 seam**。导出是宿主用现有 `EventLog` 读取的配方；导入/恢复、归档文件管理、保留策略都不在 v1。

## 1. 决策摘要

- **保留/归档是宿主/系统层能力**：何时归档、按时间还是大小、文件放哪、是否加密，全在宿主或扩展模块。内核不提供策略、调度、CLI 命令。
- **内核唯一接缝是可选 `SessionRetirement::retire`**：整会话退役，由 `asterpath-storage-sqlite` 实现并提供。
- **只允许整会话退役**：不做日志前缀裁剪、按事件/按工具输出保留、级联删除。
- **导出没有新 API**：宿主用 `EventLog::frontier()` + `scan()` 读到该 frontier 即可，天然是一致前缀。JSONL 只是宿主参考格式。
- **导入/恢复不在 v1**：它是备份恢复，不是保留；整库备份或未来的运维工具承担。
- **退役要求 `idle` + `expected_frontier` CAS + 无活子会话 + 父会话 `idle`**；墓碑同事务写入；重复退役幂等。
- **退役后一律不可见**：打开、用量、追踪返回 `NotFound`；`scan` 空流；`SessionCatalog` 只覆盖活会话。
- **不做 `VACUUM` / checkpoint**；删除释放页供复用，文件缩小是离线运维。
- **v1 不新增 `asterpath-storage-admin` crate**。

## 2. 参考实现对照

| 参考 | 会话存放 | 删除/归档 | 保留策略 | 框架合约 |
| --- | --- | --- | --- | --- |
| Pi | 每会话一个 JSONL 文件 | 删文件，或 `/resume` 里 Ctrl+D；`SessionManager` 无 delete API | 无 | 无 |
| Claude Code | 每会话 JSONL transcript | `cleanupPeriodDays` 启动时清理旧 transcript | 应用设置 | 无 |
| Codex | rollout JSONL + 状态 DB | `thread/archive` / `thread/delete` 在 app-server / thread-store；删除前查外部引用、维护索引 | 完整 rollout 无保留设置 | app-server 协议，不在 agent loop |
| DeepSeek Harness | 持久化是插件 seam | 合约里没有 delete / archive / export / retention | 无 | 无 |

结论：

- 保留策略没有一家放进框架 API；它是应用/CLI 层的设置或命令。
- 安全删除确实需要一个 store 层操作（Codex 的 `thread/delete`），因为删除必须维护索引与外部引用。Asterpath 是单 SQLite 文件 + 派生表，宿主直接删行会破坏索引与恢复，因此保留一个退役接缝有依据。
- 导出和导入不是这个接缝的必需项。导出已经是现有读能力的组合；导入是备份恢复，属于宿主/运维，不属于保留。

## 3. 归属与接缝

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| `asterpath-core` | `SessionRetirement` trait、`RetirementError` | 策略、文件格式、调度、导出 |
| `asterpath-storage-sqlite` | 退役事务、墓碑、派发行清理 | 何时退役、归档落盘、恢复 |
| 宿主 / 未来扩展模块 | 保留策略、导出与 JSONL、重试、恢复 | 直接写 SQLite、绕过写者锁 |

- `SessionRetirement` 是 core 的可选能力，与 `EventLog` / `Storage` / `SessionCatalog` 同级；存储模块在 `init` 时 `provide` 它，宿主或策略模块用 `require_optional` 接入。
- 门面提供 `Asterpath::session_retirement() -> Option<Arc<dyn SessionRetirement>>`；宿主注入自定义存储时可能为 `None`。
- 写操作只能经过存储模块：派生索引必须在同一事务维护，写者锁也由它独占。宿主不得打开同一个 SQLite 文件，也不得依赖具体模块 crate。
- 保留策略**不进** `ConfigSnapshot`，与审批策略一样是宿主代码，不持久化。

## 4. 合约

```rust
// asterpath-core

pub trait SessionRetirement: Send + Sync {
    /// 退役整条会话：写墓碑，删除日志与全部派生行。
    /// expected_frontier 必须等于当前日志末条。
    fn retire(&self, session: SessionId, expected_frontier: LogId)
        -> BoxFuture<'_, Result<(), RetirementError>>;
}

#[non_exhaustive]
pub enum RetirementError {
    NotFound,
    NotIdle,
    FrontierMoved { expected: LogId, actual: Option<LogId> },
    HasChildren,
    ParentNotIdle,
    Storage(StorageError),
}
```

- 只有一个写操作；读取、导出、归档格式都不在这个 trait 上。
- `RetirementError::Storage` 只承载后端故障；语义失败用专门变体。
- 不并入 `StorageError`：后者是 append/scan 热路径上所有后端都要处理的类型，退役是可选的。

## 5. 退役语义

| 检查（同一事务内） | 失败 |
| --- | --- |
| 墓碑存在且 `expected_frontier == last_log_id` | 幂等成功 |
| 墓碑存在但 frontier 不同 | `FrontierMoved { actual: Some(last_log_id) }` |
| 墓碑与活会话都不存在 | `NotFound` |
| `sessions.state != idle` | `NotIdle` |
| 当前 frontier != `expected_frontier` | `FrontierMoved { actual }` |
| 存在 `parent_id = session` 的活会话 | `HasChildren` |
| 父会话存在且其 `state != idle` | `ParentNotIdle` |

通过后，在**一个事务**里：写墓碑 → 删 `log` → 删 `timers` → 删 `runnable` → 删 `sessions`。

- **只删整会话**。裁前缀会产生 `seq` 洞（违反 G3）并让恢复从错误检查点折叠；按事件保留会让信封与投影失去一致性。
- **`idle` 已覆盖恢复相关的未闭合意图**：open step / 工具 / timer / signal / 子会话等待都会把状态推离 `idle`。唯一例外是 `CompactionStarted`，它没有副作用，丢弃它是安全的，因此退役不需要扫描事件。
- **`ParentNotIdle`** 是“父会话没有未闭合 spawn 意图”的可强制执行形式：父会话 `idle` 意味着它所有工具意图都已闭合，`ChildSessionCompleted` 必已写入。调用方仍负责自底向上删除；级联删除不做。
- **幂等重试**：退役是单事务，提交后、调用方收到结果前崩溃时，用同一 `expected_frontier` 重试返回成功。frontier 不同说明会话在导出后被推进过，必须先重新导出。
- 退役由单写者串行化；CAS 针对的是同进程内并发驱动，不是多进程。
- 删除后 `retired_sessions` 行**永不自动清理**。墓碑清理与 id 复用不在 v1 合约内。

## 6. 导出（宿主配方）

导出不新增合约。宿主按下面顺序做：

```text
F = EventLog::frontier(session)?
    None -> 会话不存在或已退役，按 NotFound 处理

for entry in EventLog::scan(LogQuery { session, after: None, limit: None }):
    if entry.log_id > F: break        # 快照之后的新 append 不在导出里
    write(entry)                      # 宿主自己的格式

SessionRetirement::retire(session, F)
```

- **一致性**：`frontier` 是当前日志末条；append 只会分配更大的 `log_id`，所以读到 `F` 的前缀在读取期间稳定。分页导出时用同一个 `F` 作为上界。
- **竞态**：如果另一个任务在导出期间用同一 `F` 退役成功，导出可能读到空流；宿主应串行化自己的保留任务。退役的 CAS 保证不会在 `F` 之后丢掉新记录。
- **格式**：宿主自定。参考实现用每会话一个文件；Asterpath 的参考格式是 JSONL（§7），但合约不要求它。
- **不做**：导入、恢复、跨库合并、按投影导出、upcast 后重写。

## 7. 参考归档格式（宿主约定）

```text
{"format":"asterpath-archive","version":1,"session":"<32 位小写十六进制>"}
{"log_id":41,"session":"<hex>","seq":1,"recorded_at":1729600000000,"schema_version":1,"dedup_key":null,"payload":{"SessionCreated":{...}}}
...
{"end":true,"entries":3,"frontier":43}
```

- 首行 header，末行 footer，中间一行一个 `LogEntry` 信封；UTF-8、无 BOM。
- `payload` 嵌入存储中的 JSON 文本，不重新序列化；`version` 是归档格式版本，与 `LOG_SCHEMA_VERSION` 独立。
- footer 的 `entries` / `frontier` 用于检测截断；这是宿主约定，不是内核校验。
- 不做压缩或加密；需要时由宿主在文件外层处理。

## 8. SQLite 实现

墓碑表加入初始 schema（`user_version = 1`），不是迁移：

```sql
CREATE TABLE retired_sessions (
    session_id   BLOB    PRIMARY KEY,
    parent_id    BLOB,
    last_log_id  INTEGER NOT NULL,
    retired_at   INTEGER NOT NULL       -- 诊断用
);
```

- 墓碑**不是派生索引**，无法由日志重建（日志已删）；它是退役的真相。G7 仍只覆盖 `sessions` / `timers` / `runnable`。
- 退役：`BEGIN IMMEDIATE` 内完成 §5 的检查与删除；检查与删除对 append 原子。
- 导出的稳定前缀由现有 `scan` 保证；实现不需要为导出新增读事务或快照机制。

## 9. 退役后的查询语义

| API | 行为 |
| --- | --- |
| `Asterpath::open_session` | `AgentError::NotFound` |
| `EventLog::scan` | 空流（未知 session 与空 session 不可区分） |
| `EventLog::frontier` | `None` |
| `Storage::claim` / `due_timers` | 没有该会话的行 |
| `UsageQuery` | `UsageError::NotFound { session }` |
| `UsageQuery::SessionSubtree` | 根退役 → `NotFound`；存活根只汇总存活后代 |
| `TraceQuery` | `TraceError::NotFound { session }` |
| `SessionCatalog::parent` | 退役 id 返回 `None`（与根会话不可区分） |
| `SessionCatalog::children` | 不返回退役子会话 |

归档文件是退役会话的真相；存储的查询面不拼接墓碑与归档。

## 10. 与其他能力的边界

- **压缩**：压缩只改变投影，不删日志；退役删日志，不关心投影。两者都保留，互不替代。`CompactionStarted` 未闭合不影响退役（§5）。
- **用量**：退役让子树总量变小，这是 #13 已接受的语义（U16）。用量查询不读归档。
- **追踪**：退役后 span 一并消失；trace 是日志的投影，没有独立存储。已导出的 trace 不受影响。
- **子会话**：不级联；父会话必须 `idle`；调用方自底向上删除。父记录缺失（父已退役）时子作为独立 session 继续。
- **插件**：插件清单不持久化，退役无需处理。
- **持久等待**：timer 与 runnable 行随会话删除；退役后不会再有该会话的唤醒。
- **配置**：保留策略不进 `ConfigSnapshot`，不随 session 持久化。

## 11. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| R1 | 先取 frontier、导出、期间又有 append | 导出最后一条 `log_id` = 捕获的 frontier；用该 frontier 退役返回 `FrontierMoved`，新记录不丢 |
| R2 | 导出、按 frontier 退役、再查询 | `open_session` = `NotFound`；`scan` 空流；`frontier` = `None`；用量/追踪 = `NotFound` |
| R3 | 同一 frontier 重试退役 | 返回成功；墓碑只有一行 |
| R4 | 非 idle（step_open / waiting）会话退役 | `NotIdle`；日志与派生行不变 |
| R5 | 有活子会话时退役父会话 | `HasChildren`；先删子再删父成功 |
| R6 | 父会话非 idle 时退役 idle 子会话 | `ParentNotIdle` |
| R7 | 退役后重启进程再重试 | 墓碑仍在；同一 frontier 返回成功，不重新删除 |
| R8 | 退役子会话后查询父会话子树用量 | 不报错；总量只含存活会话（U16） |
| R9 | 退役后 timer / runnable 残留 | 无该会话的行；不再产生唤醒 |

## 12. 边界与非目标

- 主机/扩展的自动保留策略（按时间/大小/永不）、调度器、阈值、告警：不在本票。
- 日志前缀裁剪、按事件/按工具输出保留：不做；破坏 `seq` 无洞与检查点恢复。
- 级联删除整棵子树：不做；G6 无跨流原子性。
- 单会话导入/恢复、跨库合并、`log_id` 重映射：v1 不做；恢复走整库备份或未来的运维工具。
- 归档文件格式作为内核合约、归档压缩/加密/签名：不做；宿主约定。
- 墓碑清理、id 复用：v1 不做。
- `VACUUM`、WAL checkpoint、`journal_size_limit`：不进合约；文件缩小是离线运维。
- v1 workspace 新增 `asterpath-storage-admin`：不做。
- 参考 CLI 的 `session delete/cleanup` 命令：宿主功能，若需要另开票。

## 13. 下游修订

- **#5**：[storage.md](storage.md) 增加 `retired_sessions` DDL、G8 退役原子性与本文件链接。
- **#13**：[usage.md](usage.md) 的 `UsageError` 增加 `NotFound`，子树语义指向 §9。
- **#12**：[tracing.md](tracing.md) 的 `TraceError` 增加 `NotFound`。
- **#15**：[packaging.md](packaging.md) 增加门面 `session_retirement()`；`asterpath-storage-admin` 仍预留。
- **#19**：崩溃注入应覆盖退役事务的提交前/提交后；是否新增注入点由 #19 决定，本票只要求原子性。
