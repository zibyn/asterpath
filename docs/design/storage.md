# 存储抽象与 SQLite 首实现

> 来源：[#5](https://github.com/zibyn/asterpath/issues/5)、[#144](https://github.com/zibyn/asterpath/issues/144)。
>
> 前置：[事件日志与检查点恢复](event-log-recovery.md)。
>
> 范围：多个进程共用一个存储、每棵会话树一个属主、崩溃安全；存储只有一个 `SessionStore` trait（日志 + 会话目录 + 可选退役），唤醒由 facade 折叠会话事实发现。
>
> 相关 ADR：[0023 存储契约](../adr/0023-storage-holds-log-and-catalog-wake-by-fold.md)、[0036 会话树属主](../adr/0036-per-session-tree-ownership.md)、[0002 日志模型](../adr/0002-log-topology-and-commit-semantics.md)。

## 1. 决策摘要

- **一个 SQLite 文件装全部 session。** 后端只实现一个 `SessionStore` trait（日志、会话目录、可选退役），由 `asterpath-storage-sqlite` 一个模块实现。
- **信封强类型列 + `payload` JSON。** `SessionEvent` 不拆成 typed columns，事件形状变化不改 DDL（ADR-0002）。
- **`journal_mode=WAL` + `synchronous=FULL` 为默认。** `NORMAL` 只作为显式配置项，供测试/开发用。
- **写者独占的单位是会话树**（ADR-0036）。多个进程可以打开同一个库；驱动一棵树要先 `claim(root)`，被别的进程持有时得到 `OwnedElsewhere`。存活靠框架的**进程租约**：每个存储句柄（一个进程）在库里有一行租约，引擎后台定期续约；树的属主行记「属主进程 + 纪元」，属主进程租约过期后任何进程可以「纪元 + 1」接管。全部是普通读写与条件写，不用文件锁或任何后端原生锁。每次认领递增属主纪元，属主写入携带纪元，过期纪元在写事务内被拒绝。
- **一条日志记录 = 一个 savepoint；同时排队的记录 group commit 进一个事务**（#97）。不设等待窗口；`seq` 在事务内用 `MAX(seq)+1` 分配，同批内的查重与 `seq` 看得到同批前面的记录，结果与逐条提交一致。
- **只有会话目录 `sessions` 在 `append` 内同一事务维护**：它是日志的派生索引，不是真相源。唤醒与空闲不物化，由会话事实（protocol 的 `SessionFacts`）按需折叠。
- **`idle` 是未闭合意图的谓词，不是缓存列。** `sessions` 没有 `state`；存在任一非 compaction 的 open intent 即非 `idle`。退役、唤醒与恢复共用同一个折叠（[event-log-recovery.md](event-log-recovery.md) §6）。
- **没有 torn tail 修复。** SQLite 提交原子；解码失败一律硬错误。
- **v1 不做投影快照。** 恢复从检查点折叠日志。

## 2. 职责拆分

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| `SessionStore`（protocol） | append、按游标 scan、frontier；会话目录 `sessions` / `parent`；`claim` / `owner` / `renew`；可选 `retire` | 调度、唤醒判定、何时续约 |
| facade 的属主登记（`owner.rs`） | 后台按间隔调 `renew`；失去属主时停驱动、拒写、释放、通知宿主 | 租约的存储形状 |
| facade `Asterpath::wakeable()` | 唤醒发现：对 `sessions()` 逐个折叠会话事实 | 何时、按什么顺序驱动（宿主策略） |
| `asterpath-storage-sqlite`（模块） | 实现 `SessionStore`；`sessions` 目录行；进程租约、属主行与纪元；WAL | 事件解码语义（归 protocol） |

## 3. 合约签名

```rust
// protocol

pub type Timestamp = i64; // Unix epoch 毫秒；唯一允许的时间表示

pub struct NewEntry {
    pub session: SessionId,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub owner: Option<OwnerToken>,  // 属主写入携带；外部写入为 None
    pub payload: serde_json::Value, // protocol 已序列化的 SessionEvent
}

/// 一次认领的凭证：根会话与认领时的纪元（ADR-0036）。
pub struct OwnerToken { pub root: SessionId, pub epoch: u64 }

/// 经一个存储句柄持有的认领；drop 释放。同一句柄对同一根的认领共享纪元，
/// 最后一个 drop 时整棵树才释放（属主行已被别的进程接管则什么都不做）。
pub struct Claim { /* token() -> OwnerToken */ }

/// 进程租约的默认 TTL；后端可配置（SQLite：`OpenOptions::lease_ttl`）。
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

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

pub trait SessionStore: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId)
        -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;

    /// 有 `SessionCreated` 且未退役的会话的目录项，按父会话与元数据
    /// 键值等值过滤（ADR-0039），按 `(created_at, session)` 降序，
    /// 游标分页（ADR-0023）；分页只数匹配的项。
    fn sessions(&self, query: SessionQuery)
        -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>>;
    /// `SessionCreated` 记录的父会话；根或未知会话为 `None`。
    fn parent(&self, session: SessionId)
        -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;

    /// 认领以 `root` 为根的会话树，同时续约本句柄的租约。本句柄已持有：同一
    /// 纪元、共享；别的句柄持有且其租约未过期：`OwnedElsewhere`；否则纪元加一。
    fn claim(&self, root: SessionId) -> BoxFuture<'_, Result<Claim, StorageError>>;
    /// 任一句柄或进程当前持有的认领；无人持有或持有者租约已过期为 `None`。
    fn owner(&self, root: SessionId)
        -> BoxFuture<'_, Result<Option<OwnerToken>, StorageError>>;
    /// 把本句柄的租约续到「现在 + TTL」；返回自上次以来被别的进程接管的
    /// 本句柄认领，每个只报一次，之后的认领不再与之共享。
    fn renew(&self) -> BoxFuture<'_, Result<Vec<OwnerToken>, StorageError>>;

    /// 整会话退役（retention.md）；默认 `RetirementError::Unsupported`。
    fn retire(&self, session: SessionId, expected_frontier: LogId)
        -> BoxFuture<'_, Result<(), RetirementError>> { /* Unsupported */ }
}

pub struct CatalogEntry {
    pub session: SessionId,
    pub parent: Option<SessionId>,
    pub created_at: Timestamp,                  // SessionCreated 的 recorded_at
    pub last_activity: Timestamp,               // 最后一条记录的 recorded_at；仅展示
    pub metadata: BTreeMap<String, String>,     // SessionCreated 的宿主元数据（ADR-0032）
}

#[non_exhaustive]
pub enum StorageError {
    OwnedElsewhere { root: SessionId },   // 树被别处持有，或属主写入的纪元已过期
    SchemaTooNew { found: i64, supported: i64 },
    InvalidEntry(String),
    Corrupt { session: Option<SessionId>, detail: String },
    Retired { session: SessionId },
    Conflict(String),  // 写冲突、BUSY、序列化失败：没有生效，事务外整体重试可能成功
    Backend(String),
}

impl StorageError {
    pub fn is_retryable(&self) -> bool; // 只有 Conflict
}
```

这套信封是 protocol 的唯一形状（[ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)）：`append` 收 `NewEntry`，因为 `seq`、`log_id`、`recorded_at` 由存储分配（#3 §4）；#2 的 `append(LogEntry)` 与 `{ kind, schema_version, payload }` 已删除。类型化解码与 upcast 不在存储，由 protocol 的 `decode_entry(&LogEntry)` 负责（[ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)）；`StorageError` 不带 `source`，符合「库契约不暴露 `Box<dyn Error>`」。

存储不提供 `enqueue` 或 ack，也不回答「哪些会话要唤醒」：唤醒是 facade 的只读折叠（§7）。一个存储句柄代表一个进程，句柄 drop（正常退出）即删除自己的租约、清掉自己名下的属主行。跨进程的单驱动由认领与纪元保证；同一进程内同一 session 的并发驱动由 `drive()` 单飞拒绝；重复驱动不产生重复记录（幂等 append）。子会话与审批的等待唤醒属于引擎的持久等待（module-contracts.md §3.4），存储不参与。

## 4. SQLite schema

```sql
PRAGMA user_version = 1;

CREATE TABLE sessions (
    session_id  BLOB    PRIMARY KEY,           -- SessionId 的 16 字节
    parent_id   BLOB,                          -- 子会话指向父会话
    created_at  INTEGER NOT NULL,               -- 诊断用，不参与任何逻辑
    metadata    TEXT    NOT NULL                -- 宿主元数据，JSON 对象（ADR-0032）
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

-- 退役会话的身份记录；退役接缝使用，见 retention.md
CREATE TABLE retired_sessions (
    session_id   BLOB    PRIMARY KEY,
    parent_id    BLOB,
    last_log_id  INTEGER NOT NULL,
    retired_at   INTEGER NOT NULL               -- 仅诊断
);

-- 每棵会话树的属主行：最近一次认领的纪元与属主进程（ADR-0036）
CREATE TABLE tree_owners (
    root     BLOB    PRIMARY KEY,
    epoch    INTEGER NOT NULL,                  -- 只增不减；释放不清零
    process  BLOB                               -- 属主进程 id；NULL = 已释放
);

-- 每个进程（存储句柄）一行租约
CREATE TABLE process_leases (
    process     BLOB    PRIMARY KEY,            -- 打开时 randomblob(16)
    expires_at  INTEGER NOT NULL                -- 续约者的墙钟 + TTL，Unix 毫秒
);
```

- `retired_sessions` **不是派生索引**：日志删除后无法重建，它是管理的真相来源。G7 只覆盖 `sessions`。`tree_owners` 与 `process_leases` 同样不是派生索引。
- 发布前只有一个 DDL 版本（1），不写迁移；旧布局的开发库删掉重建。所有表都是 `CREATE TABLE IF NOT EXISTS`，每次打开都在 `BEGIN IMMEDIATE` 里执行一遍，所以早于 `tree_owners` / `process_leases` 的版本 1 库打开时补上这两张表（#144 首版的 `owners` 表不再使用，开发库里留着无害）；并发首次打开只有一个建表。
- **日志 schema 版本**：每条记录带写入时的 `schema_version`（`LOG_SCHEMA_VERSION`，当前固定为 1）；读到高于自身支持的版本即拒绝打开，不降级。发布前不承担历史数据兼容：不写 upcaster、不维护历史 fixture、持久 payload 不用 `#[serde(default)]`，形状变化直接改，开发库删掉重建。首个发布版的兼容策略（upcast 位置、逐版本黄金 fixture、兼容性门禁）届时以新 ADR 决定。

- `UNIQUE(session_id, seq)` 同时承担 scan 顺序索引，不另建索引。
- `event_kind` 由 `append` 从 payload 顶层键派生，不进 `NewEntry` / `LogEntry`，只参与幂等查重；扫描仍按 `(session, seq)`。判别式是持久值：变体改名时映射保持旧值（[ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)）。
- `log_id` 用 `AUTOINCREMENT`：id 不复用，保持单调。
- `recorded_at` 由 `append` 分配，不接受调用方传入；只供 trace/审计读取，不参与任何逻辑。
- 无外键：派生索引，外键只会增加写放大。
- 物理 DDL 版本记在 `PRAGMA user_version`：0 → 建表；打开时发现更高版本 → `SchemaTooNew`。逐级前向迁移从首个发布版起才写。与 `LOG_SCHEMA_VERSION` 完全独立（ADR-0002）。

## 5. 写路径与事务边界

### append

```
BEGIN IMMEDIATE
  event = decode(payload)                        -- 失败 -> InvalidEntry；顶层键给出 event_kind
  kind = event_kind(event)
  if session 已退役: return Retired
  if owner 非空 且 tree_owners[owner.root] != (owner.epoch, 本进程):
      return OwnedElsewhere                      -- 纪元围栏，在写事务内；已释放的纪元同样被拒
  if dedup_key 非空:
      SELECT log_id FROM log WHERE session_id=? AND event_kind=? AND dedup_key=?
      if 命中: ROLLBACK; return 原 log_id        -- 不写
  seq = COALESCE((SELECT MAX(seq) FROM log WHERE session_id=?), 0) + 1
  recorded_at = now_ms()                        -- 存储层本地时钟
  INSERT INTO log (..., event_kind, recorded_at, ...)
  if event 是 SessionCreated:
      INSERT OR REPLACE INTO sessions (session_id, parent_id, created_at, metadata)   -- 同一事务
COMMIT                                           -- FULL 下返回时已 fsync
```

- 解码在查重之前，同时给出 `event_kind` 与目录行；未知事件变体在追加时就报 `InvalidEntry`（与 ADR-0002 的硬错误一致）。
- 唯一的派生动作：`SessionCreated` 插入 `sessions` 行（`parent_id` 取 `SessionCreated.parent`，`metadata` 取 `SessionCreated.metadata`）。其余事件只写日志。
- **宿主元数据上限**（ADR-0032）：至多 64 个键；键至多 128 字节，值至多 4096 字节（UTF-8）。超限由 `create_session` 报 `AsterError::Config`，不落日志；存储适配器原样保存，不再校验。子会话的元数据为空。
- **元数据过滤**（ADR-0039）：`SessionQuery.metadata` 的每个键值都要与目录项相等（AND，等值，区分大小写，不做前缀或范围）。SQLite 用 `json_each` 在查询里比对，没有索引，按目录大小线性扫描；需要时再按常用键加表达式索引。
- 未闭合意图、空闲与待处理输入不落表：需要时由 `SessionFacts::fold` 从该会话日志算出（`is_idle()`、`is_wakeable()`）。`ApprovalDecided` 不关闭工具意图；steering 的 `InputRecorded` 在下一个 `StepStarted` 前保持待处理，因此 `submit_input` 后崩溃仍可唤醒（S18）。
- **`idle` = 没有非 compaction 的未闭合意图。** compaction 意图只供恢复重做（#24/#27），不影响退役与唤醒发现。

### 事务边界清单

| 写入 | 事务 |
| --- | --- |
| 任意一条日志记录 + 其 `sessions` 行 | 一个 savepoint，与同时排队的写入 group commit 进一个事务；`COMMIT` 后才返回 |
| 子会话终态（子流）与 `ChildSessionCompleted`（父流） | 两个事务，不跨流；靠幂等键修复 |
| 取消 | 就是一条终态记录（`TurnCommitted{Cancelled}` 或 `ToolResult`），无额外事务 |
| `ToolCallRequested` → 调用工具 | append 提交返回后才调用 |

## 6. 持久性、并发与连接模型

- `PRAGMA journal_mode=WAL`；`synchronous=FULL`（默认）或 `NORMAL`（显式配置）。
- **FULL 的承诺**：append 返回后，条目能扛进程崩溃**与掉电**。**NORMAL 的承诺**：能扛进程崩溃，掉电可能丢最近提交（Pi Durable 的取舍），只用于测试/开发。
- **进程租约与属主行**（ADR-0036），不用文件锁：
  - 每个存储句柄打开时取一个随机进程 id。认领、续约、释放与退出清理都在 writer 线程上单独执行（不进批），各自一个 `BEGIN IMMEDIATE` 事务。
  - 认领：续约本进程租约；读属主行与其进程的租约；属主是本进程 → 共享纪元；是别的进程且 `now < expires_at` → `OwnedElsewhere`；否则 `epoch + 1`、`process = 本进程`。同一句柄对同一根的多次认领在进程内引用计数，最后一个 `Claim` drop 时执行 `UPDATE ... SET process = NULL WHERE root=? AND epoch=? AND process=本进程`，提交后 drop 才返回，别的进程可以立即认领。调用方放弃等待时 writer 撤销它刚拿到的树。
  - 续约（`renew`）：`expires_at = now + lease_ttl`，顺带删除已过期的租约行（被 kill 的进程留下的），并逐个检查本句柄持有的认领：属主行不再是「本纪元 + 本进程」的报告为已失去。
  - 句柄 drop（正常退出）：writer 排空队列后删除本进程租约、把本进程名下的属主行置为已释放。
  - 到期用读者的墙钟判断（存储的 `Clock`）；部署要求时钟同步，偏差只让接管提前或推迟，正确性只靠纪元。
  - `owner(root)`：读属主行与租约；属主进程是本句柄或其租约未过期时返回。
- **参数**：`OpenOptions::lease_ttl`（默认 `DEFAULT_LEASE_TTL` = 30 s）是租约 TTL，也就是进程被 kill 后别人要等的时间；续约间隔是 facade 的 `AsterpathConfig::lease_renew_interval`（默认 10 s），应明显小于 TTL（建议 ≤ TTL/3），否则活着的进程也会被接管（只损失可用性，不会双写）。续约在构建 `Asterpath` 时所在的 tokio runtime 上运行；没有 runtime 就不续约。
- **失去属主**（facade）：续约返回某个认领已被接管、续约本身失败（全部认领）、或一次属主写入因纪元被拒时，引擎把该认领标为已失去：该树在本进程内的 `drive` 流立刻结束并以 `OwnedElsewhere` 收尾（丢弃 future 即停止回合、等待与子会话驱动），之后用这个凭证的写入在进程内就被拒绝，认领随即释放，并调用 `Hooks::on_ownership_lost`（每个认领一次）。已在执行的不安全工具由新属主恢复时按中断收尾。
- 多个进程可以共用一个 DB（同一台主机；WAL 的共享内存不能放在网络文件系统上）。
- `busy_timeout=5000` ms：等其他进程的写事务。`SQLITE_BUSY` / `SQLITE_LOCKED` 映射为 `StorageError::Conflict`（可重试）；busy handler 之外冒出来的冲突（例如并发首次切换 WAL）在事务外以 1 ms 间隔整体重试，上限 5000 次，只重试这一类。`BEGIN IMMEDIATE`、认领、续约、释放与退出清理都这样重试，不重放已执行的 savepoint。
- 连接模型（#97）：写连接归一个专用 writer 线程；`append` / `retire` 经无界命令队列发给它，用 oneshot 回复。线程取到一条命令后把已排队的命令取完（单批上限 64），放进一个 `BEGIN IMMEDIATE` 事务，每条命令一个 `SAVEPOINT`：单条失败只回滚该条、只给该调用方回错误；`COMMIT` 成功后才回复，`COMMIT` 失败则整批都收到错误。读（`scan` / `frontier` / `sessions` / `parent`）走一条独立只读连接，在调用方同步执行，不排在 fsync 后面；append 返回后立即读必能读到。writer 线程停止后，在途与后续写都得到 `Backend`。drop 时先关队列、writer 清理本进程的租约与属主行后退出、join 线程，再关读连接。
- **取消语义**：append 请求一旦发出，丢弃其 future 不会撤回写入——记录**可能已提交**而调用方不知道 `log_id`。状态以日志折叠为准：引擎在 select 中途放弃 drive、取消宽限期丢弃 future 的路径之后都会重读日志。
- 文件创建时权限 `0600`。
- 框架不做 WAL checkpoint、`VACUUM` 或 `journal_size_limit` 调参；日志保留/归档的 seam 见 [retention.md](retention.md)。删除释放页供复用，不保证文件缩小。

## 7. 唤醒发现

- **唤醒发现归 facade，不归存储**（ADR-0023）。`Asterpath::wakeable() -> Result<Vec<SessionId>, AsterError>` 遍历 `store.sessions()`，跳过根会话被任何进程（含本进程）持有的树（有属主行且属主进程租约未过期），再逐会话 `scan_all` + `SessionFacts::fold`，保留 `is_wakeable()` 为真者（存在非 compaction 未闭合意图，或有待处理输入），按 id 排序、每个会话一次。退役会话不在 `sessions()` 中，因此永不唤醒。
- **没有持久队列或 ack；进程租约只管属主存活，不管排队。** 宿主的恢复循环是 `wakeable → open_session → claim → drive`；认领失败（`OwnedElsewhere`）说明别的进程刚拿走这棵树；同一进程内的并发驱动由 `drive()` 单飞拒绝（`AgentError::Busy`），重复记录由幂等 append 吸收。
- 成本随会话总量线性增长（启动时全量折叠）；会话量大时在 SQLite 模块内部加索引，不改接口。
- spawn 超时不经存储：截止时间在 `ChildSessionSpawned.deadline` 上，open 的子会话意图已让父会话可唤醒，到期由 spawn 重放处理（[subagents.md](subagents.md) §9）。
- 没有 `recover_at_startup`：恢复是 per-session 的（`open_session` 折叠日志）。启动顺序：打开连接 → 设 PRAGMA → 读 `user_version`（建表）→ 宿主对 `wakeable()` 结果逐个认领并驱动。只有 `CompactionStarted` 未闭合的 session 仍视为 idle（丢弃一次摘要调用是安全的）。**不**预验证日志末条，不跑 `integrity_check`；解码失败在 scan 时暴露。

## 8. 保证清单

后端必须满足（Postgres 也照此实现）：

- **G1 前缀持久**：`append` 返回后，该条记录排在崩溃可见的前缀里；FULL 下扛掉电。
- **G2 幂等**：`(session, event_kind, dedup_key)` 只有一条记录；重复 append 返回原 `LogId`，不写目录行；同 key 不同事件类型互不抑制。
- **G3 全序**：`seq` 在 session 内严格单调、无洞，只由存储分配。
- **G4 游标扫描**：`scan(after)` 只返回 `log_id > after` 的已提交记录，按 `seq` 升序，`limit` 有界。扫描结果在读取期间构成稳定前缀：append 只会分配更大的 `log_id`，不会改变已返回的条目。
- **G5 会话目录完备**：`sessions(query)` 列出每个有 `SessionCreated` 且未退役的会话（含子会话）一次，按 `(created_at, session)` 降序；父会话过滤、元数据等值过滤（与父会话过滤、分页组合）、`parent` 与 `SessionCreated.parent` 一致；游标翻页不重不漏；`last_activity` 是最后一条日志的 `recorded_at`（ADR-0023）；`metadata` 与 `SessionCreated.metadata` 一致（ADR-0032）。唤醒发现建立在它之上（§7），不是后端契约。
- **G6 无跨流原子性**：任意两个 session 的写入不构成原子组。
- **G7 索引可重建**：`sessions` 可完全由日志重建，不是真相源。
- **G8 退役原子性（可选能力）**：`SessionStore::retire` 在同一事务里折叠会话事实判定空闲、写墓碑、删日志与 `sessions` 行；要么全部生效，要么全部不生效；退役后的会话对 `SessionStore` 的全部读取不可见。契约见 [retention.md](retention.md)。
- **G9 属主（ADR-0036）**：同一时刻一棵树至多一个持有者；同一句柄的认领共享纪元，其他句柄得到 `OwnedElsewhere`；每次新认领纪元严格增大；带过期或已释放纪元的写入被拒绝且不留记录；不带凭证的写入不受属主限制；持有者的租约按读者时钟过期后，树可被以「纪元 + 1」再次认领；续约维持持有，并报告被接管的认领；句柄 drop 立即释放它名下的全部树。

trait 不暴露：SQL、WAL、fsync、`log_id` 的 rowid 性质、文件路径、文件锁、JSON 文本编码。SQLite 特有实现（`MAX(seq)`、partial index、`AUTOINCREMENT`）留在模块内。

### 多进程下的降级语义（ADR-0036）

- **规模靠单进程**：一个进程内 writer 线程合并提交；进程之间没有合并提交，各自 fsync，多进程只用于共存（CLI 与桌面端）和滚动重启。
- **`max_live_children` 是每进程上限**，不是整个存储的上限。
- **`Hooks::on_record` 在每个进程内对每条记录恰好一次**；跨进程同一条记录可能被两个进程各展示一次（例如非属主写入审批决定后，属主下一次 append 补扫到它），实时导出的宿主按 `(session, log_id)` 去重。
- **跨进程审批有轮询延迟**：属主等待审批或子会话时至少每 500 ms 重读日志，别的进程写入的 `ApprovalDecided` 在一个轮询间隔内生效。
- **属主迁移时调度器的准入缓存按纪元重建**：一棵树在新的纪元下第一次调度子会话时，丢掉旧纪元在内存里的排队与 Running 计数，从日志重建。
- **崩溃后的接管延迟等于租约 TTL**：被 kill 的进程的树要等它的租约过期才能被别的进程认领；正常退出（句柄 drop）立即释放。续约写入量与进程数成正比，与会话数无关。

`concurrency_bench` 的 `multi_process_bench`（2026-10-25，进程租约版，本地 SSD，release；每进程 8 棵树、每棵 100 次属主 append，墙钟含进程启动）：

| synchronous | 进程数 | append/s | p50 (ms) | p99 (ms) | max (ms) |
| --- | --- | --- | --- | --- | --- |
| Full | 1 | 875 | 8.3 | 14.2 | 31.3 |
| Full | 2 | 868 | 8.2 | 16.7 | 32.3 |
| Full | 4 | 827 | 8.3 | 16.3 | 33.1 |
| Normal | 1 | 10547 | 0.3 | 4.8 | 18.1 |
| Normal | 2 | 9376 | 0.5 | 13.8 | 16.3 |
| Normal | 4 | 9474 | 0.4 | 10.5 | 19.3 |

FULL 下吞吐不随进程数增长（每个进程各自 fsync，WAL 写锁串行）；NORMAL 下多进程也不增吞吐，长尾来自跨进程的写锁等待。同机多跑几次，FULL 的个别轮次会因磁盘抖动整体变慢一个量级，表里是稳定的一轮。

## 9. 故障注入与测试

`asterpath-testing` 提供五个注入点（装饰器实现，不进 protocol）；API 形状、动作与 CI 编排见 [testing.md](testing.md)。前四个点是 trait 装饰器，`clock:advance` 由 `ManualClock::advance` 提供。

| 注入点 | 模拟 |
| --- | --- |
| `append:before_commit` | 意图尚未持久就崩溃 |
| `append:after_commit` | 已持久、调用方未收到结果 |
| `tool:before_effect` | 副作用前崩溃（配合 `replay` 语义） |
| `tool:after_effect` | 副作用已发生、终态未写 |
| `clock:advance` | 可控时钟推进（spawn 截止时间、退避） |

| 场景 | 归属 |
| --- | --- |
| S4 坏 payload → `Corrupt`；S5 幂等；S8 `seq` 单调、`recorded_at` 不参与逻辑；会话目录与退役 | 共享 `session_store_contract`（sqlite 与 memory） |
| G9 属主：两个句柄并行持有不同的树、同一棵树被拒、释放后纪元增大、过期与已释放纪元被拒；续约维持持有、租约过期后被接管且续约报告失去、正常退出立即释放、被 kill 的进程在租约过期后被接管（`ManualClock`，不真等） | 共享 `session_store_contract`（`open_processes`） |
| 另一个进程认领别的树；被 SIGKILL 的属主在租约过期前仍持有、过期后被接管且其纪元被围栏 | SQLite 多进程测试 |
| `SQLITE_BUSY` 归为 `Conflict`，事务外只重试这一类 | SQLite crate 单测 |
| facade：两进程并行驱动不同的树；被持有的树只读；正常退出后立即接管；被 kill 的属主在租约过期后被接管并恢复；被接管或续约失败的属主停止驱动、不再写入并调用 `on_ownership_lost`；非属主写入的审批决定被属主轮询到 | facade `ownership` 测试 |
| 新库只有当前表；更高 `user_version` → `SchemaTooNew` | SQLite crate 单测 |
| S13 活会话不判 idle；S18 steering 输入后崩溃仍可唤醒；仅 compaction 未闭合时不可唤醒 | facade `wakeable` 双后端测试；会话事实单测 |
| S1 `Safe` 工具中 kill、S2 `Unsafe` 工具中 kill、S3 模型流中 kill、S6 父等子时 kill | #6 / #17 的进程级验收；本轮只固定注入点契约 |
| S9 spawn 超时在停机期间到期 | scheduler 测试（#65） |
| S7 `CompactionApplied` 投影 | #9 / #6 |

## 10. 留给下游

- **#6**：turn/step 的终止条件、取消宽限期，决定哪些意图在何时闭合；`idle` 由未闭合意图推导。
- **#8**：已定。父流何时 `ChildSessionCompleted`、启动时子会话缺失（R2 物化）与孤儿收尾（R4）见 [subagents.md](subagents.md) §9。
- **#9**：压缩触发条件；`CompactionApplied` 只影响投影，不删日志。
- **#17**：审批决定与子会话等待；唤醒发现见 §7。
- **#18**：已定。只保留 `SessionStore::retire` 可选能力（整会话退役、墓碑）；导出由宿主用 `frontier` + `scan` 完成，导入不在 v1。见 [retention.md](retention.md)；本文件只承载墓碑 DDL 与 G8。
- **#19**：五个注入点的工具链与 CI 场景。
