# 崩溃注入与确定性测试工具链

> 来源：[#19 崩溃注入与确定性测试工具链](https://github.com/zibyn/asterpath/issues/19)，2026-10-22 已定。
> 相关 ADR：[0017 故障注入与确定性测试工具链](../adr/0017-fault-injection-and-test-tooling.md)。
> 前置：[#5 存储](storage.md)、[#3 事件日志与恢复](event-log-recovery.md)、[#15 打包与验收](../spec/acceptance.md)。
> 范围：**测试工具链与 CI 编排**。不改变持久化、恢复或事件语义；崩溃注入点只补真实 `SIGKILL` 覆盖不到的窗口。

## 1. 决策摘要

- 六个注入点中，**五个是 `asterpath-testing` 里的 trait 装饰器**，一个是**手动时钟**；core / protocol 不新增注入钩子。
- 装饰器命中后的动作只有两档：`Halt`（写 marker 后永久挂起，由父进程 `SIGKILL`）与 `Panic`（进程内测试）。
- **不用 panic / `abort` / `exit` / 注入异常冒充崩溃**；进程级验收一律真 `SIGKILL`（[acceptance.md](../spec/acceptance.md) §1）。
- core 新增最小 `Clock { now() }` + `SystemClock`；`Storage` 保持显式 `now: Timestamp`（#5 不变）；`ManualClock` 在 `asterpath-testing`。
- 后端中立契约套件：**G2–G7 在 in-memory 与 SQLite 上都跑**；G1、G8、`Corrupt`、WAL/`flock`、迁移只测 SQLite。
- CI 分三层：单测+契约（5 分钟）、进程级验收（Linux，整套 5 分钟，单 marker 等待 15 秒）、feature 矩阵。
- 验收 fixture 单一来源：`asterpath-acceptance/fixtures/*.json`；#6 的 L 场景与 #17 的 R 场景复用，不另建。

## 2. 注入点与实现

六个规范名称（配置、marker、日志引用均用这些字符串）：

| 注入点 | 实现 | 包住的调用 |
| --- | --- | --- |
| `append:before_commit` | `FailpointEventLog` | 委托 `EventLog::append` **之前**触发 |
| `append:after_commit` | `FailpointEventLog` | 委托返回后、调用方拿到 `LogId` **之前**触发 |
| `tool:before_effect` | `FailpointTool` | 委托 `Tool::invoke` **之前**触发 |
| `tool:after_effect` | `FailpointTool` | `invoke` 返回后、agent loop 写 `ToolResult` **之前**触发 |
| `queue:before_ack` | `FailpointStorage` | 委托 `Storage::ack` **之前**触发 |
| `clock:advance` | `ManualClock::advance` | 不是装饰器；显式推进时钟而非崩溃 |

### 2.1 API 形状（`asterpath-testing`）

```rust
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

pub enum Point {
    AppendBeforeCommit,
    AppendAfterCommit,
    ToolBeforeEffect,
    ToolAfterEffect,
    QueueBeforeAck,
}

impl Point {
    pub const fn as_str(self) -> &'static str; // 上表的规范字符串
}

pub enum Action {
    Halt,   // 写 marker、flush、std::future::pending().await
    Panic,  // panic!("failpoint {point}")，仅供进程内测试
}

pub struct Failpoint {
    pub point: Point,
    pub occurrence: NonZeroUsize, // 默认 1：第 n 次到达该点时触发
    pub action: Action,
}

impl Failpoint {
    pub fn halt(point: Point) -> Self;
    pub fn panic(point: Point) -> Self;
    pub fn with_occurrence(self, n: NonZeroUsize) -> Self;
}

pub struct FailpointEventLog { inner: Arc<dyn EventLog>, failpoint: Failpoint, hits: AtomicUsize }
pub struct FailpointStorage  { inner: Arc<dyn Storage>,  failpoint: Failpoint, hits: AtomicUsize }
pub struct FailpointTool     { inner: Arc<dyn Tool>,     failpoint: Failpoint, hits: AtomicUsize }

impl FailpointEventLog {
    pub fn new(inner: Arc<dyn EventLog>, failpoint: Failpoint) -> Self;
}
impl FailpointStorage {
    pub fn new(inner: Arc<dyn Storage>, failpoint: Failpoint) -> Self;
}
impl FailpointTool {
    pub fn new(inner: Arc<dyn Tool>, failpoint: Failpoint) -> Self;
}
```

- 每个装饰器只接受自己域内的点（`FailpointEventLog` 只收 `append:*`，依此类推），构造时校验，跨域配置是测试代码错误。
- **一个装饰器承载一个注入点**；多点场景嵌套组合。计数按装饰器实例、按调用次数，固定脚本下确定性。
- 装饰器完整委托未命中时的调用；`descriptor` 等只读方法直接透传。
- 只在测试目标与 `testdrive` 中链接。facade、参考 CLI、任何库 crate 都不暴露 failpoint 类型或开关。

### 2.2 动作语义

- `Halt`：向 stdout 写一行 `{"event":"failpoint","point":"<name>"}` 并 flush，然后 `std::future::pending().await`，不再返回。进程保持存活，直到父测试 `SIGKILL`。这是进程级场景唯一的 failpoint 动作。
- `Panic`：断言消息为 `failpoint <name>`。用于进程内测试验证「未提交 / 未 ack / 未写终态」的中间状态；不用于证明崩溃安全。
- `testdrive` 的 `--failpoint <point>[=<n>]` 只构造 `Halt`；未知点或非法组合 = 配置错误，退出码 2。可重复传入以嵌套多个装饰器。

## 3. 时钟接缝

```rust
// core

pub type Timestamp = i64; // Unix epoch 毫秒

pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

pub struct SystemClock;

impl Clock for SystemClock { /* SystemTime -> epoch ms */ }
```

```rust
// asterpath-testing

pub struct ManualClock { /* AtomicI64 */ }

impl ManualClock {
    pub const EPOCH: Timestamp = 1_700_000_000_000; // 固定基准，测试可复现
    pub fn new(at: Timestamp) -> Self;
    pub fn set(&self, at: Timestamp);
    pub fn advance(&self, by: std::time::Duration);
}

impl Clock for ManualClock { /* 读 AtomicI64 */ }
```

- `Storage::claim` / `due_timers` / `recover_at_startup` 继续收显式 `now`（#5 / ADR-0005 不变）。`runtime`、`scheduler` 等调用方从注入的 `Arc<dyn Clock>` 取时间。
- `recorded_at` 仍由存储模块用自己的墙钟写入；它不参与恢复、投影或断言（#3 I8 / #12），因此存储不需要 `Clock`。
- `clock:advance` 注入点 = `ManualClock::advance`，用于 timer 到期、租约过期。它不产生 marker，也不模拟崩溃。

## 4. 后端中立契约套件

`asterpath-testing` 提供共享套件；套件代码只依赖 core/protocol。in-memory 后端在 `asterpath-testing` 自己的测试里实现 `BackendFactory` 并运行共享套件；SQLite 在 `asterpath-storage-sqlite/tests/` 里以 `[dev-dependencies] asterpath-testing` 运行同一套件。dev 边不计入模块间零依赖（[module-contracts.md](module-contracts.md) §7、[packaging.md](packaging.md) §2）。

```rust
pub struct BackendHandle {
    pub log: Arc<dyn EventLog>,
    pub storage: Arc<dyn Storage>,
}

pub trait BackendFactory {
    fn open(&self) -> BackendHandle;   // 新句柄，语义上重新打开同一后端
}

pub fn event_log_contract(factory: &impl BackendFactory);
pub fn storage_contract(factory: &impl BackendFactory);
```

| 保证 | in-memory | SQLite | 套件 |
| --- | --- | --- | --- |
| G1 前缀持久（含 FULL 掉电） | 不支持 | 进程崩溃 + FULL 掉电 | 仅 SQLite |
| G2 幂等 `(session, event_kind, dedup_key)` | 支持 | 支持 | 共享 |
| G3 `seq` 全序、无洞、存储分配 | 支持 | 支持 | 共享 |
| G4 游标扫描与 frontier | 支持 | 支持 | 共享 |
| G5 至少一次唤醒、合并、ack 不吃新一代 | 支持 | 支持 | 共享 |
| G6 无跨流原子性 | 支持 | 支持 | 共享 |
| G7 派生索引可重建 | 支持 | 支持 | 共享 |
| G8 退役原子性（`SessionRetirement`） | 不实现 | 支持 | 仅 SQLite |

共享套件清单（对应 [storage.md §8–§9](storage.md#8-保证清单)）：

- G2：同 `dedup_key` append 两次 → 一条记录、同一 `LogId`、派生行不重复；参数化 fixture 对六对共享 key 的事件（`ToolCallRequested`/`ToolResult`、`CompactionStarted`/`CompactionApplied`/`CompactionAborted`、`ChildSessionSpawned`/`ChildSessionCompleted`、`TimerScheduled`/`TimerFired`/`TimerCancelled`、`SignalAwaited`/`SignalReceived`、`SessionCreated`/`ContextForked`/`InputRecorded`）断言同 key 不同事件类型两条都落盘。
- G3：串行与并发 append 后 `seq` 严格递增无洞；只由存储分配。
- G4：`scan(after)` 只返回已提交、按 `seq` 升序、`limit` 有界；`frontier` 等于末条 `log_id`。
- G5：`append` 派生入队；同 session 合并并递增 `generation`；`ack` 代次不符不删除新唤醒；租约过期可重新 `claim`；启动清租约并入队。
- G6：两个 session 的 append 不构成原子组；一个流的可见性不受另一个流影响。
- G7：丢弃派生状态后，仅凭日志与显式唤醒查询重建 `sessions` / `timers` / `runnable`。
- S4（坏 payload → `Corrupt`）、S5（幂等）、S8（`seq` 单调、`recorded_at` 不影响逻辑）、S9（timer 恰一次）、claim/ack 不丢唤醒也在此层。

仅 SQLite（`asterpath-storage-sqlite` 自己的测试，不放进共享套件）：

- G1：`append` 返回后 `SIGKILL`，重启能读到该条；FULL 下的掉电承诺以 `synchronous=FULL` 配置测试，不在 CI 模拟真实掉电。
- `Corrupt` 硬错误、WAL 模式、`flock` → `WriterBusy`、`busy_timeout=0`、`0600` 权限、`user_version` 迁移。
- G8 与墓碑幂等（[retention.md](retention.md)）。

in-memory 后端在文档与代码中显式标注 **不满足 G1**；它不实现 `SessionRetirement`，facade 的 `session_retirement()` 对它返回 `None`。

## 5. 进程级场景编排

`testdrive` 已约定（[acceptance.md](../spec/acceptance.md) §2）：父测试用 `assert_cmd` 启动子进程，读 stdout 的 JSON 行，然后 `libc::kill(pid, SIGKILL)`。

| 场景 | marker | 崩溃边界 |
| --- | --- | --- |
| A1 / S1 | `{"event":"effect_applied"}` | `Safe` 工具副作用已发生、结果未写 |
| A2 / S2 | `{"event":"effect_applied"}` | `Unsafe` 工具副作用已发生、结果未写 |
| A3 / S3 | `{"event":"stream_open"}` | 模型流中途 |
| (c) / S6 | `{"event":"parent_waiting"}` | 父会话已写子 spawn、处于持久等待 |
| 精确边界 | `{"event":"failpoint","point":"<name>"}` | `append:before_commit` / `append:after_commit` / `queue:before_ack` |

- 有自然 marker 的场景**不用** failpoint；failpoint 只补没有自然窗口的边界。
- 父测试等待 marker 上限 **15 秒**，超时即失败并杀掉子进程；不做 `SIGTERM` 升级，直接 `SIGKILL`。
- 恢复进程用同一个数据库路径重启；断言只读 SQLite 与外部副作用，不读私有内存。
- 额外窗口（如 S10：`AssistantMessage` 之后、`StepCommitted` 之前）用 `append:after_commit` + `occurrence` 定位，属于存储/loop 的定向测试，不进 v1 验收套件。

## 6. 测试分层与 CI

| 层 | 命令 | 平台 | 预算 | 内容 |
| --- | --- | --- | --- | --- |
| 单测 + 契约 | `cargo test --workspace` | Linux（契约测试本身平台中立） | 5 分钟 | crate 单测、G2–G7 双后端、SQLite 专项 |
| 进程级验收 | `cargo test -p asterpath-acceptance --test acceptance` | 仅 Linux（`cfg(unix)`） | 整套 5 分钟；单 marker 15 秒 | A1–A3、(b)、(c)、S1/S2/S3/S6 |
| feature 矩阵 | `cargo check -p asterpath --no-default-features` 及 `anthropic` / `openai` / `sqlite` 组合 | Linux | 5 分钟 | 编译面 |

- CI 顺序保持 [packaging.md §5](packaging.md#5-交付清单与里程碑)：`check-deps` → `fmt` → `clippy` → 单测+契约 → 进程级验收 → `doc`。
- 超时由 CI 作业超时或 `timeout(1)` 包裹命令实现；不引入额外测试运行器。
- **不加 macOS / Windows CI**；v1 只为 Linux 提供进程级验收。框架代码不因此 `cfg` 掉非 Linux 逻辑。
- 进程级与 failpoint 测试**不得** `#[ignore]`；失败信息包含 session id、日志尾部与断言（acceptance.md §7）。
- 注入点测试与普通测试同层运行：`Panic` 动作在单测/契约层，`Halt` 动作只在 `testdrive`。

## 7. Fixture 归属

| 内容 | 位置 | 说明 |
| --- | --- | --- |
| 验收脚本（provider 响应、工具调用、场景参数） | `asterpath-acceptance/fixtures/*.json` | A1–A3、(b)、(c) 的单一来源 |
| `ScriptedProvider` 回放 | `asterpath-testing` | 只读 fixture，不依赖具体模块 |
| 契约测试数据 | `asterpath-testing` 内的 Rust builder | 不进 JSON |
| 各 crate 单测数据 | 各 crate `#[cfg(test)]` | 就近构造，不共享 |

- **#6 的 L 场景与 #17 的 R 场景复用** `asterpath-acceptance` 的同一 fixture 与 `testdrive` 场景，不在 `agent` / `scheduler` crate 内另写一套。
- 会组装真实模块的 fixture 一律放 `asterpath-acceptance`（`asterpath-testing` 不得依赖 facade 或模块）。
- fixture 是测试输入，不是规范；断言以 [acceptance.md](../spec/acceptance.md) 为准。

## 8. 留给下游

- **#20 / #21**：#20 已定：MCP adapter 的断连场景（stdio 进程死亡、HTTP 流中断、调用中超时）按 [mcp.md](mcp.md) §11 与本文的 `Halt` + marker 模式扩展。#21 已定：沙箱不新增注入点；`confine` 在 `Tool::invoke` 内，由 `tool:before_effect` / `tool:after_effect` 覆盖，场景见 [sandbox.md](sandbox.md) §12。
- **实现方**：failpoint 类型只存在于 `asterpath-testing`；若某个新崩溃窗口需要注入，先扩展本文的 `Point` 与装饰器，不在 core/protocol 加 hook。
