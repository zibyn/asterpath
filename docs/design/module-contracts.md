# 适配器契约、钩子与插件宿主

> 票 [#2](https://github.com/zibyn/asterpath/issues/2) 的决策记录，2026-10-22 已定。
> 相关 ADR：[0026 引擎是一个 crate](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)（取代 [0001](../adr/0001-dependency-direction.md)）、[0025 类型化钩子](../adr/0025-extension-points-are-typed-hooks.md)。
> 本文件是 v1 spec 的"模块契约"章节草稿；最终 crate 打包由 #15 收敛。
>
> **2026-10-22 范围调整**：不做通用工作流引擎（[ADR-0003](../adr/0003-single-session-stream-topology.md)）。模块图中的 `asterpath-workflow` 已移除，编排相关表述以本文件为准已更新；#10 出界，调度 API 原型见 #17。
> **2026-10-22 #32 修订 / 2026-10-23 M5 范围**：`asterpath-plugin-host` 与 MCP adapter 归 M5（post-v1，进行中），不进 v1 workspace；模块契约本身不变。
> **2026-10-23 #78 修订**（[ADR-0025](../adr/0025-extension-points-are-typed-hooks.md)）：模块宿主、服务注册表、类型化事件总线与 `Module` trait 已删除。扩展点是 protocol 的 `Hooks`（§3.1）；facade 直接构造内部组件。下文 §3.1–§3.3、§5 已按此改写。
> **2026-10-23 #79 修订**（[ADR-0026](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)）：引擎合并为 `asterpath` 一个 crate，`asterpath-core` 并入 `asterpath-protocol`。公开 trait 只剩适配器与钩子（§3、§4）；`AgentRunner`、`DurableExecutor`、`SubagentSpawner`、`ToolRegistry`、`UsageQuery`、`TraceQuery`、`TraceTap` 改为引擎内部具体类型。下文 §1–§9 已按此改写。

## 1. 决策摘要

- 公开 trait 只有适配器与钩子（ADR-0026）：`Provider`、`Tool`、存储后端（`SessionStore`：日志 + 会话目录，可选退役）、`Compactor`、`ApprovalPolicy`、`SandboxProvider`、`Clock`、`Hooks`。它们与值类型、日志信封一起在 `asterpath-protocol`。
- 引擎（agent 循环、scheduler、持久等待、工具表、trace / usage 视图、默认压缩）是 `asterpath` 的内部模块，不可替换；替换能力只经适配器，观察 / 拦截只经 `Hooks`。
- 消息只出现在跨进程与跨恢复边界；进程内是直接调用。
- 插件是外部传输，不是特权层。外部能力带 `Provenance::External`，经进程内 adapter 以 `Tool` 暴露。
- 生命周期是显式的：facade 直接构造引擎；v1 不做动态发现与热替换。
- 异步运行时是 tokio，不做运行时抽象；配置装载不在 protocol，由组装根以显式参数提供。
- 依赖规则按角色（protocol / adapter / engine / app / tool），CI 用 `cargo metadata` 白名单校验（§7）。

## 2. crate 图

```text
app
  asterpath-cli, asterpath-acceptance
        │
        ▼
engine (one crate; facade + internal modules)
  asterpath   agent · scheduler · waits · tools · views::{trace, usage}
              · compaction · commit (single append entry)
        │  re-exports / feature-gates
        ▼
adapters (depend on protocol and listed peers only)
  asterpath-provider-anthropic, asterpath-provider-openai   Provider
  asterpath-storage-sqlite                                  SessionStore
  asterpath-http                                            shared SSE / HTTP plumbing
  asterpath-plugin-host (+ asterpath-sandbox, -sandbox-linux)  external tools (M5; #80 → asterpath-mcp)
  asterpath-skills                                          load_skill tool (M6)
  asterpath-testing                                         in-memory backend, scripted provider, contract suites
        │
        ▼
protocol (no workspace deps, no features)
  asterpath-protocol   ids, log envelope, storage contracts, Clock, errors,
                       canonical model, adapter traits, Hooks, session events
```

规则：

- 箭头只能向下。
- 适配器只依赖 protocol 与白名单列出的同类；不依赖引擎。
- 引擎只依赖 protocol 与它再导出 / 按 feature 装配的适配器。
- 最终成员与角色见 [packaging.md](packaging.md) §2。

## 3. 钩子与存储契约（`asterpath-protocol`）

### 3.1 钩子（`Hooks`，protocol）

模块宿主、服务注册表与类型化事件总线已删除（[ADR-0025](../adr/0025-extension-points-are-typed-hooks.md)）。嵌入者唯一的进程内扩展点是：

```rust
pub trait Hooks: Send + Sync {
    fn before_step<'a>(&'a self, ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>>;
    fn before_tool<'a>(&'a self, ctx: &'a ToolCallCtx, args: &'a mut ToolArgs)
        -> BoxFuture<'a, Result<ToolGate, HookError>>;
    fn after_tool<'a>(&'a self, ctx: &'a ToolCallCtx, result: &'a mut ToolResultView)
        -> BoxFuture<'a, Result<(), HookError>>;
    fn on_record(&self, record: &CommittedRecord);   // 同步、不可失败
}
```

- 每个方法都有放行 / 不改的默认实现；新增钩子点 = 给 trait 加带默认实现的方法。
- 注册：`AsterpathBuilder::hooks(Arc<dyn Hooks>)`（可多次）或 `AsterpathConfig.hooks`。
- 组合：按注册顺序调用；首个 `Gate::EndTurn` / `ToolGate::Deny` 短路，参数与结果改写逐个传递；没有环绕式 `next`。
- 拦截式钩子（`before_*` / `after_tool`）的错误或 panic 按该钩子的错误处理：`before_step` → 回合 `Failed`；`before_tool` / `after_tool` → 该调用 `Failed`（`Internal`）。
- `on_record` 在每条提交记录（所有会话，含子会话）提交后按会话内顺序调用一次；panic 被捕获、`tracing::warn!` 后继续，不影响循环。
- 上下文只带数据：`StepCtx { session, turn, step_index }`、`ToolCallCtx { session, turn, step, tool_call_id, provider_call_id, tool, provenance, replay }`；两者的异步 `usage()` 返回本会话自身与子树的用量（`UsageReport`）。没有服务定位器。
- 调用点：`before_step` 在每个新 step 前（恢复的未完成 step 不再调用）；`before_tool` 在写意图前，已记录意图恢复时不调用（#28）；`after_tool` 在截断后、写结果前，框架补写的 `Interrupted` / 拒绝不经过它。

### 3.2 单一 append 入口

agent、waits、scheduler 与 facade 都经引擎内同一个 append 入口（`commit::CommitLog`）写日志：append 成功 → 依次 `on_record`。没有注册钩子时不做任何观察工作。没有 trace 推送：实时导出由宿主在 `on_record` 上自行增量折叠（ADR-0026 重审候选 4）。

### 3.3 （已删除）事件总线

原 `emit` / `waterfall` / `Effect` / `ServiceKey` 机制见 git 历史；ADR-0025 取代。

### 3.4 事件日志、存储与时间

```rust
pub type Timestamp = i64;        // Unix epoch 毫秒；唯一允许的时间表示
pub struct DedupKey(String);
pub struct SignalName(String);

// 唯一的存储适配器（ADR-0023）：日志 + 会话目录 + 可选退役。
pub trait SessionStore: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;
    // 会话目录：用量子树汇总、spawn 深度、唤醒发现与归档使用（#13/#18/#73）。
    fn sessions(&self) -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>>;
    fn parent(&self, session: SessionId)
        -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;
    fn children(&self, session: SessionId)
        -> BoxStream<'_, Result<SessionId, StorageError>>;
    // 默认 Err(RetirementError::Unsupported)；见 retention.md。
    fn retire(&self, session: SessionId, expected_frontier: LogId)
        -> BoxFuture<'_, Result<(), RetirementError>>;
}

// 一个会话的全部已提交记录（按 seq）：引擎与宿主共用的整段扫描（#77）。
pub async fn scan_all(log: &dyn SessionStore, session: SessionId)
    -> Result<Vec<LogEntry>, StorageError>;

pub type SignalPayload = serde_json::Value;

pub struct SignalDelivery {
    pub session: SessionId,
    pub signal: SignalId,
    pub name: SignalName,
    pub payload: SignalPayload,
}

pub struct AwaitSignal {
    pub session: SessionId,
    pub signal: SignalId,
    pub name: SignalName,            // tool.approval.<tool_call_id>
}

pub enum WakeCause {
    Signal(SignalId),
    ChildCompleted(SessionId),
}

pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

pub struct SystemClock;

// 审批端点的存活声明：机制，不是决策者（#26）。
pub struct ApprovalGate;
impl ApprovalGate {
    pub fn register(&self) -> ApprovalEndpoint;   // RAII；drop 注销
    pub fn is_open(&self) -> bool;
}
pub struct ApprovalEndpoint;
```

- `ApprovalGate` 由宿主经 facade 的 `Asterpath::approval_gate()` 取得；宿主在能够应答审批时 `register()`，句柄 drop 即失效。
- agent 管线在 `Ask` 前查 `is_open()`；关闭时直接 `Deny`，不发 `ApprovalRequested`、不挂起（[tools.md](tools.md) §4）。它只声明“有人在听”，不参与 `Allow | Ask | Deny` 的判定。
- 门是进程级的，因此子会话的审批请求同样受它保护；按会话路由到具体 UI 是宿主的事。
- `Clock` 是时间来源接缝：引擎（agent / scheduler）注入 `Arc<dyn Clock>` 取 `now`；存储不读时间（`recorded_at` 只供诊断）。生产用 `SystemClock`，测试用 `asterpath-testing` 的 `ManualClock`（#19，[testing.md](testing.md)）。
- 唤醒发现不在存储：facade 的 `Asterpath::wakeable()` 对 `sessions()` 逐个折叠会话事实（[storage.md](storage.md) §7）。
- 退役是 `SessionStore` 的**可选**方法（整会话退役，默认 `Unsupported`），宿主经 `store().retire(..)` 调用；签名与语义见 [retention.md](retention.md)。导出用现有 `frontier` + `scan`，导入不在 v1。
- 事件枚举与恢复算法由 #3 定，schema 版本由 #4 定。
- `SessionStore::frontier` 返回最后一条已提交记录的 `LogId`（前缘）；恢复点是它之后的下一个位置，见 [GLOSSARY](../../GLOSSARY.md) 与 [event-log-recovery.md](event-log-recovery.md) §6。
- 持久等待是引擎内部的 `waits::Waits`（`await_signal` / `deliver_signal` / `await_wake` / `wake`），不是 trait：持久等待只有审批与子会话完成，没有通用 timer / signal（[ADR-0024](../adr/0024-durable-waits-only-approval-and-child.md)）。`AwaitSignal` / `SignalDelivery` / `WakeCause` 是它的值类型，留在 protocol。v1 没有 activity / workflow。
- 审批 signal id 即 `tool_call_id`；等待意图是 open `ToolCallRequested`，`await_signal` 不写记录，`deliver_signal` 以 signal id 为 dedup key 写 `SignalReceived`，重复投递不产生第二条。
- `wake` 只在内存；终态记录的写者先 append 再 wake。等待者表不持久化，恢复靠扫描日志重建。
- 日志信封只有两个形状：`NewEntry`（调用方提交）与 `LogEntry`（存储返回）；payload 是信封层不解释的 `serde_json::Value`，信封不设 `kind` 字段（事件类型由存储从 payload 顶层键派生，用于幂等判别）。字段与语义见 [storage.md](storage.md) §3，决定见 [ADR-0020](../adr/0020-log-envelope-core-typed-decoding-protocol.md)。带类型的 `SessionEvent` 与 upcast 是同 crate 的 `decode_entry(&LogEntry)`。

### 3.5 错误与 ID

- 每类能力一个 `#[non_exhaustive]` 枚举：`StorageError`、`ExecutionError`、`RetirementError`、`ProviderError`、`ToolError`、`LogDecodeError` 都在 protocol。`LogDecodeError` 只在 protocol 内部消费，经 `AgentError::Corrupt` / `StorageError::Corrupt` 露出。
- `AsterError` 在门面 crate，对上述错误求和。
- `SessionId`、`TurnId`、`StepId`、`InputId`、`ToolCallId`、`LogId`、`SignalId` 是 protocol 里的 opaque newtype，facade 再导出。
- `anyhow` 只允许出现在 CLI/二进制；库契约不暴露 `Box<dyn Error>`。

### 3.6 运行时与配置

- tokio 是唯一支持的运行时；公共 async 签名按 tokio 生态的 `BoxFuture`/`BoxStream` 表达，不引入泛型 executor 或运行时无关抽象。
- 可测试性所需的 clock / random / timer 用注入 seam 解决，不靠替换运行时。
- protocol 与引擎都不 spawn 后台任务，驱动由宿主调用。
- 配置装载不在 protocol 与引擎：只收显式构造参数，不读文件、不读环境变量。

## 4. 适配器 trait（`asterpath-protocol`）

```rust
pub trait Provider: Send + Sync {
    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities;
    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>>;
}

pub trait Tool: Send + Sync + 'static {
    fn descriptor(&self) -> &ToolDescriptor;
    fn invoke(&self, args: ToolArgs, ctx: ToolContext)
        -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
}

pub enum Provenance {
    InProcess,
    External { transport: TransportKind },
    Unknown,                        // 仅用于 ToolCallRequested：请求的工具不在注册表
}

pub struct ToolDescriptor {
    pub name: ToolName,
    pub description: String,
    pub schema: JsonSchema,
    pub parallelism: Parallelism,   // Parallel | Exclusive
    pub replay: ReplaySafety,       // Safe | Unsafe
    pub side_effect: SideEffect,    // ReadOnly | Mutating
    pub provenance: Provenance,
    pub timeout: Option<Duration>,
    pub max_output_bytes: Option<usize>,
}

pub trait Compactor: Send + Sync { /* plan + compact，见 compaction.md §4 */ }
pub trait ApprovalPolicy: Send + Sync { /* classify，见 tools.md §4 */ }
```

| trait | 实现在 | 注入点 |
| --- | --- | --- |
| `Provider` | 两个 provider crate、`ScriptedProvider` | `AsterpathConfig.providers` |
| `Tool` | 宿主、`asterpath-skills`、plugin-host 的外部工具 | `AsterpathConfig.tools` |
| `SessionStore` | `asterpath-storage-sqlite`、`asterpath-testing` 的 in-memory 后端 | `sqlite` feature 或 `AsterpathConfig.store` |
| `Compactor` | 引擎的 `DefaultCompactor`（facade 再导出） | `AsterpathConfig.compactor` |
| `ApprovalPolicy` | 内置 `DefaultApprovalPolicy` 或宿主 | `AsterpathConfig.approval` |
| `SandboxProvider` | `asterpath-sandbox-linux`（#80 起 trait 在 protocol） | plugin-host |
| `Clock` | `SystemClock`、`ManualClock` | `AsterpathConfig.clock` |
| `Hooks` | 宿主 | `AsterpathConfig.hooks` |

- 规范条目模型、流式事件、能力标志、usage 归一化由 #11 定，见 [protocol.md](protocol.md)。
- `Provenance::Unknown` 只出现在 `ToolCallRequested`；`ToolDescriptor.provenance` 必须是 `InProcess` 或 `External`，它不进入审批（见 [tools.md](tools.md) §3/§4）。
- 工具执行管线与审批见 [tools.md](tools.md) 与 [ADR-0007](../adr/0007-approval-and-sandbox-layers.md)；`ToolDescriptor` 的最终字段以那里为准。
- 工具表是引擎内部的 `tools::ToolTable`（按名有序、重名保留先注册者），不是 trait；agent 循环是内部的 `Agent`，驱动流见 [packaging.md](packaging.md) §3.4；`RunRequest` / `AgentEvent` / `AgentError` / `CompactResult` 都在 protocol。

### 4.1 扩展模块（非内核）

参照 Pi / Codex / DeepSeek Harness 的分层：内核只留 seam，能力由模块/插件提供。以下能力以模块/插件实现，内核只提供接入点：

| 扩展 | 接入方式 | 内核提供的 seam |
| --- | --- | --- |
| 沙箱/隔离 | 进程启动点的 `SandboxProvider` 变换（独立合约 crate，v1 不建） | 内核只带 `Provenance`；见 [sandbox.md](sandbox.md) |
| 成本/预算 | 宿主钩子 | `Hooks::before_step` + `StepCtx::usage()`，或 `on_record` + `usage_of`（#13、ADR-0025） |
| trace 导出/采样 | 宿主钩子 | `Hooks::on_record`、`Asterpath::trace(session)`、事件日志 |
| 日志保留/归档 | storage admin 扩展 | `SessionStore::retire` 可选能力（#18，见 [retention.md](retention.md)） |
| 内容脱敏/密钥 | 宿主钩子 | `Hooks::after_tool` |
| 内置工具 | 可选适配器或参考 CLI | `Tool` + `AsterpathConfig.tools` |
| 插件安装/分发 | 生态工具或 host | 插件清单/加载（#16） |

这些扩展不是引擎职责，也不出界；它们通过适配器 trait 与 `Hooks` 接入。内核不内置策略、价格表、沙箱实现或脱敏规则。

## 5. 生命周期规则

1. facade（组装根）显式构造引擎；只有它知道具体类型。替换能力只经适配器字段（provider、tools、compactor、存储后端、审批策略、clock）与 `Hooks`。
2. 构造顺序固定：存储 → trace / usage 视图（读后端）→ append 入口（含钩子）→ waits → scheduler → 工具表（先注册 `spawn_agent` 描述符）→ agent（持有 scheduler）。agent 执行 `spawn_agent` 时把自己传给 `Scheduler::run_spawn_tool` 作为子会话驱动者，scheduler 不持有 agent，没有循环引用。
3. 引擎没有后台任务；`shutdown()` 只释放引擎。
4. 工具表可在运行期增加工具（只在组装时用）；session 的模型可见工具集在创建时快照。
5. v1 不做动态发现、热替换、运行期卸载；换组件 = 重启进程。
6. 外部插件经进程内 adapter 接入；其能力带 `Provenance::External`。
7. 存储与 provider 在 v1 没有外部（跨进程）接入路径，只由进程内适配器提供。
8. 引擎内部协作是同 crate 的模块调用，默认 `pub(crate)`；对外只经适配器 trait 与 `Hooks`。

## 6. 插件与信任

- 进程内代码（含 `Hooks`）随宿主编译，永远是进程内、受信的。
- 外部插件是子进程或 MCP server；由进程内 adapter 模块连接，并把它们的工具注册为 `Provenance::External { transport }`。
- 信任是**来源标注**，不是沙箱：进程内代码本来就不可沙箱，真正的安全边界是进程边界。框架只记录 provenance；隔离与内容安全由 adapter/host 负责（#7）；沙箱接缝见 [sandbox.md](sandbox.md)（#21）。
- 动态发现、热替换不属于 v1；安装/分发是 host 责任；清单、加载与版本协商见 [plugins.md](plugins.md) §7。

## 7. 可检查的解耦

- CI 读 `cargo metadata --no-deps`，逐包对照根目录 `deps-allowlist.toml` 校验依赖边与角色，违反即失败（`cargo xtask check-deps`）：

| 角色 | crate | 允许的 workspace 依赖 |
| --- | --- | --- |
| protocol | `asterpath-protocol` | 无；且不声明 feature |
| adapter | provider、sqlite、http、plugin-host、sandbox、sandbox-linux、skills、testing | `asterpath-protocol` 与白名单列出的同类适配器 |
| engine | `asterpath` | `asterpath-protocol` 与它再导出 / 按 feature 装配的适配器 |
| app | `asterpath-cli`、`asterpath-acceptance` | 任意 workspace 成员 |
| tool | `xtask`、`asterpath-mcp-fixture` | 白名单所列（当前无） |

- 每个 crate 的职责见 [packaging.md](packaging.md) §2。`asterpath-testing` 不依赖引擎；需要真实组装的测试放 facade 的 `tests/` 或 `asterpath-acceptance`。
- protocol 不带 feature flags；feature 只出现在 facade、CLI 与后端 crate，且不得改变公共契约类型。
- 跨 crate 类型只有一条路：进 protocol。适配器之间的共用代码进白名单列出的适配器同类（如 `asterpath-http`）。
- `[dev-dependencies]` 不计入：任何 crate 可以 dev-depend on `asterpath-testing` 跑共享契约套件或引擎内部测试；`cargo xtask check-deps` 只校验普通依赖边。

## 8. 对下游票的约束

| 票 | 契约落点 |
| --- | --- |
| #3 事件日志与恢复 | `SessionStore` 在 protocol；持久等待是引擎内部的 `Waits` |
| #4 日志版本与迁移 | `LogEntry.schema_version`、upcasting、重放兼容 |
| #5 存储与 SQLite | `SessionStore` 方法集与 schema；实现是适配器（#73） |
| #6 agent loop | 引擎内部的 `Agent`（不可替换，ADR-0026）；turn/step、取消、steering 见 [agent-loop.md](agent-loop.md) |
| #7 工具与审批 | 已定。`Tool` / `Provenance` 在 protocol，工具表是引擎内部的 `ToolTable`；管线与审批见 [tools.md](tools.md)、[ADR-0007](../adr/0007-approval-and-sandbox-layers.md) |
| #8 子智能体 | 已定。spawn 是同步工具调用拥有的独立子会话；调度是引擎内部的 `Scheduler`，见 [subagents.md](subagents.md) 与 [ADR-0008](../adr/0008-subagent-model.md) |
| #9 上下文压缩 | 已定。`Compactor` 在 protocol，默认实现 `DefaultCompactor` 在引擎（facade 再导出）、可选装配；见 [compaction.md](compaction.md) 与 [protocol.md](protocol.md) §6.1 |
| #17 调度 API | 已定。持久等待与调度都是引擎内部类型（`Waits`、`Scheduler`，ADR-0026）；见 [ADR-0014](../adr/0014-subagent-scheduling-api.md)；不做通用 workflow |
| #11 协议模型 | 已定。规范模型与 `Provider` 在 protocol；见 [protocol.md](protocol.md) |
| #12 追踪 | 查询是 facade 方法 `trace(session)`；没有推送，实时导出走 `Hooks::on_record` |
| #13 token 统计 | 已定。查询是 facade 方法 `usage(scope)`，`usage_of` 与 `SessionStore` 的会话目录在 protocol（实时观察走 `Hooks::on_record`）；见 [usage.md](usage.md) 与 [ADR-0012](../adr/0012-usage-accounting.md) |
| #15 打包 | 已定。最终 crate 列表、稳定面、公共驱动 API、参考 CLI 与验收设计见 [packaging.md](packaging.md)、[acceptance.md](../spec/acceptance.md) 与 [ADR-0015](../adr/0015-packaging-and-stability.md) |
| #16 插件清单 | 已定。插件清单、加载与版本协商见 [plugins.md](plugins.md)；发现与分发出界 |
| #21 沙箱 | 已定。接缝在 `asterpath-sandbox`（#80 起移入 protocol）；protocol 只增加 `ToolError::Denied`；见 [sandbox.md](sandbox.md) 与 [ADR-0019](../adr/0019-sandbox-seam-and-policy.md) |

## 9. 明确留给下游的点

- 持久等待的方法集与等待 id 派生：已定，见 §3.4 与 [ADR-0014](../adr/0014-subagent-scheduling-api.md)；等待语义归 #3。
- `SessionStore` 方法与 SQLite schema：#5 / #73。
- `LogEntry` 编码与 schema 版本：#3/#4。
- 会话生命周期事件枚举：#3/#11。
- 插件清单/加载/版本协商：已定，见 [plugins.md](plugins.md)；发现与分发出界。
- 热替换、动态发现、WASM：v1 出界。
