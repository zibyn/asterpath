# 模块边界、内核契约与插件宿主

> 票 [#2](https://github.com/zibyn/asterpath/issues/2) 的决策记录，2026-10-22 已定。
> 相关 ADR：[0001 依赖方向](../adr/0001-dependency-direction.md)。
> 本文件是 v1 spec 的"模块契约"章节草稿；最终 crate 打包由 #15 收敛。
>
> **2026-10-22 范围调整**：不做通用工作流引擎（[ADR-0003](../adr/0003-single-session-stream-topology.md)）。模块图中的 `asterpath-workflow` 已移除，编排相关表述以本文件为准已更新；#10 出界，调度 API 原型见 #17。

## 1. 决策摘要

- 内核只含三样东西：模块宿主、类型化事件总线、合约 trait/值类型。任何具体实现都不在内核。
- 模块间交互只有两种进程内形式：能力调用走 trait object，观察/拦截走类型化事件；消息只出现在跨进程与跨恢复边界。
- 内置模块与插件共享同一套 `Module` 生命周期；插件是外部传输，不是特权层。外部能力带 `Provenance::External`，经进程内 adapter 暴露。
- 生命周期是显式的：组装根构造、静态依赖、拓扑初始化、effect 回卷；v1 不做动态发现与热替换。
- 异步运行时是 tokio，不做运行时抽象；配置装载不在内核，由组装根/配置模块以显式参数提供。
- 依赖方向固定为 `core ← protocol ← modules ← app`；模块之间零依赖，CI 用 `cargo metadata` 白名单校验。

## 2. 模块图

```text
app / composition root
  asterpath-cli, asterpath (facade)
        │
        ▼
modules (each crate; no module→module edges)
  asterpath-runtime          DurableExecutor impl
  asterpath-storage-sqlite   EventLog + Storage impl
  asterpath-agent            AgentRunner impl (turn/step loop)
  asterpath-compaction       Compactor impl (#9)
  asterpath-tools            ToolRegistry impl (+ optional built-in tools)
  asterpath-provider-anthropic
  asterpath-provider-openai
  asterpath-tracing
  asterpath-usage
  asterpath-scheduler        子会话调度与持久等待 (#8/#17)
  asterpath-plugin-host      subprocess/MCP adapter (#7)
  asterpath-testing          in-memory fakes / harness
        │
        ▼
contract crates
  asterpath-protocol   canonical model, Provider/Tool/ToolRegistry/AgentRunner,
                       Compactor, session events, Provenance
        │
        ▼
  asterpath-core       Module host, ServiceId/ServiceKey, EventBus,
                       EventLog/Storage/DurableExecutor, IDs, errors
```

规则：

- 箭头只能向下。
- 模块可以依赖 core 和 protocol，不能互相依赖。
- 只有组装根/门面可以依赖具体模块。
- #15 定最终 crate 列表；本图固定的是方向与两个合约 crate。

## 3. 内核契约（`asterpath-core`）

### 3.1 模块宿主

```rust
pub trait Module: Send + Sync {
    fn name(&self) -> &'static str;
    fn dependencies(&self) -> &[ServiceId] { &[] }
    fn optional_dependencies(&self) -> &[ServiceId] { &[] }
    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>>;
}
```

- `init` 是唯一生命周期入口，没有单独的 `start`/`shutdown`。
- 清理通过 effect 注册，宿主按 LIFO 顺序 await disposer。
- `name` 用于诊断与环错误。

### 3.2 服务注册表

```rust
pub struct ServiceId { /* TypeId + display name */ }
pub struct ServiceKey<T: ?Sized + 'static> { /* ServiceId */ }

pub const fn service_key<T: ?Sized + 'static>(name: &'static str) -> ServiceKey<T>;

impl ModuleCtx {
    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &self, key: ServiceKey<T>, value: Arc<T>,
    ) -> Result<Effect, LifecycleError>;

    pub fn require<T: ?Sized + Send + Sync + 'static>(
        &self, key: ServiceKey<T>,
    ) -> Result<Arc<T>, LifecycleError>;

    pub fn require_optional<T: ?Sized + Send + Sync + 'static>(
        &self, key: ServiceKey<T>,
    ) -> Result<Option<Arc<T>>, LifecycleError>;
}
```

- 每个 `ServiceId` 只有一个活跃提供者；重复 `provide` 是启动错误。
- key 是合约 crate 里的 typed 常量，不做字符串查找。
- 集合型能力（工具、provider、模型）由各自的注册表模块管理，不进宿主。

### 3.3 事件总线

```rust
pub trait Event: Send + Sync + 'static {}
pub trait WaterfallEvent: Event { type Error: Error + Send + Sync + 'static; }

pub trait Handler<E: Event>: Send + Sync + 'static {
    fn handle(&self, event: &E) -> BoxFuture<'_, ()>;
}

pub trait Middleware<E: WaterfallEvent>: Send + Sync + 'static {
    fn handle(&self, event: E, next: Next<'_, E>)
        -> BoxFuture<'_, Result<E, E::Error>>;
}

impl ModuleCtx {
    pub fn emit<E: Event>(&self, event: E) -> BoxFuture<'_, ()>;
    pub fn waterfall<E: WaterfallEvent>(&self, event: E)
        -> BoxFuture<'_, Result<E, E::Error>>;
    pub fn on<E: Event>(&self, handler: impl Handler<E>) -> Effect;
    pub fn intercept<E: WaterfallEvent>(&self, middleware: impl Middleware<E>) -> Effect;
    pub fn effect(&self, dispose: impl FnOnce() -> BoxFuture<'static, ()> + Send + 'static)
        -> Effect;
}
```

- `emit`：按注册顺序依次 await 处理者；处理者错误记录后继续，不传播。确定性优先于并发。
- `waterfall`：中间件按注册顺序组成链，可改写事件、调用 `next` 或短路；错误传播给调用者。`Next<'_, E>` 是不透明的续行句柄。
- `on` / `intercept` 只注册；执行统一走 `emit` / `waterfall`。
- `effect`：为任意清理动作注册 `Effect`；关停时宿主按 LIFO 顺序 await 该 disposer。
- `Effect` 是已注册句柄：`provide` / `on` / `intercept` / `effect` 都产出它，不要求调用方再交回宿主。
- 不单独提供 `serial`/`bail`：有序中止用 waterfall 表达。
- 跨模块事件是 core/protocol 里的普通数据结构。

### 3.4 事件日志、存储、持久执行器

```rust
pub type Timestamp = i64;        // Unix epoch 毫秒；唯一允许的时间表示
pub struct DedupKey(String);
pub struct SignalName(String);

pub trait EventLog: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;
}

// 会话树只读视图，由存储模块实现；用量子树汇总与归档使用（#13/#18）。
pub trait SessionCatalog: Send + Sync {
    fn parent(&self, session: SessionId)
        -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;
    fn children(&self, session: SessionId)
        -> BoxStream<'_, Result<SessionId, StorageError>>;
}

pub struct TimerRequest {
    pub session: SessionId,
    pub timer: TimerId,
    pub caused_by: LogId,
    pub deadline: Timestamp,
}

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
    pub name: SignalName,
    pub caused_by: LogId,
    pub intent: SignalIntent,        // Record | AlreadyRecorded（审批）
}

pub enum SignalIntent { Record, AlreadyRecorded }

pub enum WakeCause {
    Timer(TimerId),
    Signal(SignalId),
    ChildCompleted(SessionId),
    Startup,                          // 仅启动入队使用
}

pub trait DurableExecutor: Send + Sync {
    fn schedule_timer(&self, req: TimerRequest)
        -> BoxFuture<'_, Result<TimerId, ExecutionError>>;
    fn cancel_timer(&self, session: SessionId, timer: TimerId)
        -> BoxFuture<'_, Result<(), ExecutionError>>;
    fn await_signal(&self, req: AwaitSignal)
        -> BoxFuture<'_, Result<SignalPayload, ExecutionError>>;
    fn deliver_signal(&self, delivery: SignalDelivery)
        -> BoxFuture<'_, Result<(), ExecutionError>>;
    fn await_wake(&self, session: SessionId, cause: WakeCause)
        -> BoxFuture<'_, Result<(), ExecutionError>>;
    fn wake(&self, session: SessionId, cause: WakeCause);   // 仅内存
}

pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

pub struct SystemClock;

// 审批端点的存活声明：core 机制，不是决策者（#26）。
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
- `Clock` 是时间来源接缝：`runtime` / `scheduler` 注入 `Arc<dyn Clock>` 取 `now`；`Storage` 仍收显式 `now`（#5）。生产用 `SystemClock`，测试用 `asterpath-testing` 的 `ManualClock`（#19，[testing.md](testing.md)）。
- `Storage` 负责 session、租约、队列、定时器；方法集由 #5 定。
- `SessionRetirement` 是 core 的**可选**能力（整会话退役），由存储模块 `provide`，宿主或策略模块用 `require_optional` 接入；签名与语义见 [retention.md](retention.md)。导出用现有 `EventLog::frontier` + `scan`，导入不在 v1；它不并入 `EventLog` / `Storage`，也不允许扩展直接访问后端。
- `EventLog` 负责 append/scan/frontier；事件枚举与恢复算法由 #3 定，schema 版本由 #4 定。
- `EventLog::frontier` 返回最后一条已提交记录的 `LogId`（前缘）；恢复点是它之后的下一个位置，见 [GLOSSARY](../../GLOSSARY.md) 与 [event-log-recovery.md](event-log-recovery.md) §6。
- `DurableExecutor` 是持久等待 seam；timer/signal 语义由 #3 定，API 由 #17 定（[ADR-0014](../adr/0014-subagent-scheduling-api.md)）。v1 没有 activity / workflow。
- timer id 由 `(session, caused_by, slot)` 确定性派生；审批 signal id 即 `tool_call_id`，通用 signal id 由 `(session, name)` 派生。重复注册返回同一 id，恢复重放不会制造第二个等待。
- `await_signal` 的 `SignalIntent` 区分 `Record`（写 `SignalAwaited`）与 `AlreadyRecorded`（审批：open `ToolCallRequested` 已是意图记录）。
- `wake` 只在内存；终态记录的写者先 append 再 wake。注册表不持久化，恢复靠扫描日志重建。
- 日志信封只有两个形状，定义在 core：`NewEntry`（调用方提交）与 `LogEntry`（存储返回）；payload 是 core 不解释的 `serde_json::Value`，信封不设 `kind` 字段（事件类型由存储从 payload 顶层键派生，用于幂等判别）。字段与语义见 [storage.md](storage.md) §3，决定见 [ADR-0020](../adr/0020-log-envelope-core-typed-decoding-protocol.md)。带类型的 `SessionEvent` 与 upcast 归 protocol 的 `decode_entry(&LogEntry)`，core 不依赖 protocol。

### 3.5 错误与 ID

- 每类能力一个 `#[non_exhaustive]` 枚举：`LifecycleError`、`StorageError`、`ExecutionError` 在 core；`ProviderError`、`ToolError`、`LogDecodeError` 在 protocol。`LogDecodeError` 只在 protocol 内部消费，经 `AgentError::Corrupt` / `StorageError::Corrupt` 露出。
- `AsterError` 在门面 crate，对上述错误求和；core 不反向依赖 protocol。
- `SessionId`、`TurnId`、`StepId`、`InputId`、`ToolCallId`、`LogId`、`TimerId`、`SignalId` 是 core 里的 opaque newtype。
- `anyhow` 只允许出现在 CLI/二进制；库契约不暴露 `Box<dyn Error>`。

### 3.6 运行时与配置

- tokio 是唯一支持的运行时；公共 async 签名按 tokio 生态的 `BoxFuture`/`BoxStream` 表达，不引入泛型 executor 或运行时无关抽象。
- 可测试性所需的 clock / random / timer 用注入 seam 解决，不靠替换运行时。
- core 本身不 spawn 任务；spawn 归宿主或执行器模块所有，并受 effect 约束。
- 配置装载不在内核：内核只收显式构造参数，不读文件、不读环境变量。

## 4. 协议合约（`asterpath-protocol`）

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

pub trait ToolRegistry: Send + Sync {
    fn register(&self, tool: Arc<dyn Tool>) -> Effect;
    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>>;
    fn list(&self) -> Vec<Arc<dyn Tool>>;
}

pub trait AgentRunner: Send + Sync {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>>;
    fn compact_now(&self, session: SessionId)
        -> BoxFuture<'_, Result<CompactResult, AgentError>>;
}
```

- 规范条目模型、流式事件、能力标志、usage 归一化由 #11 定，见 [protocol.md](protocol.md)。
- `Provenance::Unknown` 只出现在 `ToolCallRequested`；`ToolDescriptor.provenance` 必须是 `InProcess` 或 `External`，它不进入审批（见 [tools.md](tools.md) §3/§4）。
- 工具执行管线与审批见 [tools.md](tools.md) 与 [ADR-0007](../adr/0007-approval-and-sandbox-layers.md)；`ToolDescriptor` 的最终字段以那里为准。
- `AgentRunner` 的精确签名以 [packaging.md](packaging.md) §3.4 为准；`RunRequest` / `AgentEvent` / `AgentError` / `CompactResult` 都在 protocol。

### 4.1 扩展模块（非内核）

参照 Pi / Codex / DeepSeek Harness 的分层：内核只留 seam，能力由模块/插件提供。以下能力以模块/插件实现，内核只提供接入点：

| 扩展 | 接入方式 | 内核提供的 seam |
| --- | --- | --- |
| 沙箱/隔离 | 进程启动点的 `SandboxProvider` 变换（独立合约 crate，v1 不建） | 内核只带 `Provenance`；见 [sandbox.md](sandbox.md) |
| 成本/预算 | 消费 usage 的模块 + pre-step gate | `UsageRecorded` / `UsageQuery`、pre-step waterfall（#13） |
| trace 导出/采样 | exporter 模块订阅 tracing 模块 | 原生 trace 模型/查询、事件日志 |
| 日志保留/归档 | storage admin 扩展 | `SessionRetirement` 可选能力（#18，见 [retention.md](retention.md)） |
| 内容脱敏/密钥 | host 或 post-execute 模块 | `post-execute` hook |
| 内置工具 | 可选模块或参考 CLI | `ToolRegistry` |
| 插件安装/分发 | 生态工具或 host | 插件清单/加载（#16） |

这些扩展不是内核职责，也不出界；它们通过合约 trait/事件接入。内核不内置策略、价格表、沙箱实现或脱敏规则。

## 5. 生命周期规则

1. 组装根显式构造全部模块；只有它知道具体模块类型。
2. `Module::dependencies()` 静态声明；宿主拓扑排序；成环 = `LifecycleError::Cycle`，启动失败并打印环路径。
3. `init` 是模块向宿主注册能力/effect 的唯一时机；宿主注册表在 init 结束后关闭。能力级注册表（如 `ToolRegistry`）可在运行期自行增删，不走宿主注册表。
4. 所有注册产生 `Effect`（`provide` / `on` / `intercept` / `effect`）；关停时 LIFO 回卷，异步 disposer 逐个 await。`init` 失败时，宿主回卷该模块已注册的 effect，再报告启动失败。
5. 缺必需依赖 = 启动失败；缺可选依赖 = `None`。
6. 模块不得启动不受 effect 约束的后台任务；宿主关停后不得有任务存活。
7. v1 不做动态发现、热替换、运行期卸载；换模块 = 重启进程。
8. 外部插件经进程内 adapter 模块接入；其能力带 `Provenance::External`。
9. core 能力（EventLog/Storage/DurableExecutor/Provider）在 v1 没有外部接入路径，只由进程内模块提供。
10. cross-module 只走合约 trait/事件；模块之间没有 crate 依赖。

## 6. 插件与信任

- 进程内模块随宿主编译，`Module` 永远是进程内、受信的。
- 外部插件是子进程或 MCP server；由进程内 adapter 模块连接，并把它们的工具注册为 `Provenance::External { transport }`。
- 信任是**来源标注**，不是沙箱：进程内代码本来就不可沙箱，真正的安全边界是进程边界。框架只记录 provenance；隔离与内容安全由 adapter/host 负责（#7）；沙箱接缝见 [sandbox.md](sandbox.md)（#21）。
- 动态发现、热替换不属于 v1；安装/分发是 host 责任；清单、加载与版本协商见 [plugins.md](plugins.md) §7。

## 7. 可检查的解耦

- 一模块一 crate；CI 读 `cargo metadata --no-deps`，逐包对照白名单校验依赖边，违反即失败。
- 白名单：

| crate | 允许的 workspace 依赖 |
| --- | --- |
| `asterpath-core` | 无 |
| `asterpath-protocol` | `asterpath-core` |
| 模块 crate | `asterpath-core`、`asterpath-protocol` |
| 门面 / CLI | 任意 workspace 成员 |
| 测试替身 | `asterpath-core`、`asterpath-protocol` |

- 最终成员列表、每个 crate 的职责与 v1 交付边界见 [packaging.md](packaging.md) §2。M0 起根目录 `deps-allowlist.toml` 是白名单的机器可读来源，由 `cargo xtask check-deps` 校验；`asterpath-testing` 只依赖合约，需要真实组装的验收测试放 `asterpath-acceptance`。
- core/protocol 不带 feature flags；feature 只出现在边缘/后端 crate，且不得改变公共契约类型。
- 跨模块类型只有两条路：进合约 crate，或变成 trait/事件。"加一个模块依赖"不是选项。
- `[dev-dependencies]` 不计入模块间零依赖：模块 crate 可以 dev-depend on `asterpath-testing` 跑共享契约套件（SQLite 即如此）；`cargo xtask check-deps` 只校验普通依赖边。

## 8. 对下游票的约束

| 票 | 契约落点 |
| --- | --- |
| #3 事件日志与恢复 | `EventLog` / `Storage` / `DurableExecutor` 在 core |
| #4 日志版本与迁移 | `LogEntry.schema_version`、upcasting、重放兼容 |
| #5 存储与 SQLite | `Storage` 方法集与 schema；实现是模块 |
| #6 agent loop | `AgentRunner` 在 protocol；turn/step、取消、steering 见 [agent-loop.md](agent-loop.md) |
| #7 工具与审批 | 已定。`Tool` / `ToolRegistry` / `Provenance` 在 protocol；管线与审批见 [tools.md](tools.md)、[ADR-0007](../adr/0007-approval-and-sandbox-layers.md) |
| #8 子智能体 | 已定。spawn 是同步工具调用拥有的独立子会话；scheduler 提供 `SubagentSpawner`，见 [subagents.md](subagents.md) 与 [ADR-0008](../adr/0008-subagent-model.md) |
| #9 上下文压缩 | 已定。`Compactor` 在 protocol，默认实现 `asterpath-compaction` 可选装配；见 [compaction.md](compaction.md) 与 [protocol.md](protocol.md) §6.1 |
| #17 调度 API | 已定。`DurableExecutor` 在 core，`SubagentSpawner` 在 scheduler；见 [ADR-0014](../adr/0014-subagent-scheduling-api.md)；不做通用 workflow |
| #11 协议模型 | 已定。规范模型与 `Provider` 在 protocol；见 [protocol.md](protocol.md) |
| #12 追踪 | 订阅事件；查询 API 在自己的模块 |
| #13 token 统计 | 已定。`UsageQuery` / `UsageRecorded` 在 protocol，`SessionCatalog` 在 core；见 [usage.md](usage.md) 与 [ADR-0012](../adr/0012-usage-accounting.md) |
| #15 打包 | 已定。最终 crate 列表、稳定面、公共驱动 API、参考 CLI 与验收设计见 [packaging.md](packaging.md)、[acceptance.md](../spec/acceptance.md) 与 [ADR-0015](../adr/0015-packaging-and-stability.md) |
| #16 插件分发 | 已定。插件清单、加载与版本协商见 [plugins.md](plugins.md)；发现出界 |
| #21 沙箱 | 已定。接缝在独立合约 crate；core 零改动，protocol 只增加 `ToolError::Denied`；见 [sandbox.md](sandbox.md) 与 [ADR-0019](../adr/0019-sandbox-seam-and-policy.md) |

## 9. 明确留给下游的点

- `DurableExecutor` 的方法集与等待 id 派生：已定，见 §3.4 与 [ADR-0014](../adr/0014-subagent-scheduling-api.md)；等待语义归 #3。
- `Storage` 方法与 SQLite schema：#4。
- `LogEntry` 编码与 schema 版本：#3/#4。
- 会话生命周期事件枚举：#3/#11。
- 插件清单/加载/版本协商：已定，见 [plugins.md](plugins.md)；发现与分发出界。
- 热替换、动态发现、WASM：v1 出界。
