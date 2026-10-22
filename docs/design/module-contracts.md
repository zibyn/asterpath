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
  asterpath-tools            ToolRegistry impl + built-in tools
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
                       session events, Provenance
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

impl ModuleCtx {
    pub fn emit<E: Event>(&self, event: E);
    pub fn on<E: Event>(&self, handler: impl Handler<E>) -> Effect;
    pub fn intercept<E: WaterfallEvent>(&self, middleware: impl Middleware<E>) -> Effect;
}
```

- `emit`：按注册顺序依次 await 处理者；处理者错误记录后继续，不传播。确定性优先于并发。
- `intercept`：中间件按注册顺序组成链，可改写事件、调用 `next` 或短路；错误传播给调用者。
- 不单独提供 `serial`/`bail`：有序中止用 waterfall 表达。
- 跨模块事件是 core/protocol 里的普通数据结构。

### 3.4 事件日志、存储、持久执行器

```rust
pub trait EventLog: Send + Sync {
    fn append(&self, entry: LogEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;
}

pub trait DurableExecutor: Send + Sync {
    fn schedule_timer(&self, deadline: Timestamp)
        -> BoxFuture<'_, Result<TimerId, ExecutionError>>;
    fn await_signal(&self, name: SignalName)
        -> BoxFuture<'_, Result<SignalPayload, ExecutionError>>;
}
```

- `Storage` 负责 session、租约、队列、定时器；方法集由 #5 定。
- `EventLog` 负责 append/scan/frontier；事件枚举与恢复算法由 #3 定，schema 版本由 #4 定。
- `DurableExecutor` 是持久等待 seam；timer/signal 语义由 #3 定，API 由 #17 定。v1 没有 activity / workflow。
- `LogEntry` 是 `{ kind, schema_version, payload }`；带类型的会话事件在 protocol，core 不依赖 protocol。

### 3.5 错误与 ID

- 每类能力一个 `#[non_exhaustive]` 枚举：`LifecycleError`、`StorageError`、`ExecutionError` 在 core；`ProtocolError`、`ProviderError`、`ToolError` 在 protocol。
- `AsterError` 在门面 crate，对上述错误求和；core 不反向依赖 protocol。
- `SessionId`、`TurnId`、`StepId`、`ToolCallId`、`LogId`、`TimerId`、`SignalId` 是 core 里的 opaque newtype。
- `anyhow` 只允许出现在 CLI/二进制；库契约不暴露 `Box<dyn Error>`。

### 3.6 运行时与配置

- tokio 是唯一支持的运行时；公共 async 签名按 tokio 生态的 `BoxFuture`/`BoxStream` 表达，不引入泛型 executor 或运行时无关抽象。
- 可测试性所需的 clock / random / timer 用注入 seam 解决，不靠替换运行时。
- core 本身不 spawn 任务；spawn 归宿主或执行器模块所有，并受 effect 约束。
- 配置装载不在内核：内核只收显式构造参数，不读文件、不读环境变量。

## 4. 协议合约（`asterpath-protocol`）

```rust
pub trait Provider: Send + Sync {
    fn capabilities(&self) -> ProviderCapabilities;
    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>>;
}

pub trait Tool: Send + Sync {
    fn descriptor(&self) -> &ToolDescriptor;
    fn invoke(&self, args: ToolArgs, ctx: ToolContext)
        -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
}

pub enum Provenance {
    InProcess,
    External { transport: TransportKind },
}

pub struct ToolDescriptor {
    pub name: ToolName,
    pub schema: JsonSchema,
    pub parallelism: Parallelism,   // Parallel | Exclusive
    pub replay: ReplaySafety,       // Safe | Unsafe
    pub provenance: Provenance,
    pub timeout: Option<Duration>,
}

pub trait ToolRegistry: Send + Sync {
    fn register(&self, tool: Arc<dyn Tool>) -> Effect;
    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>>;
    fn list(&self) -> Vec<Arc<dyn Tool>>;
}

pub trait AgentRunner: Send + Sync {
    fn run(&self, input: AgentInput) -> BoxStream<'_, Result<AgentEvent, AgentError>>;
}
```

- 规范条目模型、流式事件、能力标志、usage 归一化由 #11 定。
- 工具执行管线、审批、沙箱由 #7 定；`ToolDescriptor` 只是最小 seam。
- `AgentRunner` 是 loop 契约；turn/step、取消、steering 由 #6 定。

## 5. 生命周期规则

1. 组装根显式构造全部模块；只有它知道具体模块类型。
2. `Module::dependencies()` 静态声明；宿主拓扑排序；成环 = `LifecycleError::Cycle`，启动失败并打印环路径。
3. `init` 是模块向宿主注册能力/effect 的唯一时机；宿主注册表在 init 结束后关闭。能力级注册表（如 `ToolRegistry`）可在运行期自行增删，不走宿主注册表。
4. 所有注册经 `ctx.effect()`；关停时 LIFO 回卷，异步 disposer 逐个 await。`init` 失败时，宿主回卷该模块已注册的 effect，再报告启动失败。
5. 缺必需依赖 = 启动失败；缺可选依赖 = `None`。
6. 模块不得启动不受 effect 约束的后台任务；宿主关停后不得有任务存活。
7. v1 不做动态发现、热替换、运行期卸载；换模块 = 重启进程。
8. 外部插件经进程内 adapter 模块接入；其能力带 `Provenance::External`。
9. core 能力（EventLog/Storage/DurableExecutor/Provider）在 v1 没有外部接入路径，只由进程内模块提供。
10. cross-module 只走合约 trait/事件；模块之间没有 crate 依赖。

## 6. 插件与信任

- 进程内模块随宿主编译，`Module` 永远是进程内、受信的。
- 外部插件是子进程或 MCP server；由进程内 adapter 模块连接，并把它们的工具注册为 `Provenance::External { transport }`。
- 信任是**来源标注**，不是沙箱：进程内代码本来就不可沙箱，真正的安全边界是进程边界。审批、沙箱、UI 与日志消费 provenance（#7）。
- 动态发现、分发、版本协商不属于 v1：见 #16。

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

- core/protocol 不带 feature flags；feature 只出现在边缘/后端 crate，且不得改变公共契约类型。
- 跨模块类型只有两条路：进合约 crate，或变成 trait/事件。"加一个模块依赖"不是选项。

## 8. 对下游票的约束

| 票 | 契约落点 |
| --- | --- |
| #3 事件日志与恢复 | `EventLog` / `Storage` / `DurableExecutor` 在 core |
| #4 日志版本与迁移 | `LogEntry.schema_version`、upcasting、重放兼容 |
| #5 存储与 SQLite | `Storage` 方法集与 schema；实现是模块 |
| #6 agent loop | `AgentRunner` 在 protocol；turn/step、取消、steering 见 [agent-loop.md](agent-loop.md) |
| #7 工具与审批 | `Tool` / `ToolRegistry` / `Provenance` 在 protocol；adapter 是模块 |
| #8 子智能体 | 基于 `AgentRunner` + `DurableExecutor`（timer/signal） |
| #9 上下文压缩 | 消费 `EventLog` 与 `Provider` |
| #17 调度 API | `DurableExecutor` 在 core；不做通用 workflow |
| #11 协议模型 | 规范模型与 `Provider` 在 protocol |
| #12 追踪 | 订阅事件；查询 API 在自己的模块 |
| #13 token 统计 | 订阅 usage 事件；usage 类型在 protocol |
| #15 打包 | 最终 crate 列表、门面、CLI 打包 |
| #16 插件分发 | 插件清单、发现、版本协商 |

## 9. 明确留给下游的点

- `DurableExecutor` 的完整方法集与重放绑定：#3/#10。
- `Storage` 方法与 SQLite schema：#4。
- `LogEntry` 编码与 schema 版本：#3/#4。
- 会话生命周期事件枚举：#3/#11。
- 插件发现/分发/版本协商：#16。
- 热替换、动态发现、WASM：v1 出界。
