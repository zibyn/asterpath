# 打包、公共 API 与参考 CLI

> 来源：[#15](https://github.com/zibyn/asterpath/issues/15)。本文件是 crate 图、稳定面、公共 API 与参考 CLI 的规范；验收场景在 [acceptance.md](../spec/acceptance.md)，入口在 [spec/README.md](../spec/README.md)。
>
> 相关 ADR：[0026 引擎是一个 crate](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)。

## 1. 决策摘要

- 引擎是一个 crate（[ADR-0026](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)，[#79](https://github.com/zibyn/asterpath/issues/79)）：agent、scheduler、durable waits、工具表、trace / usage 视图与默认压缩是 `asterpath` 的内部模块；`asterpath-core` 并入 `asterpath-protocol`。workspace = 13 个库/应用成员 + `xtask`（§2；#80 以 `asterpath-mcp` 取代 `asterpath-plugin-host` 与 `asterpath-sandbox`）。独立 crate 只为真实适配器或可选重依赖。
- 依赖规则按角色（`protocol` / `adapter` / `engine` / `app` / `tool`），由 `cargo xtask check-deps` 对照根目录 `deps-allowlist.toml` 检查（§2）。
- 全部成员 lockstep `0.1.0`；**稳定面只有 facade（`asterpath`）、`asterpath-protocol` 与日志 schema**，适配器 crate 是 0.x 实验面。
- v1 **不发布 crates.io**；嵌入者用 git/path 依赖。
- `protocol` 无 feature；feature 只出现在 facade、CLI 与后端 crate。
- 没有后台 driver：宿主显式调用 `SessionHandle::drive()`；同一 session 单飞；丢弃流不等于取消。
- 参考 CLI 六命令，退出码 `0/1/2`；审批在 TTY 上默认 `ask`，非 TTY 默认 `deny`。
- 三段验收由 `asterpath-acceptance` 的 `testdrive` 二进制 + 真 `SIGKILL` 驱动；#19 只负责五个注入点工具链。
- v1 的 provider/model 在 session 创建时快照，**不支持 turn 内切换**；验收 (b) 验证的是 adapter 可替换性与规范化历史等价。

## 2. Workspace 成员与依赖

| crate | 角色 | 职责 | 允许的 workspace 依赖 |
| --- | --- | --- | --- |
| `asterpath-protocol` | protocol | ID、日志信封与存储契约（`SessionStore`、`scan_all`）、`Clock`、错误；规范化模型、适配器 trait（`Provider` / `Tool` / `Compactor` / `ApprovalPolicy` / `SandboxProvider` 及沙箱类型，ADR-0027）、`Hooks`、会话事件、`ConfigSnapshot`、`AgentEvent` | 无 |
| `asterpath` | engine | facade / 组装根，暴露稳定 API；内部模块 `agent`、`scheduler`、`waits`、`tools`、`views::{trace, usage}`、`compaction`、`commit`（§2.2） | `protocol` 及其再导出 / feature 提供的适配器：`provider-anthropic`、`provider-openai`、`storage-sqlite` |
| `asterpath-http` | adapter | 适配器共用 HTTP 工具：`SseDecoder`（字节进、事件出；LF / CRLF；非法 UTF-8 报错）、`classify_status`、`retry_after`、`request_id`（#67）；`provider_options` 深合并与同 provider raw 字段提取（#101、#103）；不属于稳定面（ADR-0026 修订） | `protocol` |
| `asterpath-provider-anthropic` | adapter | Anthropic Messages adapter | `protocol`, `http` |
| `asterpath-provider-openai` | adapter | OpenAI Responses adapter | `protocol`, `http` |
| `asterpath-storage-sqlite` | adapter | `SessionStore`（含退役）的 SQLite 实现 | `protocol` |
| `asterpath-mcp` | adapter | MCP 工具来源：`McpServerConfig`、`McpServers::load` / `tools()` / `statuses()` / `shutdown()`，stdio 与 HTTP adapter 为 crate 私有（[plugins.md](plugins.md)、[mcp.md](mcp.md)，ADR-0027） | `protocol`, `http` |
| `asterpath-sandbox-linux` | adapter | Linux bwrap 参考后端（实现 protocol 的 `SandboxProvider`） | `protocol` |
| `asterpath-skills` | adapter | 技能包解析、索引渲染与 `load_skill` 工具（[skills.md](skills.md)） | `protocol` |
| `asterpath-tools` | adapter | 按根目录构造的 `read_file` / `list_dir` / `grep` / `write_file` / `edit_file` / `bash` 工具与 `read_only` / `coding` 预设（#83） | `protocol` |
| `asterpath-testing` | adapter | in-memory `SessionStore`（`InMemoryEventLog`，含会话目录与退役）、`ScriptedProvider`、四个故障注入装饰器 + `ManualClock`（五点，见 [testing.md](testing.md)）、契约套件、`FakeSandboxProvider` | `protocol` |
| `asterpath-cli` | app | 参考 CLI 二进制 `asterpath` | 任意 |
| `asterpath-acceptance` | app | `testdrive` 二进制与三段验收测试；`publish = false` | 任意 |
| `asterpath-mcp-fixture` | tool | 测试用 MCP server（stdio / HTTP）；`publish = false` | 无（仅外部依赖） |
| `xtask` | tool | `check-deps` 与 CI 辅助；`publish = false` | 无 |

- 角色规则（`xtask` 强制）：`protocol` 不依赖任何成员、不声明 feature；`adapter` 只依赖 `protocol` 与白名单列出的同类适配器；`engine` 只依赖 `protocol` 与它再导出 / 按 feature 装配的适配器；`app` / `tool` 只受各自 `allow` 列表约束（`"*"` = 任意成员，仅 app）。没有"模块之间零依赖"规则：引擎内部的协作是同 crate 的 `mod`（ADR-0026）。
- `asterpath-sandbox-macos` 仍为预留名，不建。`asterpath-storage-admin` 不建（[#18](https://github.com/zibyn/asterpath/issues/18)：策略与归档由宿主实现）；MCP adapter 在 `asterpath-mcp` 内，ACP 不做（[#20](https://github.com/zibyn/asterpath/issues/20)）。
- `asterpath-skills` 是可选适配器（M6 交付）；只依赖 `protocol` 与外部 `saphyr`、`futures-core`、`serde`、`serde_json`。技能不新增事件，日志 schema 不变。
- `asterpath-tools` 是可选适配器（#83）：每个工具接收一个绝对根目录，路径越出根目录返回 `Denied`；`bash` 只经由 `SandboxProvider` 启动进程，没有 provider 时仅 `FullAccess` 放行。工作区不进入内核、`ToolContext` 或日志；多项目宿主每个项目一个 `Asterpath` 实例。
- `asterpath-testing` **不得**依赖 facade；需要组装真实引擎的测试放 facade 的 `tests/` 或 `asterpath-acceptance`。反向的 `[dev-dependencies]` 允许：任何 crate 可 dev-depend on `asterpath-testing` 跑共享契约套件或引擎内部测试，`cargo xtask check-deps` 只校验普通依赖边。
- 机器可读白名单在根目录 `deps-allowlist.toml`；上表是它的规范副本，冲突以文件为准并由 `xtask` 报错。

### 2.1 `ConfigSnapshot`（对 #11 的补齐）

[#11](https://github.com/zibyn/asterpath/issues/11) 把 `ConfigSnapshot` 的形状留给了 protocol 但未落定。本票补齐为持久化投影：

```rust
pub struct ConfigSnapshot {
    pub model: ModelSpec,
    pub system: Vec<Part>,          // 只允许 Text
    pub tools: Vec<ToolDescriptor>, // 创建时解析后的固定集合（name / description / schema / 执行声明）
    pub request: RequestConfig,     // 回合内模型调用的请求参数（#26）
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}
```

- `SessionSpec.tools: ToolSelection`（`All | Names`）在创建时解析成显式 `Vec<ToolDescriptor>`；之后注册表增删不影响该 session 的模型可见工具集。
- 所有 `ModelRequest.tools` 只从快照渲染，恢复后重建出与崩溃前相同的工具定义。`timeout` / `parallelism` 等执行声明同样在 session 内固定；注册表升级只影响新 session。
- 执行仍调用活注册表：工具已消失走 `NotFound`（[plugins.md](plugins.md) §8）；恢复按 `ToolCallRequested` 上已记录的 `replay` / `provenance` 决定是否重跑，不重新解析描述符。
- `ReasoningBinding.tools_hash` 按快照 `tools` 的模型可见部分计算（[protocol.md](protocol.md) §9），因此同一 session 内稳定，fork 收窄工具集时自然不匹配。
- 审批策略不进快照：它是宿主代码，不持久化；日志只记录每次决定（[tools.md](tools.md) §4）。
- MCP 服务配置不进快照：它是宿主装配；进快照的是 MCP 工具解析后的描述符。会话中途增减工具见 [#108](https://github.com/zibyn/asterpath/issues/108)。

### 2.2 引擎内部模块（`crates/asterpath/src/`）

| 模块 | 内容 | 原 crate |
| --- | --- | --- |
| `agent` | `Agent`：turn / step 循环、模型调用、恢复；`spawn_agent` 调用转给 `Scheduler::run_spawn_tool`，并把自己作为子会话驱动者传入 | `asterpath-agent` |
| `agent::pipeline` | `ToolPipeline`：一个步骤的全部工具调用到结果（准入、审批、Parallel/Exclusive 锁、并发上限、截断、取消收尾）；唯一操作 `run`，依赖构造时注入，可脱离回合直接测试（#68） | — |
| `scheduler` | `Scheduler`：spawn / 物化 / 准入 / 等待 / 超时 / 取消；`SpawnTool` 只是工具表里的描述符 | `asterpath-scheduler` |
| `waits` | `Waits`：审批决定与子会话完成的持久等待 | `asterpath-runtime` |
| `tools` | `ToolTable`：按名有序、重名保留先注册者 | `asterpath-tools` |
| `views::trace` / `views::usage` | `TracingQuery` / `UsageService`：日志上的纯折叠 | `asterpath-tracing` / `asterpath-usage` |
| `compaction` | `DefaultCompactor`（facade 根部 `pub use`） | `asterpath-compaction` |
| `commit` | `CommitLog`：唯一 append 入口，向 `Hooks::on_record` 逐条送达 | —（#78） |

- 默认 `pub(crate)`；没有内部 trait。agent 持有 scheduler，scheduler 不持有 agent（按调用传参），所以没有 `Weak` 回指。
- 依赖内部类型的测试在 `src/<mod>/tests`（`#[cfg(test)]`）；经 facade 的测试在 `crates/asterpath/tests/`（[testing.md](testing.md)）。

## 3. 公共 API

类型归属：ID（`SessionId` / `InputId` / … / `LogId`）、`Clock` / `Timestamp`、存储契约与 `scan_all`、`SessionSpec` / `ToolSelection` / `UserInput` / `ConfigSnapshot` / `AgentEvent` / `AgentError` / `RunRequest` / `SessionState` / `ModelAlias` / `ApprovalDecision` / `CancelToken` / `ApprovalPolicy` / `ToolConfig` / `Compactor` / `CompactResult` / `Hooks` / `UsageScope` / `UsageReport` / `Span` 在 `asterpath-protocol`；`Asterpath` / `AsterpathBuilder` / `AsterpathConfig` / `SessionHandle` / `AsterError` / `DefaultCompactor` 在 facade。`McpServerConfig` 属于适配器 crate `asterpath-mcp`，技能相关类型属于 `asterpath-skills`，都不在稳定面。

### 3.1 稳定层级与版本

| 层级 | 内容 | 兼容承诺 |
| --- | --- | --- |
| Tier 1 | `asterpath` 的公开项；`asterpath-protocol` 的公开项；日志 schema（`LOG_SCHEMA_VERSION`） | semver；0.x 期间破坏性变更 bump minor，兼容变更 bump patch |
| Tier 2 | 适配器 crate 的公开项 | 0.x 实验面；minor 内可改，变更写入对应 design 文档 |
| Tier 3 | `asterpath-testing`、`asterpath-acceptance`、`xtask`、未在 spec 中记录的 `pub` 项 | 无承诺 |

- 公开错误枚举、事件枚举与可能增字段的结构一律 `#[non_exhaustive]`；配置结构提供 `Default` + builder，不承诺字面量构造。
- 不使用 unstable feature flag 表达稳定度。
- v1 不发布 crates.io；`0.1.0` 是 workspace 内的语义版本，不是发布承诺。

### 3.2 组装入口（`asterpath`）

```rust
pub struct AsterpathConfig {
    pub database: PathBuf,
    pub providers: Vec<Arc<dyn Provider>>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub compactor: Option<Arc<dyn Compactor>>,   // None = 压缩不可用
    pub store: Option<Arc<dyn SessionStore>>,   // 覆盖 sqlite feature；None 时按 database 打开 SQLite
    pub clock: Arc<dyn Clock>,                  // 默认 SystemClock
    pub approval: Arc<dyn ApprovalPolicy>,      // 默认见 tools.md §4
    pub tool: ToolConfig,                       // 管线并发、截断与外进程超时
    pub model_aliases: BTreeMap<ModelAlias, ModelSpec>,
    pub max_live_children: NonZeroUsize,        // 调度器全局活子会话上限，默认 32
    pub hooks: Vec<Arc<dyn Hooks>>,             // 扩展钩子，按此顺序调用（ADR-0025）
    pub spawn_tool: bool,                       // 是否注册模型可见的 spawn_agent，默认 true
}

impl Asterpath {
    pub fn builder() -> AsterpathBuilder;
    pub async fn create_session(&self, spec: SessionSpec) -> Result<SessionHandle, AsterError>;
    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError>;
    pub async fn shutdown(self) -> Result<(), AsterError>;

    pub fn store(&self) -> Arc<dyn SessionStore>;       // append 经唯一入口送达 on_record；退役 = store().retire(..)
    pub async fn wakeable(&self) -> Result<Vec<SessionId>, AsterError>;  // 唤醒发现（storage.md §7）
    pub fn approval_gate(&self) -> Arc<ApprovalGate>;   // 宿主注册审批端点；见 tools.md §4
    pub async fn usage(&self, scope: UsageScope) -> Result<UsageReport, UsageError>;
    pub async fn trace(&self, session: SessionId) -> Result<Vec<Span>, TraceError>;
}
```

- 存储只有一个注入点 `store`：提供则直接使用（不需要 `database`），未提供时由 `sqlite` feature 按 `database` 打开；没有“部分注入”。注入的存储可以不实现退役（`retire` 返回 `Unsupported`）。
- `AsterpathBuilder` 是常见配置的糖；字段与 `AsterpathConfig` 一一对应（`hooks(Arc<dyn Hooks>)` 可多次调用、逐个追加），`build().await` 按固定顺序直接构造内部组件。
- `spawn_tool(false)`（= `AsterpathConfig.spawn_tool = false`）不注册 `spawn_agent`：`ToolSelection::All` 快照不含它，`Names` 点名它是 `AsterError::Config`；调度器与子会话恢复不受影响。
- 扩展点只有 `Hooks`（`before_step` / `before_tool` / `after_tool` / `on_record`，形状与语义见 [module-contracts.md](module-contracts.md) §3.1、[ADR-0025](../adr/0025-extension-points-are-typed-hooks.md)）。facade 没有服务清单、模块宿主或事件总线。
- 查询是 facade 方法、直接返回值（ADR-0026）：`usage(scope)` 是 scope 所指会话的 `own`（`SessionSubtree` 另含 `descendants`）合计；`trace(session)` 是该会话日志折叠出的全部 span。没有 span 实时推送：实时导出由宿主在 `Hooks::on_record` 上自行增量折叠。
- 再导出：`asterpath::protocol`（= `asterpath-protocol`）、id 类型（`SessionId`、`TurnId`、`StepId`、`InputId`、`ToolCallId`、`LogId`、`Timestamp`）与 `DefaultCompactor`，嵌入者编写钩子与工具只需依赖 `asterpath`。
- 会话配置（`turn` / `compaction` / `subagents`）只有 `SessionSpec` 一个来源，进 `ConfigSnapshot`；`AsterpathConfig` 只放部署级字段（如全局 `max_live_children`）。
- 模型只有 `SessionSpec.model` 一个来源，`AsterpathConfig` 不设默认模型；CLI 用 `--provider <id>` + `--model <id>` 组装该值。
- 注入 `store` 时 `database` 被忽略；未注入时由 `sqlite` feature 从 `database` 构建，此时 `database` 必填。
- `AsterpathConfig` 手写 `Default`：`SystemClock`、内置审批策略、`ToolConfig::default()`、空 alias 集合。`approval` / `tool` / `model_aliases` 是宿主运行时配置，**不进** `ConfigSnapshot`（与审批策略同规则，[tools.md](tools.md) §4）。
- `model_aliases` 在 `build()` 时对照 `ProviderCatalog` 校验：alias 指向的 provider 必须存在，否则 `AsterError::Config`。`ModelAlias` 本身不持久化；spawn 只把解析后的 `ModelSpec` 写进子快照（[subagents.md](subagents.md) §5）。
- 关闭 `sqlite` feature 时必须注入 `store`；退役仍是可选的。
- v1 的 `AsterpathConfig` 没有 MCP 字段；MCP 服务配置属于 `asterpath-mcp`，嵌入者把 `McpServers::tools()` 交给 builder（[plugins.md](plugins.md)）。也没有技能字段：技能由宿主发现并组成 `system` / 工具，见 [skills.md](skills.md) §8。
- 没有 `agent` / `executor` 注入：agent 循环与持久等待是引擎内部类型，不可替换（ADR-0026）。
- `build()` 用 `ProviderCatalog::new` 建 provider 索引；`Provider::id()` 重复是 `AsterError::Config`。
- 内置组件不是可替换模块；替换只经上述适配器字段，观察与拦截只经 `hooks`。`shutdown()` 只释放引擎：没有后台任务，无需收尾顺序。
- `create_session` 先解析 `SessionSpec`（未知工具名、未知 `ModelSpec.provider` = `AsterError::Config`），再写 `SessionCreated { config, parent: None }`。

### 3.3 Session 驱动

```rust
pub struct SessionSpec {
    pub model: ModelSpec,
    pub system: Vec<Part>,
    pub tools: ToolSelection,
    pub request: RequestConfig,
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}

pub enum ToolSelection { All, Names(Vec<ToolName>) }

pub struct UserInput { pub id: InputId, pub parts: Vec<Part> }

impl SessionHandle {
    pub fn id(&self) -> SessionId;
    pub async fn submit_input(&self, input: UserInput) -> Result<InputId, AsterError>;
    pub fn drive(&self) -> BoxStream<'_, Result<AgentEvent, AsterError>>;
    pub async fn cancel(&self) -> Result<(), AsterError>;
    pub async fn state(&self) -> Result<SessionState, AsterError>;
    pub async fn facts(&self) -> Result<SessionFacts, AsterError>;
    pub async fn projection(&self) -> Result<Vec<ProjectedEntry>, AsterError>;
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError>;
    pub async fn approve(
        &self, call: ToolCallId, decision: ApprovalDecision,
    ) -> Result<(), AsterError>;
}
```

- `submit_input` 只 append `InputRecorded`（dedup key = `input.id`）并通知在途回合；session 空闲时**不**自动开回合，调用方随后 `drive()`。
- `state()` 返回恢复分类器的 `SessionState`（[event-log-recovery.md](event-log-recovery.md) §6）：`Idle` 或 `TurnOpen { turn, step }`，委托 `facts()`（会话事实 `SessionFacts`，对日志折叠一次）推导，不依赖驱动者。
- `projection()` 返回对日志折叠出的模型可见投影（每项带来源 log id），不读驱动者内存。
- `drive()` 单飞：同一 session 同时只能有一个驱动者，第二个返回 `AsterError::Agent(AgentError::Busy)`。
- 持久等待（审批 / 子会话）在 `drive()` 内原地 await；流在 `TurnCommitted` 或 `Idle` 后结束。
- `approve()` 写 `ApprovalDecided { tool_call_id, decision }`（dedup key = `tool_call_id`）并唤醒该调用的等待。
- `compact_now()` 仅 Idle 且无活跃驱动者时可用，委托 agent 的 `compact_now`；否则返回 `AgentError::Busy`；`NotConfigured` / `NothingToDo` / `Insufficient` 是 `Ok` 状态（[compaction.md](compaction.md) §5）。
- 没有后台 driver；框架不 spawn。驱动者的生命周期由宿主负责，宿主退出等价于崩溃，恢复走日志。
- 丢弃 stream **不**触发取消：已 append 的记录保留，未闭合意图由恢复处理。取消必须显式调用 `cancel()`。

### 3.4 驱动流（agent 在引擎内部）

```rust
// asterpath-protocol
pub struct RunRequest { pub session: SessionId, pub trigger: RunTrigger }
pub enum RunTrigger { Input(InputId), Resume }

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentEvent {
    Idle { session: SessionId },
    TurnStarted { turn: TurnId },
    ModelDelta { turn: TurnId, step: StepId, output_index: u32, part_index: u32, delta: PartDelta },
    ToolStarted { turn: TurnId, step: StepId, output_index: u32, call: ToolCallId, tool: ToolName },
    ToolFinished { turn: TurnId, step: StepId, call: ToolCallId, status: ToolResultStatus },
    ApprovalRequested(ApprovalRequest),
    TurnCommitted { turn: TurnId, reason: TurnEndReason },
}

#[non_exhaustive]
pub enum AgentError {
    Busy { session: SessionId },
    NotFound { session: SessionId },
    Corrupt { session: SessionId, detail: String },
    Storage(StorageError),
    Execution(ExecutionError),
    Compaction(CompactionError),
}
```

- agent 循环是引擎内部的 `Agent`，不是 trait、不可替换（ADR-0026）；`SessionHandle::drive()` / `compact_now()` 是它的公开入口。
- `AgentEvent` 是实时 UI 流，不是日志；恢复后的历史用 `store()` / `trace(session)`。
- `AgentEvent` 的 JSON 形状属于 Tier 1 兼容承诺：tag 为 `type`、变体名 snake_case（`PartDelta` 同样），ID 沿用现有序列化形式；`AgentEvent` / `ApprovalRequest` / `PartDelta` 只派生 `Serialize`。日志类型（`SessionEvent` 等）的 serde 形状不受影响。
- `(step, output_index)` 是流内块的身份：`ModelDelta.output_index` / `part_index` 原样取自适配器的 `StreamEvent::PartDelta`；`ToolStarted.output_index` 是该调用 item 在本 step 响应中的 `output_index`，UI 据此把此前的 `JsonArgs` 片段归属到 `call`。恢复执行的 step 不记录 `output_index`，此时为 item 在 `AssistantMessage` 中的位置。
- 回合失败（模型不可重试、取消、`MaxStepsExceeded`、`ContextExhausted`）是 `TurnCommitted { reason }`，流正常结束；`AgentError` 只表示无法运行。
- `RunTrigger::Resume` 用于恢复 open turn；`Input` 用于新输入或 steering。恢复位置完全由日志推导，`run` 不接收目标水位。
- `Idle`：没有 open turn 且没有待处理输入时立即发出并结束。
- `compact_now` 只在 Idle 时执行；`Busy` 表示 session 正在被驱动或回合未闭合。它与 pre-step 压缩共用同一 `Compactor` 与事件路径（[compaction.md](compaction.md) §5）。

### 3.5 错误

```rust
// asterpath（facade）
#[non_exhaustive]
pub enum AsterError {
    Config { message: String },
    Agent(AgentError),
    Storage(StorageError),
    Execution(ExecutionError),
    Provider(ProviderError),
    Tool(ToolError),
}
```

- 各层错误保持自己的枚举；`AsterError` 只做求和，不加语义。
- 没有 `ProtocolError`：protocol 的解码失败是 `LogDecodeError`，在 agent 恢复中归为 `AgentError::Corrupt`，存储侧归为 `StorageError::Corrupt`；facade 不暴露内部信封错误。
- 库契约不暴露 `anyhow` / `Box<dyn Error>`；`anyhow` 只允许出现在 CLI 二进制。

### 3.6 Feature flags

| crate | features | 默认 |
| --- | --- | --- |
| `asterpath` | `anthropic`, `openai`, `sqlite` | `anthropic`, `openai`, `sqlite` |
| `asterpath-cli` | 同 facade | 同 facade |
| 适配器 crate | 无 | — |
| `protocol` | 无 | — |

- `sqlite` 关闭时必须经 `AsterpathConfig.store` 注入实现。
- facade 没有 MCP feature；`asterpath-mcp` 是独立的适配器 crate（[plugins.md](plugins.md)）。
- feature 不得改变公共契约类型（ADR-0026）。

## 4. 参考 CLI

二进制名 `asterpath`，包名 `asterpath-cli`。

| 命令 | 行为边界 |
| --- | --- |
| `asterpath run [OPTIONS] <PROMPT>` | 创建新 session，`submit_input` 后 `drive()` 到 `TurnCommitted`；stdout 输出最终 assistant 文本，stderr 输出 session id 与诊断 |
| `asterpath resume [OPTIONS] <SESSION>` | `open_session` 后 `drive()`；有 open turn 或待处理输入则继续，否则输出 `idle` |
| `asterpath inspect [OPTIONS] <SESSION>` | 打印投影摘要与未闭合意图；`--json` 输出完整投影 |
| `asterpath trace [OPTIONS] <SESSION>` | 打印 span 树（`Asterpath::trace`）；`--json` |
| `asterpath usage [OPTIONS] <SESSION>` | 分列 `own` / `descendants`；`--json` |
| `asterpath config check [OPTIONS]` | 解析与校验配置，不打开数据库、不初始化引擎；加载 `[mcp_servers]` 并逐个打印状态后关闭（`mcp: <id> loaded: <tools>` / `mcp: <id> failed at <stage>: <message>`）；成功退出 0，配置错误或 `required` 服务失败退出 2 |

通用选项：`--config <path>`、`--db <path>`、`--provider <id>`、`--model <id>`、`--approval <ask|allow|deny>`、`--json`。

- `run` 不接受 `--session`；要续跑已有 session 用 `resume`。
- 退出码：`0` 完成或 idle，`1` 运行失败（`TurnCommitted` 非 `Completed`、存储/模块错误），`2` 用法或配置错误。没有"停在等待点"的退出码。
- 审批端：`drive()` 流里的 `ApprovalRequested` 由 CLI 处理。策略取 `--approval` > `[approval] default` > `ask`。TTY 上 `ask` 提示工具名、参数、`replay` / `side_effect` / `provenance`；非 TTY 上来自配置或缺省的 `ask` 一律降级为 `deny`（无法提示），而显式 `--approval ask` 是用法错误（退出码 2）。CLI 仅在会提示时（TTY + `ask`）注册端点；没注册时框架的 `Ask` 立即按 `Deny` 处理（[tools.md](tools.md) §4）。
- 审批决定经 `SessionHandle::approve()` 投递，落日志；重启后复用已记录决定，不再提示。
- MCP（ADR-0027）：`run` / `resume` 在建会话前 `McpServers::load`，工具交给 builder；非 `required` 的失败服务打印到 stderr 后跳过，`required` 失败按配置错误退出 2。外部工具（`<id>__<tool>`）默认 `Ask`，走上面的审批路径。退出时先 `Asterpath::shutdown()` 再 `McpServers::shutdown()`。
- 工作区工具（#83，`asterpath-tools`；不同于供应商的服务端工具）：`run` 默认以工作目录为根、按 `[sandbox]` 策略调用 `asterpath_tools::coding(...)` 注册六个工具；`[tools] enabled = [...]` 或 `run --tools a,b` 选子集（flag 优先），`run --no-tools` 全关，未知工具名是配置错误（退出码 2）。`resume` 总是注册全部六个，模型可见的子集由 `SessionCreated` 快照决定。`ask` 模式下 `SideEffect::Mutating` 工具（`write_file` / `edit_file` / `bash`）在端点打开时 `Ask`、关闭时以 `NO_APPROVAL_ENDPOINT` 拒绝；其余按框架默认策略。
- 压缩：`run` / `resume` 总是装配 `DefaultCompactor`；是否压缩取决于 `[model] context_window`，未配置时压缩路径为 `NotConfigured`。

### 4.1 配置

`asterpath.toml`，默认从 `--config` 或 `ASTERPATH_CONFIG` 取；不存在时使用默认值。优先级：CLI flag > 环境变量 > 文件。

```toml
[model]
context_window = 200000       # 映射到 ModelSpec.context_window；省略 = 不自动压缩

[sandbox]
mode = "workspace-write"      # read-only | workspace-write（默认，可写根 = 工作目录）| full-access

[mcp_servers.docs]            # 表名即 server id（[a-z0-9-]{1,32}），工具名为 docs__<tool>
command = "docs-mcp"          # stdio：command / args / env / cwd
args = ["--stdio"]
env = { DOCS_ROOT = "/srv/docs" }
required = false              # true：加载失败即配置错误（退出码 2）
# protocol_versions = ["2026-07-28"]   # 省略 = 适配器支持的全部版本

[mcp_servers.search]
url = "https://mcp.example.com/mcp"   # http：url / headers；与 command 二选一
headers = { "X-Team" = "core" }

[storage]
path = "~/.local/share/asterpath/sessions.db"   # 默认 XDG_DATA_HOME/asterpath/sessions.db

[providers.anthropic]
base_url = "https://api.anthropic.com"

[providers.openai]
base_url = "https://api.openai.com/v1"

# 供应商实例（ADR-0029）：表名即 provider id；非内置名必须写 protocol
# 示例取值为宿主策略，DeepSeek 端点与档位按 2026-10-24 实测（#106），不随框架维护
[providers.deepseek]
protocol = "anthropic"                          # anthropic | openai
base_url = "https://api.deepseek.com/anthropic"
api_key_env = "DEEPSEEK_API_KEY"                # 缺省取协议原有变量；key 只从环境读

[providers.deepseek.profile]                    # 实例默认；缺省字段取 LATEST
efforts = ["off", "low", "high", "max"]         # 另有 thinking（仅 anthropic）、thinking_default_on（仅 anthropic）、forced_tool_choice、sampling
thinking_default_on = true

[providers.deepseek.models."deepseek-v4-pro"]   # 按前缀，长者优先；完整 profile，不继承 profile
efforts = ["low", "high", "max"]

[turn]
# max_steps_per_turn = 100    # 省略 = 不设上限（None）；0 是配置错误；其余字段用默认值

[request]
max_output_tokens = 8192      # RequestConfig；缺省字段用 Default
tool_choice = "auto"          # auto | none | required | <tool name>
disable_parallel_tool_use = false
# [request.sampling] temperature / top_p / top_k / seed
# [request.reasoning] effort（off | minimal | low | medium | high | xhigh | max）/ budget_tokens / summary

[tools]
max_parallel_calls = 8        # ToolConfig；缺省字段用 Default
# enabled = ["read_file", "list_dir", "grep"]   # 新 session 的工作区工具；省略 = 全部六个

[model_aliases.fast]          # 宿主提供的 alias 集合，spawn 用它收窄模型
provider = "anthropic"
model = "claude-fable-5-1"

[compaction]
# CompactionConfig 字段

[subagents]
# SubagentConfig 字段（进 SessionSpec）
# max_live_children = 32      # 例外：调度器全局上限，映射到 AsterpathConfig.max_live_children

[approval]
default = "ask"               # ask | allow | deny；TTY 默认，非 TTY 降级为 deny
```

- `max_steps_per_turn = 0` 是配置错误（退出码 2）；不设上限靠省略该字段。
- API key **只从环境变量读**：`ANTHROPIC_API_KEY`、`OPENAI_API_KEY`。配置文件出现 `api_key` / `secret` / `token` 字段直接报错（退出码 2）。
- 环境变量覆盖：`ASTERPATH_CONFIG`、`ASTERPATH_DB`、`ASTERPATH_PROVIDER`、`ASTERPATH_MODEL`。
- `base_url` 是为验收与自建网关准备的；指向 mock server 时不会走真实网络。
- 未知字段（任一层级）是配置错误（退出码 2）。
- 沙箱约束 stdio MCP 服务进程（HTTP 不沙箱）与内置 `bash` 工具启动的进程；没有后端时 `bash` 同样 fail closed（返回 `Denied`，不启动进程）。Linux 上受限模式用 `LinuxSandbox`（bwrap）；找不到 bwrap 或在其他平台上，受限模式没有后端，stdio 服务按 fail closed 在 `Sandbox` 阶段加载失败，原因见状态行；只有显式 `full-access` 才不经沙箱启动。

## 5. 交付清单与里程碑

| 文件 | 内容 |
| --- | --- |
| `docs/spec/README.md` | spec 入口：目的地、阅读顺序、规范/历史、未决、验证协议 |
| `docs/spec/acceptance.md` | 三段验收场景的精确断言与自动化设计 |
| `docs/design/packaging.md` | 本文件：crate 图、API、CLI、交付清单、里程碑 |
| `docs/adr/0026-engine-is-one-crate-only-adapter-traits-public.md` | crate 划分、稳定面与驱动模型的持久决策 |
| `deps-allowlist.toml` | M0 创建；依赖白名单的机器可读来源 |
| `xtask/` | M0 创建；`cargo xtask check-deps` |

里程碑是**验收闸门**，不是实现计划：

| 里程碑 | 范围 | 通过条件 |
| --- | --- | --- |
| M0 骨架 | 7 个 crate：`core` / `protocol` / `storage-sqlite` / `agent` / `tools` / `testing` / facade，加 `xtask` 与 `ScriptedProvider` | 编译通过；`cargo xtask check-deps` 通过；单回合（模型 + 工具）可跑 |
| M1 恢复 | 场景 (a)；引入 `asterpath-acceptance` | `acceptance` 的 A1–A3 通过 |
| M2 双协议 | 两个 provider crate + adapter 测试 | 场景 (b) 通过 |
| M3 子智能体 | `runtime` / `scheduler` / 子会话 + tracing / usage；默认 compactor 可选接入 | 场景 (c) 通过 |
| M4 CLI | `asterpath-cli` 六命令、配置、审批端 | CLI 场景测试通过 |
| M5 扩展（已交付） | `asterpath-mcp`（MCP 服务配置、子进程、MCP adapter；#80 前为 `asterpath-plugin-host`）、沙箱合约（#80 起在 protocol）与 Linux 后端 | 不参与 v1 验收；闸门见 tracker 侧 M5 实现 spec（六张子票 + T6 端到端） |
| M6 技能（已交付） | `asterpath-skills`、CLI `[skills]` 与 `run --no-skills`、发现/索引/`load_skill` | 不参与 v1 验收；场景见 [skills.md](skills.md) §11（S1–S10） |

- M5 不是 v1 的验收闸门：它收纳设计已定、但不在 M0–M4 交付内的扩展（[#16](https://github.com/zibyn/asterpath/issues/16) / [#20](https://github.com/zibyn/asterpath/issues/20) / [#21](https://github.com/zibyn/asterpath/issues/21)）；M5 / M6 的成员已在 workspace 中，但不属于 v1 验收闸门。插件分发、ACP 与 WASM 出界，归 host 或生态工具。
- M6 与 M5 同性质：不属于 v1 验收闸门；技能不新增事件，日志 schema 不变。
- 上表的 crate 名是各里程碑交付时的划分；2026-10-23 起引擎各模块已并入 `asterpath`（§2.2）。
- M0 组装 `compactor: None`；默认 compactor（`DefaultCompactor`）从 M3 起可选接入，不进 M0–M4 通过条件（[compaction.md](compaction.md) §2）。
- CI 顺序：`cargo xtask check-deps` → `cargo fmt --check` → `cargo clippy --workspace --all-targets -- -D warnings` → `cargo test --workspace` → `cargo test -p asterpath-acceptance --test acceptance` → `cargo doc --workspace --no-deps`。
- 另跑 feature 矩阵：`cargo check -p asterpath --no-default-features` 与 `--features anthropic` / `openai` / `sqlite` 各自组合。
- CI 分层与超时（[testing.md](testing.md) §6）：`cargo test --workspace` 预算 5 分钟；进程级验收预算 5 分钟、单 marker 等待上限 15 秒；feature 矩阵 5 分钟。
- 验收测试只在 Linux CI 上跑（`cfg(unix)`）；框架本身不禁 Windows，但 v1 不为验收提供 Windows 的强杀实现。进程级与 failpoint 测试不得 `#[ignore]`。

### 5.1 默认值性质

下列数值是**默认值**，不是不变量。不变量只在各文档的「保证 / 验收」节。

| 值 | 性质 | 归口 |
| --- | --- | --- |
| `initial_backoff` 1s、`max_backoff` 30s、`request_timeout` 600s、`cancel_grace` 10s | 可配置默认 | `TurnConfig`（agent-loop.md §10） |
| `max_output_bytes` 64 KiB、`max_output_bytes_per_step` 256 KiB、`external_timeout` 120s、`kill_grace` 5s | 可配置默认 | `ToolConfig`（tools.md §9） |
| `max_depth` 3、每父并发 8、全局 32 | 可配置默认 | `SubagentConfig`（subagents.md） |
| `target_ratio` 0.5、`max_summary_tokens` 2048 | 可配置默认 | `CompactionConfig`（compaction.md §8） |
| `bytes/3` 估算 | 实现常量（口径，不是可调参数） | compaction 模块内部 |
| CI 5 分钟 / 单 marker 15 秒 | 运维目标（非契约） | CI 配置 |

## 6. 验证协议：spec 是否可开工

两道闸，均在**干净 worktree、全新会话**中执行；执行者没有本图、issue 或先前对话的上下文，只读 `docs/spec/README.md` 及其链接的规范章节。

### G1 计划闸（先跑，便宜）

Prompt：

```text
只读 docs/spec/README.md 与它链接的规范文档。不要提问，不要读 issue。
产出：
1. 每个 crate 的文件清单与职责；
2. M0–M4 的实现顺序；
3. 每条不变量与每个验收断言到测试的映射表。
不确定的信息写进 BLOCKERS.md，每条注明 spec 中缺失的内容。
```

通过 = `BLOCKERS.md` 为空（或只含出界项），且映射表覆盖 spec 中全部不变量。

### G2 骨架闸（后跑，时间盒半天）

Prompt：

```text
只读 docs/spec/README.md 与它链接的规范文档。不要提问，不要读 issue。
实现 M0（walking skeleton）并通过验收场景 A 的最小测试。
阻塞写进 BLOCKERS.md；超时即失败。
```

通过 = 编译通过 + A1 通过 + `BLOCKERS.md` 为空。

- 每次出现的阻塞项回灌为 spec 缺陷，由设计会话修复；不让执行者去读 issue 或问人。
- G1/G2 的结果作为 comment 记录在 spec 入口对应的 tracker issue 上。
