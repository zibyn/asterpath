# 打包、公共 API 与参考 CLI

> 来源：[#15](https://github.com/zibyn/asterpath/issues/15)，2026-10-22 已定。
> 相关 ADR：[0015 打包与稳定面](../adr/0015-packaging-and-stability.md)；依赖方向见 [ADR-0001](../adr/0001-dependency-direction.md)。
> 本文件是 v1 spec 的打包章节；验收场景的规范在 [acceptance.md](../spec/acceptance.md)，入口在 [spec/README.md](../spec/README.md)。

## 1. 决策摘要

- v1 workspace = 16 个库/应用成员 + 1 个 `xtask`，按里程碑增量创建（M0 只建 7 个 crate + `xtask`）；一模块一 crate，模块之间零依赖。M5 扩展再加 4 个成员（`asterpath-plugin-host`、`asterpath-sandbox`、`asterpath-sandbox-linux`、`asterpath-mcp-fixture`），共 20 个库/应用成员 + 1 个 `xtask`；M6 再加 1 个成员（`asterpath-skills`），共 21 个库/应用成员 + 1 个 `xtask`。
- 依赖方向固定 `core ← protocol ← modules ← app`，由 `cargo xtask check-deps` 对照根目录 `deps-allowlist.toml` 检查。
- 全部成员 lockstep `0.1.0`；**稳定面只有 facade、core、protocol 与日志 schema**，模块 crate 是 0.x 实验面。
- v1 **不发布 crates.io**；嵌入者用 git/path 依赖。
- `core` / `protocol` 无 feature；feature 只出现在 facade、CLI 与后端 crate。
- 没有后台 driver：宿主显式调用 `SessionHandle::drive()`；同一 session 单飞；丢弃流不等于取消。
- 参考 CLI 六命令，退出码 `0/1/2`；审批在 TTY 上默认 `ask`，非 TTY 默认 `deny`。
- 三段验收由 `asterpath-acceptance` 的 `testdrive` 二进制 + 真 `SIGKILL` 驱动；#19 只负责五个注入点工具链。
- v1 的 provider/model 在 session 创建时快照，**不支持 turn 内切换**；验收 (b) 验证的是 adapter 可替换性与规范化历史等价。

## 2. Workspace 成员与依赖

| crate | 职责 | 允许的 workspace 依赖 | 引入里程碑 |
| --- | --- | --- | --- |
| `asterpath-core` | 模块宿主、事件总线、`EventLog` / `Storage` / `SessionCatalog` / `DurableExecutor` 契约、ID、错误 | 无 | M0 |
| `asterpath-protocol` | 规范化模型、`Provider` / `Tool` / `ToolRegistry` / `AgentRunner` / `Compactor`、会话事件、`ConfigSnapshot`、`AgentEvent` | `core` | M0 |
| `asterpath-agent` | `AgentRunner` 实现（turn/step loop） | `core`, `protocol` | M0 |
| `asterpath-tools` | `ToolRegistry` 实现 | `core`, `protocol` | M0 |
| `asterpath-storage-sqlite` | `EventLog` + `Storage` + `SessionCatalog` + `SessionRetirement` 的 SQLite 实现 | `core`, `protocol` | M0 |
| `asterpath-runtime` | `DurableExecutor` 实现（timer / signal / wake） | `core`, `protocol` | M3 |
| `asterpath-scheduler` | `SubagentSpawner` 实现；子会话准入与等待 | `core`, `protocol` | M3 |
| `asterpath-compaction` | `Compactor` 实现（摘要压缩） | `core`, `protocol` | M3；**可选装配**，不进通过条件 |
| `asterpath-provider-anthropic` | Anthropic Messages adapter | `core`, `protocol` | M2 |
| `asterpath-provider-openai` | OpenAI Responses adapter | `core`, `protocol` | M2 |
| `asterpath-tracing` | `TraceQuery` / `TraceTap` 实现 | `core`, `protocol` | M3 |
| `asterpath-usage` | `UsageQuery` 实现 | `core`, `protocol` | M3 |
| `asterpath-testing` | in-memory fakes、`ScriptedProvider`、四个故障注入装饰器 + `ManualClock`（五点，见 [testing.md](testing.md)） | `core`, `protocol` | M0 |
| `asterpath` | facade / 默认组装根；暴露稳定 API | 任意 | M0 |
| `asterpath-cli` | 参考 CLI 二进制 `asterpath` | 任意 | M4 |
| `asterpath-acceptance` | `testdrive` 二进制与三段验收测试；`publish = false` | 任意 | M1 |
| `xtask` | `check-deps` 与 CI 辅助；`publish = false` | 无 | M0 |
| `asterpath-plugin-host` | 外部插件清单、子进程与 MCP adapter（[mcp.md](mcp.md)）；ACP 不做 | `core`, `protocol`, `sandbox` | M5（post-v1） |
| `asterpath-sandbox` | 零依赖沙箱合约：`SandboxProvider` / `SpawnRequest` / `ConfinedSpawn` / 策略与错误（[sandbox.md](sandbox.md)） | 无 | M5（post-v1） |
| `asterpath-sandbox-linux` | Linux bwrap 参考后端 | `sandbox` | M5（post-v1） |
| `asterpath-mcp-fixture` | 测试用 MCP server（stdio / HTTP）；`publish = false` | 无（仅外部依赖） | M5（post-v1） |
| `asterpath-skills` | 技能包解析、索引渲染与 `load_skill` 工具（[skills.md](skills.md)） | `protocol` | M6（post-v1） |

- `asterpath-sandbox-macos` 仍为预留名，M5 不建。`asterpath-storage-admin` 不建（[#18](https://github.com/zibyn/asterpath/issues/18)：策略与归档由宿主实现）；MCP adapter 并入 `asterpath-plugin-host`，ACP 不做（[#20](https://github.com/zibyn/asterpath/issues/20)）。
- `asterpath-skills` 是 M6（post-v1）新增的可选模块；只依赖 `protocol` 与外部 `saphyr`。创建 crate 时同步 `deps-allowlist.toml`（`role = "module"`，`allow = ["asterpath-protocol"]`）。技能不新增事件，日志 schema 不变。
- `asterpath-testing` 维持合约依赖白名单，**不得**依赖 facade 或具体模块；需要组装真实模块的测试一律放 `asterpath-acceptance`。反向的 `[dev-dependencies]` 允许：模块 crate 可 dev-depend on `asterpath-testing` 跑共享契约套件（SQLite 即如此），`cargo xtask check-deps` 只校验普通依赖边。
- 机器可读白名单在根目录 `deps-allowlist.toml`（M0 创建，随里程碑增长）；上表是它的规范副本，冲突以文件为准并由 `xtask` 报错。
- `xtask` 校验：workspace 依赖边在白名单内；任意两个模块 crate 之间零边；`core` / `protocol` 的 `features` 为空。

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
- MCP/插件配置不进快照：plugin-host 归 M5（post-v1）；将来纳入时按 log schema 规则 bump 版本（[ADR-0004](../adr/0004-log-schema-version-and-upcasting.md)）。

## 3. 公共 API

类型归属：`SessionSpec` / `ToolSelection` / `UserInput` / `ConfigSnapshot` / `AgentEvent` / `AgentError` / `RunRequest` / `SessionState` / `ModelAlias` / `ApprovalDecision` / `CancelToken` / `ApprovalPolicy` / `ToolConfig` / `Compactor` / `CompactResult` 在 `asterpath-protocol`；`InputId` 与其他 opaque ID、`Clock` / `Timestamp` 在 `asterpath-core`；`Asterpath` / `AsterpathBuilder` / `AsterpathConfig` / `SessionHandle` / `AsterError` 在 facade。`PluginManifest` 属于 M5 的 `asterpath-plugin-host`，不属于 v1 公共 API；技能相关类型属于 M6 的 `asterpath-skills`，同样不属于 v1 公共 API。

### 3.1 稳定层级与版本

| 层级 | 内容 | 兼容承诺 |
| --- | --- | --- |
| Tier 1 | `asterpath` 的公开项；`asterpath-core` 与 `asterpath-protocol` 的公开项；日志 schema（`LOG_SCHEMA_VERSION` + upcast） | semver；0.x 期间破坏性变更 bump minor，兼容变更 bump patch |
| Tier 2 | 模块 crate 的公开项 | 0.x 实验面；minor 内可改，变更写入对应 design 文档 |
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
    pub agent: Option<Arc<dyn AgentRunner>>,
    pub compactor: Option<Arc<dyn Compactor>>,   // None = 压缩不可用
    pub event_log: Option<Arc<dyn EventLog>>,   // 覆盖 sqlite feature
    pub storage: Option<Arc<dyn Storage>>,
    pub clock: Arc<dyn Clock>,                  // 默认 SystemClock
    pub approval: Arc<dyn ApprovalPolicy>,      // 默认见 tools.md §4
    pub tool: ToolConfig,                       // 管线并发、截断与外进程超时
    pub model_aliases: BTreeMap<ModelAlias, ModelSpec>,
    pub session_catalog: Option<Arc<dyn SessionCatalog>>,
    pub session_retirement: Option<Arc<dyn SessionRetirement>>,
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}

impl Asterpath {
    pub fn builder() -> AsterpathBuilder;
    pub async fn create_session(&self, spec: SessionSpec) -> Result<SessionHandle, AsterError>;
    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError>;
    pub async fn shutdown(self) -> Result<(), AsterError>;

    pub fn event_log(&self) -> Arc<dyn EventLog>;
    pub fn storage(&self) -> Arc<dyn Storage>;
    pub fn session_catalog(&self) -> Arc<dyn SessionCatalog>;
    pub fn session_retirement(&self) -> Option<Arc<dyn SessionRetirement>>;  // 后端未提供时为 None
    pub fn approval_gate(&self) -> Arc<ApprovalGate>;   // 宿主注册审批端点；见 tools.md §4
    pub fn usage(&self) -> Arc<dyn UsageQuery>;
    pub fn trace(&self) -> Arc<dyn TraceQuery>;
}
```

- `AsterpathBuilder` 是常见配置的糖；字段与 `AsterpathConfig` 一一对应，`build().await` 执行拓扑初始化。
- 模型只有 `SessionSpec.model` 一个来源，`AsterpathConfig` 不设默认模型；CLI 用 `--provider <id>` + `--model <id>` 组装该值。
- `event_log` / `storage` 要么同时注入，要么都留空；只注入其一是 `AsterError::Config`。同时注入时 `database` 被忽略；都留空时由 `sqlite` feature 从 `database` 构建，此时 `database` 必填。
- `AsterpathConfig` 手写 `Default`：`SystemClock`、内置审批策略、`ToolConfig::default()`、空 alias 集合。`approval` / `tool` / `model_aliases` 是宿主运行时配置，**不进** `ConfigSnapshot`（与审批策略同规则，[tools.md](tools.md) §4）。
- `model_aliases` 在 `build()` 时对照 `ProviderCatalog` 校验：alias 指向的 provider 必须存在，否则 `AsterError::Config`。`ModelAlias` 本身不持久化；spawn 只把解析后的 `ModelSpec` 写进子快照（[subagents.md](subagents.md) §5）。
- 关闭 `sqlite` feature 时，`event_log` / `storage` 必须注入，`session_catalog` 也必须注入；`session_retirement` 仍是可选的。
- v1 的 `AsterpathConfig` 没有插件字段；插件清单属于 M5 的 `asterpath-plugin-host`（[plugins.md](plugins.md)）。也没有技能字段：技能由宿主发现并组成 `system` / 工具，见 [skills.md](skills.md) §8。
- 没有 `executor` 注入：v1 只有一套 `DurableExecutor` 实现；需要替换它的嵌入者直接使用 `asterpath-core` 的 `ModuleHost`。
- `build()` 用 `ProviderCatalog::new` 建 provider 索引；`Provider::id()` 重复是 `AsterError::Config`。
- 替换超过一个模块、或需要自定义 `Module` 生命周期的嵌入者，直接使用 `asterpath-core` 的 `ModuleHost`；facade 不覆盖该场景。
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
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError>;
    pub async fn approve(
        &self, call: ToolCallId, decision: ApprovalDecision,
    ) -> Result<(), AsterError>;
}
```

- `submit_input` 只 append `InputRecorded`（dedup key = `input.id`）并通知在途回合；session 空闲时**不**自动开回合，调用方随后 `drive()`。
- `state()` 返回恢复分类器的 `SessionState`（[event-log-recovery.md](event-log-recovery.md) §6）：`Idle` 或 `TurnOpen { turn, step }`，由 `OpenIntents` 折叠推导，不依赖驱动者。
- `drive()` 单飞：同一 session 同时只能有一个驱动者，第二个返回 `AsterError::Agent(AgentError::Busy)`。
- 持久等待（timer / signal / 子会话）在 `drive()` 内原地 await；流在 `TurnCommitted` 或 `Idle` 后结束。
- `approve()` 是 `DurableExecutor::deliver_signal` 的便捷封装（signal name = `tool.approval.<tool_call_id>`，payload = `ApprovalDecision`）。
- `compact_now()` 仅 Idle 且无活跃驱动者时可用，委托 `AgentRunner::compact_now`；否则返回 `AgentError::Busy`；`NotConfigured` / `NothingToDo` / `Insufficient` 是 `Ok` 状态（[compaction.md](compaction.md) §5）。
- 没有后台 driver；框架不 spawn。驱动者的生命周期由宿主负责，宿主退出等价于崩溃，恢复走日志。
- 丢弃 stream **不**触发取消：已 append 的记录保留，未闭合意图由恢复处理。取消必须显式调用 `cancel()`。

### 3.4 `AgentRunner` 契约（`asterpath-protocol`）

```rust
pub struct RunRequest { pub session: SessionId, pub trigger: RunTrigger }
pub enum RunTrigger { Input(InputId), Resume }

#[non_exhaustive]
pub enum AgentEvent {
    Idle { session: SessionId },
    TurnStarted { turn: TurnId },
    ModelDelta { turn: TurnId, step: StepId, delta: PartDelta },
    ToolStarted { turn: TurnId, step: StepId, call: ToolCallId, tool: ToolName },
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
    Lifecycle(LifecycleError),
    Compaction(CompactionError),
}

pub trait AgentRunner: Send + Sync {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>>;
    fn compact_now(&self, session: SessionId)
        -> BoxFuture<'_, Result<CompactResult, AgentError>>;
}
```

- `AgentEvent` 是实时 UI 流，不是日志；恢复后的历史用 `EventLog` / `TraceQuery`。
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
    Lifecycle(LifecycleError),
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
| 模块 crate | 无 | — |
| `core` / `protocol` | 无 | — |

- `sqlite` 关闭时必须经 `AsterpathConfig.event_log` / `.storage` 注入实现。
- v1 没有 `plugin-host` feature；插件宿主是 M5 的独立扩展（[plugins.md](plugins.md)）。
- feature 不得改变公共契约类型（ADR-0001）。

## 4. 参考 CLI

二进制名 `asterpath`，包名 `asterpath-cli`。

| 命令 | 行为边界 |
| --- | --- |
| `asterpath run [OPTIONS] <PROMPT>` | 创建新 session，`submit_input` 后 `drive()` 到 `TurnCommitted`；stdout 输出最终 assistant 文本，stderr 输出 session id 与诊断 |
| `asterpath resume [OPTIONS] <SESSION>` | `open_session` 后 `drive()`；有 open turn 或待处理输入则继续，否则输出 `idle` |
| `asterpath inspect [OPTIONS] <SESSION>` | 打印投影摘要与未闭合意图；`--json` 输出完整投影 |
| `asterpath trace [OPTIONS] <SESSION>` | 打印 span 树（`TraceQuery`）；`--json` |
| `asterpath usage [OPTIONS] <SESSION>` | 分列 `own` / `descendants`；`--json` |
| `asterpath config check [OPTIONS]` | 只解析与校验配置，不初始化模块；成功退出 0，失败退出 2 |

通用选项：`--config <path>`、`--db <path>`、`--provider <id>`、`--model <id>`、`--approval <ask|allow|deny>`、`--json`。

- `run` 不接受 `--session`；要续跑已有 session 用 `resume`。
- 退出码：`0` 完成或 idle，`1` 运行失败（`TurnCommitted` 非 `Completed`、存储/模块错误），`2` 用法或配置错误。没有"停在等待点"的退出码。
- 审批端：`drive()` 流里的 `ApprovalRequested` 由 CLI 处理。策略取 `--approval` > `[approval] default` > `ask`。TTY 上 `ask` 提示工具名、参数、`replay` / `side_effect` / `provenance`；非 TTY 上来自配置或缺省的 `ask` 一律降级为 `deny`（无法提示），而显式 `--approval ask` 是用法错误（退出码 2）。CLI 仅在会提示时（TTY + `ask`）注册端点；没注册时框架的 `Ask` 立即按 `Deny` 处理（[tools.md](tools.md) §4）。
- 审批决定经 `SessionHandle::approve()` 投递，落日志；重启后复用已记录决定，不再提示。

### 4.1 配置

`asterpath.toml`，默认从 `--config` 或 `ASTERPATH_CONFIG` 取；不存在时使用默认值。优先级：CLI flag > 环境变量 > 文件。

```toml
[storage]
path = "~/.local/share/asterpath/sessions.db"   # 默认 XDG_DATA_HOME/asterpath/sessions.db

[providers.anthropic]
base_url = "https://api.anthropic.com"

[providers.openai]
base_url = "https://api.openai.com/v1"

[turn]
# max_steps_per_turn = 100    # 省略 = 不设上限（None）；0 是配置错误；其余字段用默认值

[request]
max_output_tokens = 8192      # RequestConfig；缺省字段用 Default
tool_choice = "auto"          # auto | none | required | <tool name>
disable_parallel_tool_use = false
# [request.sampling] temperature / top_p / top_k / seed
# [request.reasoning] effort / budget_tokens / summary

[tools]
max_parallel_calls = 8        # ToolConfig；缺省字段用 Default

[model_aliases.fast]          # 宿主提供的 alias 集合，spawn 用它收窄模型
provider = "anthropic"
model = "claude-fable-5-1"

[compaction]
# CompactionConfig 字段

[subagents]
# SubagentConfig 字段

[approval]
default = "ask"               # ask | allow | deny；TTY 默认，非 TTY 降级为 deny
```

- `max_steps_per_turn = 0` 是配置错误（退出码 2）；不设上限靠省略该字段。
- API key **只从环境变量读**：`ANTHROPIC_API_KEY`、`OPENAI_API_KEY`。配置文件出现 `api_key` / `secret` / `token` 字段直接报错（退出码 2）。
- 环境变量覆盖：`ASTERPATH_CONFIG`、`ASTERPATH_DB`、`ASTERPATH_PROVIDER`、`ASTERPATH_MODEL`。
- `base_url` 是为验收与自建网关准备的；指向 mock server 时不会走真实网络。

## 5. 交付清单与里程碑

| 文件 | 内容 |
| --- | --- |
| `docs/spec/README.md` | spec 入口：目的地、阅读顺序、规范/历史、未决、验证协议 |
| `docs/spec/acceptance.md` | 三段验收场景的精确断言与自动化设计 |
| `docs/design/packaging.md` | 本文件：crate 图、API、CLI、交付清单、里程碑 |
| `docs/adr/0015-packaging-and-stability.md` | crate 划分、稳定面与驱动模型的持久决策 |
| `deps-allowlist.toml` | M0 创建；依赖白名单的机器可读来源 |
| `xtask/` | M0 创建；`cargo xtask check-deps` |

里程碑是**验收闸门**，不是实现计划：

| 里程碑 | 范围 | 通过条件 |
| --- | --- | --- |
| M0 骨架 | 7 个 crate：`core` / `protocol` / `storage-sqlite` / `agent` / `tools` / `testing` / facade，加 `xtask` 与 `ScriptedProvider` | 编译通过；`cargo xtask check-deps` 通过；单回合（模型 + 工具）可跑 |
| M1 恢复 | 场景 (a)；引入 `asterpath-acceptance` | `acceptance` 的 A1–A3 通过 |
| M2 双协议 | 两个 provider crate + adapter 测试 | 场景 (b) 通过 |
| M3 子智能体 | `runtime` / `scheduler` / 子会话 + tracing / usage；`asterpath-compaction` 可选接入 | 场景 (c) 通过 |
| M4 CLI | `asterpath-cli` 六命令、配置、审批端 | CLI 场景测试通过 |
| M5 扩展（已交付） | `asterpath-plugin-host`（清单、子进程、MCP adapter）、`asterpath-sandbox` 合约与 Linux 后端 | 不参与 v1 验收；闸门见 tracker 侧 M5 实现 spec（六张子票 + T6 端到端） |
| M6 技能（已设计） | `asterpath-skills`、CLI `[skills]` 与 `run --no-skills`、发现/索引/`load_skill` | 不参与 v1 验收；场景见 [skills.md](skills.md) §11（S1–S10） |

- M5 不是 v1 的验收闸门：它收纳设计已定、但不在 M0–M4 交付内的扩展（[#16](https://github.com/zibyn/asterpath/issues/16) / [#20](https://github.com/zibyn/asterpath/issues/20) / [#21](https://github.com/zibyn/asterpath/issues/21)）；v1 workspace 在 M4 截止，M5 成员在 M5 才加入。插件分发、ACP 与 WASM 出界，归 host 或生态工具。
- M6 与 M5 同性质：设计已定、post-v1；不新增事件，日志 schema 不变；`asterpath-skills` 加入 workspace 时同步 `deps-allowlist.toml`。
- M0 组装 `compactor: None`；`asterpath-compaction` 从 M3 起可选接入，不进 M0–M4 通过条件（[compaction.md](compaction.md) §2）。
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
