# 打包、公共 API 与参考 CLI

> 来源：[#15](https://github.com/zibyn/asterpath/issues/15)，2026-10-22 已定。
> 相关 ADR：[0015 打包与稳定面](../adr/0015-packaging-and-stability.md)；依赖方向见 [ADR-0001](../adr/0001-dependency-direction.md)。
> 本文件是 v1 spec 的打包章节；验收场景的规范在 [acceptance.md](../spec/acceptance.md)，入口在 [spec/README.md](../spec/README.md)。

## 1. 决策摘要

- workspace = 17 个库/应用成员 + 1 个 `xtask`；一模块一 crate，模块之间零依赖。
- 依赖方向固定 `core ← protocol ← modules ← app`，由 `cargo xtask check-deps` 对照根目录 `deps-allowlist.toml` 检查。
- 全部成员 lockstep `0.1.0`；**稳定面只有 facade、core、protocol 与日志 schema**，模块 crate 是 0.x 实验面。
- v1 **不发布 crates.io**；嵌入者用 git/path 依赖。
- `core` / `protocol` 无 feature；feature 只出现在 facade、CLI 与后端 crate。
- 没有后台 driver：宿主显式调用 `SessionHandle::drive()`；同一 session 单飞；丢弃流不等于取消。
- 参考 CLI 七命令，退出码 `0/1/2`；审批在 TTY 上默认 `ask`，非 TTY 默认 `deny`。
- 三段验收由 `asterpath-acceptance` 的 `testdrive` 二进制 + 真 `SIGKILL` 驱动；#19 只负责六个注入点工具链。
- v1 的 provider/model 在 session 创建时快照，**不支持 turn 内切换**；验收 (b) 验证的是 adapter 可替换性与规范化历史等价。

## 2. Workspace 成员与依赖

| crate | 职责 | 允许的 workspace 依赖 | v1 交付 |
| --- | --- | --- | --- |
| `asterpath-core` | 模块宿主、事件总线、`EventLog` / `Storage` / `SessionCatalog` / `DurableExecutor` 契约、ID、错误 | 无 | 是 |
| `asterpath-protocol` | 规范化模型、`Provider` / `Tool` / `ToolRegistry` / `AgentRunner` / `Compactor`、会话事件、`ConfigSnapshot`、`AgentEvent` | `core` | 是 |
| `asterpath-agent` | `AgentRunner` 实现（turn/step loop） | `core`, `protocol` | 是 |
| `asterpath-tools` | `ToolRegistry` 实现 | `core`, `protocol` | 是 |
| `asterpath-storage-sqlite` | `EventLog` + `Storage` + `SessionCatalog` + `SessionRetirement` 的 SQLite 实现 | `core`, `protocol` | 是 |
| `asterpath-runtime` | `DurableExecutor` 实现（timer / signal / wake） | `core`, `protocol` | 是 |
| `asterpath-scheduler` | `SubagentSpawner` 实现；子会话准入与等待 | `core`, `protocol` | 是 |
| `asterpath-compaction` | `Compactor` 实现（摘要压缩） | `core`, `protocol` | 是 |
| `asterpath-provider-anthropic` | Anthropic Messages adapter | `core`, `protocol` | 是 |
| `asterpath-provider-openai` | OpenAI Responses adapter | `core`, `protocol` | 是 |
| `asterpath-tracing` | `TraceQuery` / `TraceTap` 实现 | `core`, `protocol` | 是 |
| `asterpath-usage` | `UsageQuery` 实现 | `core`, `protocol` | 是 |
| `asterpath-plugin-host` | 外部插件清单、子进程与 MCP adapter（[mcp.md](mcp.md)）；ACP 不做 | `core`, `protocol` | 图内留位，不进 v1 验收 |
| `asterpath-testing` | in-memory fakes、`ScriptedProvider`、五个故障注入装饰器 + `ManualClock`（六点，见 [testing.md](testing.md)） | `core`, `protocol` | 是 |
| `asterpath` | facade / 默认组装根；暴露稳定 API | 任意 | 是 |
| `asterpath-cli` | 参考 CLI 二进制 `asterpath` | 任意 | 是 |
| `asterpath-acceptance` | `testdrive` 二进制与三段验收测试；`publish = false` | 任意 | 是（仅测试） |
| `xtask` | `check-deps` 与 CI 辅助；`publish = false` | 无 | 是（仅工具） |

- 预留但不进 workspace 的 crate 名：`asterpath-sandbox`（[#21](https://github.com/zibyn/asterpath/issues/21)）、`asterpath-storage-admin`（[#18](https://github.com/zibyn/asterpath/issues/18) 已收敛：内核只有 `SessionRetirement` 接缝，策略与归档由宿主实现；v1 不建）、MCP adapter 并入 `asterpath-plugin-host`，ACP 不做（[#20](https://github.com/zibyn/asterpath/issues/20)）。
- `asterpath-testing` 维持合约依赖白名单，**不得**依赖 facade 或具体模块；需要组装真实模块的测试一律放 `asterpath-acceptance`。
- 机器可读白名单在根目录 `deps-allowlist.toml`（M0 创建）；上表是它的规范副本，冲突以文件为准并由 `xtask` 报错。
- `xtask` 校验：workspace 依赖边在白名单内；任意两个模块 crate 之间零边；`core` / `protocol` 的 `features` 为空。

### 2.1 `ConfigSnapshot`（对 #11 的补齐）

[#11](https://github.com/zibyn/asterpath/issues/11) 把 `ConfigSnapshot` 的形状留给了 protocol 但未落定。本票补齐为持久化投影：

```rust
pub struct ConfigSnapshot {
    pub model: ModelSpec,
    pub system: Vec<Part>,          // 只允许 Text
    pub tools: Vec<ToolName>,       // 创建时解析后的固定集合
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}
```

- `SessionSpec.tools: ToolSelection`（`All | Names`）在创建时解析成显式 `Vec<ToolName>`；之后注册表增删不影响该 session 的模型可见工具集。
- 审批策略不进快照：它是宿主代码，不持久化；日志只记录每次决定（[tools.md](tools.md) §4）。
- MCP/插件配置不进快照：plugin-host 不在 v1 交付内；将来纳入时按 log schema 规则 bump 版本（[ADR-0004](../adr/0004-log-schema-version-and-upcasting.md)）。

## 3. 公共 API

类型归属：`SessionSpec` / `ToolSelection` / `UserInput` / `ConfigSnapshot` / `AgentEvent` / `AgentError` / `RunRequest` 在 `asterpath-protocol`；`Asterpath` / `AsterpathBuilder` / `AsterpathConfig` / `SessionHandle` / `AsterError` 在 facade。

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
    pub default_model: Option<ModelSpec>,
    pub agent: Option<Arc<dyn AgentRunner>>,
    pub compactor: Option<Arc<dyn Compactor>>,
    pub event_log: Option<Arc<dyn EventLog>>,   // 覆盖 sqlite feature
    pub storage: Option<Arc<dyn Storage>>,
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
    pub fn usage(&self) -> Arc<dyn UsageQuery>;
    pub fn trace(&self) -> Arc<dyn TraceQuery>;
}
```

- `AsterpathBuilder` 是常见配置的糖；字段与 `AsterpathConfig` 一一对应，`build().await` 执行拓扑初始化。
- 替换超过一个模块、或需要自定义 `Module` 生命周期的嵌入者，直接使用 `asterpath-core` 的 `ModuleHost`；facade 不覆盖该场景。
- `create_session` 先解析 `SessionSpec`（未知工具名 = `AsterError::Config`），再写 `SessionCreated { config, parent: None }`。

### 3.3 Session 驱动

```rust
pub struct SessionSpec {
    pub model: ModelSpec,
    pub system: Vec<Part>,
    pub tools: ToolSelection,
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
    pub async fn approve(
        &self, call: ToolCallId, decision: ApprovalDecision,
    ) -> Result<(), AsterError>;
}
```

- `submit_input` 只 append `InputRecorded`（dedup key = `input.id`）并通知在途回合；session 空闲时**不**自动开回合，调用方随后 `drive()`。
- `drive()` 单飞：同一 session 同时只能有一个驱动者，第二个返回 `AsterError::Agent(AgentError::Busy)`。
- 持久等待（timer / signal / 子会话）在 `drive()` 内原地 await；流在 `TurnCommitted` 或 `Idle` 后结束。
- `approve()` 是 `DurableExecutor::deliver_signal` 的便捷封装（signal name = `tool.approval.<tool_call_id>`，payload = `Allow | Deny { reason }`）。
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
}

pub trait AgentRunner: Send + Sync {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>>;
}
```

- `AgentEvent` 是实时 UI 流，不是日志；恢复后的历史用 `EventLog` / `TraceQuery`。
- 回合失败（模型不可重试、取消、`MaxStepsExceeded`、`ContextExhausted`）是 `TurnCommitted { reason }`，流正常结束；`AgentError` 只表示无法运行。
- `RunTrigger::Resume` 用于恢复 open turn；`Input` 用于新输入或 steering。恢复位置完全由日志推导，`run` 不接收目标水位。
- `Idle`：没有 open turn 且没有待处理输入时立即发出并结束。

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
    Protocol(ProtocolError),
    Provider(ProviderError),
    Tool(ToolError),
}
```

- 各层错误保持自己的枚举；`AsterError` 只做求和，不加语义。
- 库契约不暴露 `anyhow` / `Box<dyn Error>`；`anyhow` 只允许出现在 CLI 二进制。

### 3.6 Feature flags

| crate | features | 默认 |
| --- | --- | --- |
| `asterpath` | `anthropic`, `openai`, `sqlite`, `plugin-host` | `anthropic`, `openai`, `sqlite` |
| `asterpath-cli` | 同 facade | 同 facade |
| 模块 crate | 无 | — |
| `core` / `protocol` | 无 | — |

- `sqlite` 关闭时必须经 `AsterpathConfig.event_log` / `.storage` 注入实现。
- `plugin-host` 默认关闭；关闭时 `plugin list` 命令返回用法错误（退出码 2）并说明该 build 未启用。
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
| `asterpath plugin list [OPTIONS]` | 从配置读取插件清单并显示加载状态；需要 `plugin-host` feature |
| `asterpath config check [OPTIONS]` | 只解析与校验配置，不初始化模块；成功退出 0，失败退出 2 |

通用选项：`--config <path>`、`--db <path>`、`--provider <id>`、`--model <id>`、`--approval <ask|allow|deny>`、`--json`。

- `run` 不接受 `--session`；要续跑已有 session 用 `resume`。
- 退出码：`0` 完成或 idle，`1` 运行失败（`TurnCommitted` 非 `Completed`、存储/模块错误），`2` 用法或配置错误。没有"停在等待点"的退出码。
- 审批端：`drive()` 流里的 `ApprovalRequested` 由 CLI 处理。TTY 上默认 `ask`，提示工具名、参数、`replay` / `side_effect` / `provenance`；非 TTY 默认 `deny`，显式传 `ask` 是用法错误。`allow` / `deny` 只由显式 flag 启用。
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
max_steps_per_turn = 0        # 0 = 不设上限；其余字段用默认值

[compaction]
# CompactionConfig 字段

[subagents]
# SubagentConfig 字段

[approval]
default = "ask"               # ask | allow | deny

[[plugins]]
# PluginManifest；仅 plugin-host feature
```

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
| M0 骨架 | workspace、`core` / `protocol` / `storage-sqlite` / `agent` / `tools` / `testing` / facade + `ScriptedProvider` | 编译通过；`cargo xtask check-deps` 通过；单回合（模型 + 工具）可跑 |
| M1 恢复 | 场景 (a) | `acceptance` 的 A1–A3 通过 |
| M2 双协议 | 两个 provider crate + adapter 测试 | 场景 (b) 通过 |
| M3 子智能体 | `runtime` / `scheduler` / 子会话 + tracing / usage | 场景 (c) 通过 |
| M4 CLI | `asterpath-cli` 七命令、配置、审批端 | CLI 场景测试通过 |

- `asterpath-plugin-host` 与扩展 crate 不参与 M0–M4 的通过条件；[#18](https://github.com/zibyn/asterpath/issues/18) / [#20](https://github.com/zibyn/asterpath/issues/20) 已定，[#21](https://github.com/zibyn/asterpath/issues/21) 待定，各自实现时再加闸门。
- CI 顺序：`cargo xtask check-deps` → `cargo fmt --check` → `cargo clippy --workspace --all-targets -- -D warnings` → `cargo test --workspace` → `cargo test -p asterpath-acceptance --test acceptance` → `cargo doc --workspace --no-deps`。
- 另跑 feature 矩阵：`cargo check -p asterpath --no-default-features` 与 `--features anthropic` / `openai` / `sqlite` 各自组合。
- CI 分层与超时（[testing.md](testing.md) §6）：`cargo test --workspace` 预算 5 分钟；进程级验收预算 5 分钟、单 marker 等待上限 15 秒；feature 矩阵 5 分钟。
- 验收测试只在 Linux CI 上跑（`cfg(unix)`）；框架本身不禁 Windows，但 v1 不为验收提供 Windows 的强杀实现。进程级与 failpoint 测试不得 `#[ignore]`。

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
