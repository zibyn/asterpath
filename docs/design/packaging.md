# 打包、公共 API 与参考 CLI

> 来源：[#15](https://github.com/zibyn/asterpath/issues/15)。本文件是 crate 图、稳定面、公共 API 与参考 CLI 的规范；验收场景在 [acceptance.md](../spec/acceptance.md)，入口在 [spec/README.md](../spec/README.md)。
>
> 相关 ADR：[0026 引擎是一个 crate](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)。

## 1. 决策摘要

- 引擎是一个 crate（[ADR-0026](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)，[#79](https://github.com/zibyn/asterpath/issues/79)）：agent、scheduler、durable waits、工具表、trace / usage 视图与默认压缩是 `asterpath` 的内部模块；`asterpath-core` 并入 `asterpath-protocol`。workspace = 13 个库/应用成员 + `xtask`（§2；#80 以 `asterpath-mcp` 取代 `asterpath-plugin-host` 与 `asterpath-sandbox`）。独立 crate 只为真实适配器或可选重依赖。
- 依赖规则按角色（`protocol` / `adapter` / `engine` / `host` / `app` / `tool`），由 `cargo xtask check-deps` 对照根目录 `deps-allowlist.toml` 检查（§2）。
- 全部成员 lockstep `0.1.0`；**稳定面只有 facade（`asterpath`）、`asterpath-protocol` 与日志 schema**，适配器 crate 是 0.x 实验面。
- v1 **不发布 crates.io**；嵌入者用 git/path 依赖。
- `protocol` 无 feature；feature 只出现在 facade、CLI 与后端 crate。
- 没有后台 driver：宿主认领会话树后显式调用 `OwnedSession::drive()`；同一 session 单飞；丢弃流不等于取消。
- 参考 CLI 对齐 Codex / Claude Code 共有核心子集：交互、`exec`、`doctor`、`debug`；专用 flag 有准入规则，其余配置走 `-c key=value`；退出码 `0/1/2`（§4）。
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
| `asterpath-host` | host | 参考宿主共用层（ADR-0040）：按工作根组装（工作区工具、`AGENTS.md`、技能、MCP 加载、会话环境适配器 `WorkRoot`）、宿主元数据键与续会话规则，入参是普通值；`config` feature 解析 `config.toml` 与环境变量，`clap` feature 给模式枚举加 `ValueEnum`，默认都不开；不属于稳定面 | `asterpath`, `protocol`, `mcp`, `skills`, `tools`, `sandbox-linux` |
| `asterpath-cli` | app | 参考 CLI 二进制 `asterpath` | 任意 |
| `asterpath-desktop` | app | Tauri 桌面参考宿主（`apps/desktop/src-tauri`）；命令层是库，窗口在非默认 feature `tauri` 后（ADR-0035） | 任意 |
| `asterpath-acceptance` | app | `testdrive` 二进制与三段验收测试；`publish = false` | 任意 |
| `asterpath-mcp-fixture` | tool | 测试用 MCP server（stdio / HTTP）；`publish = false` | 无（仅外部依赖） |
| `xtask` | tool | `check-deps` 与 CI 辅助；`publish = false` | 无 |

- 角色规则（`xtask` 强制）：`protocol` 不依赖任何成员、不声明 feature；`adapter` 只依赖 `protocol` 与白名单列出的同类适配器；`engine` 只依赖 `protocol` 与它再导出 / 按 feature 装配的适配器；`host` 只依赖 `protocol`、适配器与 `engine`，且只有 `app` 能依赖它；`app` / `tool` 其余只受各自 `allow` 列表约束（`"*"` = 任意成员，仅 app）。没有"模块之间零依赖"规则：引擎内部的协作是同 crate 的 `mod`（ADR-0026）。
- `asterpath-sandbox-macos` 仍为预留名，不建。`asterpath-storage-admin` 不建（[#18](https://github.com/zibyn/asterpath/issues/18)：策略与归档由宿主实现）；MCP adapter 在 `asterpath-mcp` 内，ACP 不做（[#20](https://github.com/zibyn/asterpath/issues/20)）。
- `asterpath-skills` 是可选适配器（M6 交付）；只依赖 `protocol` 与外部 `saphyr`、`futures-core`、`serde`、`serde_json`。技能不新增事件，日志 schema 不变。
- `asterpath-tools` 是可选适配器（#83）：每个工具接收一个绝对根目录，路径越出根目录返回 `Denied`；`bash` 只经由 `SandboxProvider` 启动进程，没有 provider 时仅 `FullAccess` 放行。工作区不进入内核或日志。多工作根的宿主在 builder 上注册会话环境适配器（`EnvironmentAdapter`，ADR-0037）：创建、分叉、打开会话时按根会话的宿主元数据解析出该会话的工具、沙箱策略与子进程变量（`SessionEnvironment`），子会话用根会话的；同名时会话环境的工具优先于 builder 的。认领时若解析出的描述符与生效配置不同就替换，解析不到的工具移除（追加一条 `ConfigChanged`），从不自动新增。`ToolContext::spawn` 把沙箱策略与变量交给工具；`bash` 与 MCP stdio 只继承 `INHERITED_ENV` 白名单。一个 `Asterpath`（以及 CLI、桌面端等其他进程）可以共用一个数据库，各自认领自己驱动的会话树（ADR-0036）。
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

- `SessionSpec.tools: ToolSelection`（`All | Names`）在创建时解析成显式 `Vec<ToolDescriptor>`；之后注册表增删不影响该 session 的模型可见工具集。工具集不再在会话内固定：宿主可经 `update_config` 的 `add_tools` / `remove_tools` 增删（ADR-0030）；新增的描述符同样按注册表解析后记进日志。
- 所有 `ModelRequest.tools` 只从日志里记录的描述符渲染（快照加 `ConfigChanged.add_tools`），恢复后重建出与崩溃前相同的工具定义，不重新解析。`timeout` / `parallelism` 等执行声明随描述符记录；注册表升级只影响新 session 与之后新增的工具。
- 每个 step 只有生效集合内的工具可调用：模型调用集合外的名字（包括已移除的工具，即使它仍在注册表里）走 `NotFound`。执行仍调用活注册表：工具已消失同样走 `NotFound`（[plugins.md](plugins.md) §8）；已记录意图的调用照常收尾，恢复按 `ToolCallRequested` 上已记录的 `replay` / `provenance` 决定是否重跑，不受之后移除的影响。
- `ReasoningBinding.tools_hash` 按渲染出的顶层 `tools` 的模型可见部分计算（[protocol.md](protocol.md) §9）。Anthropic inline 路径下顶层 `tools` 是基线工具集，会话中途增删不改变它；fork 收窄或退回路径改写顶层 `tools` 时自然不匹配。
- 审批策略不进快照：它是宿主代码，不持久化；日志只记录每次决定（[tools.md](tools.md) §4）。
- MCP 服务配置不进快照：它是宿主装配；进快照的是 MCP 工具解析后的描述符。会话中途增减工具见下文 `ConfigPatch`；MCP `list_changed` 接入另开票。
- 快照是**初始配置**。会话中途的改动记成 `ConfigChanged { patch: ConfigPatch }`（[ADR-0030](../adr/0030-session-config-is-a-fold-of-log-records.md)），生效配置是折叠：

  ```rust
  #[non_exhaustive]
  pub struct ConfigPatch {
      pub id: Option<UpdateId>,           // 宿主选的 dedup key；append_system 非空时必填（§3.3）
      pub model: Option<ModelSpec>,
      pub request: Option<RequestConfig>, // 出现即整体覆盖，不按子字段合并
      pub append_system: Vec<Part>,       // 追加到 system，只含 Text；不替换已有 system
      pub add_tools: Vec<ToolDescriptor>, // 新增工具：注册表中的描述符，记录后恢复只读不解析
      pub remove_tools: Vec<ToolName>,    // 移除生效集合中的工具；折叠时先删后加
  }

  /// SessionCreated.config 依次应用 seq < watermark 的 ConfigChanged
  pub fn effective_config<'a>(
      records: impl IntoIterator<Item = (&'a LogEntry, &'a SessionEvent)>,
      watermark: u64,
  ) -> Option<ConfigSnapshot>;
  ```

  可变的是 `model`、`request`、追加 system 指令与工具集。追加的指令与工具增删在请求里不改顶层 `system`，而是历史中带位置的 `Instruction` / `ToolsChanged` 条目，压缩时并入基线（[protocol.md](protocol.md) §3.3）；生效配置的 `system` 是创建时 system 加全部追加指令，`tools` 是生效工具集（`ModelRequest.tools`），基线工具集另由 `ModelRequest.baseline_tools` 携带。`turn` / `compaction` / `subagents` 始终取创建时的值。引擎只有这一个折叠入口（`ConfigSnapshot::apply` 是它的单步），agent、调度、trace 与 CLI 续会话都经它取值。

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

类型归属：ID（`SessionId` / `InputId` / … / `LogId`）、`Clock` / `Timestamp`、存储契约与 `scan_all`、`SessionSpec` / `ToolSelection` / `UserInput` / `ConfigSnapshot` / `AgentEvent` / `AgentError` / `RunRequest` / `SessionState` / `ModelAlias` / `ApprovalDecision` / `CancelToken` / `ApprovalPolicy` / `ToolConfig` / `Compactor` / `CompactResult` / `Hooks` / `UsageScope` / `UsageReport` / `Span` 在 `asterpath-protocol`；`Asterpath` / `AsterpathBuilder` / `AsterpathConfig` / `SessionHandle` / `OwnedSession` / `WatchEvent` / `AsterError` / `DefaultCompactor` 在 facade。`OwnedSession` 也在 facade；`McpServerConfig` 属于适配器 crate `asterpath-mcp`，技能相关类型属于 `asterpath-skills`，都不在稳定面。

### 3.1 稳定层级与版本

| 层级 | 内容 | 兼容承诺 |
| --- | --- | --- |
| Tier 1 | `asterpath` 的公开项；`asterpath-protocol` 的公开项；日志 schema（`LOG_SCHEMA_VERSION`） | semver；0.x 期间破坏性变更 bump minor，兼容变更 bump patch |
| Tier 2 | 适配器 crate 的公开项 | 0.x 实验面；minor 内可改，变更写入对应 design 文档 |
| Tier 3 | `asterpath-testing`、`asterpath-acceptance`、`xtask`、未在 spec 中记录的 `pub` 项 | 无承诺 |

- 公开错误枚举、事件枚举与可能增字段的结构一律 `#[non_exhaustive]`；配置结构提供 `Default` + 公开字段赋值（`Type::default()` 再赋字段），必填字段多的类型可另提供 builder，不承诺字面量构造。含无默认值必填字段的规格类型（如 `SessionSpec`、`ModelSpec`）暂不受此条约束。
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
    pub max_live_children: NonZeroUsize,        // 本进程调度器的活子会话上限，默认 32
    pub hooks: Vec<Arc<dyn Hooks>>,             // 扩展钩子，按此顺序调用（ADR-0025）
    pub spawn_tool: bool,                       // 是否注册模型可见的 spawn_agent，默认 true
}

impl Asterpath {
    pub fn builder() -> AsterpathBuilder;
    pub async fn create_session(&self, spec: SessionSpec) -> Result<SessionHandle, AsterError>;
    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError>;
    pub async fn fork_session(&self, source: SessionId, metadata: BTreeMap<String, String>)
        -> Result<SessionHandle, AsterError>;           // 会话分叉（ADR-0033）
    pub async fn shutdown(self) -> Result<(), AsterError>;

    pub fn store(&self) -> Arc<dyn SessionStore>;       // append 经唯一入口送达 on_record；退役 = store().retire(..)
    pub async fn sessions(&self, query: SessionQuery) -> Result<Vec<CatalogEntry>, AsterError>;  // 会话目录（ADR-0032）
    pub async fn wakeable(&self) -> Result<Vec<SessionId>, AsterError>;  // 无主会话的唤醒发现（storage.md §7）
    pub async fn owner(&self, session: SessionId)
        -> Result<Option<OwnerToken>, AsterError>;      // 会话树当前的认领（ADR-0036）
    pub fn approval_gate(&self) -> Arc<ApprovalGate>;   // 宿主按会话树注册审批端点；见 tools.md §4
    pub async fn usage(&self, scope: UsageScope) -> Result<UsageReport, UsageError>;
    pub async fn trace(&self, session: SessionId) -> Result<Vec<Span>, TraceError>;
}
```

- 存储只有一个注入点 `store`：提供则直接使用（不需要 `database`），未提供时由 `sqlite` feature 按 `database` 打开；没有“部分注入”。注入的存储可以不实现退役（`retire` 返回 `Unsupported`）。
- `AsterpathBuilder` 是常见配置的糖；字段与 `AsterpathConfig` 一一对应（`hooks(Arc<dyn Hooks>)` 可多次调用、逐个追加），`build().await` 按固定顺序直接构造内部组件。
- `spawn_tool(false)`（= `AsterpathConfig.spawn_tool = false`）不注册 `spawn_agent`：`ToolSelection::All` 快照不含它，`Names` 点名它是 `AsterError::Config`；调度器与子会话恢复不受影响。
- 扩展点只有 `Hooks`（`before_step` / `before_tool` / `after_tool` / `on_record` / `on_ownership_lost`，形状与语义见 [module-contracts.md](module-contracts.md) §3.1、[ADR-0025](../adr/0025-extension-points-are-typed-hooks.md)）。facade 没有服务清单、模块宿主或事件总线。
- 查询是 facade 方法、直接返回值（ADR-0026）：`usage(scope)` 是 scope 所指会话的 `own`（`SessionSubtree` 另含 `descendants`）合计；`trace(session)` 是该会话日志折叠出的全部 span。没有 span 实时推送：实时导出由宿主在 `Hooks::on_record` 上自行增量折叠。
- 再导出：`asterpath::protocol`（= `asterpath-protocol`）、id 类型（`SessionId`、`TurnId`、`StepId`、`InputId`、`ToolCallId`、`LogId`、`Timestamp`）与 `DefaultCompactor`，嵌入者编写钩子与工具只需依赖 `asterpath`。
- 会话配置（`turn` / `compaction` / `subagents`）只有 `SessionSpec` 一个来源，进 `ConfigSnapshot`；`AsterpathConfig` 只放部署级字段（如全局 `max_live_children`）。
- 模型只有 `SessionSpec.model` 一个来源，`AsterpathConfig` 不设默认模型；CLI 用 `[model] provider` + `[model] id`（`-m` 覆盖）组装该值。
- 注入 `store` 时 `database` 被忽略；未注入时由 `sqlite` feature 从 `database` 构建，此时 `database` 必填。
- `AsterpathConfig` 手写 `Default`：`SystemClock`、内置审批策略、`ToolConfig::default()`、空 alias 集合。`approval` / `tool` / `model_aliases` 是宿主运行时配置，**不进** `ConfigSnapshot`（与审批策略同规则，[tools.md](tools.md) §4）。
- `model_aliases` 在 `build()` 时对照 `ProviderCatalog` 校验：alias 指向的 provider 必须存在，否则 `AsterError::Config`。`ModelAlias` 本身不持久化；spawn 只把解析后的 `ModelSpec` 写进子快照（[subagents.md](subagents.md) §5）。
- 关闭 `sqlite` feature 时必须注入 `store`；退役仍是可选的。
- v1 的 `AsterpathConfig` 没有 MCP 字段；MCP 服务配置属于 `asterpath-mcp`，嵌入者把 `McpServers::tools()` 交给 builder（[plugins.md](plugins.md)）。也没有技能字段：技能由宿主发现并组成 `system` / 工具，见 [skills.md](skills.md) §8。
- 没有 `agent` / `executor` 注入：agent 循环与持久等待是引擎内部类型，不可替换（ADR-0026）。
- `build()` 用 `ProviderCatalog::new` 建 provider 索引；`Provider::id()` 重复是 `AsterError::Config`。
- 内置组件不是可替换模块；替换只经上述适配器字段，观察与拦截只经 `hooks`。`shutdown()` 只释放引擎：没有后台任务，无需收尾顺序。
- `create_session` 先解析 `SessionSpec`（未知工具名、未知 `ModelSpec.provider`、宿主元数据超出 [storage.md](storage.md) §5 的上限 = `AsterError::Config`），再写 `SessionCreated { config, parent: None, metadata }`。
- `fork_session(source, metadata)` 创建独立的新会话：水位是源会话最后一个已提交回合的末尾，进行中的回合不复制，源会话可以正在被驱动；配置按上下文分叉的规则继承但不收窄；元数据由宿主重新给出（同 `create_session` 的上限）。写 `SessionCreated { parent: None, metadata }` 与 `ContextForked { source: ForkSource::Session { .. } }`（[subagents.md](subagents.md) §4.2、[ADR-0033](../adr/0033-session-fork-reuses-context-fork-snapshot.md)）。未知源会话 = `AgentError::NotFound`。
- `sessions(query)` 是存储目录查询的原样转发：按父会话与元数据过滤（`SessionQuery.metadata` 的每个键值都要相等，空表示不过滤，ADR-0039），按 `(created_at, session)` 降序，`CatalogCursor` 游标分页；目录项带宿主元数据。按元数据排序、前缀或范围匹配由宿主在结果上完成（ADR-0032）。子会话不继承元数据，所以按租户键过滤只列出根会话。
- 多用户内嵌（ADR-0039）：用户是宿主写进根会话元数据的键；每个 model 请求带 `ModelRequest.session: Option<SessionContext { session, root, metadata }>`，`metadata` 是根会话的元数据，宿主包装 provider 据此选凭据或端点（摘要调用同样带上）；审批端点按会话树 `approval_gate().register(root)`。

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
    pub metadata: BTreeMap<String, String>,   // 宿主元数据（ADR-0032），不进 ConfigSnapshot
}

pub enum ToolSelection { All, Names(Vec<ToolName>) }

pub struct UserInput { pub id: InputId, pub parts: Vec<Part> }

/// 只读句柄：读取与外部写入（输入、审批决定、配置变更），任何进程都可用。
impl SessionHandle {
    pub fn id(&self) -> SessionId;
    pub async fn claim(&self) -> Result<OwnedSession, AsterError>;  // 认领会话树（ADR-0036）
    pub async fn submit_input(&self, input: UserInput) -> Result<InputId, AsterError>;
    pub async fn state(&self) -> Result<SessionState, AsterError>;
    pub async fn facts(&self) -> Result<SessionFacts, AsterError>;
    pub async fn projection(&self) -> Result<Vec<ProjectedEntry>, AsterError>;
    pub async fn update_config(&self, patch: ConfigPatch) -> Result<(), AsterError>;
    pub async fn approve(
        &self, call: ToolCallId, decision: ApprovalDecision,
    ) -> Result<(), AsterError>;
    // 观察而不驱动（ADR-0038）；不需要认领，流不会自行结束
    pub fn watch(&self, after: Option<LogId>)
        -> BoxStream<'static, Result<WatchEvent, AsterError>>;
}

#[non_exhaustive]
pub enum WatchEvent {
    Record(Box<CommittedRecord>),  // 已提交记录，按日志顺序；log_id 即续订游标
    Synced,                   // 重放追平，之后是新提交；每次 watch 一次
    Live(AgentEvent),         // 本进程内驱动的实时事件，不重放
}

/// 属主句柄：Deref 到 SessionHandle；最后一个克隆 drop 时释放会话树。
#[derive(Clone)]
pub struct OwnedSession { /* .. */ }

impl OwnedSession {
    pub fn owner(&self) -> OwnerToken;
    pub fn drive(&self) -> BoxStream<'_, Result<AgentEvent, AsterError>>;
    pub async fn cancel(&self) -> Result<(), AsterError>;
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError>;
}
```

- `create_session` / `open_session` / `fork_session` 都返回只读句柄；驱动、手动压缩、取消（子会话调度在驱动内）只在 `claim()` 得到的 `OwnedSession` 上。认领的是会话所在树的根：同一个 `Asterpath` 内重复认领共享同一纪元，另一个进程持有时得到 `AsterError::Storage(StorageError::OwnedElsewhere)`。非属主进程要取消，先用 `Asterpath::owner` 找到属主，由宿主路由过去。
- 驱动与手动压缩期间的每条 append 都携带认领的 `OwnerToken`，存储拒绝过期纪元；输入、审批决定与配置变更不携带，任何进程都可以写。
- 认领随本进程的进程租约有效：`Asterpath` 在构建时所在的 tokio runtime 上每 `lease_renew_interval`（builder，默认 10 s）续约一次，TTL 是存储的参数（SQLite `OpenOptions::lease_ttl`，默认 30 s；见 [storage.md](storage.md) §6）。`Asterpath` 与它的句柄全部 drop 即正常退出，租约清除、树立即可被别的进程认领；进程被 kill 则别人要等一个 TTL。
- 失去属主（续约失败，或树已被别的进程接管）时，该树在本进程内的 `drive` 流立刻结束并以 `OwnedElsewhere` 收尾，之后这个 `OwnedSession` 上的属主操作都得到 `OwnedElsewhere`，认领被释放，`Hooks::on_ownership_lost(owner)` 通知宿主；宿主丢掉这个 `OwnedSession`，需要时重新 `claim()`。

- `submit_input` 只 append `InputRecorded`（dedup key = `input.id`）并通知在途回合；session 空闲时**不**自动开回合，调用方随后 `drive()`。
- `state()` 返回恢复分类器的 `SessionState`（[event-log-recovery.md](event-log-recovery.md) §6）：`Idle` 或 `TurnOpen { turn, step }`，委托 `facts()`（会话事实 `SessionFacts`，对日志折叠一次）推导，不依赖驱动者。
- `projection()` 返回对日志折叠出的模型可见投影（每项带来源 log id），不读驱动者内存。
- `drive()` 单飞：同一 session 同时只能有一个驱动者，第二个返回 `AsterError::Agent(AgentError::Busy)`。
- 持久等待（审批 / 子会话）在 `drive()` 内原地 await；流在 `TurnCommitted` 或 `Idle` 后结束。
- `approve()` 写 `ApprovalDecided { tool_call_id, decision }`（dedup key = `tool_call_id`）并唤醒本进程内该调用的等待；属主在另一个进程时，它的等待至多 500 ms 后从日志读到决定。
- `update_config()` 先校验（`model.provider` 已在 `ProviderCatalog` 注册；`append_system` / `add_tools` / `remove_tools` 非空时必须带 `id`；`append_system` 只含 `Part::Text`；`add_tools` 的每个描述符必须与本 `Asterpath` 工具注册表中同名工具的描述符完全相同（与 `ToolSelection::Names` 同一来源），名字不在生效集合中、补丁内不重复；`remove_tools` 的名字必须在生效集合中；否则 `AsterError::Config`，不落日志），再把它原样记成一条 `ConfigChanged { patch }`。宿主直接构造 §2.1 的 `ConfigPatch`（`#[non_exhaustive]`，用 `ConfigPatch::default()` 再赋字段），不另设请求类型；新增工具时传入自己注册的工具的 `descriptor().clone()`。`id: Option<UpdateId>` 是宿主选的 dedup key，随补丁落日志：追加与增删工具不幂等，宿主重试同一次更新时用同一个 `id`，只记一条（日志里已有同 `id` 的工具变更时直接返回 `Ok`，不再按生效集合判冲突）；只覆盖字段的更新可以不带 `id`。生效集合按整条日志折叠（`u64::MAX` 水位）；运行期向注册表添加新工具不在范围内。回合进行中也可调用：记录立即追加，下一个 `StepStarted` 生效，进行中的 step（含其重试与强制压缩）不受影响（[agent-loop.md](agent-loop.md) §3）。
- `compact_now()` 仅 Idle 且无活跃驱动者时可用，委托 agent 的 `compact_now`；否则返回 `AgentError::Busy`；`NotConfigured` / `NothingToDo` / `Insufficient` 是 `Ok` 状态（[compaction.md](compaction.md) §5）。
- `watch(after)` 先订阅本进程的实时事件，再从游标（不含；`None` 为整条日志）重放已提交记录，追平处发一次 `Synced`，之后推送新提交。本进程的提交即时唤醒订阅者；另一进程的提交靠轮询存储，至多 500 ms 后到达。`Live` 是本进程内任何 `drive`（含父会话驱动的子会话）产生的 `AgentEvent`，扇出给该会话的所有订阅者，不只给驱动者；跨进程订阅者只看到记录。订阅者落后超过缓冲（1024 条）时跳过部分 `Live`，记录不丢。同一次唤醒中先给 `Live` 再给记录。续订时传最后一条 `Record` 的 `log_id`，不丢不重。
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
    ToolStarted { turn: TurnId, step: StepId, output_index: u32, call: ToolCallId, tool: ToolName, args: JsonString },
    ToolFinished {
        turn: TurnId, step: StepId, call: ToolCallId, status: ToolResultStatus,
        output: Option<Vec<Part>>, error: Option<ToolErrorInfo>,
    },
    ApprovalRequested(ApprovalRequest),
    TurnCommitted { turn: TurnId, reason: TurnEndReason, usage: Usage },
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

- agent 循环是引擎内部的 `Agent`，不是 trait、不可替换（ADR-0026）；`OwnedSession::drive()` / `compact_now()` 是它的公开入口。
- `AgentEvent` 是实时 UI 流，不是日志；恢复后的历史用 `store()` / `trace(session)`。
- `AgentEvent` 的 JSON 形状属于 Tier 1 兼容承诺：tag 为 `type`、变体名 snake_case（`PartDelta` 同样），ID 沿用现有序列化形式；`AgentEvent` / `ApprovalRequest` / `PartDelta` 只派生 `Serialize`。日志类型（`SessionEvent` 等）的 serde 形状不受影响。
- `(step, output_index)` 是流内块的身份：`ModelDelta.output_index` / `part_index` 原样取自适配器的 `StreamEvent::PartDelta`；`ToolStarted.output_index` 是该调用 item 在本 step 响应中的 `output_index`，UI 据此把此前的 `JsonArgs` 片段归属到 `call`。恢复执行的 step 不记录 `output_index`，此时为 item 在 `AssistantMessage` 中的位置。
- 工具与用量字段（#121，供 CLI 显示、`stream-json` 与 `json` 结果使用）：`ToolStarted.args` 是实际交给工具的参数（`before_tool` 改写之后）；`ToolFinished.output` / `error` 与同一调用落日志的 `ToolResult` 相同（截断与 `after_tool` 之后；`Interrupted` 两者皆为 `None`）。都是原值，脱敏归宿主。输出工具调用（ADR-0034）的 `ToolFinished` 是 `Completed`，`output` 为框架补上的固定文本，不是结果本身（结果是该调用的参数，在日志的 `ToolCallRequested` 里）。每个落日志的 `ToolResult` 都有一条 `ToolFinished`，每条 `ToolFinished` 之前都恰有一条同一 `call` 的 `ToolStarted`，宿主不必处理只有其一的情况。执行的调用在拿到并发许可与 step 锁、开始执行时发 `ToolStarted`；不执行的调用在结束前紧接着补发，`args` 取记录的意图参数：输出工具调用、`Interrupted`（取消时尚未开始的调用）与预失败（`Failed`，`error.kind` 为 `Denied` / `NotFound` / `Internal`，来自审批拒绝、未注册工具或 `before_tool` 拒绝与出错）。因此 `ToolStarted` 表示调用进入结局，不保证工具被执行。`TurnCommitted.usage` 是本回合模型调用与压缩用量之和，与 `usage(UsageScope::Turn)` 的 `model_calls + compaction` 一致，不含 `partial`；不另设用量事件。
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

二进制名 `asterpath`，包名 `asterpath-cli`。命令面对齐 Codex CLI 与 Claude Code 的**共有核心子集**（对比见 [cli-codex-vs-claude-code.md](../research/cli-codex-vs-claude-code.md)）：命令骨架与安全模型取 Codex，无头输出协议与续会话开关取 Claude Code。全屏 TUI、后台会话、守护进程、云端与 IDE 集成不在范围内。

**flag 准入规则**：只有两者共有核心子集里存在对应物的选项才做专用 flag；其余框架参数一律走配置文件或 `-c key=value`。满足规则是必要条件，不是充分条件。新增 flag 先过这条规则。

### 4.1 命令

| 命令 | 行为边界 |
| --- | --- |
| `asterpath [OPTIONS] [PROMPT]` | 交互会话（按行读写的 REPL）；给了 `PROMPT` 则作为第一条输入 |
| `asterpath exec [OPTIONS] [PROMPT]` | 无头运行一个回合；没有 `PROMPT` 或为 `-` 时读 stdin；同时给了 `PROMPT` 且 stdin 是管道时，stdin 作为 `<stdin>` 块追加 |
| `asterpath doctor [OPTIONS]` | 解析与校验配置，不打开数据库、不初始化引擎；加载 `[mcp_servers]` 并逐个打印状态后关闭（`mcp: <id> loaded: <tools>` / `mcp: <id> failed at <stage>: <message>`）；扫描技能并打印诊断；成功退出 0，配置错误或 `required` 服务失败退出 2 |
| `asterpath debug prompt-input <SESSION>` | 打印投影（模型可见输入）与未闭合意图，JSON |
| `asterpath debug trace <SESSION>` | 打印 span 树（`Asterpath::trace`），JSON |

`debug` 组不在顶层 `--help` 中展开。

### 4.2 选项

| 选项 | 适用 | 含义 |
| --- | --- | --- |
| `--config <path>` | 全部 | 配置文件，见 §4.5 |
| `-c, --config-override <key=value>` | 全部 | 覆盖配置文件任意键；点分路径，值按 TOML 解析，解析失败按字符串；可重复 |
| `-m, --model <id>` | 交互、`exec` | 新会话的模型 id（provider 取 `[model] provider`）；续会话时等价于一次 `update_config` 换模型 |
| `-a, --ask-for-approval <untrusted\|never>` | 交互、`exec` | 审批策略，见 §4.4 |
| `-s, --sandbox <read-only\|workspace-write\|full-access>` | 交互、`exec` | 覆盖 `[sandbox] mode` |
| `-C, --cd <dir>` | 交互、`exec` | 工作根目录（工具根、沙箱可写根、`AGENTS.md` 位置、会话 `cwd` 元数据），默认进程 cwd |
| `--continue` | 交互、`exec` | 续当前工作根目录下最后活动的会话 |
| `-r, --resume [id\|name]` | 交互、`exec` | 续指定会话；不给值时列出当前目录最近 20 个会话并读入编号（非 TTY 下是用法错误） |
| `--fork-session` | 与 `--continue` / `-r` 合用 | 不续原会话，而是以它为源做会话分叉 |
| `-n, --name <name>` | 新会话 | 写入宿主元数据 `name`；续会话时给出是用法错误 |
| `-i, --image <file>` | 交互、`exec` | 给首条输入附图（文件输入，ADR-0031） |
| `--output-format <text\|json\|stream-json>` | `exec` | 见 §4.3 |
| `--input-format <text\|stream-json>` | `exec` | 见 §4.3 |
| `--output-schema <file>` | `exec` | 结构化输出的 JSON Schema（ADR-0031）；写进新会话或分叉会话的配置，因此只用于新会话与 `--fork-session`，续原会话时是用法错误（没有单回合的请求选项，写进去会留在会话里） |
| `--output-mode <native\|tool\|auto>` | `exec` | 结构化输出的输出模式（ADR-0034），默认 `native`；不带 `--output-schema` 是用法错误；配置文件无对应字段。各模式下结果都作最终消息输出（text 的 stdout、json / stream-json 结果对象的 `result`）：模型调用了输出工具时是其参数（多个取第一个），否则是最终助手文本。`tool` 模式下模型没有调用输出工具（结果未交付）时以退出码 1 失败 |

- `--config` 与 `-c` 可写在子命令前后（`asterpath --config X doctor`）；交互模式的选项与 `PROMPT` 写在子命令前是用法错误（退出 2），不会被静默丢弃或当成提示词。
- Codex 的 `-c` 与 Claude Code 的 `-c/--continue` 冲突，本 CLI 的短选项 `-c` 归配置覆盖，续会话只有长选项 `--continue`。
- 不提供的选项（满足准入规则但不做）：`--add-dir`、`--ephemeral` / `--no-session-persistence`、`--dangerously-bypass-*`（用 `-a never -s full-access` 组合）、`mcp add/remove`。
- 已移除：`run`、`resume`、`inspect`、`trace`、`usage`、`config check` 子命令，`--db`、`--provider`、`--approval`、`--json`、`--tools`、`--no-tools`、`--no-skills`。对应配置走 `-c storage.path=…`、`-c model.provider=…`、`-c tools.enabled=[…]`、`-c skills.enabled=false`。

**续会话的驱动语义**：`--continue` / `-r` 打开会话后，先检查会话事实；有未闭合回合或待处理输入则先 `drive()` 到结束（崩溃恢复），再处理新输入。`exec -r <id>` 不带 `PROMPT` 只做恢复，没有可恢复的内容时输出 `idle`、退出 0；交互模式恢复后进入提示符。

**续会话的工作根目录**：取会话元数据里的 `cwd`，显式 `-C` 时取 `-C`；目录不存在是配置错误（退出 2）。`--continue`、选择器与 `-r <name>` 按当前工作根目录（或 `-C`）匹配元数据 `cwd`；`-r <id>` 不限目录。名字重复时 `-r <name>` 取最后活动的一个。

### 4.3 无头输出与输入

- `--output-format text`（默认）：stdout 只有最终助手文本；stderr 输出 session id 与诊断。
- `--output-format json`：stdout 一个对象 `{ "session": <hex>, "result": <最终助手文本>, "reason": <TurnEndReason>, "usage": <本回合用量> }`；字段只增不改。
- `--output-format stream-json`：stdout 每行一个 `AgentEvent` 的 serde 序列化（`tag = "type"`），最后一行是上面的结果对象。
- `--input-format stream-json`（要求 `--output-format stream-json`）：stdin 每行一个命令：`{"type":"user_input","parts":[…]}`、`{"type":"approval","call":<id>,"decision":…}`、`{"type":"cancel"}`。此时 `ApprovalRequested` 从 stdout 发出、决定从 stdin 读回；stdin 关闭且会话空闲时退出，stdin 关闭时仍有审批在等则取消该回合（没有人能回答了）。`parts` 与 `decision` 用协议类型 `Part` / `ApprovalDecision` 的 serde 形状（如 `{"Text":{"text":"hi"}}`、`"Allow"`、`{"Deny":{"reason":null}}`），`call` 取 `ApprovalRequested.tool_call_id`；无法解析的命令（或读不了 stdin）停止读入并取消进行中的回合，照常输出结果行后以用法错误退出（退出 2）。
- 退出码：`0` 完成或 idle，`1` 运行失败（`TurnCommitted` 非 `Completed`、存储/模块错误），`2` 用法或配置错误。没有"停在等待点"的退出码。

### 4.4 审批

- `-a untrusted`（默认）：进程内只读工具放行；`SideEffect::Mutating` 工具（`write_file` / `edit_file` / `bash`）与外部工具（MCP，`<id>__<tool>`）执行前问人。`-a never`：从不问人，靠沙箱兜底。优先级 `-a` > `[approval] default` > `untrusted`。
- 问人的通道：交互模式在 TTY 上提示工具名、参数、`replay` / `side_effect` / `provenance`，子会话的请求不在父会话的流里，交互模式驱动时轮询 `pending_in(root)` 一并提示；`exec` 只有 `--input-format stream-json` 时经流交给宿主。没有通道时 CLI 不注册审批端点，框架的 `Ask` 立即按 `Deny` 处理（`NO_APPROVAL_ENDPOINT`，[tools.md](tools.md) §4）；只读运行用 `-s read-only` 表达，不另设"全拒绝"取值。
- "由策略决定"：问不问人由宿主 `ApprovalPolicy` 按调用的来源、副作用等级与重放类别判断；工具没有让模型申请提权的参数，因此不提供 Codex 的 `on-request`。
- 审批决定经 `SessionHandle::approve()` 投递，落日志；重启后复用已记录决定，不再提示。

### 4.5 交互模式

- 斜杠命令：`/help`、`/new`（新建会话，沿用当前模型）、`/model <id>`（`update_config` 换模型，同 provider）、`/compact`（`compact_now`）、`/status`（会话状态与 `usage`）、`/exit`。不做 `/resume`（用启动时的 `-r`）、`/init`、`/diff`、`/mcp`。
- 回合进行中第一次 Ctrl-C 调用 `cancel()`；空闲时 Ctrl-C 或 Ctrl-D 退出。
- 显示：`ModelDelta` 文本增量直接输出；`ToolStarted` 显示工具名与参数，`ToolFinished` 显示状态与截断后的结果。显示前的脱敏是 CLI 的策略。

### 4.6 会话装配

组装规则在 `asterpath-host`（ADR-0040），CLI 只解析 flag、打印与选退出码；桌面端调用同一份。

- 新会话：宿主元数据写 `cwd`（规范化后的绝对路径）与可选 `name`；读取工作根目录下的 `AGENTS.md`（只此一个文件，不做多级合并、不读用户级文件）放进 `SessionSpec.system`，续会话时不重读。
- MCP（ADR-0027）：交互与 `exec` 在建会话前 `McpServers::load`，工具交给 builder；非 `required` 的失败服务打印到 stderr 后跳过，`required` 失败按配置错误退出 2。退出时先 `Asterpath::shutdown()` 再 `McpServers::shutdown()`。
- 工作根（ADR-0037）：CLI 不改进程当前目录。它注册一个按元数据 `cwd`（续会话时显式 `-C` 优先）解析的会话环境，工作区工具、技能加载工具与 `workspace-write` 的可写根都以工作根为根；没配 `cwd` 的 MCP stdio 服务在工作根启动。
- 工作区工具（#83，`asterpath-tools`；不同于供应商的服务端工具）：新会话以工作根目录为根、按沙箱策略调用 `asterpath_tools::coding(...)` 解析出六个工具；`[tools] enabled = [...]` 选子集，未知工具名是配置错误（退出 2）。续会话总是解析全部六个，模型可见的子集由日志里的生效配置决定。
- 子进程环境：`bash` 只看到 `PATH`、`HOME`、`LANG` 等最小白名单与 `[sandbox] env` 列出的变量。
- 技能：`[skills] enabled` 只影响新会话（[skills.md](skills.md)）。
- 压缩：总是装配 `DefaultCompactor`；是否压缩取决于 `[model] context_window`，未配置时压缩路径为 `NotConfigured`。

### 4.7 配置

默认读 `$XDG_CONFIG_HOME/asterpath/config.toml`（`--config` 或 `ASTERPATH_CONFIG` 可换文件）；默认路径不存在时使用默认值，`--config` 或 `ASTERPATH_CONFIG` 指定的文件不存在是配置错误（退出 2）。只有用户级一层，没有项目级配置。优先级：专用 flag > `-c key=value` > 文件。

```toml
[model]
provider = "anthropic"        # 新会话的 provider id；-c model.provider=… 覆盖
id = "claude-fable-5-1"       # 新会话的模型 id；-m 覆盖
context_window = 200000       # 映射到 ModelSpec.context_window；省略 = 不自动压缩

[sandbox]
mode = "workspace-write"      # read-only | workspace-write（默认，可写根 = 工作根目录）| full-access
env = ["SSH_AUTH_SOCK"]       # 传给 bash 的本进程变量名；默认只传最小白名单（ADR-0037）

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
efforts = ["off", "low", "high", "max"]
thinking_default_on = true                      # 仅 anthropic；另有 thinking（仅 anthropic）
# forced_tool_choice = true / sampling = true
# structured_output = true                      # 端点支持原生 schema 约束时打开（--output-schema）
# file_media_types = ["application/pdf"]        # 端点接受的文件附件类型
# inline_changes = true                         # 仅 anthropic
# 协议不符的字段（openai 上写 thinking / thinking_default_on / inline_changes）是配置错误（退出码 2）

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
default = "untrusted"         # untrusted | never
```

- `max_steps_per_turn = 0` 是配置错误（退出码 2）；不设上限靠省略该字段。
- API key **只从环境变量读**：`ANTHROPIC_API_KEY`、`OPENAI_API_KEY`。配置文件出现 `api_key` / `secret` / `token` 字段直接报错（退出码 2）。
- 环境变量只有 `ASTERPATH_CONFIG` 与各 provider 的 API key 变量；其余覆盖用 `-c`。
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
| M4 CLI | `asterpath-cli` 交互模式与 `exec` / `doctor` / `debug`（§4.1）、配置、审批端 | CLI 场景测试通过 |
| M5 扩展（已交付） | `asterpath-mcp`（MCP 服务配置、子进程、MCP adapter；#80 前为 `asterpath-plugin-host`）、沙箱合约（#80 起在 protocol）与 Linux 后端 | 不参与 v1 验收；闸门见 tracker 侧 M5 实现 spec（六张子票 + T6 端到端） |
| M6 技能（已交付） | `asterpath-skills`、CLI `[skills]`（`enabled` 开关）、发现/索引/`load_skill` | 不参与 v1 验收；场景见 [skills.md](skills.md) §11（S1–S10） |

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
