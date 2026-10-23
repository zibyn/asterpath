# 沙箱扩展模块：SandboxProvider 接缝与策略边界

> 来源：[#21](https://github.com/zibyn/asterpath/issues/21)，2026-10-22 已定。本文件定义沙箱的接缝形状、策略模型、调用点、强制完整性与错误分类，以及与审批/MCP 服务/子会话的边界。
>
> 对齐：[#7 工具契约](./tools.md)、[#8 子智能体](./subagents.md)、[#15 打包](./packaging.md)、[#16 MCP 服务配置](./plugins.md)、[#20 MCP adapter](./mcp.md)、[#3 事件日志](./event-log-recovery.md)。
>
> 相关 ADR：[0019 沙箱是 spawn 点变换，策略归宿主](../adr/0019-sandbox-seam-and-policy.md)。
>
> **2026-10-22 #32 范围调整 / 2026-10-23 M5 范围**：本文件的设计保留，实现归 **M5（post-v1，已交付）**；不属于 v1 交付，也不进 M0–M4 验收。M5 只实现 MCP stdio 的 spawn 点。
>
> 本票对 #7 的修订（已同步到 `tools.md`）：
>
> - `ToolError` 增加 `Denied` 变体：沙箱在进程启动前按策略拒绝 spawn 时，需要模型可见的「被拒绝」分类；`ToolErrorKind` 不变。

## 1. 决策摘要

- **沙箱是 spawn 点的纯变换**：`SandboxProvider::confine(SpawnRequest) -> ConfinedSpawn`，把「想启动的命令」变成「实际执行的命令」。它不是 `Tool` 装饰器。
- **只有要起进程的调用点能被约束**：纯进程内 Rust 工具没有进程边界，OS 层面无法沙箱；包装器只能整次拒绝或审计。
- **合约与后端分离**：合约（`SandboxProvider`、`SpawnRequest`、`ConfinedSpawn`、`SandboxPolicy`、`Enforcement`、`SandboxError`）在 `asterpath-protocol` 的 `sandbox` 模块，只用 std（#80 / ADR-0027 起，原 `asterpath-sandbox` crate 已删除）；后端 crate（`asterpath-sandbox-linux`）独立并只依赖 protocol。宿主自写的 spawn 型工具直接用 protocol 中的合约。
- **共享助手**：`SandboxPolicy::validate()`（`WorkspaceWrite` 的 `writable_roots` 必须非空且全为绝对路径，否则 `Unsupported`；provider 先调它）与 `ConfinedSpawn::unconfined(request)`（按请求原样构造 `Enforcement::Full`；`FullAccess` 直通与无 provider 路径使用）。
- **策略只建模文件系统三档**：`ReadOnly` / `WorkspaceWrite` / `FullAccess`。网络与进程可见性不在 v1 schema 里。
- **策略由宿主解析**：调用点静态配置 > session > 部署默认；模型参数不能放宽；不落日志、不进 `ConfigSnapshot`。
- **默认 fail closed**：后端不能完整强制请求的策略就拒绝启动进程；`Partial` 必须宿主显式 opt-in。
- **审批与沙箱正交**：`Allow` 不移除沙箱边界；v1 不做 escalation。
- **MCP 服务配置不声明沙箱**；`asterpath-mcp` 在每次 stdio 进程启动前调用 provider。
- **子会话继承父的已解析策略**，只能收窄。

## 2. 范围与术语

| 词 | 含义 |
| --- | --- |
| 沙箱（Sandbox） | 由扩展模块或宿主提供的进程级隔离；框架只记录 `Provenance`，不提供、不验证隔离 |
| 沙箱策略（Sandbox Policy） | 宿主解析出的文件系统约束：只读 / 工作区可写 / 完全访问 |
| 约束（Confinement） | provider 把 `SpawnRequest` 变换成实际执行的命令；可能包一层 bwrap/Seatbelt，也可能原样放行 |
| 受限策略（Restricted Policy） | 任何非 `FullAccess` 的策略 |
| 调用点（Spawn Site） | 任何要启动 OS 进程的代码位置；只有这里能施加 OS 级隔离 |
| 强制完整性（Enforcement） | 后端对策略的实际强制程度：`Full`（全部强制）或 `Partial`（有未强制部分） |

边界：本文件只定机制与形状。策略值、后端选择、审计输出、CLI 标志由宿主决定；v1 不做的事见 §11。

## 3. 接缝

```rust
pub trait SandboxProvider: Send + Sync + 'static {
    fn confine(&self, request: &SpawnRequest<'_>) -> Result<ConfinedSpawn, SandboxError>;
}

pub struct SpawnRequest<'a> {              // #[non_exhaustive]
    pub program: &'a OsStr,
    pub args: &'a [OsString],
    pub cwd: Option<&'a Path>,
    pub env: &'a BTreeMap<OsString, OsString>,  // 完整环境；调用方已合并继承值
    pub policy: &'a SandboxPolicy,
}

pub struct ConfinedSpawn {                 // #[non_exhaustive]
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<OsString, OsString>,  // 完整环境；provider 可增删
    pub enforcement: Enforcement,
}
```

规则：

- **纯变换、无状态、无 session**：策略已由调用方解析好；provider 不认 `SessionId`，可直接单测。
- **完整环境进出**：调用方负责把继承环境与覆盖项合并成完整 `env`；provider 有权净化或追加。这样「MCP 服务进程继承宿主环境」的风险有了统一的收窄点。
- **返回数据，不返回 `Command`**：调用方自己构造进程与管道；stdio 归调用方，provider 不碰。
- **不经过 shell**：`program` 与 `args` 原样传递，与 [#16](./plugins.md) 的传输规则一致。
- provider 在组装期构造；后端缺失或初始化失败时由宿主决定中止启动，还是让受限调用点 fail closed。
- `FullAccess` 可以映射为原样放行；即使没有 provider，`FullAccess` 也可以裸跑。没有 provider 时的受限策略是装配错误，不是运行时降级。

## 4. 调用点

```text
宿主组装
  ├─ 构造 SandboxProvider（后端 + allow_partial）
  ├─ 注入 asterpath-mcp ──> 每次 MCP stdio 启动 ──> confine ──> spawn
  ├─ 注入 spawn 型工具 ──> 每次子进程启动 ──> confine ──> spawn
  └─ 策略解析器（按 SessionId）
        └─ 工具调用时解析 policy ──> SpawnRequest.policy

纯进程内 Rust 工具 ──> 没有 spawn 点 ──> 只能 pre-execute / 装饰器拒绝或审计
HTTP MCP ──> 没有本地进程 ──> 不在沙箱范围
```

| 调用点 | 能否约束 | 说明 |
| --- | --- | --- |
| asterpath-mcp 启动 MCP stdio server | 是 | 启动时 constrain 一次；server 内部所有 tool call 共享同一边界，不再单独 confine |
| 宿主自带的 spawn 型工具（shell 等） | 是 | 每次 spawn 前调用 `confine` |
| 纯进程内 Rust 工具 | 否 | 没有进程边界；只能由 pre-execute 或工具包装器整次拒绝/审计 |
| HTTP MCP | 否 | 没有本地进程；网络维度不在 v1 策略里 |
| 子会话 | 继承 | 子会话里的工具按同一策略约束；子会话不是新的隔离边界 |

## 5. 策略模型

```rust
#[non_exhaustive]
pub enum SandboxPolicy {
    ReadOnly,                                         // 全盘可读，不可写
    WorkspaceWrite { writable_roots: Vec<PathBuf> },  // 全盘可读，仅 roots 可写
    FullAccess,                                       // 不约束
}
```

语义：

- `ReadOnly`：整个文件系统只读。进程通常仍需要临时目录；宿主应改用 `WorkspaceWrite` 并把临时目录放进 roots，而不是让后端偷偷放行 `/tmp`。
- `WorkspaceWrite`：只有 `writable_roots` 下可写；roots 必须是绝对路径。空 roots 是非法策略（配置错误），不静默退化成 `ReadOnly`。
- `FullAccess`：不施加约束，等价于 `Enforcement::Full` 的空变换。
- **不建模读白名单**：v1 的读范围始终是全盘。需要「隐藏 ~/.ssh」这类能力时另开策略维度，不塞进写档位。
- **不建模网络与进程可见性**：请求不存在的维度在类型上不可表达；后端不得声称强制了未建模的维度。文件系统沙箱 ≠ 网络隔离。

## 6. 策略解析与恢复

- provider 只收已解析的策略；解析是宿主代码，不是机制。
- 推荐优先级：**调用点静态配置 > session > 部署默认**。
  - 部署默认：组装期的兜底策略。
  - session：宿主解析器按 `SessionId` 保存；根会话在 `create_session` 时确定。
  - 调用点静态：宿主在工具构造或 pre-execute 中给出的覆盖。
- **模型参数不能改变策略**：工具参数里没有 sandbox 字段；模型可见的 spawn 参数只能收窄工具集或模型，不能放宽隔离。工具可以按自己的逻辑窄化策略，不能放宽 session 策略。
- **不落日志、不进 `ConfigSnapshot`**：与审批策略同构（[tools.md](./tools.md) §4）。恢复使用当前宿主装配；框架不重放、不校验策略。
- 需要「恢复后边界不变」的宿主，自己按 `SessionId` 持久化策略，并在打开会话时校验；这是宿主责任，不是内核接缝。
- v1 不支持运行中热换 provider 或策略；换配置 = 重启进程（与 [#16](./plugins.md) 一致）。

## 7. 强制完整性、失败与错误分类

```rust
#[non_exhaustive]
pub enum Enforcement {
    Full,
    Partial { unenforced: Vec<SandboxAspect> },   // 至少一个请求的限制没有真正强制
}

#[non_exhaustive]
pub enum SandboxAspect {
    FilesystemWrite,                               // v1 唯一可部分强制的维度
}

#[non_exhaustive]
pub enum SandboxError {
    Unsupported { reason: String },   // 后端无法完整强制请求的策略
    Refused { reason: String },       // 策略拒绝这次 spawn
    Setup { message: String },        // 后端构造隔离失败（缺少 bwrap、profile 生成失败等）
}
```

规则：

- **默认 fail closed**：`Unsupported` 时不启动进程，除非后端在构造时被显式配置为 `allow_partial = true`。允许 partial 时返回 `Enforcement::Partial { unenforced }`，携带未强制项供宿主审计；框架不落日志。
- `allow_partial` 是部署级配置，不是 per-call 开关；调用点不能把 `Partial` 升格成 `Full`。
- `Refused` 只用于**进程启动前**的策略拒绝；`Setup` 是后端故障。两者都不产生进程。
- 映射到工具结果：

| 情形 | 框架结果 |
| --- | --- |
| `SandboxError::Refused` | `ToolError::Denied { message }` → `ToolResult{Failed, Denied}` |
| `SandboxError::Unsupported` / `Setup` | `ToolError::Internal { message }` → `ToolResult{Failed, Internal}` |
| 受限策略 + 没有 provider | 装配错误；工具调用点返回 `Internal`，`asterpath-mcp` 让该服务 `Failed{Sandbox}` |
| 进程启动后的 `EACCES` / `EPERM` | 框架不可见；由工具自己的 `ToolError` 表达，**不是**管线的 `Denied` |

- OS 运行期拒绝没有可靠签名：退出码、stderr、errno 都可能被工具吞掉或改写。框架不做启发式分类；需要区分的工具在 `invoke` 内自己判断，并选择 `Denied` 或 `Internal`。
- bwrap 自身在 exec 前失败时，调用方只知道「子进程没有有效响应」；按 `Internal` 处理，不猜成 `Denied`。

## 8. 与审批的关系

- 审批与沙箱是两层，互不替代：`Allow` 只表示「可以做」，沙箱决定「能做到什么程度」；`Deny` 直接结束调用，不进入 spawn。
- 沙箱拒绝发生在审批之后、`invoke` 之内；它不是审批决定，不写 `SignalReceived`，也不会反向触发 `Ask`。
- **v1 不做 escalation**：没有「被沙箱拒绝 → 提权审批 → 自动重试」。Codex 的一次升级重试不在 v1。
- `ApprovalRequest` 不增加 sandbox 字段；`ApprovalPolicy` 是宿主代码，和沙箱解析器共享配置即可。
- 宿主若要「先问再放宽」，只能在自己的 pre-execute 里换用更宽的已解析策略；框架不提供这条流程，也不记录它。

## 9. 外部工具（MCP 服务）

- `McpServers::load(configs, sandbox, policy)` 注入 `Option<Arc<dyn SandboxProvider>>` 与部署级 `SandboxPolicy`；**每次 stdio 进程启动**都先 `confine`。
- **配置不声明沙箱**：`McpServerConfig` 没有 sandbox 字段（未知字段被拒绝）；让服务声明自己的约束等于让被约束方选择约束。
- 按服务的策略覆盖只作为宿主配置（按 `ServerId` 的宿主映射），不进配置，也不在 v1 规格里。
- 受限策略 + 没有 provider：该服务 `Failed{Sandbox}`（`required` 则 `McpError`），不裸跑。
- **fail closed（现状）**：provider 返回的 `enforcement` 不是 `Full` 时，该服务 `Failed{Sandbox}`，进程不启动。`Enforcement::Partial` 变体保留；放开条件：后端落地 `allow_partial` 部署配置后，`asterpath-mcp` 接受 `Partial` 并把强制级别透给宿主（例如经 `ServerStatus`）。
- MCP stdio：约束发生在 server 启动时；server 内部的 tool call 不再单独 confine——它们已经在同一个受限进程里。
- MCP HTTP：没有本地进程可包，**不在沙箱范围**；受限策略不会阻止它。宿主若需要硬边界，必须自己禁用 HTTP 传输。
- 服务重启 = 重新启动进程 = 重新 `confine`；不存在的「给运行中进程加沙箱」不做。
- 环境净化：provider 可通过 `ConfinedSpawn.env` 删除继承变量；v1 不把它定义成策略档位的一部分，后端可以做得更严。

## 10. 子会话

- **要求**：子会话继承父的已解析策略，且只能收窄；父不能通过 spawn 参数把更宽的策略传给子，子也不能自行放宽。
- **机制归宿主**：宿主解析器用 `SessionStore` 会话目录的父子边向上找到最近的已知 session，继承其策略；在 spawn 工具里拒绝任何 sandbox 覆盖参数。
- `SubagentConfig` / `ConfigSnapshot` 不变；每子沙箱 profile 不在 v1（与 [#8](./subagents.md) §5 一致）。
- 宿主解析不到子会话策略时回落到部署默认，不是 `FullAccess`。

## 11. 参考后端与 v1 范围

**Linux（M5 已实现的后端：`asterpath-sandbox-linux`）**：用 bwrap 做文件系统映射。

| 策略 | 变换 |
| --- | --- |
| `ReadOnly` | `bwrap --ro-bind / / --dev /dev --proc /proc --die-with-parent` |
| `WorkspaceWrite` | 在 `ReadOnly` 基础上，对每个 root 追加 `--bind <root> <root>` |
| `FullAccess` | 不包装 |

- 保留调用方的 `cwd`（`--chdir`）与 `env`。
- **不加 `--unshare-net`、不加 `--unshare-pid`**：网络与进程可见性未建模；加了会暗示不存在的保证。
- bwrap 缺失或无法建命名空间 → `Setup` → fail closed。
- bwrap 在 exec 前失败可能只表现为子进程异常退出；调用方按 `Internal` 处理。

**macOS**：`sandbox-exec -p <profile>` 的形态记录为第二后端，v1 不实现、不验收。**Windows**：出界。

**v1 交付（历史）**：

- 合约形状（§3–§7）、本文件、ADR-0019、验收场景由 v1 交付。
- M5 实现了合约 crate 与 Linux bwrap 后端；#80（ADR-0027）把合约并入 `asterpath-protocol`，`asterpath-sandbox-linux` 保留。

**明确不做**：seccomp、网络隔离、PID/进程可见性、读白名单、Windows、进程内工具沙箱、escalation、热替换、配置声明策略、运行中改边界、把已运行进程纳入沙箱、mode 审计日志。

## 12. 测试与验收场景

- `asterpath-testing` 在合约 crate 落地时增加 `FakeSandboxProvider`：可配置返回 `Full` / `Partial` / `Unsupported` / `Refused` / `Setup`，并记录每次 `SpawnRequest`。
- **不新增 core 注入点**：`confine` 发生在 `Tool::invoke` 内部，[#19](./testing.md) 的 `tool:before_effect` / `tool:after_effect` 已经覆盖；后端级失败（bwrap 缺失、profile 构造失败）是后端 crate 的单测。
- 运行中热换 provider 不测——v1 不支持。

| # | 场景 | 断言 |
| --- | --- | --- |
| S1 | 受限策略 + 没有 provider | 装配错误；spawn 型工具返回 `Failed{Internal}`，不产生进程 |
| S2 | `Unsupported` + 未 opt-in partial | 无进程；`Failed{Internal}` |
| S3 | `Unsupported` + `allow_partial` | 进程启动；`enforcement = Partial`；宿主可记录 |
| S4 | `Refused` | 无进程；`Failed{Denied}`，message 带 `sandbox:` 前缀 |
| S5 | 策略优先级 | 调用点静态 > session > 部署默认；模型参数无法改变策略 |
| S6 | 审批 Allow + `ReadOnly` | 仍按 `ReadOnly` 约束；Allow 不放宽沙箱 |
| S7 | `asterpath-mcp` 启动 stdio server | 以 `confine` 后的命令启动；server 内调用不再 confine |
| S8 | HTTP MCP + 受限策略 | 不受沙箱约束；文档要求宿主自担（不在验收闸门） |
| S9 | 子会话 | 继承父策略；收窄生效；放宽被拒 |
| S10 | bwrap 缺失 | `Setup` → fail closed；不裸跑 |
| S11 | 崩溃恢复 | 策略不来自日志；使用当前宿主装配；需要粘性的宿主自行校验 |
| S12 | 进程内工具 | provider 不被调用；装饰器/pre-execute 只能整次拒绝或审计 |

## 13. 留给下游

- **#15**：facade / `AsterpathConfig` 不变；将来接入时，provider 与策略由模块/工具构造注入，不进稳定面。
- **#20**：stdio 启动点调用 provider；HTTP 明确出界（[mcp.md](./mcp.md) §12）。
- **#16 / #80**：配置无 sandbox 字段；受限策略下 `asterpath-mcp` 必须有 provider，否则 `Failed{Sandbox}`；非 `Full` 强制级别 fail closed。
- **#8**：继承由宿主解析器完成；`ConfigSnapshot` 不变。
- **#19**：无新故障注入点；`FakeSandboxProvider` 与 `tool:*` 装饰器足够。
- **实现方**：合约在 protocol，fake 在 `asterpath-testing`，已接 `asterpath-mcp`；spawn 型工具同样调用 provider；参考后端按 §11 的映射实现。
