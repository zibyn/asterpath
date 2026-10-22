# 工具契约、执行管线与审批边界

> 来源：[#7](https://github.com/zibyn/asterpath/issues/7)。本文件定义 `Tool` 契约、执行管线、审批、并行调度、外部进程边界、结果形状与截断规则。
>
> 对齐：[#2 模块契约](./module-contracts.md)、[#3 事件日志](./event-log-recovery.md)、[#6 agent loop](./agent-loop.md)。
>
> 本票对 #3 的修订（已同步到 `event-log-recovery.md`）：
>
> - `ToolResult` 增加 `error` 与 `truncated`。
> - `ToolCallRequested.raw_args` 明确为 pre-execute 之后的 **effective args**。
> - 审批决定复用 `SignalReceived`，signal name = `tool.approval.<tool_call_id>`；不新增审批事件。
>
> 相关 ADR：[0007 审批是框架管线，隔离与内容安全是扩展模块责任](../adr/0007-approval-and-sandbox-layers.md)。

## 1. 决策摘要

- **管线固定**：pre-execute → 意图记录 → 审批 → 执行 → 截断 → post-execute → 结果记录。只有 pre-execute 与 post-execute 可拦截；其余只能观察。
- **审批是框架管线阶段**。框架定义策略 seam、请求/决定事件与持久等待；具体审批 UI 与策略由 host 提供。
- **审批决定复用 `SignalReceived`**，跨崩溃；审批请求是进程内 typed event，不进日志。
- **`External` 默认 fail closed**：没有审批端时按 Deny 处理；`ReplaySafety` 默认 `Unsafe`。
- **并行/独占按 step 内锁调度**；结果完成即追加，模型投影按调用顺序重排。
- **工具失败不重试**；超时、取消、panic、外部崩溃统一映射为 `ToolResult`。
- **输出截断**：默认 64 KiB/调用、256 KiB/step；完整输出不落日志。

## 2. Tool 契约

```rust
pub trait Tool: Send + Sync + 'static {
    fn descriptor(&self) -> &ToolDescriptor;
    fn invoke(&self, args: ToolArgs, ctx: ToolContext)
        -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
}

pub struct ToolDescriptor {
    pub name: ToolName,
    pub description: String,
    pub schema: JsonSchema,
    pub parallelism: Parallelism,   // Parallel | Exclusive
    pub replay: ReplaySafety,       // Safe | Unsafe
    pub side_effect: SideEffect,    // ReadOnly | Mutating
    pub provenance: Provenance,     // InProcess | External { transport }；Unknown 仅出现在请求记录
    pub timeout: Option<Duration>,
    pub max_output_bytes: Option<usize>,
}

pub struct ToolContext {           // #[non_exhaustive]
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub cancel: CancelToken,
}

pub struct ToolOutput {
    pub content: Vec<Part>,
}

#[non_exhaustive]
pub enum ToolError {
    InvalidArgs { message: String },
    Transport { message: String },
    Internal { message: String },
}
```

规则：

- `ToolArgs = JsonString`：保持 provider 原始 JSON 字符串，工具自行解析；框架不做 re-serialize。
- **超时由管线执行**，工具只观察 `CancelToken`（`is_cancelled` / `cancelled`），不自己管 deadline。
- `CancelToken` 只在本次调用内有效；具体类型归 #15 打包。
- 工具不写日志、不写 `ToolResult`；返回值由管线落盘。
- `ToolDescriptor` 在工具生命周期内不可变。
- `ToolName` 在注册表中唯一：进程内工具用裸名，字符集 `[A-Za-z0-9_-]`、长度 1–64，`__` 保留；外部工具暴露为 `<plugin_id>__<sanitized_name>`。命名空间、重名与整插件失败规则见 [plugins.md](plugins.md) §6。
- `Provenance::Unknown` 只出现在 `ToolCallRequested`（请求的工具不在注册表），描述符不得使用；它不进入审批。
- `side_effect` 是**声明**，不是强制；审批策略消费它。
- `description` 是模型可见的工具说明；补上 #2 的最小 seam。
- `max_output_bytes` 是该工具的单次上限覆盖；`None` 用默认值。

## 3. 执行管线

```text
AssistantMessage(tool_calls)
  for each call，按并行/独占规则调度：
    pre-execute waterfall          # allow / deny / ask / 改写参数
    append ToolCallRequested { effective args }
    if tool not in registry: append ToolResult{Failed, NotFound}; done
    approval policy                # Allow / Ask / Deny
    if Ask: await signal           # 可跨天、跨崩溃
    if Deny: append ToolResult{Failed, Denied}; done
    acquire Parallel / Exclusive lock
    invoke (timeout + cancel)
    truncate                       # 单次 + 本 step 累计上限
    post-execute waterfall         # 宿主/扩展模块塑形
    append ToolResult { final }
```

阶段规则：

- **每个模型发起的调用都会写 `ToolCallRequested`**，即使随后被拒绝；意图与终态一一对应（I2）。
- 工具不在注册表时：pre-execute 照常运行；若未被拒绝，写意图（`replay = Unsafe`、`provenance = Unknown`），跳过审批与执行，写 `ToolResult{Failed, NotFound}`。模型幻觉、插件升级后移除工具都走这条路径。
- pre-execute 在写意图前运行。它可以改写参数，日志记录改写后的 **effective args**；模型原始参数仍在 `AssistantMessage` 中。
- pre-execute 崩溃或报错不产生副作用；恢复时重跑该调用的 pre-execute。
- deny（来自 pre-execute、策略或审批）写 `ToolResult{Failed, error.kind = Denied}`，跳过执行与 post-execute。
- **`ToolCallRequested` 是副作用的点无回退**：写完它之后，任何执行都必须以重放安全规则收尾。
- post-execute 在结果落盘前运行，可以改写 `content` / `error.message`（格式化、脱敏、审计改写），但**不得产生副作用、不得写日志**。
- 除两个 waterfall 外，观察者只能通过 emit 接收通知，不能阻止或改写管线。

## 4. 审批

```rust
pub trait ApprovalPolicy: Send + Sync {
    fn classify(&self, req: &ApprovalRequest) -> ApprovalVerdict;
}

pub enum ApprovalVerdict {
    Allow,
    Ask,
    Deny { reason: Option<String> },
}

pub struct ApprovalRequest {        // #[non_exhaustive]
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub tool: ToolName,
    pub args: JsonString,           // effective args
    pub replay: ReplaySafety,
    pub side_effect: SideEffect,
    pub provenance: Provenance,
}
```

- 策略在 `ToolCallRequested` 之后、执行之前运行；输入是 effective args 与来源/副作用声明。
- `Ask` 时管线发一条 `ApprovalRequested` typed event（观察者可见，**不落日志**），然后 `DurableExecutor::await_signal(AwaitSignal { intent: SignalIntent::AlreadyRecorded, name: "tool.approval.<tool_call_id>", .. })`。
- 决定由 host 的审批端投递为 `SignalReceived { signal_id: tool_call_id, name: "tool.approval.<tool_call_id>", payload: {Allow | Deny, reason?} }`。
- 已记录的 Allow 让调用继续执行；Deny 写 `ToolResult{Failed, Denied}`。两者都不重新提示。
- Deny 的 `reason` 写入 `ToolResult.error.message`；没有 reason 时用固定文本。
- **恢复**：open `ToolCallRequested` + 无决定信号 + 策略为 `Ask` → 重新挂起并重新发 `ApprovalRequested`；有决定信号则按记录执行，**不再看当前策略**。
- 策略必须对同一 `ApprovalRequest` + session config 稳定；恢复用同一策略重跑，结果必须与崩溃前一致。host 不得在调用 open 期间热换策略（v1 无热替换，#2）。
- 审批粒度是单次调用；session 级「本次全部允许」是 host 的缓存策略，不落日志、不改变已记录的决定。
- 不引入独立的 `Approver` trait：host 通过订阅 `ApprovalRequested` 并投递 signal 来充当审批端；等待由 `DurableExecutor` 承担（#17）。
- signal name 只在会话内唯一；signal id 由 `(session, name)` 确定性派生（审批 name 为 `tool.approval.<tool_call_id>`），重复注册/投递不会产生第二个等待。`DurableExecutor` 按 session 作用域等待（#17）。
- 非审批的 `await_signal` 在等待开始前写 `SignalAwaited`（#3）；审批不写，因为 open 的 `ToolCallRequested` 已承担意图记录。
- 未安装任何审批端时，`Ask` 按 `Deny` 处理（fail closed）。

默认策略（未安装自定义 `ApprovalPolicy` 时）：

| provenance | 有审批端 | 默认 |
| --- | --- | --- |
| `InProcess` | 任意 | `Allow` |
| `External` | 是 | `Ask` |
| `External` | 否 | `Deny` |

- `side_effect` 只作为自定义策略的输入；默认策略不看它。
- `Provenance::Unknown` 不进入审批：工具不存在，管线直接写 `NotFound`，不会发 `ApprovalRequested`。
- 参考 CLI 的审批端与默认策略归 [#15](https://github.com/zibyn/asterpath/issues/15)。

## 5. 并行与独占

- `Parallelism::Parallel` 取本 step 的读锁；`Exclusive` 取写锁。锁的作用域是**同一 step 的工具组**，不是全局；不同 session 不互相阻塞。
- 每个调用一个 tokio task；`max_parallel_calls`（默认 8）限制并发，超出的调用排队。
- 一个 `Exclusive` 调用与同 step 内所有其他调用互斥；多个 `Exclusive` 之间的顺序不保证。
- **结果完成即追加**，不在内存里等整组结束。模型投影按 `ToolCallRequested` 的 `seq` 重排，因此日志的完成顺序不影响模型历史。
- 单个工具失败、超时或 panic 不影响兄弟调用；每个调用都会得到一个终态 `ToolResult`。只有回合取消会停止整组。
- step 等所有调用终结后才写 `StepCommitted`。

## 6. 超时、取消、panic 与外部进程

**超时**

- 单次调用取 `ToolDescriptor.timeout`；`None` 时进程内工具不设超时，external adapter 用默认 `120s`。
- 到期：发取消信号 → 宽限 `kill_grace`（默认 5s）→ drop future / SIGKILL；写 `ToolResult{Failed, Timeout}`。
- 超时不重试。

**取消**

- 回合取消向所有在途调用广播 `CancelToken`（#6）。
- 在 `cancel_grace`（默认 10s）内返回的工具按真实结果记 `Completed` / `Failed`；到期未返回的写 `Interrupted`。
- 子进程：先 SIGTERM，`kill_grace` 后 SIGKILL；在平台支持时杀进程组/树，不留孤儿。
- MCP：若 server 支持 `notifications/cancelled` 就发送；宽限内无响应则关闭连接并杀死 server 进程。

**panic**

- 每个调用在独立 task 中运行；panic 由运行时捕获，映射为 `ToolResult{Failed, Panic}`，回合继续。

**外部崩溃/协议错误**

- 子进程无有效响应退出、协议错误、断连 → `Failed{Transport}`。
- 工具在产生部分副作用后报错是工具自身责任；框架只记录终态。

**外部默认**

- `Provenance::External` 默认 `ReplaySafety::Unsafe`，除非 adapter 明确声明 `Safe` 并承担重复副作用。
- adapter 在跨进程边界前截断输出；管线再按统一上限截断（幂等）。

## 7. 结果、错误与截断

事件负载（修订 #3）：

```rust
ToolResult {
    tool_call_id: ToolCallId,
    status: ToolResultStatus,        // Completed | Failed | Interrupted
    output: Option<Vec<Part>>,       // 模型可见，已截断
    error: Option<ToolErrorInfo>,    // Failed 必带
    truncated: bool,
}

pub struct ToolErrorInfo {
    pub kind: ToolErrorKind,
    pub message: String,             // 模型可见
}

pub enum ToolErrorKind {
    InvalidArgs, Denied, Timeout, Cancelled, Panic, Transport, Internal,
    NotFound,                        // 工具不在注册表；模型可见
}
```

规则：

- `Completed`：`output` 可空，`error` 为 `None`；可以 `truncated`。
- `Failed`：`error` 必须存在；`output` 为 `None`（诊断信息放 `error.message`）。
- `Interrupted`：`output` / `error` 为 `None`；投影合成固定的中断提示。
- `Cancelled` 用于工具或外部 server 在宽限内自行报告取消；管线到期 drop 的调用写 `Interrupted`。
- 默认上限：64 KiB/调用、256 KiB/step（按模型可见 content 的序列化字节计）；工具可用 `max_output_bytes` 覆盖单次值，但不能突破 step 累计上限。
- 截断在 UTF-8 边界进行，并在 content 末尾附 `"[output truncated: N bytes omitted]"`；`truncated = true`。
- **完整输出不落日志**；日志是模型可见内容的有界记录，不是工具的原始输出存储。

状态保持三种（与 #3 一致）；`Denied` / `Timeout` / `Panic` 等通过 `error.kind` 表达，不新增 status。`NotFound` 由管线产生，不由工具返回：工具不存在时没有 `invoke`，因此 `ToolError` 不需要对应变体；`ToolErrorKind` 是事件级分类。

## 8. 结果去向

| 消费方 | 来源 | 规则 |
| --- | --- | --- |
| 事件日志 | `ToolResult` | 唯一真相；post-execute 与截断之后 |
| 模型投影 | `ToolResult.output` | 按 `ToolCallRequested` 顺序折叠；`Interrupted` 用固定提示；截断提示已含在 content |
| 追踪 | tool span | `ToolCallRequested` → `ToolResult`；默认不含参数/输出 |
| token 统计 | 无独立记录 | 工具输出进入下一次模型请求，已包含在 `AssistantMessage.usage` 的 input tokens 中 |

- 工具执行本身不消耗 token；不新增工具级 token 口径（#13）。

## 9. 默认配置

```rust
pub struct ToolConfig {
    pub max_parallel_calls: NonZeroUsize,       // 8
    pub max_output_bytes: usize,                // 64 KiB
    pub max_output_bytes_per_step: usize,       // 256 KiB
    pub external_timeout: Option<Duration>,     // Some(120s)
    pub kill_grace: Duration,                   // 5s
}
```

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `max_parallel_calls` | 8 | 每 step 的工具并发上限 |
| `max_output_bytes` | 64 KiB | 单次调用 |
| `max_output_bytes_per_step` | 256 KiB | 本 step 累计 |
| `external_timeout` | 120s | descriptor 未覆盖时 |
| `kill_grace` | 5s | 超时/取消后的强杀宽限 |
| `cancel_grace` | 10s | 回合取消宽限，归 #6 |
| external `replay` | `Unsafe` | adapter 未声明时 |
| 审批默认 | 见 §4 | `External` fail closed |
| 追踪内容 | 关闭 | 显式 opt-in |

## 10. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| T1 | pre-execute 改写参数 | `ToolCallRequested` 记 effective args；`AssistantMessage` 保留原始 args；工具按 effective args 执行 |
| T2 | pre-execute deny | 写 `ToolResult{Failed, Denied}`；不审批、不执行；恢复不重跑 |
| T3 | Ask + 批准 + 崩溃于执行前 | `SignalReceived` 已记录；恢复后直接执行，不重新提示 |
| T4 | Ask + 未决 + 崩溃 | 恢复重新挂起并重新发请求；没有 `ToolResult` |
| T5 | Ask + 拒绝 | 不执行；`ToolResult{Failed, Denied}` 模型可见 |
| T6 | 并行与独占混合 | `Exclusive` 期间无其他调用；结果按完成顺序落日志，投影按调用顺序 |
| T7 | 一个工具 panic | 该调用 `Failed{Panic}`；兄弟正常完成；step 正常提交 |
| T8 | 超时 | `Failed{Timeout}`；子进程被杀；无孤儿 |
| T9 | 工具执行中取消 | 宽限内完成记真实结果；其余 `Interrupted`；子进程被杀 |
| T10 | 输出超过上限 | 截断到 UTF-8 边界；`truncated = true`；日志不含完整输出 |
| T11 | external 未声明 replay | 按 `Unsafe`；恢复写 `Interrupted` |
| T12 | 未装审批端且策略为 Ask | 按 `Deny` 处理（fail closed） |
| T13 | 结果进入下一次模型请求 | 投影顺序 = 调用顺序；不产生独立 token 记录 |
| T14 | 模型请求注册表中不存在的工具 | 仍写 `ToolCallRequested`；`ToolResult{Failed, NotFound}`；不审批、不执行；恢复按 [plugins.md](plugins.md) §8 |

## 11. 扩展点（非内核）

以下能力由模块/插件实现，内核只提供接入点：

- **沙箱/隔离**：adapter 或工具包装模块；内核只带 `Provenance`，不建模 profile。包装器可以在 `invoke` 前后做隔离、审计或拒绝。
- **内容脱敏/密钥**：host 或 `post-execute` 模块；内核不提供默认规则。`post-execute` 在落盘前运行，是唯一接入点。
- **成本/预算**：不是工具层扩展；usage 与 pre-step gate 见 [usage.md](usage.md) / [agent-loop.md](agent-loop.md)。
- **内置工具**：可选模块或参考 CLI，不在内核。

## 12. 留给下游

- **#11**：已定。`Part`、`Item`、`JsonString`、`JsonValue`、`Usage`；工具参数保持原始 JSON 字符串；session `ToolResult` 映射为规范 `ToolResult` item（`is_error = status != Completed`）。类型与映射见 [protocol.md](protocol.md)。
- **#12**：tool span 命名与属性；默认只记元数据。
- **#13**：已定。工具输出计入下一次模型请求的 input tokens，不单独统计；见 [usage.md](usage.md)。
- **#15**：参考 CLI 的审批端/默认策略、内置工具集与配置。
- **#16**：已定。工具名命名空间与重名策略、插件清单与版本协商见 [plugins.md](plugins.md)；`ToolErrorKind::NotFound` 与未知工具路径已回写本文件。
- **#17**：已定。`DurableExecutor::await_signal` 以 `SignalIntent::AlreadyRecorded` 支持审批等待，不写 `SignalAwaited`；见 [ADR-0014](../adr/0014-subagent-scheduling-api.md)。
- **#8**：已定。spawn 是同步工具调用，沿用同一审批/等待/取消规则；`ReplaySafety::Safe` 由查找幂等保证。见 [subagents.md](subagents.md)。
