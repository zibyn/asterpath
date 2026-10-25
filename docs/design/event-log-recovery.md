# 事件日志与检查点恢复语义

> 来源：[#3](https://github.com/zibyn/asterpath/issues/3)。本文件定义日志的**逻辑语义**：只有会话流，子智能体是子会话；物理持久性见 [storage.md](storage.md)。
>
> 相关 ADR：[0002 日志模型](../adr/0002-log-topology-and-commit-semantics.md)。

## 1. 决策摘要

- 日志按 **`SessionId`** 分片：每个 session 一条 append-only 日志，session 之间无事务。**没有 `Workflow` 流类型。**
- **终态条目**是提交单位；流式 delta 不落盘。
- 不变量：**模型可见即已记录**。任何进入下一次请求上下文的内容、任何副作用意图，先于该动作提交。
- **检查点 = 最后一个完整的 `StepCommitted` 边界**，没有独立的 checkpoint 记录。
- 日志层**没有 ack**：完成 = 写了终态记录。存储也没有租约或 ack；唤醒是只读发现（[storage.md](storage.md) §7）。
- 恢复 = 只读已提交前缀 → 定位检查点 → 重放已提交事件 → 处理**未闭合意图**。
- **`Idle` 由未闭合意图定义**：存在任一非 compaction 的 open intent 即非 `Idle`。恢复、唤醒发现与退役共用同一份会话事实折叠（`SessionFacts`，§6、[storage.md](storage.md) §7）。
- **等待是事件**：持久等待只有两种——人工审批（意图是 open `ToolCallRequested`，终态是 `ApprovalDecided`）与子会话完成（`ChildSessionSpawned` → `ChildSessionCompleted`）；崩溃后重新挂起。spawn 超时是 spawn 记录上的截止时间，不是独立 timer（[ADR-0008](../adr/0008-subagent-model.md)）。
- **压缩先写意图**：摘要调用前写 `CompactionStarted`，成功由 `CompactionApplied` 闭合，失败由 `CompactionAborted` 闭合。
- 幂等：`(session, 事件类型, dedup_key)` 唯一，重复 append 返回原 `LogId`。
- 信封的 `recorded_at` 是诊断时间，只供 trace/审计读取；任何恢复、投影或分支逻辑不得读取它。`seq` 仍是唯一排序依据。
- session 恢复写一条非模型可见的 `StreamResumed`。
- compaction 的事件形状由本文件定，策略、算法与参数归 [#9](https://github.com/zibyn/asterpath/issues/9)，见 [compaction.md](compaction.md)。

## 2. 日志流模型

```rust
pub struct ParentRef {
    pub session: SessionId,
    pub turn: TurnId,
    pub tool_call: ToolCallId,
}
```

- `SessionId` 就是流身份，不再有 `StreamId` 枚举。
- 子智能体是**独立子 session**：独立日志、独立恢复与取消单位。
- 父流记 `ChildSessionSpawned`，记录父子关系；子流首条 `SessionCreated` 的 `parent` 指回父会话、turn 与 tool call。
- 子会话创建时，父流写 `ChildSessionSpawned`（携带子配置与上下文策略）；子结束时父流写终态 `ChildSessionCompleted`（幂等，key = child id）。二者在正常运行时由 scheduler 写，恢复时由 spawn 工具重放经引擎的 `Scheduler` 代写（#17）。
- 父子之间没有原子性。关闭顺序、孤儿处理、结果形状见 [subagents.md](subagents.md)。
- 等待（审批、子会话完成）是 session 事件；不引入 activity / workflow 概念，也没有通用持久 timer / signal。

## 3. 事件枚举

```rust
#[non_exhaustive]
pub enum SessionEvent {
    SessionCreated { config: ConfigSnapshot, parent: Option<ParentRef>, metadata: BTreeMap<String, String> },  // metadata：宿主元数据，ADR-0032
    ContextForked { source: ForkSource, source_log: LogId,
                    items: Vec<Item> },                           // fork 子会话或会话分叉的首个模型可见记录（ADR-0033）
    StreamResumed { from: LogId, reason: ResumeReason },          // 非模型可见
    InputRecorded { input: Vec<Part> },                           // 模型可见
    TurnStarted { turn_id: TurnId },
    StepStarted { turn_id: TurnId, step_id: StepId },
    AssistantMessage { turn_id: TurnId, step_id: StepId,
                       items: Vec<Item>, raw: Option<RawProviderData>,
                       model: ModelSpec, usage: Usage, stop_reason: StopReason },
    ModelCallAborted { turn_id: TurnId, step_id: StepId, attempt: u32,
                       kind: AbortKind, partial: Option<Vec<Item>>,
                       model: ModelSpec,
                       backoff_ms: u64,         // 下一 attempt 前的实际退避；不重试为 0
                       usage: Option<Usage> },  // 非模型可见；usage 为 best-effort
    ToolCallRequested { turn_id: TurnId, step_id: StepId,
                        tool_call_id: ToolCallId,        // 框架生成的不透明身份
                        provider_call_id: String,        // 模型响应里的原始 id
                        name: ToolName,
                        raw_args: JsonString,   // pre-execute 之后的 effective args
                        replay: ReplaySafety, provenance: Provenance },
    ToolResult { tool_call_id: ToolCallId, status: ToolResultStatus,
                 output: Option<Vec<Part>>,
                 error: Option<ToolErrorInfo>, truncated: bool },
    StepCommitted { turn_id: TurnId, step_id: StepId },  // 检查点标记
    TurnCommitted { turn_id: TurnId, reason: TurnEndReason },
    CompactionStarted { trigger: CompactionTrigger,
                        replaces_from: LogId, caused_by: LogId },
    CompactionApplied { first_kept: LogId, summary: Vec<Part>,
                        replaces: LogIdRange,
                        model: ModelSpec, usage: Usage,
                        raw: Option<RawProviderData> },
    CompactionAborted { reason: CompactionAbortReason },
    ChildSessionSpawned { child: SessionId, task: Vec<Part>,
                          config: ConfigSnapshot, context: ContextPolicy,
                          parent_tool_call_id: ToolCallId,
                          caused_by: LogId,
                          deadline: Option<Timestamp> },
    ChildSessionCompleted { child: SessionId, outcome: ChildOutcome },
    ApprovalDecided { tool_call_id: ToolCallId, decision: ApprovalDecision },
    ConfigChanged { patch: ConfigPatch },                         // 下一个 StepStarted 生效；append_system / 工具增删投影为 Instruction / ToolsChanged 条目
}

pub enum AbortKind { Transport, Rejected, Cancelled, Crash }
pub enum CompactionTrigger { Threshold, Manual, ProviderError }
pub enum CompactionAbortReason { SummaryFailed, Insufficient }
pub enum ToolResultStatus { Completed, Failed, Interrupted }
pub struct ToolErrorInfo { kind: ToolErrorKind, message: String }
pub enum ToolErrorKind {
    InvalidArgs, Denied, Timeout, Cancelled, Panic, Transport, Internal,
    NotFound,                        // 工具不在注册表；模型可见
}
pub enum TurnEndReason {
    Completed, Cancelled, Failed, Stopped { reason: String },
    MaxStepsExceeded, ContextExhausted, CompactionFailed,
    Refused,                         // 模型拒答（StopReason::Refusal）
}
// ChildOutcome 的形状见 subagents.md §6。
```

规则：

- **没有通用 `Error` 事件**。错误是它所附着事件的终态（`ModelCallAborted`、`ToolResult::Failed`、`ChildOutcome`）。
- `ToolResult` 是工具调用的唯一终态：`Failed` 必带 `error`；`Interrupted` 的 `output` / `error` 为 `None`。
- `ToolCallRequested` 的两个 id 分工固定：`tool_call_id` 由框架生成，用于 dedup key、审批决定与 open intent；`provider_call_id` 是 `AssistantMessage.items` 中 `ToolCall.call_id` 的原值。投影与恢复按 `provider_call_id` 把意图对回 `ToolCall`，`ToolResult` 只引用 `tool_call_id`，不重复存 provider id。
- `ToolCallRequested` 的 `provenance` 可以是 `Unknown`（请求的工具不在注册表），此时 `replay` 记 `Unsafe`；恢复按 [plugins.md](plugins.md) §8 处理。`NotFound` 是模型可见的工具错误类型，不新增 status。
- 审批决定是 `ApprovalDecided { tool_call_id, decision: Allow | Deny }`；等待意图由 open `ToolCallRequested` 承担。
- `StepCommitted` 是 step 的通用终态：成功、失败、取消都写它；回合结果由 `TurnCommitted.reason` 表达（#6）。
- `usage` 与 `stop_reason` 记在 `AssistantMessage`；`StepCommitted` 不再携带。`model` 记在三个承载 usage 的终态记录上，是发起请求的 `ModelSpec`（#13）。
- `usage` 的桶、聚合与重放去重规则见 [usage.md](usage.md)；日志层只负责让每条终态记录唯一。
- `CompactionStarted` 是摘要调用的意图记录，`CompactionApplied`（成功）与 `CompactionAborted`（`SummaryFailed` / `Insufficient`）是它的终态；失败的摘要 attempt 不落日志，`CompactionAborted` 因此不携带 usage / raw。`CompactionApplied` 不是 step：可在回合内 BetweenSteps / 强制压缩或回合外 Idle 追加；成功摘要调用的 `usage` / `raw` 挂在事件上。`replaces` 起点为流首或上一条 `replaces` 起点，投影只保留最新 summary（[compaction.md](compaction.md)）。
- `AbortKind::Transport` 是可重试的传输/服务端错误；`Rejected` 是 provider 拒绝请求（不可重试）。
- `caused_by` 指向触发该记录的日志记录（例如 spawn 对应的 `ToolCallRequested`、触发压缩的 frontier 记录）。
- `ChildSessionSpawned.deadline` 是 spawn 超时的绝对截止时间（`spawn 时的注入时钟 + timeout`），与 spawn 同一次 append 写入；`None` 表示不限时。计时起点是 spawn 记录，排队不计。
- `Crash` 不是现场写下的记录；恢复时检测到未闭合意图才补记（见 §6）。
- `Usage`、`Item` / `Part`、`JsonString`、`ConfigSnapshot`、`ContextPolicy`、能力标志的类型归 [#11](https://github.com/zibyn/asterpath/issues/11)，见 [protocol.md](protocol.md)；`ChildOutcome` 的形状见 [subagents.md](subagents.md)。
- `LogIdRange { first: LogId, last: LogId }` 是 `asterpath-protocol` 的值类型，结构定义见 [tracing.md](tracing.md) §2；本文件与 [compaction.md](compaction.md)、[protocol.md](protocol.md) 共用该定义。
- `ApprovalDecided` 的等待意图是 open 的 `ToolCallRequested`（[tools.md](tools.md)）。它本身不模型可见。
- `ConfigChanged` 是宿主经 `update_config` 写入的字段补丁（[ADR-0030](../adr/0030-session-config-is-a-fold-of-log-records.md)，[packaging.md](packaging.md) §2.1），不是意图，没有终态，不影响 `SessionFacts`。生效配置按水位折叠：模型请求取 `seq < StepStarted.seq` 的变更，所以在 `ConfigChanged` 之后、下一个 `StepStarted` 之前崩溃，恢复渲染出的请求与不崩溃时相同；open step 恢复时沿用原水位，step 内追加的变更仍等下一个 step。`append_system` 由投影在下一个 `StepStarted` 处生成 `Instruction` 条目（[protocol.md](protocol.md) §3.3），位置只取决于日志，恢复同样确定。
- 子会话等待的 API 与调度归 [#17](https://github.com/zibyn/asterpath/issues/17)（已定，[ADR-0008](../adr/0008-subagent-model.md)）；存储投递归 #5。

## 4. 信封与幂等键

```rust
// protocol：唯一的日志信封；字段定义以 storage.md §3 为准
pub struct NewEntry {
    pub session: SessionId,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: serde_json::Value, // 调用方已序列化；存储不解释内容
}

pub struct LogEntry {
    pub log_id: LogId,
    pub session: SessionId,
    pub seq: u64,                 // 存储层按 session 单调分配
    pub recorded_at: Timestamp,   // 存储层写入；仅 trace/审计可读
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: serde_json::Value,
}

// protocol：类型化视图
pub fn decode_entry(entry: &LogEntry) -> Result<SessionEvent, LogDecodeError>;
```

- `decode_entry` 先检查 `schema_version`（版本规则见 [storage.md](storage.md) §4），再反序列化为 `SessionEvent`；版本不符、未知变体或缺失必填字段都是 `LogDecodeError` 硬错误。本文件后续说“事件”时均指解码后的 `SessionEvent`；存储层只处理信封，不解释事件（[ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)）。
- `recorded_at` 只由 trace/审计读取，**任何恢复、投影或分支逻辑都不得读取它**。`ChildSessionSpawned.deadline` 是调度逻辑使用的截止时间，不属于诊断时间。
- `dedup_key` 在 `(session, 事件类型, key)` 上唯一；命中已存在键时 `append` 返回原 `LogId`，不报错、不写入。事件类型取自 `payload` 的顶层键（解码前），是键判别式的一部分；物理存储见 [storage.md](storage.md) §4。

| 事件 | dedup key |
| --- | --- |
| `SessionCreated` | session id |
| `ContextForked` | 新会话 id（fork 子会话或会话分叉） |
| `InputRecorded` | 调用方提供的 input id |
| `AssistantMessage` | `(turn_id, step_id)` |
| `ToolCallRequested` | 意图提交时生成的 `tool_call_id` |
| `ToolResult` | `tool_call_id` |
| `StepCommitted` / `TurnCommitted` | step / turn id |
| `CompactionStarted` / `CompactionApplied` / `CompactionAborted` | `compaction:<意图提交前 frontier LogId>` |
| `ChildSessionSpawned` / `ChildSessionCompleted` | child session id |
| `ApprovalDecided` | `tool_call_id` |
| `ConfigChanged` | `ConfigPatch.id`（宿主提供；可缺省，`append_system` / `add_tools` / `remove_tools` 非空时必填） |

表中的 key 按事件类型隔离：`ToolCallRequested`、`ApprovalDecided` 与 `ToolResult` 都用 `tool_call_id`，但属于不同事件类型，互不抑制；`StepStarted` / `TurnStarted` 不设 dedup key，恢复时不重复写。`ConfigChanged` 只由宿主调用写入，恢复从不补写；只覆盖字段的补丁重复写入时折叠结果不变，可以不带键；追加 system 与增删工具不幂等，所以带 `append_system` / `add_tools` / `remove_tools` 的更新必须带 `id`，宿主重试用同一个 `id`。**判别式是持久值**：带 dedup key 的事件变体名一经发布不得改名；确需改名时，事件类型到判别式的映射必须保持旧值，upcast 只改内存表示（[ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)）。

幂等键只保证**记录**不重复，不保证**副作用**不重复——后者由 `ReplaySafety` 与工具自身负责。

## 5. 不变量

- **I1 模型可见即已记录**：投影所需的一切都在日志里；内存不得补写历史。
- **I2 只有终态**：任何意图最终要么有终态记录，要么保持未闭合；不存在第三种状态。
- **I3 顺序**：`seq` 在 session 内严格单调；逻辑只按 `seq` 排序。
- **I4 时间无关**：`recorded_at` 只影响 trace/审计的耗时呈现，不参与任何恢复、分支、比较或投影。
- **I5 恢复只处理未闭合意图**：已闭合的历史不重做。
- **I6 等待是已记录的**：每个等待都有意图记录与最终终态（审批：`ToolCallRequested` → `ApprovalDecided` / `ToolResult`；子会话：`ChildSessionSpawned` → `ChildSessionCompleted`）；恢复不依赖内存中的等待者。
- **I7 投影是折叠**：模型历史 = 对日志做只读折叠，`CompactionApplied` 丢弃 `replaces` 区间（含旧 summary）并在 `first_kept` 之前插入新 summary；原始记录保留。
- **I8 幂等 append**：同一 `(session, 事件类型, dedup_key)` 永远只有一条记录。
- **I9 部分输出不投影**：`ModelCallAborted.partial` 与中断工具的结果不进入模型历史，除非写成显式 `ToolResult::Interrupted`。以 `StopReason::Refusal` 结束的 `AssistantMessage` 完整写入日志，但投影跳过它的 `items`；其中的工具调用不写意图、不执行，回合以 `Refused` 结束（恢复路径同样）。

## 6. 检查点、恢复点与恢复

### 状态

`SessionState` 同时是 `SessionHandle::state()` 的返回类型（[packaging.md](packaging.md) §3.3）；它由会话事实（`SessionFacts`）折叠推导，与恢复共用同一分类器。

```rust
pub enum SessionState {
    Idle,                                   // 没有非 compaction 的未闭合意图
    TurnOpen { turn: TurnId, step: Option<StepState> }, // TurnStarted 无 TurnCommitted
}

pub enum StepState {
    InProgress,   // StepStarted 无 StepCommitted
    Waiting,      // InProgress 且本 step 有未终结的子会话等待
}
```

「未闭合意图」的精确定义：`TurnStarted` 无 `TurnCommitted`；`StepStarted` 无 `StepCommitted`；`ToolCallRequested` 无 `ToolResult`；`ChildSessionSpawned` 无 `ChildSessionCompleted`；`CompactionStarted` 无 `CompactionApplied` / `CompactionAborted`。`ToolPending`（`AssistantMessage` 已写、仍有工具未终结）可由日志推导，不单列状态。

- `SessionFacts`（protocol，会话事实）是唯一的折叠实现：`fold(&[LogEntry])` 批量解码折叠，`apply(seq, log_id, dedup_key, &event)` 逐事件折叠；`is_idle()` 排除 `compaction`。它同时回答未闭合意图的元数据（打开时 seq、所属回合/步骤、工具名、子会话的父工具调用与 `deadline`）、open step 及其起始 seq、是否有待处理输入、父会话。agent 恢复、scheduler 准入、facade `state()` / `facts()` / `wakeable()`、存储退役的空闲判定与 CLI `inspect` 都读它，不各写折叠。`is_wakeable()` = 非 idle 或有待处理输入；不存在物化的意图表（#73，ADR-0023）。
- 关闭事件按键精确匹配。`ApprovalDecided` 不关闭任何意图（工具意图由 `ToolResult` 关闭）；`InputRecorded` 不是意图。
- `Idle` 只看非 compaction 意图：只有 `CompactionStarted` 未闭合时仍是 `Idle`（丢弃一次摘要调用是安全的，见 [retention.md](retention.md) §5）。

### 算法

```
recover(session):
    entries   = event_log.scan(session)                    # 后端保证只返回已提交前缀（#5）
    history   = project(entries)                          # 折叠，尊重 CompactionApplied
    open      = entries.unclosed_intents()
    open_steps = open.of_step()                           # StepStarted 无 StepCommitted
    open_waits = open.waits()                             # child
    state     = classify(entries)

    append(StreamResumed { from: entries.last().log_id,
                           reason: Crash })               # 非模型可见

    # 1. 已决定、但未提交终态的模型调用
    for step in open_steps, 按 seq:
        if AssistantMessage(turn, step) 不存在:
            match last ModelCallAborted(turn, step):
                Cancelled -> append(StepCommitted); append(TurnCommitted(Cancelled))
                Rejected  -> append(StepCommitted); append(TurnCommitted(Failed))
                             # abort 不携带上下文耗尽标志；崩溃窗口内按 Failed 收尾
                _         -> {}                          # 交给驱动方重放

    # 2. 等待意图
    for wait in open_waits, 按 seq:
        ChildSession -> 不在本步骤预收尾；交给 spawn 工具重放：
                          spawn 做 R1/R2，await_completion 先查子终态（R3），
                          无终态且 deadline 已过则写 Cancelled{Timeout} 并取消子会话，
                          否则重新挂起到 deadline（subagents.md §9）
        # 审批等待没有独立记录：open ToolCallRequested 由第 3 步按审批信号处理

    # 2b. 未完成的压缩：CompactionStarted 无终态
    for started in open compactions, 按 seq:
        -> 若装配了 Compactor：重新 plan 后重做摘要（同一意图键）；
           成功 append(CompactionApplied)，失败 append(CompactionAborted)，
           再由驱动方按 [compaction.md](compaction.md) 收尾
        -> 未装配 Compactor：跳过并保持 open；它不影响 Idle（§6）

    # 3. 其余 open step 由驱动方按 agent-loop.md 的恢复入口重放：
    #    有 AssistantMessage 跳过模型；无 abort 补 Crash abort 后重试；
    #    未终结工具若 provenance = Unknown 直接写 Failed{NotFound}（不查审批、不重跑）；
    #    否则先查审批信号：有 Allow 则执行，有 Deny 则写 Failed{Denied}，
    #    无决定且策略为 Ask 则重新挂起；否则 Safe 重跑 / Unsafe Interrupted；
    #    全部终结后写 StepCommitted。

    return ResumePoint { state, history, next: resume_point(entries) }
```

- **检查点**：`entries.iter().rposition(|e| e 是 StepCommitted).unwrap_or(0)`。
- **恢复点**：最后一个已提交边界之后的位置；恢复直接跳到这里，不从 session 头部重跑。前缘（`SessionStore::frontier`，最后一条已提交记录）在它之前一条。
- **已完成但未确认**：本层不存在这个状态。终态记录即完成；存储侧没有 ack，重复驱动由幂等 append 与 `drive()` 单飞吸收。
- 补记完成后，驱动方从恢复点继续该回合（gate → drain → 下一个 step 或 `TurnCommitted`），见 [agent-loop.md](agent-loop.md)。
- 恢复次数上限不在内核：宿主 supervisor 用 `StreamResumed` 记录计数并决定停车；压缩失败有终态，不会因同一意图自循环。
- 恢复期间的补记（`StreamResumed`、`ModelCallAborted{Crash}`、`ToolResult::Interrupted`、`ChildSessionCompleted`）都受 §4 的幂等键保护。

## 7. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| S1 | `kill -9` 于 `Safe` 工具执行中 | 重启后该工具恰好多执行一次；history 中恰有一条 `ToolResult::Completed` |
| S2 | `kill -9` 于 `Unsafe` 工具执行中 | 重启后不重跑；history 含一条 `ToolResult::Interrupted`，模型可见 |
| S3 | `kill -9` 于模型流中途 | 补记 `ModelCallAborted{Crash}`；该 step 重试；最终恰有一条 `AssistantMessage` |
| S4 | 后端只暴露已提交前缀；payload 损坏 | SQLite 上不存在半条记录，坏 payload 报 `Corrupt`；非原子后端自行截断 |
| S5 | 同 `dedup_key` append 两次 | 只有一条记录；两次返回同一 `LogId` |
| S6 | 父会话等待子会话时崩溃 | 重启后重新挂起；子完成后恰有一条 `ChildSessionCompleted`，父 history 正确 |
| S7 | 含 `CompactionApplied` 的历史投影 | 模型只看到最新 summary + `first_kept` 之后的内容；旧 summary 被覆盖；原始记录仍可扫描 |
| S8 | 任意恢复路径 | `seq` 严格单调；`recorded_at` 不改变恢复或投影输出，只改变 trace 耗时 |
| S9 | spawn 超时在进程停机期间到期，子会话无终态 | 重启后恰好一条 `ChildSessionCompleted{Cancelled{Timeout}}`；子会话被取消；子终态先到则子终态胜（R3） |
| S10 | `kill -9` 于 `AssistantMessage` 之后、`StepCommitted` 之前 | 恢复不重调模型；恰一条 `AssistantMessage`，usage 保留；未完成工具按 S1/S2 收尾；step 最终 `StepCommitted` |
| S11 | 工具执行中取消 | 宽限内完成记真实结果；到期写 `Interrupted`；`StepCommitted` + `TurnCommitted(Cancelled)`；恢复不重试 |
| S12 | 同一 key、不同事件类型各 append 一次（共享 key 见 §4） | 两条记录都落盘，互不抑制 |
| S13 | 在 open step 中到达 `InputRecorded`（steering）、`ApprovalDecided`（审批）、`ChildSessionCompleted` | 会话仍非 `Idle`；退役 `NotIdle`；重启后出现在 `Asterpath::wakeable()` |
| S14 | 摘要调用失败 / 压后仍超窗 | `CompactionAborted{SummaryFailed}` / `{Insufficient}` 写入；无 open `CompactionStarted`；恢复不再重做 |
| S15 | 崩溃于批量写 `ToolCallRequested` 之间 | 恢复按 `provider_call_id` 补齐剩余意图，不重复、不串位；每条 `ToolResult` 绑定自己的 `ToolCall` |
| S16 | 崩溃于 pre-execute 之后、意图写入之前 / 意图写入之后 | 前者恢复重跑 pre-execute；后者不重跑，工具按已记录的 effective args 执行 |
| S17 | 恢复遇到 `ToolCallRequested{provenance: Unknown}` | 恰写一条 `ToolResult{Failed, NotFound}`；不查审批、不重跑；模型可见 |
| S18 | `submit_input` 之后、`drive()` 之前崩溃 | 重启后该 session 出现在 `Asterpath::wakeable()`；宿主 `open_session` + `claim()` + `drive()` 后输入恰被投影一次 |

## 8. 留给下游

- **#4**：`schema_version` 与发布前的兼容口径见 [storage.md](storage.md) §4；首个发布版的兼容策略届时另立 ADR。
- **#5**：已定。存储契约、DDL、属主锁（ADR-0036）、事务边界、故障注入点见 [storage.md](storage.md) 与 [ADR-0023](../adr/0023-storage-holds-log-and-catalog-wake-by-fold.md)。SQLite 提交原子，因此没有 torn-tail 修复；坏数据一律硬错误。
- **#6**：已定。turn/step、取消、steering、默认配置见 [agent-loop.md](agent-loop.md) 与 [ADR-0006](../adr/0006-agent-loop-execution-model.md)。
- **#7**：已定。工具契约、执行管线与审批边界见 [tools.md](tools.md) 与 [ADR-0007](../adr/0007-approval-and-sandbox-layers.md)。
- **#8**：已定。spawn 是同步工具调用；上下文策略、结果形状、取消与孤儿规则见 [subagents.md](subagents.md) 与 [ADR-0008](../adr/0008-subagent-model.md)。
- **#9**：已定。触发、保留、摘要调用与归因见 [compaction.md](compaction.md) 与 [ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)。
- **#17**：已定。子等待恢复委托给 spawn 工具重放；持久等待与调度见 [subagents.md](subagents.md) §3.1/§9 与 [ADR-0008](../adr/0008-subagent-model.md)。
- **#12**：已定。trace 是日志的确定性投影；`recorded_at`、`CompactionStarted` 的追踪挂接见 [tracing.md](tracing.md) 与 [ADR-0011](../adr/0011-native-tracing.md)。
- **#16**：已定。插件清单、加载、版本协商与外部能力边界见 [plugins.md](plugins.md)；v1 不做插件发现。`Provenance` 新增 `Unknown`，只用于工具不存在的请求记录。
