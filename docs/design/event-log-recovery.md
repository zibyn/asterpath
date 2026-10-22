# 事件日志与检查点恢复语义

> 来源：[#3](https://github.com/zibyn/asterpath/issues/3)。本文件定义日志的**逻辑语义**；物理持久性归 [#5](https://github.com/zibyn/asterpath/issues/5)，schema 版本与 upcasting 归 [#4](https://github.com/zibyn/asterpath/issues/4)。
>
> **2026-10-22 范围调整**：移除 `Workflow` 流、确定性重放与 activity/timer 编排引擎，见 [ADR-0003](../adr/0003-single-session-stream-topology.md)。本文件只描述 session 流；子智能体是子 session。
>
> **2026-10-22 #12 修订**：信封增加诊断时间 `recorded_at`（只供 trace/审计读取，不参与逻辑）；新增 `CompactionStarted` 与 `SignalAwaited` 意图记录。追踪模型见 [tracing.md](tracing.md) 与 [ADR-0011](../adr/0011-native-tracing.md)。
>
> **2026-10-22 #13 修订**：`AssistantMessage` / `ModelCallAborted` / `CompactionApplied` 增加 `model: ModelSpec`；用量桶、聚合与去重口径见 [usage.md](usage.md) 与 [ADR-0012](../adr/0012-usage-accounting.md)。`UsageRecorded` 是事件总线上的瞬时通知，不是 session 事件，不落日志。

## 1. 决策摘要

- 日志按 **`SessionId`** 分片：每个 session 一条 append-only 日志，session 之间无事务。**没有 `Workflow` 流类型。**
- **终态条目**是提交单位；流式 delta 不落盘。
- 不变量：**模型可见即已记录**。任何进入下一次请求上下文的内容、任何副作用意图，先于该动作提交。
- **检查点 = 最后一个完整的 `StepCommitted` 边界**，没有独立的 checkpoint 记录。
- **2026-10-22 #6 修订**：`AssistantMessage` 携带 `usage` / `stop_reason`，`StepCommitted` 只作检查点标记；新增 `TimerCancelled`；恢复时若 `AssistantMessage` 已存在则不重调模型。loop 语义见 [agent-loop.md](agent-loop.md)。
- **2026-10-22 #7 修订**：`ToolResult` 增加 `error` / `truncated`；`ToolCallRequested.raw_args` 是 pre-execute 之后的 effective args；审批决定复用 `SignalReceived`（name = `tool.approval.<tool_call_id>`），不新增审批事件。工具语义见 [tools.md](tools.md)。
- **2026-10-22 #8 修订**：新增子流事件 `ContextForked`；`ChildSessionSpawned` 增加 `config` 与 `context`；`ChildOutcome` 定形。子智能体语义见 [subagents.md](subagents.md)。
- 日志层**没有 ack**：完成 = 写了终态记录。租约与 ack 只属于存储的调度队列。
- 恢复 = 只读已提交前缀 → 定位检查点 → 重放已提交事件 → 处理**未闭合意图**。
- **等待是事件**：timer、外部 signal、子会话完成都以意图 + 终态记录，崩溃后重新挂起；signal 等待的意图记录是 `SignalAwaited`。
- **压缩先写意图**：摘要调用前写 `CompactionStarted`，成功由 `CompactionApplied` 闭合。
- 幂等：`LogEntry.dedup_key` 在 session 内唯一，重复 append 返回原 `LogId`。
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
- 子会话创建时，父流写 `ChildSessionSpawned`（携带子配置与上下文策略）；子结束时父流写终态 `ChildSessionCompleted`（幂等，key = child id）。二者在正常运行时由 scheduler 写，恢复时由父驱动方代写。
- 父子之间没有原子性。关闭顺序、孤儿处理、结果形状见 [subagents.md](subagents.md)。
- 等待原语（timer / signal）是 session 事件；不引入 activity / workflow 概念。

## 3. 事件枚举

```rust
pub enum SessionEvent {
    SessionCreated { config: ConfigSnapshot, parent: Option<ParentRef> },
    ContextForked { source: ParentRef, source_log: LogId,
                    items: Vec<Item> },                           // fork 子会话的首个模型可见记录
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
                       usage: Option<Usage> },  // 非模型可见；usage 为 best-effort
    ToolCallRequested { turn_id: TurnId, step_id: StepId,
                        tool_call_id: ToolCallId, name: ToolName,
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
    ChildSessionSpawned { child: SessionId, task: Vec<Part>,
                          config: ConfigSnapshot, context: ContextPolicy,
                          parent_tool_call_id: ToolCallId,
                          caused_by: LogId },
    ChildSessionCompleted { child: SessionId, outcome: ChildOutcome },
    TimerScheduled { timer_id: TimerId, deadline: Timestamp,
                     caused_by: LogId },
    TimerFired { timer_id: TimerId },
    TimerCancelled { timer_id: TimerId },
    SignalAwaited { signal_id: SignalId, name: SignalName, caused_by: LogId },
    SignalReceived { signal_id: SignalId, name: SignalName, payload: JsonValue },
}

pub enum AbortKind { Transport, Rejected, Cancelled, Crash }
pub enum CompactionTrigger { Threshold, Manual, ProviderError }
pub enum ToolResultStatus { Completed, Failed, Interrupted }
pub struct ToolErrorInfo { kind: ToolErrorKind, message: String }
pub enum ToolErrorKind {
    InvalidArgs, Denied, Timeout, Cancelled, Panic, Transport, Internal,
    NotFound,                        // 工具不在注册表；模型可见
}
pub enum TurnEndReason {
    Completed, Cancelled, Failed, Stopped { reason: String },
    MaxStepsExceeded, ContextExhausted, CompactionFailed,
}
// ChildOutcome 的形状见 subagents.md §6。
```

规则：

- **没有通用 `Error` 事件**。错误是它所附着事件的终态（`ModelCallAborted`、`ToolResult::Failed`、`ChildOutcome`）。
- `ToolResult` 是工具调用的唯一终态：`Failed` 必带 `error`；`Interrupted` 的 `output` / `error` 为 `None`。
- `ToolCallRequested` 的 `provenance` 可以是 `Unknown`（请求的工具不在注册表），此时 `replay` 记 `Unsafe`；恢复按 [plugins.md](plugins.md) §8 处理。`NotFound` 是模型可见的工具错误类型，不新增 status。
- 审批没有独立事件：决定是 `SignalReceived { name: "tool.approval.<tool_call_id>", payload: {Allow | Deny} }`；等待意图由 open `ToolCallRequested` 承担。
- `StepCommitted` 是 step 的通用终态：成功、失败、取消都写它；回合结果由 `TurnCommitted.reason` 表达（#6）。
- `usage` 与 `stop_reason` 记在 `AssistantMessage`；`StepCommitted` 不再携带。`model` 记在三个承载 usage 的终态记录上，是发起请求的 `ModelSpec`（#13）。
- `usage` 的桶、聚合与重放去重规则见 [usage.md](usage.md)；日志层只负责让每条终态记录唯一。
- `CompactionStarted` 是摘要调用的意图记录，`CompactionApplied` 是它的终态；失败的摘要 attempt 不落日志。`CompactionApplied` 不是 step：可在回合内 BetweenSteps 或回合外 Idle 追加；成功摘要调用的 `usage` / `raw` 挂在事件上。`replaces` 起点为流首或上一条 `replaces` 起点，投影只保留最新 summary（[compaction.md](compaction.md)）。
- `AbortKind::Transport` 是可重试的传输/服务端错误；`Rejected` 是 provider 拒绝请求（不可重试）。
- 同一 timer 只写 `TimerFired` 或 `TimerCancelled` 之一（执行器保证）；dedup key 只防同一事件重复。`caused_by` 指向触发该等待或压缩的日志记录（例如开启审批的 `ToolCallRequested`、声明等待的 `SignalAwaited`、触发压缩的 frontier 记录）。
- `Crash` 不是现场写下的记录；恢复时检测到未闭合意图才补记（见 §6）。
- `Usage`、`Item` / `Part`、`JsonString`、`ConfigSnapshot`、`ContextPolicy`、能力标志的类型归 [#11](https://github.com/zibyn/asterpath/issues/11)，见 [protocol.md](protocol.md)；`ChildOutcome` 的形状见 [subagents.md](subagents.md)。
- `SignalAwaited` 是 signal 等待的意图记录；审批不走它，因为 open 的 `ToolCallRequested` 已经承担意图（[tools.md](tools.md)）。`SignalReceived` 本身不一定模型可见；若要进入上下文，由消费方写一条 `InputRecorded`（归 #6）。
- timer / signal 的 API 与调度归 [#17](https://github.com/zibyn/asterpath/issues/17)；存储投递归 #5。

## 4. 信封与幂等键

```rust
pub struct LogEntry {
    pub session: SessionId,
    pub seq: u64,                 // 存储层按 session 单调分配
    pub log_id: LogId,
    pub recorded_at: Timestamp,   // 存储层写入；仅 trace/审计可读
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: SessionEvent,
}
```

- `recorded_at` 只由 trace/审计读取，**任何恢复、投影或分支逻辑都不得读取它**。`TimerScheduled.deadline` 是调度逻辑使用的截止时间，不属于诊断时间。
- `dedup_key` 在 `(session, key)` 上唯一；命中已存在键时 `append` 返回原 `LogId`，不报错、不写入。

| 事件 | dedup key |
| --- | --- |
| `SessionCreated` | session id |
| `ContextForked` | child session id |
| `InputRecorded` | 调用方提供的 input id |
| `AssistantMessage` | `(turn_id, step_id)` |
| `ToolCallRequested` | 意图提交时生成的 `tool_call_id` |
| `ToolResult` | `tool_call_id` |
| `StepCommitted` / `TurnCommitted` | step / turn id |
| `CompactionStarted` / `CompactionApplied` | `compaction:<意图提交前 frontier LogId>` |
| `ChildSessionSpawned` / `ChildSessionCompleted` | child session id |
| `TimerScheduled` / `TimerFired` / `TimerCancelled` | timer id（`TimerFired` 与 `TimerCancelled` 由执行器保证互斥） |
| `SignalAwaited` / `SignalReceived` | signal id（审批用 `tool_call_id`） |

表中的 key 按事件类型隔离：`ToolCallRequested` 与 `ToolResult` 都用 `tool_call_id`，但属于不同事件类型，互不抑制；`StepStarted` / `TurnStarted` 不设 dedup key，恢复时不重复写。

幂等键只保证**记录**不重复，不保证**副作用**不重复——后者由 `ReplaySafety` 与工具自身负责。

## 5. 不变量

- **I1 模型可见即已记录**：投影所需的一切都在日志里；内存不得补写历史。
- **I2 只有终态**：任何意图最终要么有终态记录，要么保持未闭合；不存在第三种状态。
- **I3 顺序**：`seq` 在 session 内严格单调；逻辑只按 `seq` 排序。
- **I4 时间无关**：`recorded_at` 只影响 trace/审计的耗时呈现，不参与任何恢复、分支、比较或投影。
- **I5 恢复只处理未闭合意图**：已闭合的历史不重做。
- **I6 等待是已记录的**：每个等待都有意图记录与最终终态（fire / receive / complete / cancel）；恢复不依赖内存中的等待者。
- **I7 投影是折叠**：模型历史 = 对日志做只读折叠，`CompactionApplied` 丢弃 `replaces` 区间（含旧 summary）并在 `first_kept` 之前插入新 summary；原始记录保留。
- **I8 幂等 append**：同一 `(session, dedup_key)` 永远只有一条记录。
- **I9 部分输出不投影**：`ModelCallAborted.partial` 与中断工具的结果不进入模型历史，除非写成显式 `ToolResult::Interrupted`。

## 6. 检查点、frontier 与恢复

### 状态

```rust
pub enum StreamState {
    Idle,                                   // 最后一条是 TurnCommitted，或 stream 刚创建
    TurnOpen { step: Option<StepState> },   // TurnStarted 无 TurnCommitted
}

pub enum StepState {
    InProgress,   // StepStarted 无 StepCommitted
    Waiting,      // InProgress 且本 step 有未终结的 timer / child / signal 等待
}
```

「未闭合意图」的精确定义：`StepStarted` 无 `StepCommitted`；`ToolCallRequested` 无 `ToolResult`；`TimerScheduled` 无 `TimerFired` / `TimerCancelled`；`ChildSessionSpawned` 无 `ChildSessionCompleted`；`SignalAwaited` 无 `SignalReceived`；`CompactionStarted` 无 `CompactionApplied`。`ToolPending`（`AssistantMessage` 已写、仍有工具未终结）可由日志推导，不单列状态。

### 算法

```
recover(session):
    entries   = event_log.scan(session)                    # 后端保证只返回已提交前缀（#5）
    history   = project(entries)                          # 折叠，尊重 CompactionApplied
    open      = entries.unclosed_intents()
    open_steps = open.of_step()                           # StepStarted 无 StepCommitted
    open_waits = open.waits()                             # timer / child / signal
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
        Timer        -> if caused_by 已终结: append(TimerCancelled{timer_id})
                        else if deadline 已过: 立即入队 fire
                        else: 重新挂起
        ChildSession -> if caused_by 已终结: append(ChildSessionCompleted{Cancelled})
                        else 查询子会话最终状态（[subagents.md](subagents.md) §9）：
                              已完成 -> append(ChildSessionCompleted)
                              运行中 -> 重新挂起等待
                              未物化 -> R2 物化并启动
                              不可读 -> append(ChildSessionCompleted{Lost})
        SignalAwaited -> 保持等待；到达时由投递方 append(SignalReceived)

    # 2b. 未完成的压缩：CompactionStarted 无 CompactionApplied
    for started in open compactions, 按 seq:
        -> 重做摘要（同一意图键）；成功 append(CompactionApplied)，
           失败由驱动方按 [compaction.md](compaction.md) 收尾

    # 3. 其余 open step 由驱动方按 agent-loop.md 的恢复入口重放：
    #    有 AssistantMessage 跳过模型；无 abort 补 Crash abort 后重试；
    #    未终结工具先查审批信号：有 Allow 则执行，有 Deny 则写 Failed{Denied}，
    #    无决定且策略为 Ask 则重新挂起；否则 Safe 重跑 / Unsafe Interrupted；
    #    全部终结后写 StepCommitted。

    return ResumePoint { state, history, next: frontier(entries) }
```

- **检查点**：`entries.iter().rposition(|e| e 是 StepCommitted).unwrap_or(0)`。
- **frontier**：最后一个已提交边界之后的位置；恢复直接跳到这里，不从 session 头部重跑。
- **已完成但未 ack**：本层不存在这个状态。调度队列的 ack 属于 `Storage`（#5）。
- 补记完成后，驱动方从 frontier 继续该回合（gate → drain → 下一个 step 或 `TurnCommitted`），见 [agent-loop.md](agent-loop.md)。
- 恢复期间的补记（`StreamResumed`、`ModelCallAborted{Crash}`、`ToolResult::Interrupted`、`TimerCancelled`、`ChildSessionCompleted`）都受 §4 的幂等键保护。

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
| S9 | timer 在进程停机期间到期 | 重启后恰好触发一次 `TimerFired` |
| S10 | `kill -9` 于 `AssistantMessage` 之后、`StepCommitted` 之前 | 恢复不重调模型；恰一条 `AssistantMessage`，usage 保留；未完成工具按 S1/S2 收尾；step 最终 `StepCommitted` |
| S11 | 工具执行中取消 | 宽限内完成记真实结果；到期写 `Interrupted`；`StepCommitted` + `TurnCommitted(Cancelled)`；恢复不重试 |

## 8. 留给下游

- **#4**：`schema_version`、upcasting 时机、未知事件与未知字段、兼容性测试（ADR-0004）。
- **#5**：已定。存储契约、DDL、写者锁、事务边界、故障注入点见 [storage.md](storage.md) 与 [ADR-0005](../adr/0005-storage-contract-and-sqlite.md)。SQLite 提交原子，因此没有 torn-tail 修复；坏数据一律硬错误。
- **#6**：已定。turn/step、取消、steering、默认配置见 [agent-loop.md](agent-loop.md) 与 [ADR-0006](../adr/0006-agent-loop-execution-model.md)。
- **#7**：已定。工具契约、执行管线与审批边界见 [tools.md](tools.md) 与 [ADR-0007](../adr/0007-approval-and-sandbox-layers.md)。
- **#8**：已定。spawn 是同步工具调用；上下文策略、结果形状、取消与孤儿规则见 [subagents.md](subagents.md) 与 [ADR-0008](../adr/0008-subagent-model.md)。
- **#9**：已定。触发、保留、摘要调用与归因见 [compaction.md](compaction.md) 与 [ADR-0009](../adr/0009-context-compaction.md)。
- **#17**：子智能体调度 API、spawn/等待/取消/恢复的接缝、timer/signal API。
- **#12**：已定。trace 是日志的确定性投影；`recorded_at`、`CompactionStarted`、`SignalAwaited` 的追踪挂接见 [tracing.md](tracing.md) 与 [ADR-0011](../adr/0011-native-tracing.md)。
- **#16**：已定。插件清单、加载、版本协商与外部能力边界见 [plugins.md](plugins.md)；v1 不做插件发现。`Provenance` 新增 `Unknown`，只用于工具不存在的请求记录。
