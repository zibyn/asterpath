# 事件日志与检查点恢复语义

> 来源：[#3](https://github.com/zibyn/asterpath/issues/3)。本文件定义日志的**逻辑语义**；物理持久性归 [#5](https://github.com/zibyn/asterpath/issues/5)，schema 版本与 upcasting 归 [#4](https://github.com/zibyn/asterpath/issues/4)。

## 1. 决策摘要

- 日志按 **`StreamId`** 分片：`Session(SessionId)` 或 `Workflow(InstanceId)`；每个 stream 一条 append-only 日志，stream 之间无事务。
- **终态条目**是提交单位；流式 delta 不落盘。
- 不变量：**模型可见即已记录**。任何进入下一次请求上下文的内容、任何副作用意图，先于该动作提交。
- **检查点 = 最后一个完整的 `StepCommitted` 边界**，没有独立的 checkpoint 记录。
- 日志层**没有 ack**：完成 = 写了终态记录。租约与 ack 只属于存储的调度队列。
- 恢复 = 截断 torn tail → 定位检查点 → 重放已提交事件 → 处理**未闭合意图**。
- 幂等：`LogEntry.dedup_key` 在 stream 内唯一，重复 append 返回原 `LogId`。
- 信封不含墙钟时间；`seq` 是唯一排序依据。
- 工作流实例的确定性重放**不写记录**；session 恢复写一条非模型可见的 `StreamResumed`。
- compaction 的事件形状由本文件定，算法与参数归 [#9](https://github.com/zibyn/asterpath/issues/9)。

## 2. 日志流模型

```rust
pub enum StreamId {
    Session(SessionId),
    Workflow(InstanceId),
}

pub struct ParentRef {
    pub session: SessionId,
    pub turn: TurnId,
    pub tool_call: ToolCallId,
}
```

- 子智能体是**独立 session stream**，父 stream 里只有一条 `ChildSessionSpawned`，记录父子关系。
- 子 stream 的首条记录用 `caused_by: LogId` 指回父记录；其余关联靠 payload 里的 id。
- 子流是独立恢复、独立取消的单位。父子关闭顺序、孤儿处理、fork 语义归 [#8](https://github.com/zibyn/asterpath/issues/8)。
- 工作流实例使用同一个 `EventLog` trait 与同一套 `LogEntry` 信封，只是 stream 类型不同。

## 3. 事件枚举

```rust
pub enum EventPayload {
    Session(SessionEvent),
    Workflow(WorkflowEvent),
}

pub enum SessionEvent {
    SessionCreated { config: ConfigSnapshot, parent: Option<ParentRef> },
    StreamResumed { from: LogId, reason: ResumeReason },          // 非模型可见
    InputRecorded { input: Vec<Item> },                           // 模型可见
    TurnStarted { turn_id: TurnId },
    StepStarted { turn_id: TurnId, step_id: StepId },
    AssistantMessage { turn_id: TurnId, step_id: StepId,
                       items: Vec<Item>, raw: Option<RawProviderData> },
    ModelCallAborted { turn_id: TurnId, step_id: StepId, attempt: u32,
                       kind: AbortKind, partial: Option<Vec<Item>> },  // 非模型可见
    ToolCallRequested { turn_id: TurnId, step_id: StepId,
                        tool_call_id: ToolCallId, name: ToolName,
                        raw_args: JsonString, replay: ReplaySafety,
                        provenance: Provenance },
    ToolResult { tool_call_id: ToolCallId, status: ToolResultStatus,
                 output: Option<Vec<Item>> },
    StepCommitted { turn_id: TurnId, step_id: StepId,
                    usage: Usage, stop_reason: StopReason },
    TurnCommitted { turn_id: TurnId, reason: TurnEndReason },
    CompactionApplied { first_kept: LogId, summary: Vec<Item>,
                        replaces: LogIdRange },
    ChildSessionSpawned { child: SessionId, task: Vec<Item>,
                          parent_tool_call_id: ToolCallId,
                          caused_by: LogId },
}

pub enum AbortKind { Transport, Cancelled, Crash }
pub enum ToolResultStatus { Completed, Failed, Interrupted }
pub enum TurnEndReason { Completed, Cancelled, Failed, BudgetExhausted, MaxTurns }

pub enum WorkflowEvent {
    WorkflowStarted { name: WorkflowName, input: JsonValue },
    ActivityScheduled { activity_id: ActivityId, name: ActivityName,
                        input: JsonValue, replay_safety: ReplaySafety },
    ActivityCompleted { activity_id: ActivityId, output: JsonValue },
    ActivityFailed { activity_id: ActivityId, error: String, retryable: bool },
    TimerScheduled { timer_id: TimerId, deadline: Timestamp },
    TimerFired { timer_id: TimerId },
    SignalReceived { name: SignalName, payload: JsonValue },
    ChildWorkflowStarted { child: InstanceId, name: WorkflowName },
    ChildWorkflowCompleted { child: InstanceId, output: JsonValue },
    WorkflowCompleted { output: JsonValue },
}
```

规则：

- **没有通用 `Error` 事件**。错误是它所附着事件的终态（`ModelCallAborted`、`ToolResult::Failed`、`ActivityFailed`）。
- `Crash` 不是现场写下的记录；恢复时检测到未闭合意图才补记（见 §6）。
- `Usage`、`Item`、`JsonString`、能力标志的类型归 [#11](https://github.com/zibyn/asterpath/issues/11)。
- timer / signal / activity 只出现在 `Workflow` stream；它们的 API 与重放算法归 [#10](https://github.com/zibyn/asterpath/issues/10)。

## 4. 信封与幂等键

```rust
pub struct LogEntry {
    pub stream: StreamId,
    pub seq: u64,                 // 存储层按 stream 单调分配
    pub log_id: LogId,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: EventPayload,
}
```

- `ts` 不在信封里。诊断事件（`StreamResumed`、`ModelCallAborted`、`TimerScheduled`）可在 payload 带时间，**任何逻辑都不得读取它**。
- `dedup_key` 在 `(stream, key)` 上唯一；命中已存在键时 `append` 返回原 `LogId`，不报错、不写入。

| 事件 | dedup key |
| --- | --- |
| `SessionCreated` / `WorkflowStarted` | session / instance id |
| `InputRecorded` | 调用方提供的 input id |
| `AssistantMessage` | `(turn_id, step_id)` |
| `ToolCallRequested` | 意图提交时生成的 `tool_call_id` |
| `ToolResult` | `tool_call_id` |
| `StepCommitted` / `TurnCommitted` | step / turn id |
| `Activity*` / `Timer*` / `ChildWorkflow*` | 对应 id |

幂等键只保证**记录**不重复，不保证**副作用**不重复——后者由 `ReplaySafety` 与工具/activity 自身负责。

## 5. 不变量

- **I1 模型可见即已记录**：投影所需的一切都在日志里；内存不得补写历史。
- **I2 只有终态**：任何意图最终要么有终态记录，要么保持未闭合；不存在第三种状态。
- **I3 顺序**：`seq` 在 stream 内严格单调；逻辑只按 `seq` 排序。
- **I4 时间无关**：`ts` 不参与任何分支、比较或投影。
- **I5 恢复只处理未闭合意图**：已闭合的历史不重做。
- **I6 重放只读**：工作流实例的确定性重放不产生任何 append。
- **I7 投影是折叠**：模型历史 = 对日志做只读折叠，`CompactionApplied` 跳过被替换区间，原始记录保留。
- **I8 幂等 append**：同一 `(stream, dedup_key)` 永远只有一条记录。
- **I9 部分输出不投影**：`ModelCallAborted.partial` 与中断工具的结果不进入模型历史，除非写成显式 `ToolResult::Interrupted`。

## 6. 检查点、frontier 与恢复

### 状态

```rust
pub enum StreamState {
    Idle,          // 最后一条是 TurnCommitted，或 stream 刚创建
    StepOpen,      // StepStarted 无对应 StepCommitted
    ToolPending,   // ToolCallRequested 无对应 ToolResult
}
```

「未闭合意图」的精确定义：`StepStarted` 无 `StepCommitted`；`ToolCallRequested` 无 `ToolResult`；`ActivityScheduled` 无终态；`TimerScheduled` 无 `TimerFired`；`ChildWorkflowStarted` 无 `ChildWorkflowCompleted`。

### 算法

```
recover(stream):
    tail      = storage.truncate_torn_tail(stream)        # 物理细节归 #5
    entries   = event_log.scan(stream, ..=tail)
    history   = project(entries)                          # 折叠，尊重 CompactionApplied
    open      = entries.unclosed_intents()
    state     = classify(entries)

    append(StreamResumed { from: tail, reason: Crash })   # 非模型可见

    for intent in open, 按 seq 顺序:
        match intent:
            ModelCall  -> append(ModelCallAborted { kind: Crash, partial: 丢弃 })
                          retry_step(intent.step_id)
            ToolCall   -> if intent.replay == Safe:
                              re_execute(intent.tool_call_id)      # 结果按 dedup_key 写一次
                          else:
                              append(ToolResult { status: Interrupted, output: None })
            Activity   -> 交给执行器判定：Safe 重派，否则标记失败（#5/#10）
            Timer      -> 重新入队到 deadline（#5）
            ChildWf    -> 查询子实例最终状态，补写完成或失败记录

    return ResumePoint { state, history, next: frontier(entries) }
```

- **检查点**：`entries.iter().rposition(|e| e 是 StepCommitted).unwrap_or(0)`。
- **frontier**：最后一个已提交边界之后的位置；恢复直接跳到这里，不从 stream 头部重跑。
- **已完成但未 ack**：本层不存在这个状态。调度队列的 ack 属于 `Storage`（#5），恢复看到的是「意图在、终态不在」或「终态在」。
- 恢复期间的补记（`StreamResumed`、`ModelCallAborted{Crash}`、`ToolResult::Interrupted`）是非模型可见或有意的模型可见终态，且都受 §4 的幂等键保护。

## 7. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| S1 | `kill -9` 于 `Safe` 工具执行中 | 重启后该工具恰好多执行一次；history 中恰有一条 `ToolResult::Completed` |
| S2 | `kill -9` 于 `Unsafe` 工具执行中 | 重启后不重跑；history 含一条 `ToolResult::Interrupted`，模型可见 |
| S3 | `kill -9` 于模型流中途 | 补记 `ModelCallAborted{Crash}`；该 step 重试；最终恰有一条 `AssistantMessage` |
| S4 | 日志尾部写一半崩溃 | torn tail 被截断；前缀完整；不存在半条记录 |
| S5 | 同 `dedup_key` append 两次 | 只有一条记录；两次返回同一 `LogId` |
| S6 | 工作流实例重放 | append 次数为 0；action 序列不一致时以非确定性错误失败 |
| S7 | 含 `CompactionApplied` 的历史投影 | 模型只看到 summary + `first_kept` 之后的内容；原始记录仍可扫描 |
| S8 | 任意恢复路径 | `seq` 严格单调；`ts` 不改变任何输出 |

## 8. 留给下游

- **#4**：`schema_version`、upcasting 时机、未知事件与未知字段、重放兼容性测试、迁移。
- **#5**：单写者租约、原子提交、WAL/fsync、torn-tail 截断、队列表、SQLite DDL、故障注入工具。
- **#6**：turn/step 的终止条件、steering 注入点、取消宽限期（它们决定这些事件**何时**写）。
- **#7**：审批/沙箱管线；tool 的 `ReplaySafety` 声明与执行语义。
- **#8**：父子日志关闭顺序、孤儿恢复、fork session。
- **#9**：压缩触发条件、保留策略、摘要模型。
- **#10**：`DurableExecutor` 方法集、action 比对、重试策略、timer/signal API。
- **#12**：`StreamResumed` 与 `caused_by` 如何映射到 trace；重放是否产生新 trace。
