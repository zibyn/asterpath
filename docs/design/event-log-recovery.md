# 事件日志与检查点恢复语义

> 来源：[#3](https://github.com/zibyn/asterpath/issues/3)。本文件定义日志的**逻辑语义**；物理持久性归 [#5](https://github.com/zibyn/asterpath/issues/5)，schema 版本与 upcasting 归 [#4](https://github.com/zibyn/asterpath/issues/4)。
>
> **2026-10-22 范围调整**：移除 `Workflow` 流、确定性重放与 activity/timer 编排引擎，见 [ADR-0003](../adr/0003-single-session-stream-topology.md)。本文件只描述 session 流；子智能体是子 session。

## 1. 决策摘要

- 日志按 **`SessionId`** 分片：每个 session 一条 append-only 日志，session 之间无事务。**没有 `Workflow` 流类型。**
- **终态条目**是提交单位；流式 delta 不落盘。
- 不变量：**模型可见即已记录**。任何进入下一次请求上下文的内容、任何副作用意图，先于该动作提交。
- **检查点 = 最后一个完整的 `StepCommitted` 边界**，没有独立的 checkpoint 记录。
- 日志层**没有 ack**：完成 = 写了终态记录。租约与 ack 只属于存储的调度队列。
- 恢复 = 截断 torn tail → 定位检查点 → 重放已提交事件 → 处理**未闭合意图**。
- **等待是事件**：timer、外部 signal、子会话完成都以意图 + 终态记录，崩溃后重新挂起。
- 幂等：`LogEntry.dedup_key` 在 session 内唯一，重复 append 返回原 `LogId`。
- 信封不含墙钟时间；`seq` 是唯一排序依据。
- session 恢复写一条非模型可见的 `StreamResumed`。
- compaction 的事件形状由本文件定，算法与参数归 [#9](https://github.com/zibyn/asterpath/issues/9)。

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
- 子会话结束时，父流写终态 `ChildSessionCompleted`（幂等，key = child id）。
- 父子之间没有原子性。关闭顺序、孤儿处理、结果形状归 [#8](https://github.com/zibyn/asterpath/issues/8)。
- 等待原语（timer / signal）是 session 事件；不引入 activity / workflow 概念。

## 3. 事件枚举

```rust
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
    ChildSessionCompleted { child: SessionId, outcome: ChildOutcome },
    TimerScheduled { timer_id: TimerId, deadline: Timestamp },
    TimerFired { timer_id: TimerId },
    SignalReceived { signal_id: SignalId, name: SignalName, payload: JsonValue },
}

pub enum AbortKind { Transport, Cancelled, Crash }
pub enum ToolResultStatus { Completed, Failed, Interrupted }
pub enum TurnEndReason { Completed, Cancelled, Failed, BudgetExhausted, MaxTurns }
// ChildOutcome 的形状归 #8。
```

规则：

- **没有通用 `Error` 事件**。错误是它所附着事件的终态（`ModelCallAborted`、`ToolResult::Failed`、`ChildOutcome`）。
- `Crash` 不是现场写下的记录；恢复时检测到未闭合意图才补记（见 §6）。
- `Usage`、`Item`、`JsonString`、能力标志的类型归 [#11](https://github.com/zibyn/asterpath/issues/11)。
- `SignalReceived` 本身不一定模型可见；若要进入上下文，由消费方写一条 `InputRecorded`（归 #6）。
- timer / signal 的 API 与调度归 [#17](https://github.com/zibyn/asterpath/issues/17)；存储投递归 #5。

## 4. 信封与幂等键

```rust
pub struct LogEntry {
    pub session: SessionId,
    pub seq: u64,                 // 存储层按 session 单调分配
    pub log_id: LogId,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: SessionEvent,
}
```

- `ts` 不在信封里。诊断事件（`StreamResumed`、`ModelCallAborted`、`TimerScheduled`）可在 payload 带时间，**任何逻辑都不得读取它**。
- `dedup_key` 在 `(session, key)` 上唯一；命中已存在键时 `append` 返回原 `LogId`，不报错、不写入。

| 事件 | dedup key |
| --- | --- |
| `SessionCreated` | session id |
| `InputRecorded` | 调用方提供的 input id |
| `AssistantMessage` | `(turn_id, step_id)` |
| `ToolCallRequested` | 意图提交时生成的 `tool_call_id` |
| `ToolResult` | `tool_call_id` |
| `StepCommitted` / `TurnCommitted` | step / turn id |
| `ChildSessionSpawned` / `ChildSessionCompleted` | child session id |
| `TimerScheduled` / `TimerFired` | timer id |
| `SignalReceived` | 生产者提供的 signal id |

幂等键只保证**记录**不重复，不保证**副作用**不重复——后者由 `ReplaySafety` 与工具自身负责。

## 5. 不变量

- **I1 模型可见即已记录**：投影所需的一切都在日志里；内存不得补写历史。
- **I2 只有终态**：任何意图最终要么有终态记录，要么保持未闭合；不存在第三种状态。
- **I3 顺序**：`seq` 在 session 内严格单调；逻辑只按 `seq` 排序。
- **I4 时间无关**：`ts` 不参与任何分支、比较或投影。
- **I5 恢复只处理未闭合意图**：已闭合的历史不重做。
- **I6 等待是已记录的**：每个等待都有意图记录与最终终态（fire / receive / complete / cancel）；恢复不依赖内存中的等待者。
- **I7 投影是折叠**：模型历史 = 对日志做只读折叠，`CompactionApplied` 跳过被替换区间，原始记录保留。
- **I8 幂等 append**：同一 `(session, dedup_key)` 永远只有一条记录。
- **I9 部分输出不投影**：`ModelCallAborted.partial` 与中断工具的结果不进入模型历史，除非写成显式 `ToolResult::Interrupted`。

## 6. 检查点、frontier 与恢复

### 状态

```rust
pub enum StreamState {
    Idle,          // 最后一条是 TurnCommitted，或 stream 刚创建
    StepOpen,      // StepStarted 无对应 StepCommitted
    ToolPending,   // ToolCallRequested 无对应 ToolResult
    Waiting,       // TimerScheduled 未 fire / ChildSessionSpawned 未完成 / 等待 signal
}
```

「未闭合意图」的精确定义：`StepStarted` 无 `StepCommitted`；`ToolCallRequested` 无 `ToolResult`；`TimerScheduled` 无 `TimerFired`；`ChildSessionSpawned` 无 `ChildSessionCompleted`；已声明等待的 signal 尚未到达。

### 算法

```
recover(session):
    tail      = storage.truncate_torn_tail(session)        # 物理细节归 #5
    entries   = event_log.scan(session, ..=tail)
    history   = project(entries)                          # 折叠，尊重 CompactionApplied
    open      = entries.unclosed_intents()
    state     = classify(entries)

    append(StreamResumed { from: tail, reason: Crash })   # 非模型可见

    for intent in open, 按 seq 顺序:
        match intent:
            ModelCall    -> append(ModelCallAborted { kind: Crash, partial: 丢弃 })
                            retry_step(intent.step_id)
            ToolCall     -> if intent.replay == Safe:
                               re_execute(intent.tool_call_id)      # 结果按 dedup_key 写一次
                           else:
                               append(ToolResult { status: Interrupted, output: None })
            Timer        -> deadline 已过则立即入队 fire；否则重新入队到 deadline
            ChildSession -> 查询子会话最终状态：
                              已完成 -> append(ChildSessionCompleted)
                              运行中 -> 重新挂起等待
                              丢失/失败 -> 按 #8 的孤儿策略处理
            Signal       -> 保持等待；到达时由投递方 append(SignalReceived)

    return ResumePoint { state, history, next: frontier(entries) }
```

- **检查点**：`entries.iter().rposition(|e| e 是 StepCommitted).unwrap_or(0)`。
- **frontier**：最后一个已提交边界之后的位置；恢复直接跳到这里，不从 session 头部重跑。
- **已完成但未 ack**：本层不存在这个状态。调度队列的 ack 属于 `Storage`（#5）。
- 恢复期间的补记（`StreamResumed`、`ModelCallAborted{Crash}`、`ToolResult::Interrupted`、`ChildSessionCompleted`）都受 §4 的幂等键保护。

## 7. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| S1 | `kill -9` 于 `Safe` 工具执行中 | 重启后该工具恰好多执行一次；history 中恰有一条 `ToolResult::Completed` |
| S2 | `kill -9` 于 `Unsafe` 工具执行中 | 重启后不重跑；history 含一条 `ToolResult::Interrupted`，模型可见 |
| S3 | `kill -9` 于模型流中途 | 补记 `ModelCallAborted{Crash}`；该 step 重试；最终恰有一条 `AssistantMessage` |
| S4 | 日志尾部写一半崩溃 | torn tail 被截断；前缀完整；不存在半条记录 |
| S5 | 同 `dedup_key` append 两次 | 只有一条记录；两次返回同一 `LogId` |
| S6 | 父会话等待子会话时崩溃 | 重启后重新挂起；子完成后恰有一条 `ChildSessionCompleted`，父 history 正确 |
| S7 | 含 `CompactionApplied` 的历史投影 | 模型只看到 summary + `first_kept` 之后的内容；原始记录仍可扫描 |
| S8 | 任意恢复路径 | `seq` 严格单调；`ts` 不改变任何输出 |
| S9 | timer 在进程停机期间到期 | 重启后恰好触发一次 `TimerFired` |

## 8. 留给下游

- **#4**：`schema_version`、upcasting 时机、未知事件与未知字段、兼容性测试（ADR-0004）。
- **#5**：单写者租约、原子提交、WAL/fsync、torn-tail 截断、队列表、SQLite DDL、故障注入工具、timer 投递。
- **#6**：turn/step 的终止条件、steering 注入点、取消宽限期、signal 如何变成模型输入。
- **#7**：审批/沙箱管线；tool 的 `ReplaySafety` 声明与执行语义。
- **#8**：子会话完成结果、父子关闭顺序、孤儿恢复、fork session、并发与深度上限。
- **#9**：压缩触发条件、保留策略、摘要模型。
- **#17**：子智能体调度 API、spawn/等待/取消/恢复的接缝、timer/signal API。
- **#12**：`StreamResumed` 与 `caused_by` 如何映射到 trace。
- **#16**：插件发现与外部能力边界。
