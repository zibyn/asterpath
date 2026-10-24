# Agent loop：turn / step、取消与 steering

> 来源：[#6](https://github.com/zibyn/asterpath/issues/6)。本文件定义 loop 的状态机、步进管线、取消与 steering 规则、默认配置。
>
> 对齐：[事件日志与检查点恢复](./event-log-recovery.md)、[模块契约](./module-contracts.md)。
>
> 相关 ADR：[0006 Agent loop](../adr/0006-agent-loop-execution-model.md)。

## 1. 决策摘要

- **回合（turn）由输入驱动**：`InputRecorded` 触发 `TurnStarted`，直到不再需要继续时 `TurnCommitted`。
- **步骤（step）= 一次模型请求 + 由它触发的全部工具调用**；工具全部终结后才写 `StepCommitted`。回合可以零个 step。
- `StepCommitted` 是 step 的**通用终态**：成功、失败、取消都写它；回合结果由 `TurnCommitted.reason` 表达。
- 模型请求的投影水位是 `StepStarted.seq`：只投影它之前的模型可见记录；`CompactionApplied` 作为重写算子不受水位限制。重试复用同一投影；强制压缩后按新投影重试同一 step；steering 在 step 边界进入下一次投影。
- steering 只排队，**不打断**在途模型流；输入到达即写 `InputRecorded`。
- 取消是回合级、协作式；宽限期后 drop 在途工具并写 `Interrupted`；取消同时闭合本 step 的等待意图。
- 模型失败在 step 内按 attempt 重试；工具失败不重试，作为模型可见结果交回模型。
- 默认不设 step 上限；宿主通过 `Hooks::before_step`（pre-step gate，ADR-0025）实施自己的停止策略。

## 2. 回合与步骤

**回合**：

- 由输入驱动。`InputRecorded` 到达即落盘；驱动方调用 `SessionHandle::drive`（引擎 agent 的 `run`）时，session 空闲则写 `TurnStarted` 开始新回合，已有回合则把输入作为 steering 并入当前回合。
- 包含零或多个 step。零 step 的回合只可能由 pre-step gate 或压缩路径（失败 / 仍超窗）结束。
- 由 `TurnCommitted { reason }` 结束；之后 session 回到 Idle。

**步骤**：

- `StepStarted` 是意图记录；它之后的模型请求与工具执行都属于该 step。
- `StepCommitted` 是 step 的通用终态：成功、模型失败、取消都写它。`TurnCommitted.reason` 表达回合结果。
- 一个 step 至多一次成功的模型请求（多个 attempt 中至多一个 `AssistantMessage`），零或多个工具调用。
- 模型请求的投影水位是 `StepStarted.seq`：模型可见记录只投影 `seq < StepStarted.seq`；`CompactionApplied` 不受水位限制，始终折叠到 frontier。重试复用同一投影；唯一例外是上下文超限触发的强制压缩，压缩后按新投影重试同一 step（[compaction.md](compaction.md)）；新输入只有落在下一个 `StepStarted` 之前才可见。

**与检查点、追踪、token 的对齐**：

| 概念 | 日志锚点 | 追踪 span | token |
| --- | --- | --- | --- |
| session | `SessionCreated` → | session span | 累计 |
| turn | `TurnStarted` → `TurnCommitted` | turn span | 求和 |
| step | `StepStarted` → `StepCommitted` | step span | 该 step 的模型调用 |
| model attempt | 一次 `AssistantMessage` / 每次 `ModelCallAborted` | model span | `AssistantMessage.usage` |
| tool call | `ToolCallRequested` → `ToolResult` | tool span | — |

## 3. 状态机

状态是日志的投影，不是额外记录：

- `Idle`：没有未闭合的 turn / step / tool / wait 意图（compaction 除外）。
- `TurnOpen`：`TurnStarted` 无 `TurnCommitted`。
  - `BetweenSteps`：没有 open step（`TurnStarted` 之后、`StepStarted` 之前，或刚 `StepCommitted`）。
  - `ModelCalling`：`StepStarted` 无 `AssistantMessage`。
  - `ToolsPending`：`AssistantMessage` 已写，仍有 `ToolCallRequested` 无 `ToolResult`。
  - `Waiting`：`ToolsPending` 且本 step 有未终结的子会话等待。

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> BetweenSteps: InputRecorded / TurnStarted
    BetweenSteps --> BetweenSteps: compaction + gate Continue
    BetweenSteps --> TurnCommitted: gate EndTurn / CompactionFailed / ContextExhausted
    BetweenSteps --> ModelCalling: StepStarted
    ModelCalling --> ModelCalling: ModelCallAborted (retryable, attempt++)
    ModelCalling --> ToolsPending: AssistantMessage + tool calls
    ModelCalling --> StepDone: AssistantMessage 无 tool calls
    ModelCalling --> StepDone: fatal / 重试耗尽 / cancel
    ToolsPending --> ToolsPending: ToolResult
    ToolsPending --> StepDone: 全部 ToolResult / cancel 宽限到期
    StepDone --> BetweenSteps: 继续（tool calls / pause_turn / pending input）
    StepDone --> TurnCommitted: 不再继续
    TurnCommitted --> Idle
```

`StepDone` 在图上只是 `StepCommitted` 的瞬时标记；落在日志里就是 `StepCommitted`。

## 4. 步进管线

```
run_turn(session):
    append(TurnStarted)

    loop:
        if cancelled:
            append(TurnCommitted(Cancelled)); return      # 零 step 取消
        # pre-step：压缩（#9）→ 宿主 gate
        match compact_if_needed(session):                 # 可写 CompactionStarted / CompactionApplied / CompactionAborted
            Ok(NotNeeded | NothingToDo | NotConfigured | Applied) -> {}
            Ok(Insufficient) -> append(TurnCommitted(ContextExhausted)); return
            Err(_)           -> append(TurnCommitted(CompactionFailed)); return
        match before_step(session):                       # Hooks, in order
            EndTurn(reason) -> append(TurnCommitted(reason)); return
            Err(_)          -> append(TurnCommitted(Failed)); return
            Continue        -> {}

        if max_steps_per_turn == Some(steps_started):
            append(TurnCommitted(MaxStepsExceeded)); return

        step = next_step_id()
        append(StepStarted{turn, step})
        config = effective_config(log, watermark = StepStarted.seq)       # 本 step 的模型与请求参数
        projection = project(frontier, visible_before = StepStarted.seq)  # CompactionApplied 不受水位限制
        force_compacted = false

        # 同一 step 的内层循环：attempt 重试 + 至多一次强制压缩
        loop:
            match call_model(projection, step):            # 内部按 attempt 重试
                Ok(msg):
                    append(AssistantMessage{turn, step, items: msg.items,
                                            raw: msg.raw, usage: msg.usage,
                                            stop_reason: msg.stop_reason})
                    break
                Cancelled:
                    append(ModelCallAborted{turn, step, attempt, kind: Cancelled})
                    append(StepCommitted{turn, step})
                    append(TurnCommitted(Cancelled)); return
                Err(err):
                    append(ModelCallAborted{turn, step, attempt,
                                            kind: err.kind,        # Rejected | Transport
                                            partial: err.partial,
                                            usage: err.partial_usage})
                    # 上下文超限且本 step 未强制压缩过：压缩一次，重试同一 step
                    if err.context_exceeded && !force_compacted:
                        force_compacted = true
                        match compact_forced(session):    # 可写 CompactionStarted / CompactionApplied / CompactionAborted
                            Ok(Applied):
                                projection = project(frontier,
                                                     visible_before = StepStarted.seq)
                                continue              # 内层重试，不新开 step
                            Ok(_):                    # Insufficient / NothingToDo / NotNeeded / NotConfigured
                                append(StepCommitted{turn, step})
                                append(TurnCommitted(ContextExhausted)); return
                            Err(_):
                                append(StepCommitted{turn, step})
                                append(TurnCommitted(CompactionFailed)); return
                    append(StepCommitted{turn, step})
                    append(TurnCommitted(if err.context_exceeded
                                         { ContextExhausted } else { Failed })); return

        calls = msg.tool_calls
        if !calls.is_empty():
            for call in calls:
                append(ToolCallRequested{turn, step, tool_call_id, provider_call_id,
                                         name, raw_args, replay, provenance})
            results = execute_tools(calls)                 # #7：审批、并行/独占、超时、截断
            for r in results:
                append(ToolResult{tool_call_id, status, output})
            if cancelled:
                await_up_to(cancel_grace)                  # 工具在宽限内自行收尾
                close_step_waits()                         # 到期后 loop 收起剩余等待
                for unfinished: append(ToolResult{Interrupted, None})
                append(StepCommitted{turn, step})
                append(TurnCommitted(Cancelled)); return

        append(StepCommitted{turn, step})

        if stop_reason == ContextWindowExceeded:
            append(TurnCommitted(ContextExhausted)); return
        if !calls.is_empty() || stop_reason == PauseTurn || has_pending_input(session):
            continue
        append(TurnCommitted(Completed)); return
```

- `call_model` 的每次失败都写 `ModelCallAborted`；只有成功的 attempt 写 `AssistantMessage`。重试不新开 step，投影不变。
- 未装配 Compactor 时 `compact_if_needed` 返回 `NotConfigured` 并继续；`compact_forced` 返回 `NotConfigured`，按内层 `Ok(_)` 分支收尾为 `ContextExhausted`（[compaction.md](compaction.md) §6）。
- `has_pending_input` = 本回合最后一个 `StepStarted` 之后存在 `InputRecorded`。
- 工具审批 `Ask` 会让 step 在 `Waiting` 中跨天保持 open；决定记录为 `ApprovalDecided`，恢复与调度见 [tools.md](tools.md)。
- 工具执行中取消的收尾顺序见 §6。
- **每 step 折叠配置**（[ADR-0030](../adr/0030-session-config-is-a-fold-of-log-records.md)）：`config` 是 `SessionCreated.config` 依次应用 `seq < StepStarted.seq` 的 `ConfigChanged` 的结果。本 step 的模型调用、重试、`ModelCallAborted` / `AssistantMessage` 的 `model`、强制压缩都用它；step 内追加的 `ConfigChanged` 等下一个 `StepStarted`，与 steering 同构（§5）。`turn` / `compaction` / `subagents` 不可变，仍是创建时的值。step 之间的阈值压缩与 Idle 的 `compact_now` 按整条日志折叠，用压缩发生时的生效模型。spawn 的子会话继承父会话在 spawn 所在 step 水位的生效配置。
- 换模型后，旧模型产出的 reasoning 由 adapter 的既有绑定门控丢弃；换回原模型后再次回放。上下文窗口变小在下一个 step 的阈值检查里自然触发压缩，不设专门路径。

### 4.1 恢复入口

驱动方对已打开的回合调用同一管线，但：

- 不重复写 `TurnStarted`。
- 若 frontier 落在 open step 内，不重复写 `StepStarted`，复用原 `StepStarted.seq` 作为投影水位与配置水位，因此恢复渲染出与崩溃前相同的请求。补写的 `ModelCallAborted{Crash}` 的 `model` 也取该水位的生效值。
- 若该 step 已有 `AssistantMessage`，跳过模型调用，直接执行未终结的工具。已存在的 `ToolCallRequested` 按 `provider_call_id` 对回 `AssistantMessage` 的 `ToolCall`；其 effective args 是权威值，不重跑 `before_tool`。只有尚无意图的调用才重跑 `before_tool`。恢复的未完成 step 不再调用 `before_step`；框架补写的 `Interrupted` 不经过 `after_tool`。
- 若该 step 无 `AssistantMessage`，把本 step 的尝试历史（`ModelCallAborted` 与本 step 内的强制压缩记录，按 `seq`）交给**尝试策略**（§7.1）重放，按它给出的下一动作继续：重试（带编号）、强制压缩或提交 step 并结束回合。一条 abort 都没有时先补写 `ModelCallAborted{attempt: 1, Crash}`，再按 `Crash` 询问。
- 若 frontier 之后存在未闭合的 `CompactionStarted`，先按 [event-log-recovery.md](event-log-recovery.md) §2b 补终态（`ProviderError` 触发的按开放 step 的水位线重做，与实时一致），再由尝试策略决定是否重试同一 step。
- 若 frontier 落在两个 step 之间（上一 step 已 `StepCommitted`、回合未提交），按上一 step 的结果决定：有 `AssistantMessage` 则按 §4 的继续规则；没有则由尝试策略重放得出结束原因并写 `TurnCommitted`，不开新 step。
- 工具完成后写 `StepCommitted`，再按 §4 的继续规则决定下一个 step 或 `TurnCommitted`。
- open 的 spawn 工具按 `Safe` 重放；`spawn` / `await_completion` 吸收 R1–R3（[subagents.md](subagents.md) §9）。恢复不为 child 等待另设预收尾路径。

### 4.2 事件序列示例

**正常两步（第一步有工具调用）**

```
InputRecorded
TurnStarted
StepStarted
AssistantMessage{usage, stop_reason: ToolUse}
ToolCallRequested
ToolResult{Completed}
StepCommitted
StepStarted
AssistantMessage{usage, stop_reason: EndTurn}
StepCommitted
TurnCommitted{Completed}
```

**step 运行中到达 steering**

```
StepStarted(step 1)
AssistantMessage{ToolUse}
ToolCallRequested
InputRecorded              # 到达即落盘；step 1 的投影不含它
ToolResult{Completed}
StepCommitted
StepStarted(step 2)        # 投影含 InputRecorded
...
```

**工具执行中取消（宽限到期）**

```
StepStarted
AssistantMessage{ToolUse}
ToolCallRequested
[cancel]
[grace 到期]
ToolResult{Interrupted}
StepCommitted
TurnCommitted{Cancelled}
```

**崩溃于 AssistantMessage 之后**

```
StepStarted
AssistantMessage{ToolUse}
ToolCallRequested
[kill -9]
StreamResumed
ToolResult{Completed}      # Safe 工具重跑一次；Unsafe 写 Interrupted
StepCommitted
...
```

## 5. Steering

- 入口：`SessionHandle::submit_input(input)` 立即 `append(InputRecorded)`（dedup key = 调用方 input id），并通知正在运行的回合。
- **不打断**在途模型流。drain 点是 step 边界：`StepCommitted` 之后、下一个 `StepStarted` 之前。
- 多个输入按 `seq` 顺序进入下一次投影。
- 模型刚返回且无工具调用时，若有 pending input，同一回合继续下一个 step。
- 回合结束后到达的输入不属于旧回合；驱动方在 Idle 时开始新回合。
- 持久等待中的输入不会唤醒等待：等待由审批决定或子会话完成驱动；要放弃等待用取消（然后开新回合）。

## 6. 取消

- `SessionHandle::cancel()`：回合级、幂等。Idle 时是 no-op；取消后 session 仍接受新输入（新回合）。
- **模型流中**：立即 abort，写 `ModelCallAborted{Cancelled}`，不重试；`StepCommitted`；`TurnCommitted(Cancelled)`。
- **退避等待中**：打断 sleep，同上。
- **工具执行中**：向所有在途工具发取消信号；在 `cancel_grace` 内返回的工具按真实结果记 `Completed` / `Failed`；到期仍未返回的 drop future 并写 `ToolResult{Interrupted, output: None}`。一个调用只有一个终态写入者；取消接管后不再触发该调用的超时（[tools.md](tools.md) §6）。
- **等待意图**：到期后由驱动方收尾——`ChildSessionSpawned` 请求 scheduler 取消子会话并写 `ChildSessionCompleted{Cancelled{Parent}}`；若 spawn 工具 future 已在宽限到期时被丢弃、scheduler 来不及写，则由关闭 step 的驱动方补写同一条 `Cancelled{Parent}`，与 scheduler 先写者赢（同类型 dedup，[subagents.md](subagents.md) §8）；审批等待由工具的 `ToolResult{Interrupted}` 收尾。子会话自己的终态见 [subagents.md](subagents.md)。
- 顺序：取消信号 → 宽限 → 收尾等待 → `StepCommitted` → `TurnCommitted(Cancelled)`。
- 取消不重试、不恢复：已提交的取消记录让恢复跳过该 step。

## 7. 失败与重试

- `ProviderError`（#11）提供 `retryable` 与规范化分类（含上下文窗口耗尽）。
- 只在 `retryable && attempt < 预算 && !cancelled` 时重试；预算是 `max_attempts`，本 step 做过强制压缩时为 `max_attempts + 1`（C5 重试不占预算）。每次失败写 `ModelCallAborted{attempt, kind: Transport}`。
- 不可重试错误：`ModelCallAborted{kind: Rejected}`，结束回合（`Failed`；上下文窗口耗尽时先强制压缩一次并重试同一 step，仍超限才 `ContextExhausted`；压缩调用失败则 `CompactionFailed`，见 [compaction.md](compaction.md)）。
- 退避：指数 ×2，jitter，cap 30s；可被取消打断。
- 每次 attempt 受 `request_timeout` 约束；超时按可重试 `Transport` 处理。
- 部分流：不写 `AssistantMessage`；`partial` 仅作诊断，不投影（I9）。
- 失败 attempt 的 usage 是 best-effort（`ProviderError.partial_usage` → `ModelCallAborted.usage`）；只进 `partial` 桶，不计入总量（[usage.md](usage.md)）。

### 7.1 尝试策略

实时路径与恢复路径的模型尝试决策只有一份：`crates/asterpath/src/agent/attempt.rs`（GLOSSARY「尝试策略」）。输入是本 step 的尝试历史（最近一次的编号、是否已强制压缩）与最新结果；输出是下一动作。

| 最新结果 | 下一动作 |
| --- | --- |
| 成功 | 不经策略：写 `AssistantMessage`，进入工具管线 |
| 可重试失败 / 崩溃 | `attempt < 预算` 则以 `attempt + 1` 重试（实时带退避），否则 `Failed` |
| 拒绝 | `Failed` |
| 上下文超限 | 本 step 未强制压缩过则强制压缩（水位线 = `StepStarted.seq`），否则 `ContextExhausted` |
| 取消 | `Cancelled` |
| 强制压缩 `Applied` / 不足 / 失败 | 以 `attempt + 1` 重试（不退避、不占预算）/ `ContextExhausted` / `CompactionFailed` |

C5 重试每 step 至多一次，且不占 `max_attempts` 预算：即使超限发生在最后一次预算内尝试，压缩成功后仍重试一次。

恢复按同一张表重放日志中的尝试历史。以下差异是有意的（崩溃点等价测试 `agent/tests/crash_points.rs` 逐条断言）：

- **D1**：上下文超限写的是 `ModelCallAborted{Rejected}`，`AbortKind` 无法区分超限与其它拒绝。若崩溃在写完该 abort、尚未写 `CompactionStarted` 时，恢复按拒绝处理（`Failed`），不再强制压缩。新增 `AbortKind` 变体需等下一次 schema 升级。
- **D3**：恢复不按已记录的 `backoff_ms` 退避，立即重试；恢复不得读 `recorded_at`（I4），重启本身已耗时。
- **D4**：第 k>1 次尝试在途时崩溃，恢复不补记 `Crash`，以同一编号 k 重做（日志无法区分 k 是否已发出，保守）。只有一条 abort 都没有时补记 `Crash{1}` 并从 2 开始，即首次在途尝试计入预算。
- **工具失败不重试**：`ToolResult{Failed}` 是模型可见结果，回合继续。超时、panic 的映射见 [tools.md](tools.md) §6。

## 8. 结束路径

| 条件 | `TurnEndReason` |
| --- | --- |
| 无工具调用、无 `pause_turn`、无 pending input | `Completed` |
| refusal / 空响应 / 输出 `max_tokens` 截断 | `Completed`（`stop_reason` 留在 `AssistantMessage`） |
| 用户取消 | `Cancelled` |
| 模型错误不可重试 / 重试耗尽 | `Failed` |
| `max_steps_per_turn` 达到 | `MaxStepsExceeded` |
| 宿主 gate 返回停止 | `Stopped { reason }` |
| 上下文窗口耗尽：`stop_reason` 超限，或强制压缩后仍超限 | `ContextExhausted` |
| 压缩阶段失败 | `CompactionFailed` |
| gate 自身出错 | `Failed` |

- `pause_turn` 是「继续」控制信号，不新增输入、不结束回合。
- 不自动续写被截断的输出；宿主可以再发输入。

## 9. 追踪与 token 对齐

- span 层级：session → turn → step → model_call / tool_call / wait；transport attempt 折叠为 `model_call` span 上的 event。见 [tracing.md](tracing.md)。
- 成功的模型调用在 `AssistantMessage` 收尾，`usage` 挂在这里；失败的 attempt 在 `ModelCallAborted` 收尾。
- step 在 `StepCommitted` 收尾，回合在 `TurnCommitted` 收尾。
- token 总量 = 该范围内 `AssistantMessage.usage` 求和；中断 attempt 的 partial usage 单独标注（[usage.md](usage.md)）。
- 承载 usage 的终态记录（`AssistantMessage` / `CompactionApplied` / `ModelCallAborted`）与其他记录一样经 append 入口送给 `Hooks::on_record`；用量桶判定用 protocol 的 `usage_of`（#13、#74、ADR-0025）。

## 10. 可配置默认

数值是 `TurnConfig` 的默认值，不是不变量（[packaging.md](packaging.md) §5.1）。

```rust
pub struct TurnConfig {
    pub max_steps_per_turn: Option<u32>,   // None
    pub max_attempts: NonZeroU32,          // 3
    pub initial_backoff: Duration,         // 1s
    pub max_backoff: Duration,             // 30s
    pub request_timeout: Option<Duration>, // Some(600s)
    pub cancel_grace: Duration,            // 10s
}
```

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `max_steps_per_turn` | `None` | 宿主可设；`Some(0)` 在构建期被拒绝（`AsterError::Config`）；超限 `MaxStepsExceeded` |
| `max_attempts` | `3` | 含首次 |
| `retry_backoff` | 1s 指数 ×2，cap 30s，jitter | 仅 retryable |
| `request_timeout` | `600s` | 每次 attempt；`None` 关闭 |
| `cancel_grace` | `10s` | 0 = 立即 drop |
| `tool_timeout` | `None` | 由 `ToolDescriptor` 决定；external 缺省 120s |
| `kill_grace` | `5s` | 超时/取消后强杀外部工具的宽限；属 `ToolConfig`（[tools.md](tools.md) §9，#7） |
| pre-step gate | 无 | 宿主注册 `Hooks::before_step` |
| host stop | 无 | 宿主经 gate 返回 `Stopped { reason }` |
| cost/budget 扩展 | 无 | `before_step` 读 `StepCtx::usage()` 返回 `Gate::EndTurn` → `Stopped { reason }`；最多超支一个 step；价格表/币种不在内核（#13） |

## 11. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| L1 | 两步正常回合（第一步工具调用） | 事件序列符合 §4.2 示例；第二步投影含 `ToolResult`；恰一条 `TurnCommitted(Completed)` |
| L2 | step 运行中提交 steering | 输入立即 `InputRecorded`；本 step 投影不含它；下一 step 投影含它；同一回合 |
| L3 | 模型流中取消 | `ModelCallAborted{Cancelled}` + `StepCommitted` + `TurnCommitted(Cancelled)`；恢复不重试 |
| L4 | 工具执行中取消，宽限到期 | 未返回工具写 `Interrupted`；回合 `Cancelled`；open 子会话等待写 `Cancelled{Parent}` |
| L5 | 可重试错误两次后成功 | 两条 `ModelCallAborted{attempt 1,2}`；恰一条 `AssistantMessage`；usage 只来自成功调用 |
| L6 | 不可重试错误 | `ModelCallAborted{Rejected}` + `StepCommitted` + `TurnCommitted(Failed)`；恢复不重试 |
| L7 | `max_steps_per_turn = 1` 且第一步有工具调用 | 工具完成后 `TurnCommitted(MaxStepsExceeded)` |
| L8 | 空响应 / refusal | `AssistantMessage` 空或含 refusal；`TurnCommitted(Completed)`；`stop_reason` 保留 |
| L9 | 上下文窗口耗尽 | 错误路径：同一 `StepStarted` 内强制压缩一次并重试（无第二个 `StepStarted`，重试投影含压缩结果），成功则回合继续，仍超限则 `ContextExhausted`；压缩调用失败则 `CompactionFailed`；`stop_reason` 路径：不压缩，直接 `ContextExhausted`；每 step 至多一次强制压缩 |
| L10 | 压缩阶段失败 | 零 step；`CompactionAborted` + `TurnCommitted(CompactionFailed)` |
| L11 | 崩溃于 `AssistantMessage` 之后、`StepCommitted` 之前 | 恢复不重调模型；恰一条 `AssistantMessage`；usage 保留；补完未完成工具后提交 step |
| L12 | 投影水位 | step 中记录的 steering 不会被同一 step 的重试看到；只被下一 step 看到 |
| L13 | 崩溃于 `ModelCallAborted{Cancelled}` 之后、`StepCommitted` 之前 | 恢复写 `StepCommitted` + `TurnCommitted(Cancelled)`；不重调模型 |

## 12. 留给下游

- **#7**：已定。工具契约、审批、并行/独占调度、超时与结果形状见 [tools.md](tools.md) 与 [ADR-0007](../adr/0007-approval-and-sandbox-layers.md)。
- **#8**：已定。spawn 是同步工具调用；取消、`ChildSessionCompleted{Cancelled}` 与孤儿策略见 [subagents.md](subagents.md)。
- **#9**：已定。触发、保留、摘要调用、事件与归因见 [compaction.md](compaction.md) 与 [ADR-0002](../adr/0002-log-topology-and-commit-semantics.md)。
- **#11**：已定。`ProviderError` 的 retryable/上下文分类、`Usage`、`StopReason`（含 `PauseTurn`、`ContextWindowExceeded`）见 [protocol.md](protocol.md)。
- **#12**：span 命名与属性映射。
- **#13**：已定。token 聚合、`partial` 桶与实时观察（`on_record` + `usage_of`）见 [usage.md](usage.md)。
- **#15**：已定。`SessionHandle::drive` / `AgentEvent` / `AgentError` 的精确签名（agent 是引擎内部类型，ADR-0026）与驱动模型见 [packaging.md](packaging.md) §3.3–§3.4。
- **#17**：已定。spawn 工具重放吸收 R1–R3、子会话等待见 [ADR-0008](../adr/0008-subagent-model.md)。
- **#27**：已定。`Compactor` 契约、未装配时的 `NotConfigured` 路径见 [protocol.md](protocol.md) §6.1 与 [packaging.md](packaging.md) §3.3–§3.4。
