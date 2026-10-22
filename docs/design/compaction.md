# 上下文压缩与窗口管理

> 来源：[#9](https://github.com/zibyn/asterpath/issues/9)。本文件定义触发条件、保留策略、摘要调用、事件与投影语义、默认参数，以及与缓存 / 追踪 / token 统计的交互。
>
> 对齐：[#3 事件日志与恢复](./event-log-recovery.md)、[#6 Agent loop](./agent-loop.md)、[#2 模块契约](./module-contracts.md)。

## 1. 决策摘要

- 压缩是**能力模块**（`Compactor`），不是内核策略；loop 只在每步之前的 BetweenSteps 调用它。
- 触发：pre-step 自动阈值、手动、provider 错误路径的**每 step 一次**强制压缩；`stop_reason` 超限不重试；不做 mid-step 触发。
- 窗口大小**显式配置**，不从 API 探测；token 估算复用最近一次 `usage` + 保守估算，不引入 tokenizer。
- v1 **只做摘要**，不单独折叠旧工具输出。
- 压缩**不是 step**：摘要调用前写 `CompactionStarted` 意图，成功由 `CompactionApplied` 闭合，失败由 `CompactionAborted` 闭合；append 是唯一提交点，崩溃在 append 前重做，无副作用。
- 强制压缩在**当前 step 内**完成：不新开 `StepStarted`、不写 `StepCommitted`，压缩后按新投影重试同一 step；每 step 至多一次。
- 摘要输入是**文本视图**：丢弃 `Reasoning`、provider raw 与非文本 `Part`，只把文本与工具文本形式交给摘要模型。
- 窗口只有 `ModelSpec.context_window` 一个来源；`CompactionConfig` 不重复定义。
- 进程重启上限归宿主：内核每次恢复写 `StreamResumed`，宿主据此计数并决定停车；内核不持有恢复计数器。
- 保留最近一个完整 step，目标为 `0.5 × usable window`；每回合最多 1 次自动压缩。
- 压缩后仍超窗 → `ContextExhausted`，不再压第二次。
- 接受每次压缩带来一次前缀缓存未命中。
- 手动压缩仅在 Idle 时可用，不包成零 step 回合。
- 摘要调用的 usage 记在 `CompactionApplied`，统计上单列 `compaction` 桶。

## 2. 归属与调用点

```text
asterpath-agent（loop） --调用--> Compactor（protocol trait）
                                    └── asterpath-compaction（默认实现）
                                          ├── EventLog（读投影）
                                          └── Provider（摘要调用）
```

- `Compactor` 放在 `asterpath-protocol`；默认实现是新模块 `asterpath-compaction`；精确签名归 [#15](https://github.com/zibyn/asterpath/issues/15)。
- `asterpath-agent` 声明必需依赖；组装根可替换实现，loop 不感知策略。
- **Compactor 只读不写**：它读取投影、调用摘要模型，返回 `CompactionOutcome { first_kept, summary, replaces, usage, raw }`；由 agent 追加 `CompactionApplied`，使 append 与 dedup key 只有一个写入点。
- 内核不含窗口、阈值、摘要提示词或保留规则。

语义结果：

```rust
pub enum CompactResult {
    NotNeeded,      // 未达阈值
    NothingToDo,    // 没有可删前缀
    Applied(CompactionOutcome),
    Insufficient,   // 压完仍超 usable window
}
```

## 3. 触发

| 触发 | 时机 | 规则 |
| --- | --- | --- |
| 自动阈值 | 每个 `BetweenSteps` | 估算输入超过 usable window，且存在可删前缀，且本回合自动压缩次数 < `max_auto_compactions_per_turn` |
| 手动 | 仅 `Idle` | `SessionHandle::compact_now()`；不写 `TurnStarted` / `TurnCommitted` |
| provider 错误 | step 内，尚无 `AssistantMessage` | `ProviderError::context_exceeded`：每 step 强制压缩一次并重试同一 step |
| `stop_reason` | 已有 `AssistantMessage` | `StopReason::ContextWindowExceeded`：不压缩、不重试，回合 `ContextExhausted` |

- **不做 mid-step 触发**：工具结果撑大上下文由下一次 pre-step 处理。
- 强制压缩复用同一 `StepStarted`（不新开 step，不写 `StepCommitted`）：成功后在**同一 step 的内层重试**循环里重算投影、重试模型，不回到外层 pre-step；同一 step 至多一次；再次超限即 `ContextExhausted`。
- 强制压缩的其它结果：`Insufficient` / `NothingToDo` / `NotNeeded` / `NotConfigured` → `StepCommitted` 后 `TurnCommitted(ContextExhausted)`；摘要调用失败 → `CompactionAborted{SummaryFailed}`，`StepCommitted` 后 `TurnCommitted(CompactionFailed)`。
- 未配置 `ModelSpec.context_window` → 自动与手动压缩都不可用（`NotConfigured`）；强制压缩同样不可用，错误路径直接 `ContextExhausted`。

### 窗口与估算

- **窗口**：`ModelSpec.context_window: Option<u32>` 是**唯一来源**，由宿主配置；`ProviderCapabilities` 可给 advisory 默认值，但不作为真相（两家 API 都不返回窗口）。类型归 [#11](https://github.com/zibyn/asterpath/issues/11)。窗口判断取当前回合下一次请求的 `ModelSpec`（模型路由见 [#26](https://github.com/zibyn/asterpath/issues/26)）；`CompactionConfig` 不再定义窗口。
- **usable window** = `context_window − reserve_output_tokens`。
- **估算**：`estimated_input = 最近一次 AssistantMessage.usage 的归一化 input tokens + 其后新增模型可见内容的 bytes/3`。
  - 归一化沿用 #11 的 exclusive 口径（Anthropic 三者相加，OpenAI inclusive），聚合见 [usage.md](usage.md)。
  - 最近一次 usage 已含 system / tools 的静态前缀，因此**不需要**单独估算它们。
  - `bytes/3` 是刻意偏高估（中文约 1 token/字；英文约 4 bytes/token），宁可早压。
  - 第一个 step 之前没有 usage，不做自动压缩。
- 触发条件：`estimated_input > usable window`。

## 4. 保留与摘要

### 切点

- `first_kept` 只能落在**步边界**：某条 `StepStarted` 的 `LogId`；绝不切开一个 step 的工具调用与其结果。
- 至少保留最近一个完整 step；没有可删的前缀 → `NothingToDo`。
- 从最新 step 往回累计保留量，直到 ≥ `target = target_ratio × usable window`（默认 0.5），得到最早的保留 step。
- `replaces` 起点 = 上一条 `CompactionApplied.replaces` 的起点；没有则为流内首条模型可见记录。终点 = `first_kept` 之前一条。这样每次压缩都重新概括「摘要 + 其后的记录」，即**摘要的摘要**。
- 压缩输入 = 当前投影中 `first_kept` 之前的全部内容（含上一条 summary），截断口径与模型请求一致。
- 压后校验：`summary tokens + first_kept 之后 tokens ≤ usable window`，否则返回 `Insufficient`，不追加事件。
- 反复压缩的边界：每回合最多 `max_auto_compactions_per_turn`（默认 1）次自动压缩；`Insufficient` 直接 `ContextExhausted`，不再重试。

### 摘要

- **输入只含文本**（避免把 reasoning / raw / 图片交给摘要模型，触发 400 或无界图片）：
  - 整条丢弃 `Reasoning` item 及其 binding，丢弃 provider raw 与 `ProviderOpaque`；
  - 每个模型可见 item 只保留文本 `Part`；非文本 `Part`（图片、音频、文件等）替换为固定占位文本，不携带原始字节；
  - 工具调用与结果保留文本形式（名称、参数、文本输出）；
  - 摘要请求不挂工具，不启用 reasoning / thinking。
- 摘要模型：默认 = 当前会话的 provider + 模型；可配 `compaction.summary_model` 单独指定；**不做跨 provider 自动回退**。
- 调用由 compactor 独立构造请求，**不继承** `SessionSpec.request`：`tools = []`、`tool_choice = None`、`disable_parallel_tool_use = false`、`sampling = None`、`reasoning = None`、`max_output_tokens = max_summary_tokens`（默认 2048）；输出达到上限按截断文本接受，不重试。
- 提示词是 compactor 内的固定模板，v1 不开放给宿主；输入是待替换区间的投影，输出一段文本，固定包含：目标 / 已完成 / 关键事实与文件 / 未决 / 下一步。
- 产出是单条 `Item::Compaction { parts: Vec<Part> }`（类型归 #11）；v1 两边都渲染为文本——Anthropic 为用户文本块，OpenAI 为普通 input 文本消息；原生 compaction item 出界。

## 5. 事件、投影与恢复

### 事件

```rust
CompactionStarted {
    trigger: CompactionTrigger,   // Threshold | Manual | ProviderError
    replaces_from: LogId,         // 本次将被替换区间的起点
    caused_by: LogId,             // 触发点的日志锚点
}

CompactionApplied {
    first_kept: LogId,
    summary: Vec<Part>,
    replaces: LogIdRange,
    model: ModelSpec,
    usage: Usage,
    raw: Option<RawProviderData>,
}

CompactionAborted {
    reason: CompactionAbortReason,   // 非模型可见
}

pub enum CompactionAbortReason { SummaryFailed, Insufficient }
```

- 压缩不是 step：不写 `StepStarted` / `StepCommitted`，可以发生在回合内（BetweenSteps / 强制压缩）或回合外的 Idle。
- `CompactionStarted` 是意图记录；终态是 `CompactionApplied`（成功）或 `CompactionAborted`（`SummaryFailed` / `Insufficient`）。失败 attempt 的 usage 与原始数据不落日志（只有 trace tap 可见），`CompactionAborted` 因此不携带 `usage` / `raw`；被丢弃的摘要调用成本不计入统计。
- 唯一提交点是终态的 append；崩溃在 append 之前等于什么都没发生，恢复后重做压缩（会重复消耗摘要 token，可接受）。
- dedup key = `compaction:<意图提交前的 frontier LogId>`；`CompactionStarted` / `CompactionApplied` / `CompactionAborted` 共用该键（按事件类型隔离），同一 frontier 重放得到同一条记录。`CompactionAborted` 本身推进 frontier，同一 frontier 的再次尝试因此使用新键。

### 投影

```text
project(entries):
  history = []                                   # (origin, items)
  for e in entries by seq:
    if e is CompactionApplied { first_kept, summary, replaces }:
        history.retain(|(origin, _)| !overlaps(origin, replaces))
        insert (Summary(replaces), summary) 紧邻 first_kept 之前
    else if model_visible(e):
        history.push((Log(e.log_id), items))
  return history
```

- `origin` 为 `Log(LogId)` 或 `Summary(LogIdRange)`；`overlaps` 让新压缩丢掉范围内的原始记录**和**旧摘要。
- 因为 `replaces` 起点固定为流首（或上一条 replaces 起点），只需保留最新一条 summary；更早的 summary 被它的范围覆盖。
- **投影水位只管模型可见记录**：模型请求仍只投影 `seq < StepStarted.seq` 的模型可见记录，但 `CompactionApplied` 是重写算子，**始终折叠到 frontier**，不受水位限制。强制压缩必然发生在 `StepStarted` 之后，回合内不存在其它压缩来源，所以这个例外只会看到当前 step 的强制压缩。
- 原始记录始终保留，可扫描；投影是只读折叠（I7）。

### 手动压缩

- `SessionHandle::compact_now()`：仅 `StreamState::Idle` 允许；`TurnOpen` 返回错误，宿主先取消或等回合结束。
- 直接追加 `CompactionApplied`，不写回合记录；无可删前缀返回 `NothingToDo`，未配置窗口返回 `NotConfigured`。
- 摘要调用失败追加 `CompactionAborted{SummaryFailed}`；压后仍超窗追加 `CompactionAborted{Insufficient}`，返回错误，不改投影。
- 与自动压缩共用同一策略与事件；不占每回合自动压缩额度。

### 恢复

- `CompactionStarted` 无 `CompactionApplied` / `CompactionAborted` 是未闭合意图：恢复后重做摘要并补写终态；失败写 `CompactionAborted` 后按本文件 §6 收尾。
- 崩溃在摘要调用中 → 重新进入 pre-step，重新压缩；崩溃在 append 后 → 事件已提交，投影与下一次请求一致。
- 恢复次数上限不在内核：宿主 supervisor 用 `StreamResumed` 记录计数并决定停车；压缩失败有终态，不会因同一意图自循环。

## 6. 失败映射

| 情况 | 结果 |
| --- | --- |
| 摘要调用失败 / 重试耗尽（沿用 `TurnConfig` 的 attempt / 退避） | `CompactionAborted{SummaryFailed}`；回合内 → `TurnCommitted(CompactionFailed)`，手动 → 返回错误 |
| 没有可删前缀且仍超窗 | `TurnCommitted(ContextExhausted)`，无事件 |
| 压完仍超 usable window | `CompactionAborted{Insufficient}`；回合内 → `TurnCommitted(ContextExhausted)`，手动 → 返回错误 |
| `stop_reason` 超限 | `TurnCommitted(ContextExhausted)`，无事件 |
| 强制压缩：`Insufficient` / `NothingToDo` / `NotNeeded` / 未配置窗口 | `StepCommitted` + `TurnCommitted(ContextExhausted)` |
| 强制压缩：摘要调用失败 | `CompactionAborted{SummaryFailed}` + `StepCommitted` + `TurnCommitted(CompactionFailed)` |
| 手动压缩未配置窗口 | `NotConfigured`，不改日志 |

## 7. 缓存、追踪与统计

- **前缀缓存**：压缩必然使 Anthropic 显式断点与 OpenAI 自动前缀失效。接受每次压缩一次全价缓存写；不为缓存调整触发点或保留边界；压缩后下一次请求自然建立新前缀；system / tools 顺序不变。这是已知成本，写进吞吐预算。
- **追踪**：`CompactionStarted` 到 `CompactionApplied` / `CompactionAborted` 构成一个 `compaction` span，父为当前 step / turn / session（见 [tracing.md](tracing.md)）。失败 attempt 只在实时 tap 中可见，没有日志记录。
- **token 统计**：`CompactionApplied.usage` 单列 `compaction` 桶；计入 session 总量；若位于回合内，也计入该 turn 总量，但不属于任何 step 的 usage；子会话的压缩只进子会话，不 rollup 到父会话。turn 归属取 `CompactionStarted.caused_by` 所在回合；最终口径见 [usage.md](usage.md)（#13）。

## 8. 默认配置

```rust
pub struct CompactionConfig {
    pub reserve_output_tokens: Option<u32>,   // None：min(8192, ModelSpec.context_window / 4)
    pub target_ratio: f32,                    // 0.5
    pub max_auto_compactions_per_turn: u32,   // 1
    pub max_summary_tokens: u32,              // 2048
    pub summary_model: Option<ModelSpec>,     // None：当前模型
}
```

窗口不在本结构里：唯一来源是 `ModelSpec.context_window`，未配置即压缩不可用。

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `reserve_output_tokens` | `min(8192, window/4)` | usable window 的预留；`window` 取自 `ModelSpec.context_window` |
| `trigger` | `estimated_input > usable window` | 无单独比例参数 |
| `target_ratio` | `0.5` | 保留量目标 |
| `max_auto_compactions_per_turn` | `1` | 不含手动与每 step 一次的强制压缩 |
| `max_summary_tokens` | `2048` | 摘要输出上限 |
| `summary_model` | 当前模型 | 不做跨 provider 回退 |
| 估算 | `bytes/3` | 刻意高估；不引入 tokenizer |

## 9. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| C1 | 投影超 usable window | 恰一条 `CompactionApplied`；摘要请求不含 `Reasoning` / raw / 非文本 Part；下一次请求只含 summary + `first_kept` 之后内容；原始日志仍可扫描 |
| C2 | 只有最近一个 step | `NothingToDo`；仍超窗则 `TurnCommitted(ContextExhausted)`，无事件 |
| C3 | 摘要调用中 `kill -9` | 有 `CompactionStarted`、无终态；恢复后重做；成功 → 最终恰一条 `CompactionApplied`；再次失败 → `CompactionAborted`，无 open `CompactionStarted` |
| C4 | append 后 `kill -9` | 事件保留；恢复不再压缩；投影包含 summary |
| C5 | provider 错误 `context_exceeded`（无 `AssistantMessage`） | 同一 `StepStarted` 内强制压缩一次并重试（事件序列无第二个 `StepStarted`）；重试投影含压缩结果；成功则回合继续；同一 step 第二次超限 → `ContextExhausted` |
| C6 | `stop_reason` 超限（已有 `AssistantMessage`） | 不写 `CompactionApplied`；`TurnCommitted(ContextExhausted)` |
| C7 | 手动压缩：Idle / TurnOpen | Idle 写一条事件、无回合记录；TurnOpen 返回错误、日志不变 |
| C8 | 连续两次压缩 | 投影只含最新 summary + 保留内容；旧 summary 被覆盖；原始记录仍在 |
| C9 | `max_auto_compactions_per_turn = 1` 且压后仍超 | 不压缩第二次；`ContextExhausted` |
| C10 | 同一 frontier 重放 append | 返回同一 `LogId`；`CompactionStarted` / `CompactionApplied` 各恰一条 |
| C11 | token 归因 | 摘要 usage 在 `compaction` 桶；不计入任何 step |
| C12 | fork 快照 | 子会话物化父的投影（含压缩结果）；之后父子压缩互不影响 |
| C13 | 未配置 `ModelSpec.context_window` | 自动 / 手动压缩返回 `NotConfigured`；错误路径直接 `ContextExhausted` |
| C14 | 摘要输入含 reasoning / raw / 图片 | 请求中全部剥离；非文本 Part 变占位文本；请求不挂工具、不启用 reasoning |
| C15 | 强制压缩摘要失败 / `Insufficient` | 分别 `CompactionAborted{SummaryFailed}` + `TurnCommitted(CompactionFailed)`、`CompactionAborted{Insufficient}` + `TurnCommitted(ContextExhausted)`；无 open 压缩意图 |

## 10. 留给下游

- **#11**：已定。`Item::Compaction` 的形状与双协议渲染、`ModelSpec.context_window`、`Usage`、`Compactor` trait 见 [protocol.md](protocol.md)。
- **#12**：已定。`CompactionStarted` → `CompactionApplied` 的 span 见 [tracing.md](tracing.md)。
- **#13**：已定。`compaction` 桶、turn / session / 子会话 rollup 与估算边界见 [usage.md](usage.md)。
- **#15**：`Compactor`、`SessionHandle::compact_now()` 的精确签名与错误类型。
- **#19**：C3 / C4 / C10 的崩溃注入点与确定性测试。
