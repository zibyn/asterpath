# 原生用量统计

> 来源：[#13](https://github.com/zibyn/asterpath/issues/13)。本文件定义 token 用量的采集口径、桶划分、聚合层级与父子归因、查询 API、重放去重规则，以及成本/预算扩展的接入接缝。
>
> 对齐：[协议模型](protocol.md)、[事件日志](event-log-recovery.md)、[Agent loop](agent-loop.md)、[子智能体](subagents.md)、[上下文压缩](compaction.md)、[追踪](tracing.md)。
>
> 相关 ADR：[0011 追踪与用量是日志投影](../adr/0011-native-tracing.md)。

## 1. 决策摘要

- **只有 provider 返回值进账**。`Usage` 只来自协议响应；本地 tokenizer、`bytes/3` 估算与宿主猜测永不进入 `Usage` 或总量。`#9` 的 `estimated_input` 只活在压缩模块内部，仅用于窗口判断。
- **归一化沿用 #11 的 exclusive 口径**：`input_total = uncached + cache_read + cache_write`；`output.total` 含推理，`reasoning` 是子集、不叠加；供应商 `total_tokens` 忽略；缺失字段按 0；原始 usage 留在 `raw`。
- **三个桶**：`model_calls`（成功的 `AssistantMessage`）、`compaction`（成功的 `CompactionApplied`）、`partial`（失败/中断 attempt 已确认的用量，只作诊断）。`total = model_calls + compaction`；`partial` 不进总量。
- **用量不是独立日志事件**：它是 `AssistantMessage` / `CompactionApplied` / `ModelCallAborted` 上的字段；聚合是事件日志的确定性投影，恢复与重放天然幂等。
- **归因按会话树**：子会话 usage 只写子流，父日志不含子 usage。`own` 与 `SessionSubtree` 分开返回，`own + descendants` 结构上不会重复计数；`fork` 不继承父用量。
- **查询纯折叠**：facade 方法 `Asterpath::usage(scope)`（pull，ADR-0026）从 `SessionStore` 的日志折叠，v1 不建 usage 派生表；实时观察是 `Hooks::on_record`（每条提交记录一次）配合 `usage_of`。
- **成本/预算不在内核**：价格表、币种、告警、预算执行都在扩展模块；内核只提供用量、`usage_of`、`Hooks::on_record` 与 `Hooks::before_step`（#6 的 pre-step gate）。
- **全局跨 session 聚合出界**：v1 查询面限于单会话及其子树。

## 2. 类型（`asterpath-protocol`）

```rust
pub enum UsageBucket {
    ModelCall,      // 成功的 AssistantMessage
    Compaction,     // 成功的 CompactionApplied
    Partial,        // 失败/中断 attempt 的 best-effort 用量，仅诊断
}

pub enum UsageScope {
    Session(SessionId),
    SessionSubtree(SessionId),               // 唯一包含后代的 scope
    Turn { session: SessionId, turn: TurnId },
    Step { session: SessionId, turn: TurnId, step: StepId },
}

pub struct UsageFilter {                     // 全部 Option，AND 组合
    pub scope: UsageScope,
    pub bucket: Option<UsageBucket>,
    pub model: Option<ModelId>,
    pub since: Option<Timestamp>,             // 含；读 recorded_at
    pub until: Option<Timestamp>,             // 不含；读 recorded_at
    pub limit: Option<usize>,                 // 只作用于 records()；totals() 忽略
}

pub struct UsageRecord {
    pub session: SessionId,
    pub turn: Option<TurnId>,
    pub step: Option<StepId>,
    pub model: ModelSpec,
    pub bucket: UsageBucket,
    pub usage: Usage,
    pub log: LogId,                           // 该样本所在的终态记录
    pub recorded_at: Timestamp,
}

pub struct UsageTotals {
    pub model_calls: Usage,
    pub compaction: Usage,
    pub partial: Usage,                       // 诊断用；不属于 total()
}

impl UsageTotals {
    /// 只含 model_calls + compaction；partial 不计入。
    pub fn total(&self) -> u64 {
        self.model_calls.total() + self.compaction.total()
    }
}

pub struct UsageReport {
    pub own: UsageTotals,
    pub descendants: UsageTotals,             // 非 subtree scope 恒为零
}

// asterpath（facade）
impl Asterpath {
    /// scope 所指会话的 `own`；`SessionSubtree` 另含 `descendants`。
    pub async fn usage(&self, scope: UsageScope) -> Result<UsageReport, UsageError>;
}

#[non_exhaustive]
pub enum UsageError {
    NotFound { session: SessionId },   // 会话已退役或不存在
    Corrupt { session: SessionId, detail: String },
    Backend(String),
}
```

- 值类型在 `asterpath-protocol`，折叠是引擎内部的 `views::usage::UsageService`；它只依赖注入的 `SessionStore`（日志与会话目录），不读 trace。`UsageFilter`（bucket / model / since / until / limit）与逐条 `UsageRecord` 是该折叠的内部形状：facade 只报 scope 合计，钩子上下文的 `usage()` 报会话子树合计。
- `Usage`、`ModelSpec`、`Timestamp`、各类 ID 的既有定义见 [protocol.md](protocol.md) 与 [module-contracts.md](module-contracts.md)；本票不新增 token 计数类型。

## 3. 桶与计数规则

| 日志记录 | 条件 | 桶 | 计入 `total` |
| --- | --- | --- | --- |
| `AssistantMessage` | 总是 | `ModelCall` | 是 |
| `CompactionApplied` | 总是 | `Compaction` | 是 |
| `ModelCallAborted` | `usage: Some` | `Partial` | 否 |
| `ModelCallAborted` | `usage: None` | 不产生记录 | — |

场景口径：

| 场景 | 记账 |
| --- | --- |
| 可重试失败后成功 | 成功调用进 `ModelCall`；失败 attempt 若有 `partial_usage` 进 `Partial`，不进总量 |
| 重试耗尽 / `Rejected` | 失败 attempt 的 `partial_usage` 进 `Partial`；没有 `ModelCall` |
| 取消且无 `AssistantMessage` | 不计；adapter 若能给出 `partial_usage`，进 `Partial` |
| 取消但 `AssistantMessage` 已落盘 | 进 `ModelCall`；恢复不重调模型 |
| 回合内压缩（BetweenSteps 或强制压缩） | 进 `Compaction`；计入该 turn 的 `compaction` 桶；不计入任何 step |
| Idle 压缩 | 进 `Compaction`；只计入 session，`turn = None` |
| 压缩调用失败 / 压后仍超窗 | 写 `CompactionAborted`（不带 usage），该次调用不计；崩溃于终态前重做，只记最终 append 的 `CompactionApplied` |
| `stop_reason = ContextWindowExceeded` 但响应完成 | 进 `ModelCall`（这是一次已答完的调用） |
| 使用量为全零 | 仍产生记录（一次调用就是一次调用） |

- `Partial` 是**下界**：provider 只给了输入侧时 `output = 0`；绝不估算补齐。
- `ModelCallAborted.usage` 由 adapter 经 `ProviderError.partial_usage` 提供；loop 原样透传（[protocol.md](protocol.md) §8）。
- 供应商的 `total_tokens` 不参与任何计算。

## 4. 归因与聚合

### 层级

| 层级 | 来源 | 包含子会话 |
| --- | --- | --- |
| model call | `AssistantMessage` / `CompactionApplied` / `ModelCallAborted` | — |
| step | 同一 `(turn_id, step_id)` 的 `ModelCall` | 否 |
| turn | 同一 `turn_id` 的 `ModelCall` + 归属该 turn 的 `Compaction` | 否 |
| session | 该 session 日志中的全部桶 | 否（`Session` scope） |
| session 树 | 自身 + 全部后代 | 是（`SessionSubtree` scope） |

- **Turn / Step 只算本人日志**；子会话用量只通过 `SessionSubtree` 进入汇总。想按 spawn 归因的宿主可用 `ChildSessionSpawned.caused_by` 自行关联。
- 子会话用量只写子流；父日志不复制（#8）。`own + descendants` 不会重复计数。
- `fork` 只物化投影条目，不复制日志，因此子会话**不继承**父用量；用量归实际发起模型调用的会话。
- 没有独立的 subagent 用量桶：子会话本身就是一级 session，子树汇总即父子归因。

### 桶判定：`usage_of`

```rust
// asterpath-protocol
pub struct UsageEntry {
    pub bucket: UsageBucket,
    pub usage: Usage,
    pub model: ModelSpec,
    pub turn: Option<TurnId>,
    pub step: Option<StepId>,
}

/// `compaction_turn`：同 dedup key 的 `CompactionStarted.turn`，仅对 `CompactionApplied` 生效。
pub fn usage_of(event: &SessionEvent, compaction_turn: Option<TurnId>) -> Option<UsageEntry>;
```

- “一条记录算哪个桶、归哪个 turn / step”只有这一份判定。引擎的用量折叠与 `on_record` 里的成本 / 预算观察者都调用它，不各写一份。
- 不承载用量的记录（含 `usage: None` 的 `ModelCallAborted`）返回 `None`。

### 压缩的 turn 归属

- 回合由写入方记录：agent 写 `CompactionStarted.turn`（回合内为 `Some(该回合)`，Idle 为 `None`）。折叠到 `CompactionApplied` 时取与其同 dedup key 的 `CompactionStarted.turn`，不再从日志位置推导。
- `step` 恒为 `None`：即使强制压缩发生在 step 的错误路径中，按 #9 也不把用量下沉到 step。这与 tracing 把 compaction span 挂到 step 的做法有意不同（trace 表达因果，usage 表达计费归属）。

### 会话树

- 树边以 `SessionCreated.parent`（存储的 `sessions.parent_id`）为准，经 protocol 的 `SessionStore` 会话目录（`parent` / `children`，见 [storage.md](storage.md) §3）读取。

- `SessionSubtree(root)` 返回 `own = root 自身`、`descendants = 全部后代之和`；后代表达式按深度优先或逐层展开均可，顺序不影响结果。
- 查询只覆盖当前仍在存储中的日志。已退役的会话对 `usage(scope)` 返回 `NotFound`；存活会话的子树只汇总仍存在的后代，删掉后代日志时总量相应变小（[retention.md](retention.md) §9）。

## 5. 查询语义

- `records()`：流式折叠，内存有界；单 session 内按日志序；`SessionSubtree` 跨 session 的顺序不保证（每个 session 内部仍按日志序）。需要按时间排序的调用方用 `recorded_at` 自行排序。
- `totals()`：对同一 filter 折叠求和；不物化 `records`。`limit` 对它无意义，忽略。
- `since` 含、`until` 不含，都用信封的 `recorded_at`。`recorded_at` 只用于呈现与审计，不参与归因、投影或重放（#12）。
- 时间分桶（按小时/天）由调用方对 `records()` 自行分组；v1 没有 `group_by` API。
- 任一目标 session 的日志损坏 → `UsageError::Corrupt`，整个查询失败，不跳过坏 session（与 #5 的硬错误一致）。
- v1 不建 usage 派生表、不维护计数器；日志是唯一真相。查询成本与日志长度线性，可流式提前终止。
- 没有 `UsageScope::All`：全局报表由宿主自己的会话注册表遍历根会话完成；会话目录不提供 `roots()`。

## 6. 实时事件与成本接缝

### 实时观察：`Hooks::on_record`

- 每条提交记录（所有会话，含子会话）append 成功后，引擎的单一 append 入口按会话内顺序调用每个钩子的 `on_record(&CommittedRecord { session, log_id, seq, recorded_at, event })`。
- 观察者对 `record.event` 调用 `usage_of` 得到桶、用量、model、turn / step；`CompactionApplied` 的 turn 取同 dedup key 的 `CompactionStarted.turn`（观察者可在看到 `CompactionStarted` 时记下）。
- `on_record` 同步、不可失败；panic 被捕获并记录，不影响循环。
- 恢复与重放不重发：只有新提交的记录会被看到；dedup 命中已有记录的 append 不再通知。历史用量用 `usage(scope)` 重建。

### 成本/预算扩展

- 价格表、币种、告警阈值、预算策略全部在宿主钩子，内核不提供相关类型。
- 执行点是 `Hooks::before_step`：用 `StepCtx::usage()`（本会话 `own` 与子树 `descendants`）判定，超预算返回 `Gate::EndTurn { reason }` → `Stopped { reason }`。gate 只在 step 之间运行，**最多超支一个 step**；不在模型流中间打断。这是明确接受的语义。
- 跨会话累计（例如按 root session）由钩子在 `on_record` 里用 `SessionStore::parent` 归并；进程重启后用 `usage(scope)` 重建。
- 是否把 `Partial` 计入预算由宿主决定。

## 7. 重放与幂等

- 三个承载体都有 dedup key（`AssistantMessage` = `(turn_id, step_id)`；`CompactionApplied` 与意图共用 `compaction:<frontier>`；`ModelCallAborted` 按 attempt）。同一逻辑记录最多一条，因此折叠求和恰好一次。
- **恢复不重发事件、不重写记录**：已 append 的记录保留原 `LogId`；恢复只在缺少终态时补写。
- 崩溃窗口：
  - `ResponseEnded` 后、`AssistantMessage` append 前崩溃 → 该次调用的 usage 永久丢失；恢复后重调模型，只记新调用。接受低估，不用估算补齐。
  - `AssistantMessage` append 后、`StepCommitted` 前崩溃 → usage 保留，恢复不重调模型（#6）。
  - 压缩在 `CompactionApplied` 前崩溃 → 重做摘要；被丢弃的那次调用不计，只计最终 append 的 `CompactionApplied`。
- 宿主/扩展若持久化累计值，必须按 `(session, log_id)` 记忆并能从日志重建；不得使用无键的加法计数器。

## 8. 边界与非目标

- 本地 tokenizer / 估算用量进账：不做；估算只属于压缩模块的窗口判断。
- 全局跨 session 聚合、会话目录的 `roots()`：v1 不做。
- 价格、币种、预算、告警、成本归因到工具/用户：扩展模块或宿主。
- 把 `partial` 混入总量、或对丢失的用量做估算补偿：不做。
- turn / step 级子树汇总：不做；子会话只进 session 树。
- usage 派生表 / 物化视图 / 独立 usage 日志事件：v1 不做。
- 非 chat 调用（embedding、图片生成等）的用量：v1 不覆盖；它们不是 `Provider` 模型调用。
- 缓存断点的成本优化、按缓存命中率重排：不在统计层。

## 9. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| U1 | 两步成功回合 | `ModelCall` 两条；step/turn/session 的 `model_calls` 分别等于对应 `AssistantMessage.usage` 之和；`total()` 正确 |
| U2 | 两次可重试失败后成功 | 失败 attempt 有 `partial_usage` 时产生 `Partial` 记录，不进 `total()`；`ModelCall` 恰一条 |
| U3 | 流中断且 provider 无 usage | 不产生任何记录；总量不变 |
| U4 | 模型流中取消、无 `AssistantMessage` | 不产生 `ModelCall`；有 `partial_usage` 时只产生 `Partial` |
| U5 | `AssistantMessage` 后取消 | `ModelCall` 已计入；恢复不重调模型、不重复计数 |
| U6 | 回合内压缩 | `Compaction` 桶；turn 的 `compaction` 计入，step 的 usage 不含它；`step = None` |
| U7 | Idle 压缩 | `Compaction` 桶；`turn = None`；只进 session |
| U8 | 父子归因 | 父 `Session` 的 own 不含子用量；`SessionSubtree` 的 `descendants` = 子会话之和；`own + descendants` 无重复；fork 子不继承父用量 |
| U9 | 崩溃于 `AssistantMessage` 后、`StepCommitted` 前 | 恢复后该 step 的 usage 恰一份；`on_record` 不重发该记录 |
| U10 | 崩溃于 `ResponseEnded` 后、`AssistantMessage` 前 | 恢复重调模型；最终恰一条 `ModelCall`；第一次调用的用量不可见 |
| U11 | 实时观察顺序 | 每条提交记录恰一次送达 `on_record`、会话内有序；下一 step 的 `before_step` 经 `usage()` 能看到；重放不发 |
| U12 | 与 trace 对账 | 同一 scope 的 `total()` = `model_call` span 的 `usage` 之和 + `compaction` span 的 `usage` 之和（`partial` 除外） |
| U13 | session 日志损坏 | `UsageError::Corrupt`；不返回部分结果 |
| U14 | 过滤 | `bucket` / `model` / `since` / `until` 组合只返回匹配记录；`until` 不含边界 |
| U15 | 全零用量 | 仍产生 `ModelCall` 记录；`totals` 不变 |
| U16 | 子树覆盖缺失日志 | 子会话日志已被归档/删除时，子树只汇总现存日志；不报错、不伪造 |

## 10. 留给下游

- **#15**：已定，#79 修订：折叠在引擎内部，门面暴露 `usage(scope)`；参考 CLI 的用量展示与验收场景。
- **#18**：已定。退役会话返回 `NotFound`；子树只汇总存活会话，见 [retention.md](retention.md)。
- **#19**：崩溃注入要覆盖 U9 / U10 两个 usage 窗口。
- **#14**：exporter 可把 `Usage` 映射到 OTel `gen_ai.usage.*`，但内部模型不采用 OTel 的 token 语义。
- **#17**：无接口变化；子会话调度不影响用量归因。
