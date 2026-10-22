# 子智能体是同步工具调用拥有的独立子会话

**Status**: accepted (2026-10-22)
**来源**: [#8](https://github.com/zibyn/asterpath/issues/8)
**2026-10-22 #17 修订**: `ChildOutcome::Cancelled` 携带 `CancelReason { Parent, Host, Timeout }`；API 与持久超时见 [ADR-0014](0014-subagent-scheduling-api.md)。
**2026-10-22 #29 修订**: 父流记录不变量纳入 spawn timeout 的 timer 记录与审批 `SignalReceived`；子 `ConfigSnapshot` 字段集以 [packaging.md](../design/packaging.md) §2.1 为准。

## Context

[#3](https://github.com/zibyn/asterpath/issues/3) 已把日志拓扑定为「一个 session 一条流」，并规定子智能体是独立子会话、父流只记 `ChildSessionSpawned` / `ChildSessionCompleted`；[#6](https://github.com/zibyn/asterpath/issues/6) 已把等待变成已记录事件；[#7](https://github.com/zibyn/asterpath/issues/7) 已让工具调用成为带审批、取消与重放语义的唯一副作用入口。

需要决定：子智能体如何被创建与等待、默认上下文是什么、结果返回什么、配置如何继承、取消如何传播、孤儿如何收尾。参考实现给出三条不同路线：Codex 默认全新 thread、可 fork 全历史、异步 spawn；Claude Code 普通子智能体全新上下文、fork 继承全历史、只返回总结；Pi Durable 把子会话建模为 task 拥有的 conversation。

本 effort 的固定约束：不做通用工作流引擎；调度与恢复是内核之上的模块能力；父子流之间没有跨流事务。

## Decision

- **spawn 是一次同步工具调用。** 模型可见的 spawn 工具由 `asterpath-scheduler` 提供，走 #7 的 pre-execute → 意图 → 审批 → 执行管线；调用在子会话完成前保持 open，父 step 处于 `Waiting`，崩溃后重新挂起。fan-out = 同一 `AssistantMessage` 里多个 spawn 调用并行。v1 不做后台/异步 spawn。
- **子智能体是独立子会话。** 独立日志流、独立恢复与取消单位；父流只记 spawn 自身的管线记录与子生命周期：spawn 的 `ToolCallRequested`、`ChildSessionSpawned`、timeout 的 timer 记录、审批 `SignalReceived`、`ChildSessionCompleted` 与 spawn 的 `ToolResult`。子的 step、工具与计划只在子流。
- **默认 fresh，fork 是物化快照。** 子默认只拿到任务与继承的 system prompt；`context: fork` 按次 opt-in，快照为父会话在 spawn 所在 step 水位的投影，物化进子流的 `ContextForked`，不是对父日志的活引用。父后续压缩/归档不影响子。
- **结果是状态 + 任务总结。** `ChildOutcome { Completed | Failed | Cancelled{reason} | Lost }` 携带子最后一条已提交助手消息的 text/refusal；`CancelReason` 区分父取消、子宿主取消与超时。任务失败仍是成功的工具结果；只有基础设施问题（参数、深度、丢失、超时）才是工具失败。完整 transcript 不自动进入父上下文。
- **配置在 spawn 时刻快照继承。** 子会话的 `ConfigSnapshot` 同时写入 `SessionCreated` 与父流的 `ChildSessionSpawned`；之后父配置变化不影响子。模型可见参数只能收窄（宿主 alias 集合、工具 allowlist），不能放宽审批或替换 system prompt。沙箱仍归扩展模块/宿主。
- **取消传播，父先收尾。** 父取消向子发取消并等 `cancel_grace`；进入 cancelling 的等待一律记 `Cancelled{Parent}`，父在 `ToolResult` 之前写 `ChildSessionCompleted`。父不写子流，子不写父流。
- **孤儿 = 父已不再等待。** 子恢复时若父流已有针对自己的 `ChildSessionCompleted`，按取消收尾并停止，不再开新 turn。spawn 按 `parent_tool_call_id` 查找幂等，恢复绝不重复创建子会话。
- **usage 不复制。** 子的 usage 只写子流；父子归因靠 session 树，查询时 rollup，`ChildSessionCompleted` 不携带 usage。

详见 [`docs/design/subagents.md`](../design/subagents.md)。

## Consequences

- 长任务会让父 step 跨小时/跨天保持 open，与审批等待同构；这是持久等待模型的直接结果，不是缺陷。
- 没有后台子智能体：父模型必须在同一个 step 里等结果。需要并行时靠同一 step 的多个 spawn 调用；需要"发射后不管"的宿主需自建工具（v2 再评估）。
- fork 复制父上下文一次，成本受父上下文上界约束；这是换取子会话独立恢复的代价。
- 任务总结可能为空（子最后一步只有工具调用）；调用方应在任务说明里要求子以总结结束，框架不代劳。
- 深度 3 / 每父并发 8 / 全局 32 是保守默认，防止递归 fan-out 打爆进程；宿主可下调，v1 不提供公平调度或抢占。
- 子会话完成后只保留在存储里，不能续聊；需要多轮协作时，每次都是新子会话 + 显式上下文。

## Considered alternatives

- **异步 spawn + 完成时注入父上下文**：更接近 Codex 的后台线程，但需要新增"子完成 → 父输入"的路径，与 #6 的 steering drain、父回合生命周期纠缠；同步等待已满足 fan-out 与崩溃恢复的验收。拒绝（v1）。
- **task 拥有 conversation（Pi Durable）**：把子会话塞进父会话或 task 状态机，省掉一层 session 拓扑；但会破坏 #3 的「一条 session 一条流、独立恢复」以及 #6 的 turn/step 语义。拒绝。
- **fork 为对父日志的活引用**：存储省、语义"共享"；但父压缩、归档或损坏会直接打断子会话，且子恢复依赖跨流读取。拒绝，改为物化快照。
- **返回完整 transcript**：信息最全，但父上下文成本不可控，且把子的中间计划带进父；与「父不持久化子的计划」冲突。拒绝，只返回状态 + 总结。
- **spawn 作为 host-only API**：更像库原语，但模型无法自主 fan-out，任务编排就落回宿主；作为 `SubagentSpawner` 能力保留，工具是薄包装。
