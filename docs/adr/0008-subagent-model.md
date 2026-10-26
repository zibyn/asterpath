# 子智能体是工具调用拥有的独立子会话

**Status**: accepted (2026-10-22)；2026-10-24 整理：并入 ADR-0014；部分扩展 by [ADR-0033](0033-session-fork-reuses-context-fork-snapshot.md)（fork 快照也用于宿主发起的会话分叉）；「不能替换 system」部分取代 by [ADR-0053](0053-subagents-select-application-agent-profiles.md)（模型可按名字选用应用提供的智能体档案）
**来源**: [#8](https://github.com/zibyn/asterpath/issues/8)、[#17](https://github.com/zibyn/asterpath/issues/17)、[#29](https://github.com/zibyn/asterpath/issues/29)
**规范**: [subagents.md](../design/subagents.md)

## Context

日志只有会话流（[ADR-0002](0002-log-topology-and-commit-semantics.md)），工具调用是带审批、取消与重放语义的唯一副作用入口（[ADR-0007](0007-approval-and-sandbox-layers.md)）。需要决定子智能体如何创建与等待、默认上下文、返回什么、配置如何继承、取消如何传播、孤儿如何收尾、恢复由谁负责。参考：Codex 默认全新 thread、可 fork、异步 spawn；Claude Code 普通子智能体全新上下文、只返回总结；Pi Durable 把子会话建模为 task 拥有的 conversation。

## Decision

- **子智能体是独立子会话**：独立日志流、独立恢复与取消单位。父流只记 spawn 调用的管线记录与子生命周期（`ChildSessionSpawned`、`ChildSessionCompleted`）；子的 step、工具与计划只在子流。
- **spawn 是一次模型可见的工具调用**，走普通工具管线（审批、意图、取消、重放）。同一 `AssistantMessage` 里的多个 spawn 调用并行执行，框架不提供 gather / join API。
- **创建与等待分离**：创建按父工具调用 id 幂等、物化子流；等待先读已记录的完成，再持久等待。恢复不设专门入口，委托给 spawn 工具的正常重放。
- **默认 fresh，fork 是物化快照**：fork 把父会话在 spawn 所在 step 水位的投影复制进子流，不是对父日志的活引用。
- **结果是状态 + 任务总结**：`Completed | Failed | Cancelled{reason} | Lost`，携带子最后一条助手消息的文本；完整 transcript 不进入父上下文。取消原因区分父取消、宿主取消与超时。
- **配置在 spawn 时刻快照继承**，模型可见参数只能收窄（模型别名、工具集），不能放宽审批或替换 system。
- **超时是记录在 spawn 记录上的绝对截止时间**，跨崩溃生效。
- **取消传播，父先收尾**；父不写子流，子不写父流。父已不再等待的子会话（孤儿）在恢复时按取消收尾。
- **usage 不复制**：子 usage 只在子流，父子归因靠会话树（[ADR-0011](0011-native-tracing.md)）。

当前实现中父的 spawn 调用同步等待子会话完成。异步 / 后台子智能体不在本 ADR 范围内，没有被排除：父流已经记录 `ChildSessionCompleted`，异步主要是投影与回合生命周期的设计问题。准入上限等参数见 subagents.md。

## Consequences

- 同步等待让父 step 在子会话运行期间保持 open；结合 [ADR-0006](0006-agent-loop-execution-model.md) 的 steering 规则，期间对父会话的输入要等子会话结束才可见。
- fork 复制父上下文一次，换取子会话独立恢复，父后续压缩或退役不影响子。
- 任务总结可能为空；调用方应在任务说明里要求子以总结结束。
- 子会话完成后不能续聊；多轮协作每次都是新子会话 + 显式上下文。

## Considered alternatives

- **task 拥有 conversation（Pi Durable）**：破坏「一个会话一条流、独立恢复」。
- **fork 为对父日志的活引用**：父压缩、退役或损坏会打断子会话。
- **返回完整 transcript**：父上下文成本不可控，并把子的中间计划带进父。
- **单个阻塞 `spawn_and_wait`**：把记录意图与等待绑死，恢复无法复用普通工具重放。
- **内存 `tokio::time::timeout` 做超时**：崩溃后截止时间丢失。
- **独立 `recover()` 入口**：多一个容易漏调的恢复路径，与工具重放重复。
- **spawn 只作 host API**：模型无法自主 fan-out。
