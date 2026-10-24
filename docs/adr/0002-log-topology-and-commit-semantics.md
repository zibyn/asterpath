# 每个会话一条 append-only 日志，只提交终态，不做工作流引擎

**Status**: accepted (2026-10-22)；2026-10-24 整理：并入 ADR-0003、ADR-0009 的压缩原则与 ADR-0020
**来源**: [#3](https://github.com/zibyn/asterpath/issues/3)、[#25](https://github.com/zibyn/asterpath/issues/25)
**规范**: [event-log-recovery.md](../design/event-log-recovery.md)、[storage.md](../design/storage.md)、[compaction.md](../design/compaction.md)

## Context

恢复承诺是「崩溃后重放到最后一条已提交记录，且已完成的副作用不重复」。需要决定日志的形状、什么算已提交、检查点在哪，以及编排是否由框架以确定性重放承担。参考实现给出两类答案：Duroxide / Temporal 用确定性工作流重放 action 序列；Pi 用 intent-first 提交、按会话恢复。

## Decision

- **日志流只有一种：会话。** 每个 `SessionId` 一条 append-only 日志，流之间没有事务。子智能体是独立的子会话（[ADR-0008](0008-subagent-model.md)）。
- **不做工作流引擎**：没有确定性重放、action 序列比对、代码版本栅栏或 activity。恢复是检查点语义，与框架和工具的代码版本无关。
- **提交单位是终态记录**（完整 assistant message、工具调用意图、工具结果、step/turn 提交），不是流式 delta。
- **模型可见即已记录**：进入下一次模型请求或产生副作用之前，先提交记录。
- **日志层没有 ack**：完成 = 写了终态记录。检查点 = 最后一个 `StepCommitted`，不写独立 checkpoint 记录。
- **恢复只处理未闭合意图**：`Safe` 重跑，`Unsafe` 补写 `Interrupted`，等待类意图重新挂起；恢复写一条非模型可见的 `StreamResumed`。
- **追加幂等**：带 dedup key 的记录按 `(session, 事件类型, key)` 去重。事件类型名因此是持久判别式，发布后不得改名。
- **信封与事件分离**：存储只认信封（session、schema 版本、dedup key、seq、`recorded_at`）和不解释的 JSON payload；类型化解码在 protocol。事件形状变化不需要改存储后端或 DDL。
- **日志从不原地改写**：压缩只追加记录、只改变投影，不删原始记录，也不是一个 step；删除只有整会话退役（[ADR-0023](0023-storage-holds-log-and-catalog-wake-by-fold.md)）。

## Consequences

- 恢复算法只有一条主路径：找检查点、折叠投影、处理未闭合意图。
- 没有逐 token 持久性：崩溃丢失的是本次未提交的流。
- 父子会话之间没有原子性，跨流的不一致只能靠幂等的创建记录与恢复规则修复。
- 无法表达「确定性重放一段编排函数」；需要时必须作为新的执行模型设计，而不是在会话流上打补丁。
- 日志体积只增不减，长会话靠压缩管理上下文，靠宿主管理保留。

## Considered alternatives

- **单条全局日志 + scope 字段**：写简单，但每个会话的恢复要扫描无关记录。
- **物理嵌套日志树**：父子天然原子，但独立恢复和取消需要拆树，存储要理解嵌套。
- **保留 workflow 流作为内部机制**：内核要背两套执行模型，且没有调用方。
- **逐 delta 持久化**：写入量高一个数量级，也没有 provider 保证 SSE 可续传。
- **在日志层保留 ack**：多出一个不携带信息的中间状态。
- **信封直接携带类型化事件**：存储后端会随事件形状一起变化。
