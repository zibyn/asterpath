# 日志按 stream 分片，只提交终态，日志层没有 ack

**Status**: accepted (2026-10-22)

**来源**: [#3 事件日志与检查点恢复语义](https://github.com/zibyn/asterpath/issues/3)

## Context

恢复承诺是「崩溃后重放到 frontier，且已完成的副作用不重复」。可选的日志形状有三种：一条全局日志、按执行单位分片的 append-only 流、或物理嵌套的日志树。与此同时，流式输出让「什么算已提交」变成一个必须显式回答的问题；Duroxide/Temporal 的 action 绑定与 Pi 的 intent-first 提交给出了两种不同的答案。

## Decision

- `EventLog` 以 `StreamId`（`Session` | `Workflow`）分片，每个 stream 一条 append-only 日志；stream 之间无事务。子智能体是独立 session stream，父流只记一条 `ChildSessionSpawned`。
- 提交单位是**终态条目**（完整 assistant message、工具调用意图、工具结果、step/turn 提交），不是流式 delta。
- 不变量「模型可见即已记录」：进入下一次请求或产生副作用之前，先提交记录。
- 日志层**没有 ack**。完成 = 写了终态记录。租约与 ack 只属于存储的调度队列。
- 检查点 = 最后一个 `StepCommitted` 边界，不写独立 checkpoint 记录。
- 恢复只处理未闭合意图：`Safe` 重跑，`Unsafe` 补写 `Interrupted` 终态。
- 工作流实例的确定性重放只读，不写记录；session 恢复写一条非模型可见的 `StreamResumed`。

## Consequences

- 恢复算法只有一条主路径：找检查点、折叠投影、处理未闭合意图；不需要区分「已完成未 ack」这类中间状态。
- 每次模型调用至少一次完整落盘，没有逐 token 持久性；崩溃丢失的是本次未提交的流。
- 子智能体可独立恢复与取消，但父子之间没有原子性；关闭顺序与孤儿处理必须由 #8 定义。
- stream 之间的一致性（例如父记录已写、子流未创建）只能靠幂等的创建记录修复。
- 同一枚举承载 session 与 workflow 事件，schema 只有一套，代价是枚举变大。

## Considered alternatives

- **单条全局日志 + scope 字段**：写简单，但每个 session 的恢复要扫描无关记录，租约也必须全局化。
- **物理嵌套日志树**：父子事务天然原子，但独立恢复/取消需要拆树，且存储层要理解嵌套。
- **逐 delta 持久化**：崩溃可精确续流，但写入量高一个数量级，且没有一家 provider 保证可续传 SSE。
- **在日志层保留 ack**：多出一个中间状态，恢复要为「已完成但未确认」写专门分支，而它并不携带信息。
