# 子智能体调度接缝：spawn/await 分离，等待与超时持久化

**Status**: accepted (2026-10-22)
**来源**: [#17](https://github.com/zibyn/asterpath/issues/17)
**2026-10-22 #29 修订**: 排队按 open spawn `ToolCallRequested` 的 `seq` 重建；spawn timeout timer 归 spawn 重放收尾，先查子终态（R3）再处理 deadline。
**2026-10-23 ADR-0024 修订**: spawn 超时改为 `ChildSessionSpawned.deadline`（绝对时间，与 spawn 同一次 append），不再写 timer 记录；R3 先查子终态不变，见 [ADR-0024](0024-durable-waits-only-approval-and-child.md)。

## Context

[#8](0008-subagent-model.md) 已把子智能体定为「同步工具调用拥有的独立子会话」，并留了四项给本票：`SubagentSpawner` 的精确签名、排队与唤醒实现、与 `DurableExecutor` 的接缝、以及 R1–R4 恢复规则的落点。[#3](../design/event-log-recovery.md) 已规定「等待是已记录的事件」，[#7](../design/tools.md) 已把审批决定表达为 `SignalReceived`。

需要回答的是：spawn 返回什么、等待由谁承担、超时与取消在 API 上如何区分、父崩溃后 open 的 child 等待由谁收尾。原型（`prototype/subagent-api` 分支）用假日志驱动了正常路径、四个崩溃窗口、fan-out 部分失败与子会话审批。

## Decision

- **spawn / await 分离。** `spawn` 记录 `ChildSessionSpawned`、按 `parent_tool_call_id` 去重（R1）、物化子流（R2）并返回 `ChildRef`；它不等待完成。`await_completion(&ChildRef)` 先读已记录的 `ChildSessionCompleted`（R3），再幂等补齐 R2，然后 `DurableExecutor::await_wake` 持久等待。`cancel(&ChildRef)` 请求取消、等 `cancel_grace`，未完成则写 `ChildSessionCompleted{Cancelled{Parent}}`。v1 没有后台句柄。
- **等待是已记录事件，超时也是。** `SpawnRequest.timeout` 由 `DurableExecutor::schedule_timer` 写成父流的 `TimerScheduled`（`caused_by` = `ChildSessionSpawned` 的 `LogId`），deadline 是绝对时间。停机期间到期的 timer 不在通用 timer 预扫描里收尾，而是交给 spawn 重放：先查子终态（R3），有则记 `Completed` + `TimerCancelled`；无且已过期则补 `TimerFired`、取消子会话并写 `Cancelled{Timeout}`。超时不续跑子会话。
- **取消必须带原因。** `ChildOutcome::Cancelled { reason: CancelReason }`，`CancelReason = Parent | Host | Timeout`。这使恢复能稳定还原 `ToolResult{Interrupted}`（父取消）、`Completed` + cancelled（子宿主取消）与 `Failed{Timeout}`（超时），而不必从 timer 记录反推。#8 的裸 `Cancelled` 由此修订。
- **恢复委托给 spawn 工具重放。** 父恢复不单独预收尾 child 等待（#3 §6.2 的 child 分支改为委托）：open 的 spawn 工具是 `ReplaySafety::Safe`，照常重放；`spawn` 吸收 R1/R2，`await_completion` 吸收 R3。写者仍只有 scheduler（正常）或经 spawner 被驱动方调用（恢复），不存在两条并发写路径。不新增 `recover()` 入口。
- **fan-out 没有框架 API。** 同一 `AssistantMessage` 的多个 spawn 调用由 #7 的并行工具管线执行，scheduler 只提供 FIFO 准入（每父 8 / 全局 32）；每个调用独立 outcome，部分失败互不影响，汇总由父模型或宿主完成。不提供 `spawn_many` / join / gather。
- **`DurableExecutor` 扩为等待原语集合。** `schedule_timer` / `cancel_timer` / `await_signal` / `deliver_signal` / `await_wake` / `wake`（仅内存）。`AwaitSignal.intent = AlreadyRecorded` 供审批使用，不写 `SignalAwaited`；通用 signal 用 `Record`。timer id 由 `(session, caused_by, slot)` 确定性派生；审批 signal id 即 `tool_call_id`；通用 signal id 由 `(session, name)` 派生。重复注册返回同一 id，崩溃重放不会制造第二个等待。`wake` 由终态记录的写者在 append 之后调用；注册表只在内存，恢复靠扫描日志重建。
- **审批不新增接缝。** spawn 工具默认 `InProcess → Allow`，宿主可用普通 `ApprovalPolicy` 收紧；子会话内的审批走子会话自己的工具管线与 session 作用域 signal 等待，父审批不覆盖子。

```rust
pub trait SubagentSpawner: Send + Sync {
    fn spawn(&self, req: SpawnRequest) -> BoxFuture<'_, Result<ChildRef, SpawnError>>;
    fn await_completion(&self, child: &ChildRef)
        -> BoxFuture<'_, Result<ChildOutcome, WaitError>>;
    fn cancel(&self, child: &ChildRef) -> BoxFuture<'_, Result<(), WaitError>>;
}

pub struct ChildRef { pub parent: ParentRef, pub child: SessionId }

pub enum SpawnError { DepthExceeded, InvalidArgs { message: String }, Execution(ExecutionError) }
pub enum WaitError  { Cancelled, Timeout, Execution(ExecutionError) }
```

`SpawnError` 表示没有创建子会话（深度、参数、调度器故障）；任务失败不在这里，它是 `ChildOutcome::Failed`。`WaitError::Cancelled` / `Timeout` 是父侧等待的结局，分别映射 `ToolResult{Interrupted}` / `Failed{Timeout}`；子被自己的宿主取消返回 `Ok(Cancelled{Host})`，映射 `ToolResult{Completed}` 并在 output 标明 cancelled。

## Consequences

- 工具包装器很薄：`spawn` → `await_completion` → 按 `ChildOutcome` 写 `ToolResult`；恢复路径不特判，正常重放即可。
- 持久 timer 让超时跨崩溃可重现；代价是每次带 timeout 的 spawn 多两条记录（`TimerScheduled` + `TimerFired`/`TimerCancelled`）。
- `CancelReason` 是日志 schema 的字段增量；旧记录 upcast 为 `Host`（v1 之前没有带原因的超时记录，且 #17 之前未实现）。
- 恢复不再走 #3 §6.2 的预收尾分支；驱动方只需重放 open 工具。子等待若没有对应 open 工具（不应出现），视为日志损坏。
- 排队状态、并发计数、wake 注册表都在内存；崩溃后按 open spawn `ToolCallRequested`（无对应 `ChildSessionSpawned`）的 `seq` 重建 FIFO，不新增事件。`ChildSessionSpawned` 只表示已准入。
- 原型证明了 R1–R3 在同一对方法里闭合，但也暴露出：`await_completion` 必须再次执行 R2，否则「`ChildSessionSpawned` 已写、子流未写」的窗口无法靠工具重放覆盖。

## Considered alternatives

- **单个阻塞 `spawn_and_wait`**：工具包装更短，但把「记录意图」与「等待」绑死，恢复时无法复用普通工具重放，host 也看不到 spawn 时刻。拒绝。
- **返回后台句柄（JoinHandle 类）**：需要后台 spawn 与完成注入，直接违反 #8 的同步持久等待与「不做编排引擎」。拒绝（v1）。
- **内存 `tokio::time::timeout`**：实现最省，但崩溃后 deadline 丢失、等待重新计时，违反 I6。拒绝。
- **保持裸 `ChildOutcome::Cancelled`，从 timer 反推超时**：父取消与超时在记录上不可区分，恢复可能改变 `ToolResult` 形态。拒绝。
- **独立 `SubagentSpawner::recover()` 入口**：语义清楚，但多一个驱动方容易漏调的恢复路径，且与工具重放重复。拒绝，委托给 spawn/await。
- **`spawn_many` / join / gather**：把滚动并发、部分失败与取消传播的批量语义带进内核，与「不做通用工作流引擎」冲突。拒绝；并发由 #7 管线与 scheduler FIFO 提供。
- **审批作为独立 subagent 接缝**：会复制 #7 已有的策略、signal 与恢复语义。拒绝，复用子会话自己的工具管线。
