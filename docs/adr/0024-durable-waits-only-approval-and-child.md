# 持久等待只保留审批与子会话完成

**Status**: accepted (2026-10-23)
**来源**: [#65](https://github.com/zibyn/asterpath/issues/65)（越界设计审计候选 2）
**前置**: [ADR-0003](0003-single-session-stream-topology.md)、[ADR-0007](0007-approval-and-sandbox-layers.md)、[ADR-0014](0014-subagent-scheduling-api.md)、[ADR-0023](0023-storage-holds-log-and-catalog-wake-by-fold.md)
**修订**: ADR-0003 的持久等待、ADR-0005 的 timer 事务、ADR-0011 的等待 span、ADR-0014 的超时 timer、ADR-0023 的可唤醒判定

## Context

#1 charter 承诺“跨天 timer / 外部 signal / 人工审批”，内核因此有通用持久 timer（`TimerScheduled` / `TimerFired` / `TimerCancelled`、`schedule_timer` / `cancel_timer`）与通用 signal（`SignalAwaited`、`SignalIntent::Record`、按名字派生 id）。实现后核对：

- 唯一的 `schedule_timer` 调用是 scheduler 的 spawn 超时（`scheduler.rs:681`）；scheduler 自己 sleep 到期，runtime 的等待分支在生产中不可达；`WakeCause::Startup` 从未构造。
- 唯一的 signal 是审批，始终走 `SignalIntent::AlreadyRecorded`；`SignalAwaited` 只在测试中写入。
- 宿主无路径使用通用 timer / signal：facade 不暴露执行器，`ToolContext` 不带执行器，宿主模块没有可挂持久等待的工具调用锚点。
- Pi Durable 的持久 sleep 依附于带 phase 的 task（工作流式模型，ADR-0003 已拒绝）；Codex、Claude Code 无持久 timer；DeepSeek Harness 把 schedules 做成插件。

## Decision

- **持久等待只有两种**：人工审批（`SignalReceived`，形状与 `tool.approval.<id>` 命名不变，ADR-0007）与子会话完成。
- **删除通用 timer**：三个 timer 事件、`schedule_timer` / `cancel_timer`、`TimerRequest`、`DueTimer`、`OpenIntent::Timer`、`WakeCause::Timer` / `Startup`、runtime timer 代码、tracing timer span、agent `recover_waits`。
- **删除通用 signal**：`SignalAwaited`、`SignalIntent::Record`、`OpenIntent::Signal`、按 `(session, name)` 派生 id 的路径、tracing 通用 signal span。
- **spawn 超时是 spawn 记录上的截止时间**：`ChildSessionSpawned.deadline: Option<Timestamp>`（绝对时间，`#[serde(default)]`），与 spawn 同一次 append 写入；计时起点仍是 `ChildSessionSpawned`。scheduler sleep 到期；恢复时先按 R3 查子终态，无终态且已过期则写 `ChildSessionCompleted{Cancelled{Timeout}}` 并取消子会话。
- **`DurableExecutor` 保留**，只剩 `await_signal`、`deliver_signal`、`wake`、`await_wake`；runtime crate 保留。
- **发布前删除事件变体，不做 upcast**，`LOG_SCHEMA_VERSION` 不变；含已删变体的开发库需重建。

## Consequences

- charter 恢复承诺改为：崩溃重放到 frontier + 跨崩溃的人工审批与子会话等待。
- spawn 与超时之间不再有崩溃窗口，`ensure_timeout` / `stop_timeout` 删除。
- 会话事实与可唤醒判定不再含到期 timer（ADR-0023 修订）。
- 将来工具需要持久 sleep 时，以新增事件变体加回（ADR-0004 下新增变体兼容），不复活通用执行器接口。

## Considered alternatives

- **保留通用 timer / signal 作为宿主能力**：宿主无路径使用，却横穿 protocol、runtime、存储与 tracing；拒绝。
- **保留已删变体只解码不使用**：v1 未发布、无黄金 fixture；拒绝。
- **超时继续用独立 timer 记录**：需要 `ensure_timeout` 补写与配对关闭，截止时间放在 spawn 记录上即可；拒绝。
