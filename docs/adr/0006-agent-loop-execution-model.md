# Agent loop 以 step 为检查点，取消/失败统一闭合，steering 不打断模型流

**Status**: accepted (2026-10-22)
**来源**: [#6](https://github.com/zibyn/asterpath/issues/6)
**2026-10-23 ADR-0025 修订**: pre-step waterfall gate 改为 `Hooks::before_step`，语义不变，见 [ADR-0025](0025-extension-points-are-typed-hooks.md)。

## Context

[#3](https://github.com/zibyn/asterpath/issues/3) 定了事件与恢复，但 loop 的边界没定：`StepCommitted` 是检查点，`StreamState` 里同时有 `StepOpen` 与 `ToolPending`；模型调用的 usage 记在 `StepCommitted`，而它要到工具跑完才写；运行中用户输入（steering）没有注入点；取消/失败如何闭合一个已开始的 step 也没有终态事件。这些决定直接塑造日志 schema 与恢复路径，实现后再改代价高。

## Decision

- **回合由输入驱动，step = 一次模型请求 + 其全部工具调用。** `StepCommitted` 在工具全部终结后写；回合可以零 step。
- **`StepCommitted` 是 step 的通用终态**：成功、失败、取消都写它；回合结果由 `TurnCommitted.reason` 表达。不新增 `StepCancelled` / `StepFailed`。
- **模型请求的投影水位是 `StepStarted.seq`**：只投影它之前的记录；重试复用同一投影；新输入只在下一个 step 可见。
- **steering 只排队，不打断模型流**：输入到达即 `InputRecorded`，在 step 边界 drain；无工具调用时若有 pending input，同一回合继续。
- **取消是回合级、协作式**：模型流立即 abort；工具给 `cancel_grace`（默认 10s），到期 drop 并写 `Interrupted`；等待意图由 loop 收尾（`TimerCancelled` / `ChildSessionCompleted{Cancelled{Parent}}`）。
- **模型失败在 step 内按 attempt 重试**；工具失败不重试，作为模型可见结果。默认 `max_attempts = 3`，指数退避 + jitter。
- **usage 与 stop_reason 记在 `AssistantMessage`**，`StepCommitted` 只作检查点标记；恢复时若 `AssistantMessage` 已存在则不重调模型。
- **默认不设 step 上限**；宿主通过 pre-step waterfall gate 实施停止策略；`MaxTurns` 改名 `MaxStepsExceeded`，并新增 `ContextExhausted`、`CompactionFailed`。

## Consequences

- 崩溃于工具执行中不再丢 usage；恢复不会重复调用已有 `AssistantMessage` 的模型。
- 失败/取消的 step 也是检查点，恢复不会重试；日志里不会出现永远 open 的 step。
- steering 的代价是最多晚一个 step 生效；想立即停止用取消。
- 审批/子会话等待会让一个 step 跨天 open；这是 #3 的 `Waiting` 设计，不是缺陷。
- 宿主必须自己实现停止与步数策略；框架只提供 gate。
- 不自动续写被截断的输出；宿主可以再发输入。

## Considered alternatives

- **step 只包模型请求，工具落在 step 外**：模型响应更早成为检查点，但恢复要区分「模型已提交、工具未提交」，usage/停止原因也会分散在两层。
- **新增 `StepCancelled` / `StepFailed` 终态**：语义更直白，但每加一种结束方式都要加一个事件，恢复和投影也要多分支；`StepCommitted` 作为通用终态更小。
- **立即打断模型流来注入 steering**：响应更快，但会吞掉在途请求的成本，并让 step 边界不再可预测；Codex / Claude Code 都选择排队。
- **usage 留在 `StepCommitted`**：schema 改动最小，但工具中崩溃会永久丢该 step 的 usage，且恢复可能重调模型。
- **框架内建 `max_turns` / 停止策略**：宿主策略被固化进库；回合由输入驱动，也没有可数的「回合上限」。
