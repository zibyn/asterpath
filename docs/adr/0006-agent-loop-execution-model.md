# Agent loop 以 step 为检查点，取消/失败统一闭合，steering 不打断模型流

**Status**: accepted (2026-10-22)；2026-10-24 整理
**来源**: [#6](https://github.com/zibyn/asterpath/issues/6)
**规范**: [agent-loop.md](../design/agent-loop.md)

## Context

日志与恢复语义（[ADR-0002](0002-log-topology-and-commit-semantics.md)）要求 loop 的边界落在可提交的记录上：检查点在哪、usage 记在哪、运行中输入从哪注入、取消与失败如何闭合一个已开始的 step。这些决定直接塑造日志 schema 与恢复路径。

## Decision

- **回合由输入驱动，step = 一次模型请求 + 其全部工具调用。** `StepCommitted` 在工具全部终结后写；回合可以零 step。
- **`StepCommitted` 是 step 的通用终态**：成功、失败、取消都写它；回合结果由 `TurnCommitted.reason` 表达。
- **模型请求只投影 `StepStarted` 之前的记录**；重试复用同一投影，新输入在下一个 step 可见。
- **steering 只排队，不打断模型流**：输入到达即记录，在 step 边界 drain；没有工具调用但有待处理输入时，同一回合继续。
- **取消是回合级、协作式**：模型流立即中止，工具有宽限期，到期写 `Interrupted`；等待意图由 loop 收尾。
- **模型失败在 step 内按 attempt 重试**；工具失败不重试，作为模型可见结果。
- **usage 与 stop_reason 记在 `AssistantMessage`**；恢复时若 `AssistantMessage` 已存在则不重调模型。
- **框架不内建停止策略**：没有默认 step 上限，宿主经 `Hooks::before_step`（[ADR-0025](0025-extension-points-are-typed-hooks.md)）实施。

宽限期、重试次数与退避等参数见 agent-loop.md。

## Consequences

- 崩溃于工具执行中不丢 usage，恢复不重复调用已有结果的模型。
- 失败或取消的 step 也是检查点，日志里不会出现永远 open 的 step。
- steering 最多晚一个 step 生效；step 内有长时间工具或子会话等待时，输入要等到它们结束（见 [ADR-0008](0008-subagent-model.md)）。想立即停止用取消。
- 宿主自己实现停止与步数策略。

## Considered alternatives

- **step 只包模型请求，工具在 step 外**：恢复要区分「模型已提交、工具未提交」，usage 分散在两层。
- **`StepCancelled` / `StepFailed` 终态**：每种结束方式一个事件，恢复和投影多分支。
- **立即打断模型流注入 steering**：吞掉在途请求的成本，step 边界不可预测；Codex / Claude Code 都选择排队。
- **框架内建 `max_turns`**：把宿主策略固化进库。
