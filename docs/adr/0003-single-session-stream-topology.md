# 日志拓扑收敛为单一 Session 流，不做工作流引擎

**Status**: accepted (2026-10-22)
**Supersedes in part**: [ADR-0002](0002-log-topology-and-commit-semantics.md)（仅 Workflow 流类型与工作流重放部分）

**来源**: 地图范围调整（2026-10-22）。原 [#3](https://github.com/zibyn/asterpath/issues/3) 的日志语义保留；[#10 工作流 / 编排 API](https://github.com/zibyn/asterpath/issues/10) 出界。

## Context

原 charter 让 Rust async 确定性工作流承担编排：workflow 实例一条流、action 序列重放、子 workflow、timer/signal。该设计的存在理由是「编排层确定性重放」。地图重画后，目标收敛为**子智能体调度与恢复**，而不是通用工作流引擎：应用需要的是派生一个子智能体、等它完成、崩溃后接着等，而不是让框架重放一段用户写的编排函数。

子智能体本来就是独立 session（#3 已定），父会话需要的等待（子完成、定时器、外部 signal）可以用已记录的 session 事件表达。继续保留 workflow 流会让内核背两套执行模型、一套只为不存在的调用方服务。

## Decision

- **只有一种日志流：`SessionId`。** 删除 `StreamId` 枚举、`Workflow(InstanceId)` 变体与全部 `WorkflowEvent`。
- 子智能体是**子 session**：独立流、独立恢复与取消单位。父流记 `ChildSessionSpawned` 与终态 `ChildSessionCompleted`。
- **持久等待是 session 事件**：`TimerScheduled` / `TimerFired` / `SignalReceived`。人工审批、跨天等待、子会话完成唤醒都走这三种记录。
- **没有确定性重放**：不做 action 序列比对、`WorkflowVersion` 栅栏、`continue_as_new`、activity。
- 恢复沿用 #3 的检查点算法：截断 torn tail → 定位检查点 → 折叠投影 → 处理未闭合意图；等待类意图重新挂起，而不是重做。（物理层由 [ADR-0005](0005-storage-contract-and-sqlite.md) 落实：SQLite 提交原子，没有 torn tail；该步骤只对非原子后端有意义。）
- ADR-0002 的其余决定（append-only、终态提交、日志层无 ack、检查点 = 最后一个 `StepCommitted`、幂等 append）**原样保留**。
- 子会话的完成结果形状、父子关闭顺序、孤儿策略归 [#8](https://github.com/zibyn/asterpath/issues/8)；调度 API 已由 [#17](https://github.com/zibyn/asterpath/issues/17) 定形（ADR-0014），子等待恢复委托给 spawn 工具重放。

## Consequences

- 内核只有一套执行模型：session 检查点。恢复路径不分支。
- 升级不再需要 workflow 代码版本栅栏；只剩日志 schema 版本（ADR-0004）。
- 应用需要复杂编排时，自行组合子会话与信号；框架不提供通用 workflow API 或模式库。
- 跨天等待由存储的定时器与信号投递支撑（#5），不依赖协调器重放。
- 代价：无法表达「确定性重放一段编排函数」；若未来需要，必须作为新的执行模型重新设计，而不是在 session 流上打补丁。

## Considered alternatives

- **保留 Workflow 作为内部机制**：对外只暴露子智能体 API。实现翻倍，且一旦内部存在，schema、恢复、追踪都要承担两套语义。
- **抽一个极小的 durable 等待引擎给 session 和其它调用方共用**：v1 没有第二个调用方；等出现时再抽。
