# 压缩是能力模块：只摘要、不是 step、窗口显式配置

**Status**: accepted (2026-10-22)
**来源**: [#9](https://github.com/zibyn/asterpath/issues/9)
**2026-10-22 #24 审计修订**: 强制压缩改为同 step 内层重试；压缩失败以 `CompactionAborted` 闭合；摘要输入只保留文本；窗口唯一来源是 `ModelSpec.context_window`；恢复次数上限归宿主。
**2026-10-22 #27 审计修订**: `Compactor` 改为两段式 `plan` / `compact`；默认实现可选装配（M0 为 `None`）；意图由 agent 在 `plan` 判定 `Summarize` 后写；`CompactionOutcome` 携带摘要调用实际使用的 `model`。
**2026-10-23 ADR-0026 修订**: `asterpath-compaction` 并入引擎作为默认 compactor；`Compactor` trait 仍在 protocol、可替换，见 [ADR-0026](0026-engine-is-one-crate-only-adapter-traits-public.md)。

## Context

[#3](https://github.com/zibyn/asterpath/issues/3) 已定 `CompactionApplied` 只影响投影、原始日志保留；[#6](https://github.com/zibyn/asterpath/issues/6) 定了 pre-step 调用点与 `CompactionFailed`，并把 provider 上下文超限直接映射为 `ContextExhausted`。剩下的问题会决定日志事件字段、错误路径与 token 归因：谁拥有压缩策略、摘要调用是否是一个 step、窗口从哪来、以及超限后是否还能在回合内救回来。

## Decision

- **压缩是可选 `Compactor` 能力模块**（trait 在 protocol，默认实现 `asterpath-compaction`；`asterpath-agent` 只依赖 trait，默认实现由组装根注入，`None` = 压缩不可用）；loop 只在 BetweenSteps 调用，内核不含策略。Compactor 分 `plan`（纯函数，决定切点）与 `compact`（摘要调用）两段，只读、返回结果；agent 在 `plan` 返回 `Summarize` 后写 `CompactionStarted`，并写终态。
- **v1 只做摘要**：被替换区间内的一切（含工具输出）由单条摘要代表；不单独折叠工具输出，也不引入第二种压缩事件。
- **压缩不是 step**：不写 `StepStarted` / `StepCommitted`；成功只追加一条 `CompactionApplied`，扩展携带 `usage` 与 `raw`；失败的摘要 attempt 不落日志。意图 `CompactionStarted` 由 `CompactionApplied` 或 `CompactionAborted{SummaryFailed|Insufficient}` 闭合，终态 append 是唯一提交点，崩溃在 append 前重做。
- **窗口显式配置**（`ModelSpec.context_window` 是唯一来源，`CompactionConfig` 不重复定义），不从 API 探测；token 用最近一次 usage + `bytes/3` 保守估算，不引入 tokenizer。
- **触发**：pre-step 自动阈值、手动（仅 Idle）、provider 错误路径每 step 一次的强制压缩；`stop_reason` 超限不重试；不做 mid-step 触发。修订 #6：错误路径压缩后**在同一 `StepStarted` 内**重试同一 step（不新开 step），只有再次超限才 `ContextExhausted`；摘要调用失败则 `CompactionFailed`。
- **摘要输入是文本视图**：丢弃 `Reasoning` 与 provider raw，非文本 `Part` 换占位文本，工具调用/结果保留文本形式；请求不挂工具、不启用 reasoning。
- **保留最近一个完整 step**，目标 `0.5 × usable window`，只在步边界切；每回合最多 1 次自动压缩；压后仍超 → `ContextExhausted`。
- **`replaces` 始终从流首（或上一条 replaces 起点）开始**，因此每次压缩重新概括旧摘要，投影只保留最新 summary。
- **接受前缀缓存未命中**：不为缓存调整触发或保留边界。
- **摘要 usage 单列 `compaction` 桶**；手动压缩不包成零 step 回合，直接追加事件。

## Consequences

- 宿主必须配置窗口，否则压缩不可用；框架不做模型知识表。
- 宿主可以完全不装配 Compactor（M0 即如此）：压缩返回 `NotConfigured`，强制压缩路径直接 `ContextExhausted`，框架不因此启动失败。
- 错误路径多一次强制压缩，回合可能因此在原错误后存活；这是对 #6 L9 的唯一修订。
- 每次压缩至少一次摘要调用成本与一次缓存写；多次压缩不会累积多条摘要。失败或 `Insufficient` 的摘要调用成本不进入统计，只在 trace tap 可见。
- 恢复次数上限不在内核：宿主 supervisor 用 `StreamResumed` 记录计数并决定停车；压缩失败有终态，不会因同一意图自循环。
- 事件日志保留全部原始记录；整会话退役是可选管理能力（#18 已定，见 [ADR-0016](0016-retention-and-archive-seam.md)），压缩本身从不删除记录；投影折叠语义需要能移除旧 summary。
- 工具输出折叠推迟到 v1 之后；若将来加入，需要扩展事件或新增事件。

## Considered alternatives

- **压缩写进 agent 模块**：少一个 crate，但宿主无法替换策略，与「agent loop / provider / 日志实现可替换」的章程冲突。
- **把摘要调用记成一个 step**：复用 step 的重试与 usage 记账，但压缩发生在 step 之外，会凭空生成检查点，且破坏「step = 一次模型请求 + 其工具」的定义。
- **两段式（先折叠工具输出再摘要）**：保留更多近期非工具上下文，但折叠占位必须持久化才能让投影确定，需要额外事件字段；v1 没有实测需求。
- **从 API 或模型注册表推断窗口**：两家 API 都不返回窗口；内置模型表会过期，违背「配置由组装根提供」。
- **`stop_reason` 超限也压缩重试**：模型已产出消息，重试会重复已提交的输出；只在尚无 `AssistantMessage` 的错误路径救回。
- **保留旧摘要、只概括新增区间**：信息损失更小，但摘要会累积、投影与事件语义更复杂；选择「摘要的摘要」。
- **cache-aware 压缩调度**：能省一次缓存写，但把缓存细节渗进策略层，收益不足以抵消复杂度。
