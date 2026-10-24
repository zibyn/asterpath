# 追踪与用量都是事件日志的确定性投影，成本不在内核

**Status**: accepted (2026-10-22)；2026-10-24 整理：并入 ADR-0012
**来源**: [#12](https://github.com/zibyn/asterpath/issues/12)、[#13](https://github.com/zibyn/asterpath/issues/13)
**规范**: [tracing.md](../design/tracing.md)、[usage.md](../design/usage.md)

## Context

框架要求原生可审计的执行链追踪与 token 统计，不依赖第三方 agent / 追踪框架，跨崩溃与重放不重复计数。事件日志已经是恢复与模型历史的唯一真相源。OTel GenAI 约定没有 session / turn / step / subagent span，也没有 replay、attempt、去重语义（[调研](../research/otel-genai-conventions.md)）。成本、价格表、预算与告警是宿主策略。

## Decision

- **trace 与 usage 都是日志的纯折叠**：不建独立 sink、不落派生表，查询时从日志折叠；facade 提供 `trace(session)` 与 `usage(scope)`。实时观察经唯一的提交后出口 `Hooks::on_record`（[ADR-0025](0025-extension-points-are-typed-hooks.md)），框架不提供专用推送接口。
- **`recorded_at` 只用于呈现**：存储在 append 时写墙钟时间，只供 trace 计算耗时与目录展示；恢复、投影、分支逻辑不得读取。
- **span 身份确定**：由开启记录派生，恢复不会为同一逻辑操作产生第二个 span；一个逻辑模型调用一个 span，transport attempt 折叠为 span event。
- **子会话是独立 trace**，通过链接指向父 trace 的 spawn span，不合并成一条跨天 trace。
- **只有 provider 返回值进账**：本地估算永不进入用量；`partial` 单列失败 attempt 的下界，不计入总量。
- **归因按会话树**：子 usage 只在子流，查询分别返回本会话与后代，不提供含糊的单一父总量。
- **内核不提供成本、币种、预算类型**：成本模块经钩子观察用量，用 `before_step` 停止回合。
- **映射到 OTel 等外部格式由 exporter / 宿主负责**；内容只以日志范围引用，采样、过滤、脱敏由宿主决定。

## Consequences

- 恢复不重写历史，也不新建 trace；未闭合的 span 以无结束时间呈现。
- 查询成本与日志长度线性；需要大规模报表时由宿主物化，而不是引入第二真相源。
- 已知崩溃窗口：`ResponseEnded` 后、`AssistantMessage` 落盘前的用量丢失。
- trace 与 usage 是同一日志的两条投影，必须对账；tracing 不拥有统计。

## Considered alternatives

- **独立 trace sink / usage 事件或派生表**：第二真相源与一致性窗口。
- **不记录时间，运行时内存采集耗时**：重建后的历史 span 没有耗时。
- **每个 transport attempt 一个 span**：重复计时与 usage。
- **以 OTel SDK / `gen_ai.*` 为核心模型**：约定仍是 Development，且不覆盖 session/turn/step。
- **本地 tokenizer 估算进账、`partial` 计入总量、父日志复制子 usage**：总量变得半真半假或双计。
- **成本放进内核**：价格与预算随宿主变化。
