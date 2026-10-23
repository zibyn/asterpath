# 追踪是事件日志的确定性投影

**Status**: accepted (2026-10-22)
**来源**: [#12](https://github.com/zibyn/asterpath/issues/12)
**2026-10-23 ADR-0024 修订**: 等待 span 只剩审批与子会话；timer span 与通用 signal span 删除，见 [ADR-0024](0024-durable-waits-only-approval-and-child.md)。

## Context

Asterpath 要求原生可审计的执行链追踪：不集成第三方 agent/追踪框架，trace 必须能从事务日志重建。事件日志已经是恢复与模型历史的唯一真相源；OTel GenAI 约定（Development）没有 session / turn / step / subagent span，也没有 replay、attempt、去重语义（[#14](../research/otel-genai-conventions.md)）。

难点是时间：`#3` 刻意让信封不含墙钟时间，`seq` 是唯一排序依据，以保证恢复与投影的时间无关（I4）。但 trace 的 span 必须有起止与耗时，否则跨天等待、重试、审批无法审计。

## Decision

- **trace 是事件日志的确定性投影**。不建独立 trace sink，不落 trace 表；查询时从 `EventLog` 折叠，实时导出由 tracing 模块在事件总线上发出。
- **`LogEntry` 增加 `recorded_at`（Unix ms）**，由存储层在 append 时写入。它只供 trace/审计计算耗时；恢复、投影、分支逻辑一律不得读取。I4 改写成“时间不影响逻辑，只影响耗时呈现”。
- **SpanId 由开启记录确定性派生**：`hash(session, opening_log_id, kind)`。恢复复用已存在的开启记录，因此不会为同一逻辑操作产生第二个 span；`TraceId = SessionId`。
- **一个逻辑模型调用一个 span**，transport attempt 折叠为 span event；timer / signal / approval / child 等待各自成 span；compaction 是独立 span，并以新增的 `CompactionStarted` 意图记录定位起点。
- **类型进 `asterpath-protocol`，实现进 `asterpath-tracing`**。`TraceQuery`（pull）与 `TraceTap`（push）就是导出 seam；内容只以 `log_range` 引用，采样/过滤/脱敏/格式由 exporter/host 决定。
- **子会话是独立 trace**，通过 `parent_trace` / `parent_span` 与父 trace 的 spawn tool span 链接，不合并成一条跨天超长 trace。

## Consequences

- 日志每条记录增加 8 字节，换来 trace 的耗时、审批等待、跨天 timer 与崩溃窗口全部可重建。
- “信封不含时间”的旧表述被修订：时间可以存在，但不许参与决策。S8 拆成“逻辑输出不受时间影响”与“trace 耗时随时间变化”。
- 恢复不重写历史，也不新建 trace；trace 在与日志相同的 frontier 处自然闭合。未闭合的 span 以 `ended_at = None` 呈现。
- `CompactionStarted` 补上了压缩的意图记录，符合“意图先提交后执行”；失败 attempt 仍不落日志，只在实时 tap 中可见。
- `SignalAwaited` 让“已声明等待的 signal”从一句定义变成可记录、可重建的意图；API 由 #17 落地。
- exporter 需要内容时由 host 注入 `EventLog`；默认导出只有结构、状态、token 与日志引用，内容安全留在边缘。
- 旧日志若缺少 `recorded_at`，迁移时补 0；此类历史 span 只失去耗时，不失去结构。

## Considered alternatives

- **独立 trace sink（自己的表/文件）**：时间与内容都现成，但引入第二真相源和跨 sink 一致性窗口，违背“日志是唯一来源”。
- **不记录时间，运行时内存采集耗时**：重建后的历史 span 没有耗时，审计跨天等待时只剩空洞；与本票“span 必须具备耗时”冲突。
- **每次 transport attempt 一个 span**：会重复计时与 usage，OTel 也明确要求自动重试折叠进一个逻辑 span。
- **一条 trace 覆盖整个会话树**：子会话可独立崩溃、恢复、归档；合并后一条 trace 永远不结束，也与 `TraceId = SessionId` 冲突。
- **把 OTel SDK/`gen_ai.*` 作为核心模型**：GenAI 约定仍是 Development，且不覆盖 session/turn/step；核心只保留原生类型，映射留在 exporter。
- **trace 落 SQLite 派生表**：查询更快，但多一份可重建的数据和迁移负担；v1 直接折叠日志，必要时由 exporter 物化。
