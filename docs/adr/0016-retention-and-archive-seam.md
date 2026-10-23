# 只保留安全退役接缝，归档与恢复归宿主

**Status**: accepted (2026-10-22)
**来源**: [#18 日志保留/归档扩展的 seam](https://github.com/zibyn/asterpath/issues/18)
**前置**: [ADR-0004](0004-log-schema-version-and-upcasting.md)、[ADR-0005](0005-storage-contract-and-sqlite.md)、[ADR-0009](0009-context-compaction.md)、[ADR-0012](0012-usage-accounting.md)
**2026-10-23 ADR-0023 修订**: 退役的 `idle` 判定改为在退役事务内折叠本会话与父会话日志，不再读派生表，见 [ADR-0023](0023-storage-holds-log-and-catalog-wake-by-fold.md)。

## Context

[#5](../design/storage.md) 把日志定为 append-only、单写者、派生索引同事务维护，并明确保留/归档由扩展模块或宿主负责、内核不内置策略。[#9](../design/compaction.md) 的压缩只改变投影，不删记录。[#13](../design/usage.md) 承认删掉后代日志会让子树用量变小。

需要回答的是：内核到底该为“保留/归档”留多少接缝。三个事实约束了答案：

- 写者锁由存储模块独占，派生索引必须在 `append` 事务里维护，所以任何删除写操作只能经过存储模块；扩展 crate 不能依赖具体模块（模块间零依赖）。
- G3 要求 `seq` 在 session 内无洞，恢复从最后一个 `StepCommitted` 折叠前缀；裁掉日志前缀会同时破坏两者。
- G6 明确没有跨流原子性；级联删除会在崩溃时留下删了一半的会话树。

参考实现给出了分层线索：Pi 的会话是 JSONL 文件，删除就是删文件；Claude Code 用 `cleanupPeriodDays` 应用设置清理 transcript；Codex 的 `thread/archive` / `thread/delete` 在 app-server / thread-store 层，删除前检查外部引用并维护索引；DeepSeek Harness 的持久化合约里没有 delete / archive / export / retention。**保留策略都在系统/应用层，只有“安全删除”落在 store 层。**

## Decision

- **内核只留一个可选写接缝：`SessionRetirement::retire(session, expected_frontier)`**，由 `asterpath-storage-sqlite` 实现并提供，策略模块经 `require_optional` 接入。不扩展 `EventLog` / `Storage` 热路径合约，不允许扩展直接开库。
- **退役只允许整 session**：写墓碑、删日志、删全部派生行，单事务。要求 `idle`、frontier CAS、无活子会话；父会话存在时必须 `idle`。墓碑记录 id、父边、最后 `LogId` 与时间，永不自动清理。
- **删除幂等**：墓碑已存在且 frontier 相同 → 成功；不同 → `FrontierMoved`；没有 → `NotFound`。
- **导出不新增 API**：宿主用 `EventLog::frontier()` 取快照上界，再 `scan()` 到该 `log_id`。append 只会分配更大 id，因此前缀稳定。JSONL 只是宿主参考格式。
- **导入/恢复不在 v1**：它是备份恢复，不是保留；整库备份或未来的运维工具承担。
- **退役后一律不可见**：`open_session` / 用量 / 追踪返回 `NotFound`，`scan` 空流，`SessionCatalog` 只覆盖活会话。
- **`RetirementError` 独立于 `StorageError`**，避免管理变体污染所有后端都要实现的 append/scan 错误面。
- **v1 不新增 `asterpath-storage-admin` crate**，不提供 CLI 的 `session delete/cleanup` 命令；需要时是宿主功能。
- **不做 `VACUUM` / checkpoint**；删除释放页供复用，文件缩小是离线运维。

## Consequences

- 内核不会变成保留系统：没有策略、没有调度、没有归档文件管理，也没有导出/导入 API。
- 宿主仍然可以安全地实现“按时间清理”：导出用现有读能力，删除用退役接缝；参考 CLI 若要提供命令，消费的是同一个接缝。
- 墓碑让 id 不复用、重试幂等；代价是墓碑永久保留，v1 不提供清理。
- 退役会让子树用量与 trace 变小或消失；这是显式接受的语义，归档文件承担审计真相。
- 单会话恢复不在框架内；宿主需要时走整库备份，或未来单独设计一个带 `log_id` 重映射的运维工具。
- 长会话的上下文管理仍靠压缩，不靠删除；退役是生命周期操作，不是窗口管理。

## Considered alternatives

- **三件套 `StorageAdmin`（导出 / 退役 / 导入）**：功能完整，但导出与现有 `scan` 重复，导入引入保真重放、跨库撞号与 `log_id` 规则，把备份恢复写进了内核合约；参考实现都没有这层。拒绝，收敛为退役。
- **扩展 `EventLog` / `Storage`**：少一个 trait，但把运维面塞进所有后端都必须实现的热路径合约，且退役变成强制。
- **扩展模块直接操作 SQLite**：绕过写者锁与派生索引事务，破坏 G1/G7，并与模块零依赖规则冲突。
- **完全不做接缝，宿主停进程后直接删行**：最贴近“系统层面”，但删除会绕过派生索引与父引用检查，风险由宿主承担，且与 Codex thread-store 的做法相反。
- **允许裁前缀**：会制造 `seq` 洞并破坏检查点恢复；要支持就得引入基线/快照事件，超出 v1。
- **级联删除子树**：一次调用更省事，但 G6 没有跨流原子性，崩溃后留下部分删除的树；拒绝，改由调用方自底向上。
- **不写墓碑**：状态最少，但无法区分“从未存在”与“已退役”，退役重试也不幂等。
- **把导出做成内核 API**：能保证快照，但与 `frontier` + `scan` 重复；多一个 API 只增加稳定面。
- **v1 就发布 `asterpath-storage-admin`**：自动保留策略不是内核能力，v1 验收也不需要它；先固定退役接缝，策略实现留给宿主。
- **退役时 `VACUUM` 回收磁盘**：与 #5 的“框架不调 WAL/VACUUM”冲突，且代价与时机应由宿主决定。
