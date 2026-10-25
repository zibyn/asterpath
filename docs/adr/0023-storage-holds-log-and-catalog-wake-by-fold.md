# 存储契约只有日志、会话目录与整会话退役；单写者；唤醒由引擎折叠发现

**Status**: accepted (2026-10-23)；2026-10-24 整理：并入 ADR-0005、ADR-0016、ADR-0028 的存储原则；部分扩展 by [ADR-0032](0032-sessions-carry-opaque-host-metadata.md)（目录项携带宿主元数据）；单进程独占写者部分取代 by [ADR-0036](0036-per-session-tree-ownership.md)；「不物化派生状态」曾由 [ADR-0041](0041-wake-discovery-caches-folds.md) 提议修订，已否决（ADR-0047），本文仍有效
**来源**: [#5](https://github.com/zibyn/asterpath/issues/5)、[#18](https://github.com/zibyn/asterpath/issues/18)、[#32](https://github.com/zibyn/asterpath/issues/32)、[#73](https://github.com/zibyn/asterpath/issues/73)、[#84](https://github.com/zibyn/asterpath/issues/84)
**规范**: [storage.md](../design/storage.md)、[retention.md](../design/retention.md)

## Context

日志语义已定（[ADR-0002](0002-log-topology-and-commit-semantics.md)）。剩下的问题：存储后端要实现多少、谁有资格写、哪些结构是真相源、「哪些会话未收尾」由谁发现、保留与删除归谁。早期设计让后端在 append 事务里维护三张派生表供唤醒与退役读取；实现后核对，三张表没有生产读者，同一套唤醒规则写了三份，而 memory 后端用折叠实现已经足够。

## Decision

- **后端只实现一个 `SessionStore` trait**：追加、扫描、frontier、会话目录（`sessions(query)`、`parent`）和可选的整会话退役（默认 `Unsupported`）。
- **单进程独占写者**：一个进程持有写者身份，第二个进程打开即失败；没有租约、ack 或调度队列。多进程 / 多 worker 只能作为新后端或新执行模型重新设计。
- **append 返回即已持久**：生产配置下掉电不丢已返回的记录（「模型可见即已记录」依赖它）。
- **存储契约不物化派生状态**：「空闲」「可唤醒」是会话事实，由引擎折叠日志得到，`Asterpath::wakeable()` 与退役共用同一判定。何时恢复、恢复哪些由宿主决定。
- **会话目录只放已有的事实**：父会话、创建时间、最后活动时间，供宿主展示；标题、状态等由宿主折叠或自建。
- **保留策略归宿主**：框架只提供安全的整会话退役（写墓碑、删日志、frontier CAS、要求空闲且无活子会话），不裁前缀、不级联删除、不内置导出/导入；导出用 `frontier` + `scan`。

SQLite 的 WAL / 同步级别、写者锁、group commit、DDL 与目录排序见 storage.md；退役细节见 retention.md。

## Consequences

- 第三个存储后端只需能追加、扫描、列会话。
- 启动发现读取全部会话日志，成本随会话总量线性增长；出现实测瓶颈时再给契约加索引。
- 退役读取本会话与父会话的完整日志；退役是低频管理操作。
- 墓碑永久保留，id 不复用、退役幂等。

## Considered alternatives

- **在后端维护派生表**：三份实现、无生产读者。
- **宿主按会话 id 显式恢复，框架不做唤醒发现**（Codex、Claude Code）：宿主被迫重写意图配对规则。
- **心跳租约 / 多 worker 队列**（Duroxide）：单写者下只是额外故障面。
- **扩展直接操作数据库删除**：绕过写者与父引用检查。
- **裁前缀 / 级联删除子树**：制造 seq 洞，或在崩溃时留下删了一半的会话树。
