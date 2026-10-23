# 存储只存日志与会话目录，唤醒由会话事实折叠发现

**Status**: accepted (2026-10-23)
**来源**: [#73](https://github.com/zibyn/asterpath/issues/73)（越界设计审计候选 1）
**前置**: [ADR-0005](0005-storage-contract-and-sqlite.md)、[ADR-0016](0016-retention-and-archive-seam.md)、[ADR-0022](0022-facade-accepts-host-modules.md)
**修订**: ADR-0005 的派生索引、ADR-0016 的退役空闲判定、ADR-0022 的服务清单
**2026-10-23 ADR-0024 修订**: `is_wakeable` 不再含到期 timer（通用 timer 已删除），见 [ADR-0024](0024-durable-waits-only-approval-and-child.md)。

## Context

ADR-0005 让 SQLite 在 `append` 同事务内维护 `open_intents`、`timers`、`pending_inputs` 三张派生表，供 `Storage::wakeable` / `due_timers` 与 `SessionRetirement::retire` 读取。实现后核对：

- 三张表只被这三个方法读取；`due_timers` 无任何调用方，`wakeable` 只有验收 testdrive 使用，库与参考 CLI 均不调用。
- 同一套唤醒规则写了三份：sqlite `apply_derived` 等（约 160 行）、memory 后端的折叠（约 100 行）、契约套件的 oracle（约 60 行）。memory 后端用折叠实现 `wakeable`，已证明折叠足够。
- Pi Durable 由框架在 `resume()` 中发现未完成工作，宿主只决定何时调用；其 README 未描述索引表。

发现“哪些会话未收尾”需要日志语义（意图配对、步骤水位、steering 输入），属于框架机制；何时恢复、恢复哪些属于宿主策略。机制要留在框架内，但不需要物化在存储层。

## Decision

- **删除 `Storage` trait**（`wakeable`、`due_timers`、`STORAGE_SERVICE`）。后端契约只剩 `EventLog`、`SessionCatalog` 与可选的 `SessionRetirement`。
- **`SessionCatalog::sessions()`** 列出全部未退役会话（含子会话）。
- **唤醒发现归 facade**：`Asterpath::wakeable(now)` 对 `sessions()` 逐个做会话事实折叠，返回集合与原 `Storage::wakeable` 相同。“空闲”与“可唤醒”是会话事实上的判定，facade 与退役共用。
- **退役在事务内折叠**：`retire` 在同一 `BEGIN IMMEDIATE` 内折叠本会话与父会话日志判定空闲；CAS、墓碑与幂等语义不变。
- **SQLite 只维护 `sessions` 与 `retired_sessions`**；`migrate_v2` 删除三张派生表。日志 schema 版本不变。
- facade 注入入口由 `storage` 改为 `session_catalog`；`Asterpath::storage()` 由 `wakeable(now)` 取代。

## Consequences

- 唤醒规则只有一份，由会话事实（#64）实现；第三个存储后端只需能追加、扫描、列会话。
- 启动发现读取全部会话日志，成本随会话总量线性增长。会话量大时在 SQLite 模块内部加索引，不改接口。
- 退役读取两个会话的完整日志；退役是低频管理操作，可接受。
- G5 的唤醒断言从后端契约套件移到 facade 测试（双后端）；契约套件只验证 `sessions()`。

## Considered alternatives

- **保留派生表**：拒绝，三份实现、无生产读者。
- **删除 `wakeable`，宿主按会话 id 显式恢复**（Codex、Claude Code 的做法）：宿主将被迫重写意图配对规则；拒绝。
- **`wakeable` 留在每个后端、内部调用折叠**：每个后端仍要接线一次，且存储契约不收缩；拒绝。
