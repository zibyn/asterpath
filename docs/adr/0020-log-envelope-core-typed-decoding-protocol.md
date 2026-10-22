# 日志信封归 core，类型化解码归 protocol

**Status**: accepted (2026-10-22)
**来源**: [#25 统一 EventLog/LogEntry 契约与 append 签名](https://github.com/zibyn/asterpath/issues/25)
**前置**: [ADR-0001](0001-dependency-direction.md)、[ADR-0004](0004-log-schema-version-and-upcasting.md)、[ADR-0005](0005-storage-contract-and-sqlite.md)

## Context

同一个日志记录在三份设计里长出了三种形状：#2 的 `append(LogEntry)` 配 `{ kind, schema_version, payload }`；#3 的信封把 `payload` 写成类型化的 `SessionEvent`；#5 的 `NewEntry` / `LogEntry` 又把 `payload` 当不透明 JSON。

`protocol` 依赖 `core`，反向禁止（ADR-0001）。因此 `SessionEvent` 不可能出现在 core 的 `EventLog` trait 上，类型化信封也无法实现；而 #2 的 `append(LogEntry)` 又和存储分配 `log_id` / `seq` 的事实冲突。三种形状不能同时存在，实现智能体无法写出一个 trait。

## Decision

- **core 只定义两个信封形状**：`NewEntry`（调用方提交 `session` / `schema_version` / `dedup_key` / `payload`）与 `LogEntry`（存储再补 `log_id` / `seq` / `recorded_at`）。`EventLog::append` 收 `NewEntry`，`scan` 返回 `LogEntry`。
- **`payload` 是 `serde_json::Value`**，core 与存储层都不解释事件语义。信封不设 `kind` 字段；事件类型由存储在 `append` 时从 payload 顶层键派生，只用于幂等判别（ADR-0005）。
- **protocol 拥有类型化视图**：`decode_entry(&LogEntry) -> Result<SessionEvent, LogDecodeError>`，先按 `schema_version` 逐级 upcast（ADR-0004），再反序列化。未知变体、缺失必填字段或无法 upcast 都是 `LogDecodeError` 硬错误；不新增包装类型。
- `event-log-recovery.md`、`storage.md`、`module-contracts.md` 只使用这一套信封定义；#2 的第三种形状删除。

## Consequences

- core 保持对 protocol 零依赖；存储只负责信封、序号与派生索引，不背事件语义。
- 每个需要事件语义的消费者付一次解码成本；v1 不做解码缓存（ADR-0004）。
- 信封字段是 Tier 1 公开契约（ADR-0015），改动信封形状是破坏性变更；payload 内部形状仍由 `LOG_SCHEMA_VERSION` 管。
- 事件类型不落信封，派生依赖 payload 顶层键的稳定性；变体改名受 ADR-0004 的持久判别式规则约束。
- 日志仍是 JSON，按字段查询要走 SQLite JSON 函数或全扫（ADR-0005）。

## Considered alternatives

- **core 信封直接带 `SessionEvent`**：类型最紧，但要求 `core` 依赖 `protocol`，违反 ADR-0001，且 `protocol` 又需要 core 的 ID 类型，会成环。
- **payload 用不透明 bytes**：存储完全不看内容，但事件类型派生要前移到 protocol，存储无法在 `append` 内完成幂等查重；改动更大，收益为零。
- **公开 `SessionRecord { entry, event }` 包装**：调用方手里已有 `LogEntry`，包装类型没有消费者，只是多一个需要维护的公开面。
- **保留三种形状、按 crate 各自解释**：实现时必然要选一个，文档等于没定。
