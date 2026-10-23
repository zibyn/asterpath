# 会话目录返回目录项，待审批请求只在进程内

**Status**: accepted (2026-10-23)
**来源**: [#84](https://github.com/zibyn/asterpath/issues/84)
**修订**: [ADR-0023](0023-storage-holds-log-and-catalog-wake-by-fold.md) 的会话目录契约

## Context

宿主的侧边栏、刷新后恢复对话、重连后找回待审批项都要自己翻日志：`sessions()` 只返回 id，不分页；`ApprovalRequested` 只是 drive 流上的实时事件，新连上的客户端看不到。

参考实现（2026-10 读源码）：Codex `thread/list`、opencode `session.list`、pi `SessionManager.list` 都返回父会话、创建时间与最后活动时间；Codex 与 opencode 用游标分页。待审批请求三家都不持久化：Codex app-server 在 `thread/resume` 时把内存中未应答的请求重发给新连接，opencode 在进程内 `Map` 上提供列表接口。

## Decision

- **`sessions(query: SessionQuery) -> Vec<CatalogEntry>` 取代 `sessions()` 与 `children()`。** 查询含父会话过滤（全部 / 仅根 / 某会话的子会话）、游标与 `limit`。`parent()` 保留。
- **目录项（`CatalogEntry`）** 只有 `session`、`parent`、`created_at`、`last_activity`。`last_activity` 是该会话最后一条已提交日志的 `recorded_at`，只供展示，恢复与唤醒不读。标题、预览、状态归宿主；状态由宿主按需调 `facts()`。
- **按 `(created_at, session)` 降序**，游标是上一页最后一项的 `(created_at, session)`，类型化而非不透明字符串。`SessionId` 不按时间排序，不能单用 id 做游标。
- **不支持按 `last_activity` 排序。** 跨会话按活动排序需要每次 append 维护一列派生值，即 ADR-0023 刚删掉的那类派生索引，还会给写入热路径加一次 UPDATE。
- **待审批请求由 `ApprovalGate` 在内存中持有**，`ApprovalGate::pending()` 返回全部会话的请求，宿主按 `ApprovalRequest::session` 过滤。等待因任何原因结束（决定、取消、宿主丢弃 drive 流）即移除。进程重启后由恢复重新发出 `ApprovalRequested`，不需要持久化。

## Considered alternatives

- **把 `ApprovalRequested` 写进日志，由 facts 查询**：为一个 UI 问题改日志 schema，并把「询问」变成持久事实；拒绝。
- **不提供待审批查询，宿主自己缓存**：每个宿主都要重写同一份缓存；框架的 gate 本来就在进程内；拒绝。
- **框架不做会话列表，宿主经 `on_record` 自建**：父子关系与创建时间是目录本来就有的事实，宿主不应重建；拒绝。
- **目录项带 `state`**：状态是会话事实折叠的结果，放进目录要么每行折叠一次，要么物化派生索引；拒绝。
- **名为 `SessionSummary`**：与压缩「摘要」、「任务总结」撞名；改为 `CatalogEntry`。
