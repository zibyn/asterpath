# 会话目录返回目录项，待审批请求只在进程内

**Status**: merged（2026-10-24 ADR 整理）——会话目录项与待审批列表是 API 细节。

- 仍有效的决定：存储原则并入 [ADR-0023](0023-storage-holds-log-and-catalog-wake-by-fold.md)，待审批只在进程内并入 [ADR-0007](0007-approval-and-sandbox-layers.md)。
- 规范细节：[storage.md](../design/storage.md)、[tools.md](../design/tools.md)。
- 原文：`git show 8b79295:docs/adr/0028-catalog-entries-and-in-process-pending-approvals.md`
