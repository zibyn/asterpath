# 存储用单 SQLite 文件、全同步 WAL 与进程独占写者

**Status**: retired（2026-10-24 ADR 整理）——SQLite 的同步级别、写者锁、事务与 group commit 是一个适配器的实现选择，且已多次修订。

- 仍有效的决定：单进程单写者与「append 返回即持久」并入 [ADR-0023](0023-storage-holds-log-and-catalog-wake-by-fold.md)。
- 规范细节：[storage.md](../design/storage.md)。
- 原文：`git show 8b79295:docs/adr/0005-storage-contract-and-sqlite.md`
