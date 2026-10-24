# 日志 schema 用单调整数版本，只在读取时 upcast

**Status**: retired（2026-10-24 ADR 整理）——发布前日志 schema 不承担兼容（版本固定为 1、不写 upcaster），原文大半是「首发后才生效」的计划，不是当前决策。

- 仍有效的决定：持久判别式规则并入 [ADR-0002](0002-log-topology-and-commit-semantics.md)；首发时的兼容策略届时另立 ADR。
- 规范细节：[storage.md](../design/storage.md)。
- 原文：`git show 8b79295:docs/adr/0004-log-schema-version-and-upcasting.md`
