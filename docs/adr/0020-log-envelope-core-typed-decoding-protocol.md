# 日志信封归 core，类型化解码归 protocol

**Status**: merged（2026-10-24 ADR 整理）——原前提（protocol 依赖 core）已随 ADR-0026 消失；仍成立的理由是存储后端不随事件形状变化。

- 仍有效的决定：并入 [ADR-0002](0002-log-topology-and-commit-semantics.md)。
- 规范细节：[storage.md](../design/storage.md)。
- 原文：`git show 8b79295:docs/adr/0020-log-envelope-core-typed-decoding-protocol.md`
