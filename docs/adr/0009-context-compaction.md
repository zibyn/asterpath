# 压缩是能力模块：只摘要、不是 step、窗口显式配置

**Status**: retired（2026-10-24 ADR 整理）——压缩的阈值、保留范围、估算方式与触发规则是可调策略，不是架构决策。

- 仍有效的决定：「压缩只追加、只改投影、不是 step」并入 [ADR-0002](0002-log-topology-and-commit-semantics.md)；`Compactor` 是适配器 trait（[ADR-0026](0026-engine-is-one-crate-only-adapter-traits-public.md)）。
- 规范细节：[compaction.md](../design/compaction.md)。
- 原文：`git show 8b79295:docs/adr/0009-context-compaction.md`
