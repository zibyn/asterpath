# 引擎是一个 crate，只有适配器 trait 公开

**Status**: accepted (2026-10-23)
**来源**: [#79](https://github.com/zibyn/asterpath/issues/79)（无 ADR 约束重审候选 2）
**取代**: [ADR-0001](0001-dependency-direction.md)
**修订**: [ADR-0009](0009-context-compaction.md)、[ADR-0011](0011-native-tracing.md)、[ADR-0012](0012-usage-accounting.md)、[ADR-0014](0014-subagent-scheduling-api.md)、[ADR-0015](0015-packaging-and-stability.md)
**前置**: [ADR-0025](0025-extension-points-are-typed-hooks.md)
**2026-10-23 重审候选 4 修订**: facade 只提供 `usage(session, scope)` 与 `trace(session)`，不提供 `trace_updates()`。
**2026-10-23 重审候选 5 修订**: 新增适配器同类 crate `asterpath-http`（依赖 protocol 与 reqwest，提供 `SseDecoder`、通用 HTTP 状态分类、retry-after、request-id），供两个 provider 与 plugin-host 共用；不属于稳定面；合并后 crate 数为 14。
**2026-10-23 ADR-0027 修订**: `asterpath-plugin-host` 与 `asterpath-sandbox` 由 `asterpath-mcp` 取代（`SandboxProvider` 移入 protocol），crate 数为 13，见 [ADR-0027](0027-mcp-is-a-tool-source-not-a-plugin-host.md)。

## Context

ADR-0001 / ADR-0015 规定一模块一 crate、模块 crate 之间零依赖，依赖方向 `core ← protocol ← modules ← app`，由 xtask 白名单强制；charter 要求 agent loop、工具注册表、会话日志实现、provider 均可替换。实现后核对（2026-10-23）：

- `AgentRunner`、`DurableExecutor`、`SubagentSpawner`、`ToolRegistry`、`UsageQuery`、`TraceQuery`、`TraceTap` 各只有 1 个生产实现；“第二个实现”全是测试替身，而替身存在正是因为这些组件被拆在不同 crate、只能经 trait 互取。`AgentRunner` 的替换路径只被一条测试使用，测试证明的是替换后取消失效。
- 零依赖规则带来重复与绕路：append + 通知手写 4 份、整段扫描 7 份、`DurableExecutor` 替身 8 份、agent ↔ scheduler 循环靠 `Weak` 的 `set_runner` 打破。
- 嵌入者至少依赖 3 个 crate（facade、protocol、core）。
- 参考框架里 agent 循环都不可替换：codex-rs 的 core 是一个大 crate；Pi 是 pi-ai / pi-agent-core / pi-coding-agent 三层。

## Decision

- **引擎是一个 crate**：agent、scheduler、runtime、tools、tracing、usage、compaction 并入 `asterpath`，作为内部模块；facade 与引擎同 crate。
- **core 并入 `asterpath-protocol`**：值类型、id、日志信封、适配器 trait、钩子、clock、错误。
- **公开 trait 只有适配器与钩子**：`Provider`、`Tool`、存储后端（`EventLog` + 会话目录）、`Compactor`、`ApprovalPolicy`、`SandboxProvider`、`Clock`、`Hooks`。上述 7 个单实现 trait 改为引擎内部具体类型；agent 循环不可替换，删除 `AsterpathConfig.agent`。
- **查询是 facade 方法**：`usage(session, scope)`、`trace(session)`、`trace_updates()` 返回值，不返回 trait 对象。
- **独立 crate 只为可选重依赖或真实适配器**：两个 provider、sqlite、plugin-host、sandbox、sandbox-linux、skills；外加 testing、acceptance、cli、mcp-fixture。
- **依赖规则按角色**：protocol 不依赖任何成员、不声明 feature；适配器 crate 只依赖 protocol 与白名单列出的同类；引擎依赖 protocol 及其再导出 / feature 提供的适配器。模块间零依赖规则删除。
- **测试**：依赖内部类型的测试放在 crate 内（`src/<mod>/tests/`，`#[cfg(test)]`）；经 facade 的测试在 `asterpath/tests/`。

## Consequences

- 稳定面 = facade（`asterpath`）+ `asterpath-protocol` + 日志 schema。
- 内部重复（append 入口、扫描、测试替身、循环引用）随合并消失；会话事实、工具管线、尝试策略等深模块建在引擎内。
- 引擎 crate 变大（约 7–8k 行源码），编译单元变粗；内部用 `mod` 与 `pub(crate)` 维持局部性。
- 嵌入者只依赖 `asterpath`；替换能力只经适配器 trait 与钩子。

## Considered alternatives

- **保留一模块一 crate**：维持现有重复与替身，且没有任何嵌入者替换这些组件；拒绝。
- **只合并 core 与 protocol**：不解决引擎内部的 trait 与循环；拒绝。
- **为测试开 `#[doc(hidden)] pub mod internals`**：把内部类型变成事实上的公开面；拒绝。
