# 引擎是一个 crate，只有适配器 trait 与钩子公开，宿主显式驱动

**Status**: accepted (2026-10-23)；2026-10-24 整理：并入 ADR-0015 仍有效的打包原则；依赖角色去掉 host 见 [ADR-0048](0048-no-host-tier-workspace-and-config-are-facade-features.md)
**来源**: [#15](https://github.com/zibyn/asterpath/issues/15)、[#79](https://github.com/zibyn/asterpath/issues/79)
**取代**: [ADR-0001](0001-dependency-direction.md)、[ADR-0015](0015-packaging-and-stability.md)
**规范**: [packaging.md](../design/packaging.md)、`deps-allowlist.toml`

## Context

早期设计规定一模块一 crate、模块 crate 之间零依赖，所有组件经 trait 互取，以保证 agent loop、工具注册表、日志实现、provider 都可替换。实现后核对（2026-10-23）：agent runner、持久等待、子智能体调度、工具注册表、用量与追踪查询各只有 1 个生产实现，「第二个实现」全是测试替身，而替身存在正是因为组件被拆在不同 crate；零依赖规则带来多份重复的 append + 通知与扫描代码，agent 与 scheduler 的循环依赖靠 `Weak` 打破；嵌入者至少依赖 3 个 crate。参考框架里 agent 循环都不可替换：codex-rs 的 core 是一个大 crate，Pi 是三层。

## Decision

- **引擎是一个 crate**：agent、scheduler、持久等待、工具表、trace / usage 视图与默认 compactor 是 `asterpath` 的内部模块，facade 与引擎同 crate。
- **`asterpath-protocol` 是唯一的合约 crate**：值类型、id、日志信封、适配器 trait、钩子、clock、错误；不依赖任何 workspace 成员、不声明 feature。
- **公开的扩展面只有适配器 trait 与钩子**：`Provider`、`Tool`、`SessionStore`、`Compactor`、`ApprovalPolicy`、`SandboxProvider`、`Clock`、`Hooks`。agent 循环不可替换。只有一个实现的组件不做成 trait。
- **独立 crate 只为真实适配器或可选重依赖**（provider、存储后端、MCP、沙箱后端、技能、文件工具等）。
- **依赖规则按角色**（protocol / adapter / engine / app / tool），由 `cargo xtask check-deps` 对照 `deps-allowlist.toml` 强制。
- **稳定面 = facade + `asterpath-protocol` + 日志 schema**；适配器 crate 是 0.x 实验面。
- **没有后台 driver**：宿主显式调用 `SessionHandle::drive()`；同一会话单飞；丢弃流不等于取消。回合失败是 `TurnCommitted { reason }`，不是流错误。

crate 列表、feature 组织与参考 CLI 见 packaging.md。

## Consequences

- 嵌入者只依赖 `asterpath`；替换能力只经适配器 trait 与钩子。
- 内部重复（append 入口、扫描、测试替身、循环引用）随合并消失，深模块建在引擎内，内部用 `mod` 与 `pub(crate)` 维持局部性。
- 依赖内部类型的测试放在 crate 内；经 facade 的测试在 `asterpath/tests/`。
- 框架不藏后台任务；崩溃与宿主放弃走同一条恢复路径，验收可以用真 `SIGKILL`。
- 出现第二个真实实现时，再把对应组件提升为适配器 trait。

## Considered alternatives

- **保留一模块一 crate**：维持重复与替身，而没有嵌入者替换这些组件。
- **只合并 core 与 protocol**：不解决引擎内部的 trait 与循环。
- **为测试开 `#[doc(hidden)] pub mod internals`**：内部类型变成事实上的公开面。
- **facade 自带后台 driver**：框架开始 spawn 任务，恢复与取消语义分叉。
