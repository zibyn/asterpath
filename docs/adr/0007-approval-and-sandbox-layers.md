# 审批是框架管线，隔离与内容安全是扩展模块责任

**Status**: accepted (2026-10-22)
**来源**: [#7](https://github.com/zibyn/asterpath/issues/7)
**修订**: 2026-10-22（[#26](https://github.com/zibyn/asterpath/issues/26)）— 「有审批端」由 `ApprovalGate` 的 RAII 注册门表达；门关闭时 `Ask` 立即 `Denied`，不发 `ApprovalRequested`、不挂起。2026-10-22（[#28](https://github.com/zibyn/asterpath/issues/28)）— 工具调用采用框架 `tool_call_id` + provider `provider_call_id` 双身份；已记录意图不重跑 pre-execute；每个调用由单一 supervisor 写终态。2026-10-23（ADR-0025）— pre-execute / post-execute waterfall 改为 `Hooks::before_tool` / `after_tool`，调用点与语义不变，见 [ADR-0025](0025-extension-points-are-typed-hooks.md)。

## Context

工具是模型与外部世界之间唯一的副作用入口，也是崩溃恢复的关键边界。[#2](https://github.com/zibyn/asterpath/issues/2) 把 `Provenance` 定为来源标注而非沙箱，并明确「真正的安全边界是进程边界」；[#3](https://github.com/zibyn/asterpath/issues/3) / [#6](https://github.com/zibyn/asterpath/issues/6) 让等待成为已记录事件，审批可以让一个 step 跨天保持 open。

需要决定：审批由谁执行、如何跨崩溃、默认允许还是默认拒绝，以及框架在工具安全上到底建模什么。答案必须让基础框架只承担机制，不替宿主决定隔离、内容安全或业务策略。

## Decision

- **审批是框架管线阶段。** `ApprovalPolicy::classify` 给出 `Allow | Ask | Deny`；`Ask` 通过 `ApprovalRequested` typed event 提示宿主，并通过 `SignalReceived`（name = `tool.approval.<tool_call_id>`）记录决定。等待由 `DurableExecutor` 承担，可跨越崩溃；恢复时无决定信号就重新挂起。
- **不新增审批事件。** 决定复用 `SignalReceived`；请求本身不落日志。这与 [ADR-0003](0003-single-session-stream-topology.md) 的「持久等待是 session 事件」一致。
- **框架只建模 `Provenance` 与 `SideEffect`。** `Provenance` 说明来源，`SideEffect` 是工具的只读/变更声明；二者供策略使用，框架不验证。隔离、内容安全、凭据、计价、保留、UI 由扩展模块/宿主负责；内核只提供 `Provenance`、`post-execute` 与 usage/gate 等 seam。
- **默认 fail closed。** 未安装自定义策略时：`InProcess → Allow`；`External → 有审批端则 Ask，没有则 Deny`。`External` 的 `ReplaySafety` 默认 `Unsafe`。
- **调用身份与终态由管线固定。** `tool_call_id` 是框架身份（dedup / 审批 / open intent），`provider_call_id` 只用于把意图对回模型响应；`ToolCallRequested` 一旦写入，effective args 即权威，恢复不重跑 pre-execute。每个调用只有一个 supervisor 写 `ToolResult`，真实结果优先，取消优先于超时。

## Consequences

- 外部插件在没有审批端时不可用，除非宿主显式放宽策略；这是有意的安全默认。
- 审批可以让 step 跨天 open；恢复后重新提示是正常路径，不是错误。
- 框架不提供任何内容安全保证：扩展模块或宿主不处理时，工具输出会原样进入本地日志与模型上下文。
- 隔离的可信度取决于 adapter/扩展模块的实现；框架无法检测谎报。
- 工具执行不与审批耦合：策略、UI、隔离实现都可以独立替换。

## Considered alternatives

- **把审批与隔离合成一个权限门**：实现简单，但把「能不能做」和「隔离到什么程度」混成一个维度，宿主无法分别替换；Codex/Claude Code 的经验也支持分层。
- **内核实现 OS 沙箱（Seatbelt / bwrap / seccomp）**：平台特定、重复 adapter 的工作，而且进程内工具根本无法沙箱；作为扩展模块/适配器提供。
- **新增 `ToolApprovalRequested` / `ToolApprovalDecided` 事件**：状态更显式，但与 `SignalReceived` 的持久等待重复，还要改 #3 的事件枚举；拒绝。
- **默认允许 external 工具**：方便，但失败方向错误；需要审批的调用不会被提示。拒绝。
