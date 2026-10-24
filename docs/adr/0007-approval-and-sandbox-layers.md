# 审批是框架管线，隔离与内容安全是扩展责任

**Status**: accepted (2026-10-22)；2026-10-24 整理：并入 ADR-0028 的待审批决定
**来源**: [#7](https://github.com/zibyn/asterpath/issues/7)、[#26](https://github.com/zibyn/asterpath/issues/26)、[#28](https://github.com/zibyn/asterpath/issues/28)、[#84](https://github.com/zibyn/asterpath/issues/84)
**规范**: [tools.md](../design/tools.md)

## Context

工具是模型与外部世界之间唯一的副作用入口，也是崩溃恢复的关键边界。需要决定：审批由谁执行、如何跨崩溃、默认允许还是拒绝，以及框架在工具安全上到底建模什么。基础框架只承担机制，不替宿主决定隔离、内容安全或业务策略。

## Decision

- **审批是工具管线的一个阶段**：`ApprovalPolicy` 对每次调用给出 `Allow | Ask | Deny`。`Ask` 向宿主发实时的 `ApprovalRequested`，宿主的决定作为日志记录持久化；等待可跨崩溃，恢复时没有决定就重新询问。
- **询问不是持久事实**：`ApprovalRequested` 不落日志；待审批列表由 `ApprovalGate` 在进程内持有（`pending()`），重启后由恢复重新发出。
- **没有审批端就 fail closed**：宿主经 `ApprovalGate` 声明自己能应答；没有注册时 `Ask` 立即按 `Deny` 收尾，不挂起。默认策略：`InProcess → Allow`；`External → 有审批端则 Ask，否则 Deny`。
- **框架只建模 `Provenance` 与 `SideEffect`**：前者说明来源，后者是工具的只读/变更声明，供策略使用，框架不验证。隔离、内容安全、凭据、计价、保留、UI 归宿主或扩展。
- **调用身份与终态由管线固定**：框架 `tool_call_id` 是身份（去重、审批、未闭合意图），provider 的 call id 只用于对回模型响应；意图写入后 effective args 即权威，恢复不重跑前置钩子；每个调用只有一个写者写结果。

## Consequences

- 外部工具在没有审批端时不可用，除非宿主显式放宽策略。
- 审批可以让 step 跨天 open；恢复后重新提示是正常路径。
- 框架不提供内容安全保证：宿主不处理时，工具输出原样进入本地日志与模型上下文。
- 策略、UI、隔离实现可以独立替换。

## Considered alternatives

- **审批与隔离合成一个权限门**：把「能不能做」和「隔离到什么程度」混成一个维度；Codex / Claude Code 都分层。
- **内核实现 OS 沙箱**：平台特定，且进程内工具无法沙箱；沙箱是 spawn 点接缝（[ADR-0019](0019-sandbox-seam-and-policy.md)）。
- **默认允许 external 工具**：失败方向错误。
- **把 `ApprovalRequested` 写进日志**：为一个 UI 问题把「询问」变成持久事实。
