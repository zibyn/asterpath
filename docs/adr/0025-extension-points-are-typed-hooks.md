# 扩展点是固定的类型化钩子，内核不再有模块宿主与事件总线

**Status**: accepted (2026-10-23)
**来源**: [#78](https://github.com/zibyn/asterpath/issues/78)（无 ADR 约束重审候选 1）
**取代**: [ADR-0022](0022-facade-accepts-host-modules.md)
**修订**: [ADR-0001](0001-dependency-direction.md) 的内核构成、[ADR-0006](0006-agent-loop-execution-model.md) 的 pre-step gate、[ADR-0007](0007-approval-and-sandbox-layers.md) 的 pre/post-execute 接缝、[ADR-0011](0011-native-tracing.md) 的实时导出、[ADR-0015](0015-packaging-and-stability.md) 的宿主模块口径
**2026-10-23 重审候选 4 修订**: tracing / usage 不提供专用推送接口；`on_record` 是唯一的提交后观察出口。

## Context

ADR-0001 按 DeepSeek Harness / Cordis 的思路，让内核提供模块宿主、服务注册表与类型化事件总线（emit / waterfall），所有能力经注册表互取、观察与拦截经总线。实现后核对（2026-10-23）：

- 9 个服务键只有 1 处生产 `require`（scheduler 取工具注册表）；facade 用具体 `Arc` 装配全部组件，注册表基本只写不读。
- 3 个 waterfall 拦截点（pre-step gate、pre-execute、post-execute）没有生产使用者；`RecordAppended` 唯一生产订阅者是 tracing；`UsageRecorded` 没有订阅者。
- 总线对嵌入者私有。嵌入者要做的脱敏、预算、实时观察只能复制 facade 约 170 行装配并分裂总线。ADR-0022 打算把整套注册表与服务清单开放并纳入 semver。
- Pi（`pi.on(...)`）、Claude Code SDK（hook 回调）都以固定的生命周期钩子集合作为扩展点，不提供服务定位器；循环本身不可替换。

## Decision

- **删除模块宿主、服务注册表、类型化事件总线与 `Module` trait**（core 的 `module.rs` / `event.rs` / `service.rs`、全部服务键、`ModuleCtx`、`Effect`）。facade 直接构造内部组件，关停按固定顺序收尾。
- **扩展点是 protocol 中的一个 `Hooks` trait**，方法均有默认实现，builder 可注册多个：
  - `before_step(&StepCtx) -> Gate`（异步）：`EndTurn` 以 `Stopped` 结束回合；错误使回合 `Failed`。
  - `before_tool(&ToolCallCtx, &mut ToolArgs) -> ToolGate`（异步）：改写参数或拒绝；在意图写入前调用，已记录意图恢复时不重跑。
  - `after_tool(&ToolCallCtx, &mut ToolResultView)`（异步）：改写输出与错误消息；在截断后、结果写入前调用。
  - `on_record(&CommittedRecord)`（同步、不可失败）：每条提交记录（所有会话，含子会话）提交后按会话内顺序调用，不影响循环。
- **组合**：按注册顺序依次调用；首个 `EndTurn` / `Deny` 短路，改写逐个传递；无环绕（`next`）。
- **上下文只带数据**：`StepCtx { session, turn, step_index }`、`ToolCallCtx { session, turn, step, tool_call_id, provider_call_id, tool, provenance, replay }`，两者提供异步 `usage()`（本会话自身与子树）。不提供服务定位器。
- **调用点与恢复语义与原 waterfall 完全一致**；panic 按该钩子错误处理，`on_record` panic 捕获后记录并继续。
- `Hooks` 与上下文类型在 protocol；facade 再导出 protocol 与 core 的 id 类型，嵌入者只依赖 `asterpath`。
- 引擎内部的 tracing / usage 由 append 入口直接调用，不经钩子；其推送接口另行决定。

## Consequences

- 扩展点可列举、可文档化；新增钩子点是给 trait 加带默认实现的方法，向后兼容。
- 脱敏（`after_tool`）、预算（`before_step` + `usage()`）、实时观察（`on_record`）经 facade 一步可用。
- 失去任意服务互取与环绕式中间件；内置组件不再是可替换模块，替换仍只经现有配置字段（provider、tools、compactor、存储后端、审批策略）。
- 钩子只在进程内；跨进程扩展仍走 MCP 工具。

## Considered alternatives

- **按 ADR-0022 开放注册表与总线**：通用但无人使用，且进入 semver 后难以收回；拒绝。
- **保留模块宿主仅作内部生命周期**：内部组件无依赖拓扑与回卷需求；拒绝。
- **每个钩子点一个 trait / 闭包注册**：前者一个扩展要多次注册，后者异步签名难写；拒绝。
- **保留环绕式中间件**：现有三个拦截点无使用者，参考框架的钩子都是顺序调用；拒绝。
