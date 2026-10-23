# 内核只持有合约，依赖单向流动

**Status**: superseded by [ADR-0026](0026-engine-is-one-crate-only-adapter-traits-public.md) (2026-10-23)
**2026-10-23 ADR-0025 修订**: 内核不再包含模块宿主、服务注册表与类型化事件总线；扩展点是 protocol 中的 `Hooks` trait，见 [ADR-0025](0025-extension-points-are-typed-hooks.md)。

## Context

Asterpath 的核心目标是模块解耦：agent loop、工具注册表、会话日志实现、provider 都必须可替换。如果实现和接口放在同一个 crate，替换就变成修改内核；如果模块之间可以互相依赖，解耦只能靠口头约定。

## Decision

- 合约分两层：`asterpath-core`（模块宿主、类型化事件总线、EventLog/Storage/DurableExecutor 契约、ID、错误）与 `asterpath-protocol`（规范化模型、Provider/Tool/ToolRegistry/AgentRunner、会话事件、能力来源）。`protocol` 依赖 `core`，反向禁止。
- 所有具体实现都是模块 crate，只依赖合约 crate，模块之间零依赖。
- 只有组装根/门面可以依赖具体模块。
- CI 读 `cargo metadata --no-deps`，对照白名单校验依赖边；违反即失败。
- core/protocol 不带 feature flags；feature 只出现在边缘后端 crate，且不得改变公共契约类型。

## Consequences

- 任何跨模块类型都必须进入合约 crate，或变成 trait/事件；这是一次有意的设计动作，不是随手加依赖。
- 组装根是唯一知道具体模块类型的地方，嵌入者获得清晰的替换点。
- crate 数量与间接层增加；换来的是可检查的边界和可替换性。
- 把实现放回内核、或让模块互相依赖，都会被 CI 挡住。
- 日志信封是这条方向的直接结果：core 只定义不透明 payload 的 `NewEntry`/`LogEntry`，类型化解码在 protocol（[ADR-0020](0020-log-envelope-core-typed-decoding-protocol.md)）。

## Considered alternatives

- **单 crate + 模块纪律**：简单，但不可检查，边界会腐化。
- **字符串 key 的服务定位器**：运行时灵活，但丢失类型安全，错误推迟到运行期。
- **feature-gated 单体**：编译期裁剪，但模块会编译进彼此，替换仍需改内核。
