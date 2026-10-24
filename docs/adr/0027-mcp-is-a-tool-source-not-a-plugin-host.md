# MCP 是工具来源，不是插件宿主；ACP 不属于工具层

**Status**: accepted (2026-10-23)；2026-10-24 整理：并入 ADR-0018 的 ACP 范围决定
**来源**: [#20](https://github.com/zibyn/asterpath/issues/20)、[#80](https://github.com/zibyn/asterpath/issues/80)
**取代**: [ADR-0013](0013-plugin-boundary.md)
**规范**: [plugins.md](../design/plugins.md)、[mcp.md](../design/mcp.md)

## Context

早期设计把外部扩展建模为「插件」：清单（协议 `Mcp | Acp`、协议版本、能力集、传输）、通用 adapter 接缝、插件宿主向工具注册表注册。实现后核对（2026-10-23）：只支持 MCP 与工具能力，`Acp` 一律拒绝，能力只能是 `Tools`，两个 adapter 都是 MCP。Codex（`[mcp_servers.<name>]`）与 Claude Code（`mcpServers`）只有 MCP 服务配置，没有插件抽象。主流框架都把 ACP 放在 loop 外：Codex 与 Claude Code 的 ACP 支持是包在 App Server / Agent SDK 外的 agent 适配器，工具仍走 MCP。

## Decision

- **`asterpath-mcp` 是一个工具来源**：`McpServers::load(configs, sandbox, policy)` 产出 `Vec<Arc<dyn Tool>>`，嵌入者交给 builder，并负责关停顺序（先引擎、后 MCP）。facade 不新增工具来源接口。
- **配置只描述 MCP 服务**，拒绝未知字段；没有插件清单、协议或能力占位字段。
- **MCP 工具与其他工具同等**：走普通工具管线，来源标为 `External`，审批默认按 [ADR-0007](0007-approval-and-sandbox-layers.md)。
- **MCP 进程启动经 protocol 的 `SandboxProvider`**（[ADR-0019](0019-sandbox-seam-and-policy.md)）；沙箱合约不放在 MCP crate，宿主自写工具也能用。
- **ACP 是宿主 / 会话接口，不是工具来源**；将来实现时是 agent 前端或宿主客户端，不复用 MCP 工具来源。

支持的 MCP 版本与传输、命名空间、注解映射、发现与重连行为见 mcp.md。

## Consequences

- 嵌入者接入 MCP：依赖 `asterpath` + `asterpath-mcp`（需要时加沙箱后端）。
- 外部工具的语义完全由 `Tool` 契约承担，没有第二套扩展机制。

## Considered alternatives

- **facade 新增 `ToolSource` trait**：只有 MCP 一个实现，是假接缝。
- **保留插件清单与 `Acp` / `Capability` 占位**：占位字段只为报错。
- **沙箱合约并入 `asterpath-mcp`**：迫使宿主工具依赖 MCP。
- **把 ACP 映射为工具**：ACP 没有工具调用方法，映射会丢会话、权限与文件系统语义。
