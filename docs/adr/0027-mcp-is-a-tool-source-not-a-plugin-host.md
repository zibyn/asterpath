# MCP 是工具来源，不是插件宿主

**Status**: accepted (2026-10-23)
**来源**: [#80](https://github.com/zibyn/asterpath/issues/80)（无 ADR 约束重审候选 6）
**取代**: [ADR-0013](0013-plugin-boundary.md)
**修订**: [ADR-0018](0018-mcp-adapter-and-acp-scope.md)、[ADR-0019](0019-sandbox-seam-and-policy.md)、[ADR-0026](0026-engine-is-one-crate-only-adapter-traits-public.md)
**前置**: [ADR-0026](0026-engine-is-one-crate-only-adapter-traits-public.md)

## Context

ADR-0013 把外部扩展建模为“插件”：清单（`PluginManifest`：manifest_version、protocol `Mcp | Acp`、protocol_versions、capabilities、transport、required）、通用 `PluginAdapter` 接缝、`PluginHost` 向 `ToolRegistry` 注册外部工具。实现后核对（2026-10-23）：

- 只支持 MCP 与工具能力；`Acp` 一律拒绝，`capabilities` 只能是 `Tools`；两个 adapter（stdio、http）都是 MCP。
- facade 的工具注册表私有，嵌入者要另建注册表再把 `list()` 交给 builder；依赖 7 个 crate；`Asterpath::shutdown` 不优雅关闭 MCP 进程（只在工具全部 drop 时 kill）；参考 CLI 未接入（#71）。
- 沙箱合约独立成零依赖 crate `asterpath-sandbox`，生产唯一调用点是 plugin-host 的进程启动。
- Codex（`[mcp_servers.<name>]`）与 Claude Code（`mcpServers`）只有 MCP 服务配置，没有插件抽象。

## Decision

- **`asterpath-plugin-host` 改名 `asterpath-mcp`**，是一个工具来源：`McpServers::load(configs, sandbox, policy) -> Result<McpServers, McpError>`，提供 `tools() -> Vec<Arc<dyn Tool>>`、`statuses()`、`async shutdown()`（优雅关闭；drop 时仍兜底 kill）。嵌入者把工具交给 builder，关停顺序由嵌入者负责（先引擎、后 MCP）。facade 不新增工具来源接口。
- **配置是 `McpServerConfig { id, transport, required, protocol_versions }`**，`deny_unknown_fields`；删除 manifest_version、protocol（`PluginProtocol`）、capabilities（`PluginCapability`）与只服务通用插件的加载阶段。adapter 类型为 crate 私有。
- **`SandboxProvider` 与沙箱类型移入 `asterpath-protocol`**（适配器 trait 归 protocol，ADR-0026）；删除 `asterpath-sandbox`。`asterpath-sandbox-linux` 保留为独立 crate、依赖 protocol。策略校验与 FullAccess 直通在 protocol；MCP 进程启动前遇非 `Full` 强制级别 fail closed。沙箱语义（ADR-0019：三档策略、宿主解析、审批与沙箱正交）不变。
- 外部工具命名空间（`<id>__<name>`）、replay / provenance 映射、启动期快照与不自动重连（ADR-0018）不变。

## Consequences

- 嵌入者接入 MCP：依赖 `asterpath` + `asterpath-mcp`（+ 需要时 `asterpath-sandbox-linux`），三行代码。
- 宿主自写的 shell 类工具可直接使用 protocol 中的 `SandboxProvider`。
- 合并后 crate 数为 13（`asterpath-plugin-host` 与 `asterpath-sandbox` 由 `asterpath-mcp` 取代）。
- ACP 若将来实现，是宿主 / 会话接口，不复用 MCP 工具来源。

## Considered alternatives

- **facade 新增 `ToolSource` trait 负责加载与关停**：只有 MCP 一个实现，是假接缝；拒绝。
- **保留插件清单与 `Acp` / `Capability` 占位**：无第二种协议或能力，占位字段只为报错；改为不含这些字段的 MCP 配置并拒绝未知字段；拒绝保留。
- **沙箱合约并入 `asterpath-mcp`**：宿主自写工具也需要沙箱，放进 MCP crate 会迫使其依赖 MCP；拒绝。
