# MCP adapter 无会话、只做快照；ACP 不属于工具层

**Status**: accepted (2026-10-22)
**来源**: [#20](https://github.com/zibyn/asterpath/issues/20)

## Context

[#7](https://github.com/zibyn/asterpath/issues/7) 定了 `Tool` 契约与执行管线，[#16](https://github.com/zibyn/asterpath/issues/16) 定了外部插件清单、命名空间与加载失败隔离，并把这些留给 #20：传输实现、MCP 版本常量、`server/discover` / `tools/list` / `tools/call`、取消、断连/重连与协议错误映射。MCP 的 `2026-07-28` 修订改成了无状态模型：没有 `initialize`、没有会话、没有服务端反向请求；每个请求自带协议版本与客户端能力，用 `server/discover` 协商能力。

同期主流框架对 ACP 的处理一致：Pi 内核没有 ACP（第三方 `pi-acp` 是 ACP agent），Codex 与 Claude Code 的 ACP 支持都是包在 App Server / Agent SDK 外的 agent 适配器，DeepSeek Harness 把 ACP 当宿主与子智能体接口。它们的工具仍走 MCP；`session/new.mcpServers` 是客户端把 MCP servers 交给 agent，不是 ACP 工具调用。

需要决定：支持哪些 MCP 版本与传输、连接死亡后怎么办、运行期工具集是否可变，以及 ACP 是否属于工具层。

## Decision

- **只支持 MCP `2026-07-28`。** `protocol_versions` 与 adapter 支持集求交，空集即 `Failed{Handshake}`；不做旧握手回退。
- **stdio + Streamable HTTP。** HTTP 是 POST-only，接受 JSON 或请求级 SSE；不做旧 HTTP+SSE、WebSocket、GET 流、`Last-Event-ID`、OAuth。
- **启动期发现、注册表快照。** 不打开 `subscriptions/listen`，忽略 `tools/list_changed`；运行期不增删工具。
- **不自动重连/重启。** stdio server 死亡 = 插件在本进程内不可用，后续调用 `Failed{Transport}`；HTTP 每次调用独立，失败只影响该调用。
- **注解只用于声明。** `readOnlyHint = true` → `ReadOnly` + `Safe`；其余 `Mutating` + `Unsafe`。来源仍是 `External`，审批策略不变。
- **结果与错误不新增事件类型。** `Part` 与 `ToolErrorKind` 沿用 #7/#11；只为运行期 `NotFound` 增加 `ToolError::NotFound` 变体。
- **ACP 出界。** 它是宿主/会话接口；v1 不实现，`PluginProtocol::Acp` 加载时失败。

## Consequences

- `2025-11-25` 及更早的 server 不可用；宿主必须升级 server 或自行桥接。
- stdio server 一次崩溃、一次超时取消会让插件下线直到进程重启；HTTP 没有这个代价。
- 只读工具的“重放安全”依赖 server 诚实声明；谎报会造成重复副作用，这是 v1 的信任边界。
- 运行期工具变化不可见；server 删除工具表现为 `NotFound`，新增工具要重启宿主。
- ACP 以后要做，是 agent 前端或宿主客户端，不复用 `PluginAdapter`。

## Considered alternatives

- **双时代（`2026-07-28` + `2025-11-25` 握手）**：兼容旧 server，但两套生命周期渗进 adapter 契约；plugin-host 不进 v1 验收，不值得。
- **自动重连 / 懒重启**：可用性更好，但需要运行期重新发现，工具集漂移会与在途调用、恢复语义纠缠；与 #16“没有热替换”冲突。
- **运行期刷新工具集**：需要注册表删除与并发语义，v1 不做。
- **把 ACP 映射为工具**：ACP 没有工具调用方法，映射会丢会话、权限与文件系统语义；主流框架都把它放在 loop 外。
- **为 MCP 增加 `Part` / `ToolErrorKind` 变体**：语义更细，但波及两个 provider adapter、日志 schema 与 #7；v1 用现有类型足够。
