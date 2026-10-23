# MCP 外部工具 adapter 与 ACP 边界

> 来源：[#20](https://github.com/zibyn/asterpath/issues/20)，2026-10-22 已定。本文件定义 MCP 工具 adapter 的协议版本、传输、生命周期、adapter 契约、字段与错误映射、失败矩阵，以及 ACP 的边界。
>
> 对齐：[#2 模块契约](./module-contracts.md)、[#7 工具契约与执行管线](./tools.md)、[#16 插件清单与加载](./plugins.md)、[#11 规范协议模型](./protocol.md)。
>
> 相关 ADR：[0018 MCP adapter 无会话、只做快照；ACP 不属于工具层](../adr/0018-mcp-adapter-and-acp-scope.md)。
>
> **2026-10-22 #32 范围调整 / 2026-10-23 M5 范围**：本文件的设计保留，实现归 **M5（post-v1，已交付）**；M5 只实现 MCP 传输；不属于 v1 交付，也不进 M0–M4 验收。
>
> 本票对 #7 的修订（已同步到 tools.md）：
>
> - `ToolError` 增加 `NotFound` 变体：运行期目标消失（MCP server 报 unknown tool）需要它；`ToolErrorKind` 不变。
> - 截断以 `Part` 为单位：文本 Part 可在 UTF-8 边界截断，非文本 Part 不拆分。
>
> 本票对 #16 的修订（已同步到 plugins.md）：
>
> - MCP 只支持 `2026-07-28`；`server/discover` 取代 `initialize`；不自动重连/重启；ACP 不实现。

## 1. 决策摘要

- **只支持 MCP `2026-07-28` 无状态修订**。没有 `initialize`、没有会话、没有服务端反向请求；`2025-11-25` 及更早的 server 加载失败。
- **传输只有 stdio 与 Streamable HTTP**（POST-only，JSON 或请求级 SSE）。不做旧 HTTP+SSE、WebSocket、GET 监听流、`Last-Event-ID` 续传、OAuth。
- **启动期一次性发现**：`server/discover` 协商版本与能力，`tools/list`（跟随 cursor）发现工具，原子注册为 `Provenance::External`。运行期不增删工具。
- **不自动重连/重启**：stdio server 死亡后该插件在本进程内不可用；HTTP 每次调用独立。恢复靠重启宿主。
- **结果与错误映射不新增事件类型**：`Part` 与 `ToolErrorKind` 保持 #7/#11 的形状；只为运行期 `NotFound` 增加一个 `ToolError` 变体。
- **ACP 不在工具层**：它是宿主/会话接口，v1 不实现；`PluginProtocol::Acp` 保持“识别但不支持”。
- **adapter 在 `asterpath-plugin-host` 内**；`Provenance` 只携带 `TransportKind::{Stdio, Http}`，不含端点或凭据。

## 2. 范围与术语

| 词 | 含义 |
| --- | --- |
| MCP server | 通过 MCP 协议暴露工具的外部进程（stdio）或 HTTP 端点；在框架内以 `Provenance::External` 出现 |
| 无状态请求 | `2026-07-28` 的请求单位：自带协议版本与客户端能力，不依赖会话 |
| 服务端发现（Server Discovery） | `server/discover` 请求；协商版本与能力，不建立会话 |
| 工具快照（Tool Snapshot） | 插件加载时发现并原子注册的工具集合；进程内不随服务端变化 |
| 传输类别（Transport Kind） | `Provenance::External` 携带的 `Stdio \| Http` 标签；不含端点、命令或凭据 |

边界：本票只定 MCP → `Tool` 的映射与连接生命周期。清单形状、命名空间、加载失败隔离归 [#16](./plugins.md)；审批、截断与恢复归 [#7](./tools.md)；crate 位置归 [#15](./packaging.md)。ACP 只做边界判定（§9）。

## 3. 协议版本与生命周期

### 3.1 版本

- adapter 支持集：`MCP_PROTOCOL_VERSIONS = ["2026-07-28"]`。
- manifest 的 `protocol_versions` 与 adapter 支持集求交；交集为空 → `Failed{Handshake}`。
- 精确字符串匹配，不做 semver 范围、不降级、不回退旧握手。
- `2026-07-28` 没有 `initialize`、`notifications/initialized`、session id、`Last-Event-ID`，也没有服务端发起的 JSON-RPC 请求。
- 每个请求（包括 `server/discover`）在 `_meta` 携带：
  - `io.modelcontextprotocol/protocolVersion`
  - `io.modelcontextprotocol/clientCapabilities`（v1 恒为 `{}`；不声明 elicitation）
  - `io.modelcontextprotocol/clientInfo`（`{ "name": "asterpath", "version": ... }`，仅诊断）

### 3.2 启动发现

```text
manifest 校验 (#16)
  -> 版本交集检查（空 = Failed{Handshake}）
  -> 启动/连接传输
  -> server/discover（以候选最高版本）
       -32022 -> 与 data.supported 求交，重试一次
       -> 协商版本在 supportedVersions 内 + capabilities.tools 存在
  -> tools/list（跟随 nextCursor；重复 cursor 或超过 100 页 = 失败）
  -> 合成名称、冲突校验、原子注册 (#16)
  -> Loaded { tools } / Failed { stage }
```

- `server/discover` 失败、返回 `-32601`、或没有公共版本 → `Failed{Handshake}`。
- 服务端未声明 `tools` 能力 → `Failed{Handshake}`。
- `tools/list` 的 `resultType` 缺失视为 `complete`；其他值 → `Failed{Discovery}`。
- `ttlMs`、`cacheScope`、`serverInfo`、`instructions` 忽略；v1 不把 server instructions 注入模型。
- 零工具 = `Loaded { tools: [] }`，不是失败。

### 3.3 连接形态

- **stdio**：一行一个 JSON-RPC 消息，消息内不得含换行；stdout 只含 MCP 消息；stderr 排空后丢弃（不进日志，避免泄密）。关停：关闭 stdin → 等 `kill_grace` → SIGTERM → SIGKILL；平台支持时杀进程树。
- **Streamable HTTP**：每个消息一次 POST；请求头 `Content-Type: application/json`、`Accept: application/json, text/event-stream`、`MCP-Protocol-Version`、`Mcp-Method`；`tools/call` 另带 `Mcp-Name`（原始工具名）。响应按 `Content-Type` 解析：JSON 直接读；SSE 读到匹配 id 的响应为止，通知忽略。静态认证 `headers` 来自 manifest，`Debug` 脱敏。
- **并发**：stdio 连接按 JSON-RPC id 多路复用；HTTP 调用彼此独立。
- **不打开** `subscriptions/listen`，因此收不到 `tools/list_changed`。

## 4. adapter 契约

```rust
pub trait PluginAdapter: Send + Sync + 'static {
    fn handshake(&mut self) -> BoxFuture<'_, Result<AdapterHandshake, AdapterError>>;
    fn discover(&mut self) -> BoxFuture<'_, Result<Vec<AdapterTool>, AdapterError>>;
    fn call(
        &self,
        original_name: &str,
        args: JsonString,
        ctx: &ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
    fn shutdown(&mut self) -> BoxFuture<'_, ()>;
}

pub struct AdapterHandshake {
    pub protocol_version: String,
}

pub struct AdapterTool {
    pub original_name: String,
    pub description: String,          // 缺失时为 ""
    pub schema: JsonSchema,
    pub replay: ReplaySafety,
    pub side_effect: SideEffect,
    pub parallelism: Parallelism,     // MCP 一律 Parallel
}

#[non_exhaustive]
pub enum AdapterError {
    Spawn { message: String },
    Handshake { message: String },
    Discovery { message: String },
    Transport { message: String },
}
```

- adapter 只暴露原始工具名；`<plugin_id>__<sanitized>` 合成、`__` 校验、碰撞与原子注册由 plugin host 按 [#16](./plugins.md) §6 执行。
- host 为每个工具构造 `ToolDescriptor`：`provenance = External { transport }`、`timeout = manifest.timeouts.call`（缺省 120s）、`max_output_bytes = None`、`parallelism = Parallel`。
- `call` 收到的是合成名对应的原始名；`ToolCallRequested` 记录合成名，恢复与追踪都以合成名为准。
- `shutdown` 幂等；关停时所有未完成调用以 `Failed{Transport}` 结束。
- adapter 不写日志、不写 `ToolResult`；错误信息不得包含 URL、headers 或 env 值。

## 5. 工具发现与描述符映射

| MCP | Asterpath | 规则 |
| --- | --- | --- |
| `name` | 合成名 | MCP 字符集 `[A-Za-z0-9_.-]`；`.` sanitize 为 `_`；合成名 1–64，碰撞/超长 = 整插件失败 |
| `description` | `ToolDescriptor.description` | 缺失 → 空串 |
| `inputSchema` | `ToolDescriptor.schema` | 根必须是 object；原样保存；框架不校验参数，server 拒绝 → `InvalidArgs` |
| `annotations.readOnlyHint` | `side_effect` / `replay` | `true` → `ReadOnly` + `Safe`；否则 `Mutating` + `Unsafe` |
| `annotations.destructiveHint` / `idempotentHint` / `openWorldHint` | 忽略 | 只是 hint，不参与 |
| `title` / `icons` / `outputSchema` / `_meta` | 忽略 | `outputSchema` 不校验 `structuredContent` |
| `ttlMs` / `cacheScope` | 忽略 | 快照只取一次 |
| `nextCursor` | 分页 | 跟随到缺失；重复 cursor 或超过 100 页 = `Failed{Discovery}` |

- `inputSchema` 根不是 object → 该插件 `Failed{Discovery}`，不注册任何工具。
- 注解是 server 的未验证声明；来源仍是 `External`，审批策略不变。宿主不信任某个 server 时，用包装工具或自定义 `ApprovalPolicy` 覆盖。
- `readOnlyHint = true` 推出的 `Safe` 是重放承诺：恢复时可能重跑该调用。server 谎报会导致重复副作用，这是 v1 的信任边界。

## 6. 调用与结果映射

### 6.1 请求

- `tools/call` params：`name`（原始名）、`arguments`（JSON 对象）。
- `ToolArgs` 的原始文本是权威值；adapter 用 `RawValue` 嵌入请求，避免重新序列化改变字节。
- `arguments` 不是 JSON 对象 → `InvalidArgs`，不发送请求。
- `_meta` 同 §3.1；不发送 `progressToken`（v1 不使用 progress）。
- 审批、意图记录、取消 token、超时都由 [#7](./tools.md) 管线负责；adapter 只观察 `CancelToken`。

### 6.2 结果

| MCP content | Asterpath |
| --- | --- |
| `text` | `Part::Text` |
| `image`（`data` + `mimeType`） | `Part::Image { media_type, source: Base64 }` |
| `audio` | `Part::Text` 占位：`[audio content: <mimeType>]` |
| `resource_link` | `Part::Text`：名称、URI 与 mime |
| `resource` text | `Part::Text` |
| `resource` blob 且图片 mime | `Part::Image` |
| `resource` blob 其他 | `Part::Text` 占位 |
| 未知类型 | `Part::Text` 占位：`[unsupported MCP content: <type>]` |
| `structuredContent` | 追加一条 `Part::Text`（紧凑 JSON 文本） |
| 块 `annotations` / `_meta` | 忽略 |

- `resultType` 缺失或 `"complete"` 才继续；`"input_required"`（MRTR）或未知值 → `Failed{Internal}`，v1 不支持多轮工具请求。
- `isError: true` → `Failed{Internal}`；text 块按顺序用换行拼接进 `error.message`，没有 text 块时用固定文本。`output` 为 `None`（#7）。拼接后的 message 同样受单次输出上限约束。
- 空 `content` 且无 `structuredContent` → `Completed`，输出为空。
- `structuredContent` 是任意 JSON，按紧凑文本序列化；它不是 provider raw，允许序列化。
- 截断：adapter 不预截断；[#7](./tools.md) 管线按上限处理。文本 Part 可在 UTF-8 边界截断；非文本 Part 不拆分，整块放不下时丢弃并追加一条文本提示。

## 7. 错误映射与失败矩阵

### 7.1 调用期

| 条件 | 结果 |
| --- | --- |
| 连接/EOF/HTTP 非 2xx/JSON 解析失败/SSE 提前关闭 | `Failed{Transport}` |
| JSON-RPC `-32602`，server 明确报 unknown tool | `Failed{NotFound}`（`ToolError::NotFound`） |
| JSON-RPC `-32602`（其他） | `Failed{InvalidArgs}` |
| `-32020` / `-32021` / `-32022` / `-32600` / `-32601` / `-32603` / `-32700` | `Failed{Transport}` |
| `isError: true` | `Failed{Internal}` |
| `resultType: "input_required"` 或未知值 | `Failed{Internal}` |
| 管线超时 | `Failed{Timeout}` |
| 宽限内 server 自行取消 | `Failed{Cancelled}`；宽限到期由管线写 `Interrupted` |
| 响应 `content` 过大 | 按 §6.2 截断；完整输出不落日志 |

- 协议错误不自动重试（与 #7“工具失败不重试”一致）；MCP 规范建议的无状态重发由模型/宿主决定。
- 错误 `message` 只含 JSON-RPC 消息与状态码，不含 URL、headers 或 env。

### 7.2 加载与连接

| 条件 | `PluginStatus` |
| --- | --- |
| `protocol_versions` 与 adapter 支持集无交集 | `Failed{Handshake}` |
| spawn / HTTP 连接失败 | `Failed{Spawn}` |
| `server/discover` 缺失、返回 `-32601`、无公共版本 | `Failed{Handshake}` |
| 未声明 `tools` 能力 | `Failed{Handshake}` |
| `tools/list` 错误、分页循环、`resultType` 非 complete、`inputSchema` 根非 object | `Failed{Discovery}` |
| 名称非法/超长/碰撞/sanitize 后为空 | `Failed{Registration}`（#16） |
| 零工具 | `Loaded{tools: []}` |

### 7.3 连接死亡

- **stdio**：进程退出、EOF、协议错误 → 该 adapter 在本进程内标记 dead；所有在途调用 `Failed{Transport}`，后续调用立即 `Transport`，不再 I/O。恢复 = 重启宿主。
- **HTTP**：每次调用独立；网络错误、5xx、流中断只失败该调用，下一次调用重新 POST。收到 `-32022` / `-32021` 说明协商漂移，标记 adapter dead。
- 取消/超时导致 stdio server 被杀（#7）同样按 dead 处理；这是 v1 的明确代价。

## 8. 取消、超时与恢复

- **stdio 取消**：发 `notifications/cancelled { requestId, reason }`；等待 `kill_grace`；仍未返回则关闭连接、SIGTERM/SIGKILL。管线在宽限到期写 `Interrupted`；超时写 `Timeout`。
- **HTTP 取消**：关闭/丢弃该请求的 SSE 流或 HTTP future；不发 `notifications/cancelled`。
- 超时固定取 manifest `timeouts.call`（缺省 120s）；`notifications/progress` 不重置 deadline（v1 不发送 `progressToken`）。
- **崩溃恢复**：
  - `ToolCallRequested` 已记录、无终态：按记录的 `replay` 处理（#3/#7）。
  - `Safe`（只读工具）→ 重新发现后重跑；新进程加载插件时重新 `server/discover` / `tools/list`。工具不在快照 → `Failed{NotFound}`；server 不可用 → `Transport`。
  - `Unsafe` → `Failed{Interrupted}`，不重跑。
  - 审批中的调用按 #7 重新挂起或使用已记录决定，不受 adapter 影响。

## 9. ACP 边界

**ACP 不是工具协议。** 主流框架都把 ACP 放在 agent loop 之外：

| 项目 | ACP 位置 | 工具怎么走 |
| --- | --- | --- |
| Pi | 内核无；第三方 `pi-acp` 是 ACP agent（server） | Pi 本地执行并上报 `tool_call`；`session/new.mcpServers` 未接入 |
| Codex CLI | 内核无；`agentclientprotocol/codex-acp` 包 Codex App Server | 客户端 MCP servers 转成 Codex `mcp_servers` |
| Claude Code | 上游无；`agentclientprotocol/claude-agent-acp` 包 Claude Agent SDK | SDK 执行工具并映射 `tool_call`；客户端 MCP 合并进 SDK |
| DeepSeek Harness | 一等 ACP server + `subagent-acp` 客户端 | MCP 独立挂载；ACP 从不传工具 |
| 生态 | Zed external agents；Rust SDK 支持 client / agent / proxy | `session/new.mcpServers` 交给 agent 自己连 |

- ACP 规范没有工具调用方法；`tool_call` 是 agent 上报给客户端的活动。MCP-over-ACP 只是 MCP 的传输。
- v1 **不实现** ACP：它要么是宿主侧客户端，要么是包在 `SessionHandle` 外的 ACP agent 前端，不是 `PluginAdapter`。
- `PluginProtocol::Acp` 保持清单识别值；加载时 `Failed{Manifest}`（#16）。将来支持作为独立 effort，不复活本票。

## 10. 默认值

| 项 | 默认 |
| --- | --- |
| MCP 版本 | 仅 `2026-07-28` |
| 传输 | stdio / Streamable HTTP |
| 调用超时 | manifest `call`，缺省 120s |
| 取消宽限 | `kill_grace` 5s（#7） |
| 并行性 | `Parallel`，受 step 级 8 路限流 |
| 无注解工具 | `Mutating` + `Unsafe` |
| 零工具插件 | `Loaded{tools: []}` |
| 重连 | 无 |
| 工具集更新 | 仅进程重启 |

## 11. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| M1 | stdio 启动、发现、调用 | `server/discover` + `tools/list`；工具以合成名注册；调用成功 |
| M2 | HTTP JSON 响应 | 无会话；每次 POST 独立；结果映射正确 |
| M3 | HTTP 请求级 SSE | 忽略通知，按 id 取最终响应 |
| M4 | 版本不匹配 | `-32022` 后与 `supported` 求交重试；无交集 → `Failed{Handshake}` |
| M5 | `tools/list` 分页 | 跟随 cursor；重复 cursor/超过 100 页 → `Failed{Discovery}` |
| M6 | 名称冲突/超长/`inputSchema` 根非 object | 整插件失败；其他插件不受影响（#16） |
| M7 | `isError: true` | `Failed{Internal}`；text 拼接进模型可见错误 |
| M8 | unknown tool 错误 | `Failed{NotFound}`，不重试 |
| M9 | 超时 | 发取消；宽限后杀 stdio server；`Failed{Timeout}`；插件 dead |
| M10 | stdio 调用中进程崩溃 | 在途调用 `Transport`；后续调用立即 `Transport` |
| M11 | HTTP 流中断 | 仅该调用 `Transport`；下一次调用可成功 |
| M12 | `readOnlyHint: true` 后崩溃恢复 | 记录 `Safe`；恢复重跑；不重复写意图 |
| M13 | 无注解工具崩溃恢复 | `Unsafe` → `Interrupted`，不重跑 |
| M14 | 内容映射 | text/image/audio/resource_link/resource/structuredContent 按表映射 |
| M15 | `input_required` | `Failed{Internal}`，不发送 MRTR 重试 |
| M16 | 审批与来源 | `External{Stdio\|Http}`；默认 Ask/Deny；错误与日志不含 URL/headers/env |
| M17 | `tools/list_changed` | 忽略；快照不变 |

## 12. 留给下游

- **#21**：已定。stdio server 在启动时经 `SandboxProvider` 约束，server 内部调用不再单独 confine；HTTP 没有本地进程，不在沙箱范围。见 [sandbox.md](sandbox.md)。
- **#15**：`TransportKind` 与 `PluginAdapter` 的最终 crate 位置；plugin-host 仍不进 v1 验收。
- **v1 明确不做**：`Mcp-Param-*` 头投影、OAuth 2.1、`subscriptions/listen`、MRTR、server `instructions` 注入、resources/prompts/completions、`outputSchema` 校验、自动重试/重连。
