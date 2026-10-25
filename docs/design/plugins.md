# MCP 服务配置与工具来源

> 来源：[#16](https://github.com/zibyn/asterpath/issues/16)、[#80](https://github.com/zibyn/asterpath/issues/80)。外部扩展只有 MCP 服务配置，`asterpath-mcp` 是一个工具来源。本文件定义配置形状、加载规则、工具命名空间与冲突策略、版本协商与失败隔离。文件名保留以免断链。
>
> 对齐：[模块契约](./module-contracts.md)、[事件日志](./event-log-recovery.md)、[工具契约](./tools.md)、[MCP adapter](./mcp.md)、[沙箱](./sandbox.md)、[打包](./packaging.md)。
>
> 相关 ADR：[0027 MCP 是工具来源](../adr/0027-mcp-is-a-tool-source-not-a-plugin-host.md)、[0019 沙箱接缝](../adr/0019-sandbox-seam-and-policy.md)。

## 1. 决策摘要

- **进程内模块与 MCP 服务分开**：进程内模块随应用编译，由组装根显式选择；只有 MCP 服务有配置。
- **配置是嵌入者提供的数据，框架只定义形状**：`McpServerConfig` 在 `asterpath-mcp`；框架不读配置文件、不做发现、不展开环境变量、不持久化配置。
- **工具来源，不是插件宿主**：`McpServers::load` 返回已加载的工具；嵌入者把 `tools()` 交给 engine builder。facade 不新增工具来源接口（ADR-0027）。
- **加载是启动期一次性动作**：不做动态发现、热替换、运行期安装/卸载；换服务 = 重启进程。
- **命名空间强制**：外部工具以 `<server_id>__<tool_name>` 暴露；`__` 保留；碰撞或超长 = 整个服务加载失败，绝不 last-write-wins。
- **版本协商是精确白名单**：`protocol_versions` 列出接受的协议版本；握手结果必须命中。
- **失败按服务隔离**：可选服务失败不阻止应用启动；`required: true` 的服务失败中止启动；没有部分能力降级。
- **秘密不进日志**：`env` 与 HTTP `headers` 在 `Debug` 中脱敏，配置永不进事件日志。
- **沙箱 fail closed**：stdio 进程启动前，provider 给出的强制级别不是 `Full` 即加载失败（§5、[sandbox.md](sandbox.md) §9）。

## 2. 术语

见 `GLOSSARY.md`「MCP 服务配置」「MCP 服务端」「工具命名空间」「必需插件」（对 MCP 服务同义）。

边界：

- 进程内模块（provider、storage、agent loop 等）没有配置；由组装根选择。
- MCP 服务只提供工具，不提供 provider、storage 等核心能力（#2）。
- 服务之间没有依赖关系；加载顺序不影响可用工具集。

## 3. 嵌入

```rust
let mcp = McpServers::load(configs, Some(Arc::new(LinuxSandbox::new())), policy).await?;
let engine = AsterpathBuilder::new().tools(mcp.tools()) /* … */ .build().await?;
// …运行…
engine.shutdown().await; mcp.shutdown().await; // 先引擎、后 MCP
```

- 依赖：`asterpath` + `asterpath-mcp`（受限策略再加 `asterpath-sandbox-linux` 或应用自己的 `SandboxProvider`）。
- `statuses()` 按输入顺序给出每个配置的结果，供应用展示。
- `shutdown(self)` 对每个服务优雅关闭（stdio：关 stdin → 等宽限 → SIGTERM → SIGKILL）；之后仍被引擎持有的工具调用返回 `Transport`。不调用 `shutdown` 时，所有工具 drop 后 stdio 进程仍会被 kill。

## 4. MCP 服务配置

```rust
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    pub id: ServerId,                    // [a-z0-9-]{1,32}
    pub transport: McpTransport,
    pub required: bool,                  // 缺省 false
    pub protocol_versions: Vec<String>,  // 非空；精确匹配，不做范围
    pub timeouts: McpTimeouts,           // 缺省值见下
}

#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum McpTransport {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,   // 追加到继承环境；同键覆盖
        cwd: Option<PathBuf>,            // 缺省为应用进程 cwd
    },
    Http {
        url: String,
        headers: BTreeMap<String, String>, // Debug 脱敏，不展开变量
    },
}

#[serde(deny_unknown_fields)]
pub struct McpTimeouts {
    pub startup: Option<Duration>,       // 字段 startup_ms；None -> 30s
    pub call: Option<Duration>,          // 字段 call_ms；None -> 120s
}
```

序列化规则：

- **未知字段一律拒绝**（顶层、`transport`、`timeouts`），包括旧插件清单的 `manifest_version`、`protocol`、`capabilities`、`name`、`version`。
- 未知 `kind`、缺失必填字段、非正超时都是解析错误。

示例：

```json
{
  "id": "weather",
  "required": false,
  "protocol_versions": ["2026-07-28"],
  "transport": {
    "kind": "stdio",
    "command": "weather-mcp",
    "args": ["--stdio"],
    "env": { "WEATHER_API_KEY": "<secret>" }
  },
  "timeouts": { "startup_ms": 30000, "call_ms": 120000 }
}
```

### 4.1 传输与环境

- `command` 直接交给操作系统执行，**不经过 shell**；`args` 原样传递。
- `env` 追加到应用进程的环境；同键覆盖。框架不做 `${VAR}` 展开。
- 秘密可以放在 `env` 或 HTTP `headers` 里；`Debug` 脱敏，错误信息只报字段名，不报值。
- 服务进程**继承应用进程的环境变量**；配置不声明沙箱档位，受限策略由应用的 `SandboxProvider` 在进程启动前施加，provider 可经 `ConfinedSpawn.env` 收窄环境（[sandbox.md](sandbox.md)）。

## 5. 加载规则

```text
配置校验 -> 沙箱（仅 stdio） -> 启动/连接 -> 握手与版本协商 -> 能力发现 -> 名称合成与冲突校验 -> 原子登记
```

- **配置校验**在启动任何进程之前完成：`id` 字符集、`protocol_versions` 非空且与 adapter 支持集有交集、超时为正。
- **重复 `id`**：加载集合里共享同一 `id` 的所有配置都失败（`Config`）；其中任一 `required` 则加载返回 `McpError`。
- **沙箱**：策略非 `FullAccess` 而没有 provider、provider 返回错误、或强制级别不是 `Full` → `Failed{Sandbox}`，进程不启动。`Enforcement::Partial` 暂时一律失败；`allow_partial` 落地后放开，并把强制级别透给应用。
- **握手、发现**由 adapter 完成（[mcp.md](mcp.md)）。
- **原子登记**：先校验该服务的全部合成名称，再一次性加入工具集；任何名称非法、超长或与已加载工具碰撞，该服务不贡献任何工具。
- 首个 `required` 失败即中止并返回 `McpError`；此前已加载的服务随之 drop（进程被 kill）。

## 6. 工具命名空间与冲突

- **进程内工具**：裸名 `[A-Za-z0-9_-]{1,64}`；`__` 保留。
- **外部工具**：`<server_id>__<sanitized_tool_name>`；sanitize 把 `[A-Za-z0-9_-]` 之外的字符替换为 `_`，不截断；合成名 1–64，超长或为空 = 该服务失败。
- 不同服务的 `id` 不同且不含 `_`，跨服务碰撞不可能；同一服务内两个原始名 sanitize 后相同 = 整个服务失败。
- `ToolCallRequested` 记录合成名；adapter 用原始名调用。

## 7. 版本兼容

- `protocol_versions` 是精确字符串白名单；adapter 协商结果必须命中，否则 `Failed{Handshake}`。不降级、不回退。
- MCP 只支持 `2026-07-28`（无状态修订），见 [mcp.md](mcp.md)。
- 配置形状与 `LOG_SCHEMA_VERSION` 无关；配置不进日志。

## 8. 升级、恢复与在途调用

- **没有热替换**：升级服务 = 重启应用进程；会话靠日志恢复。
- **工具消失**：新请求写 `ToolCallRequested`（`provenance = Unknown`，`replay = Unsafe`）并以 `Failed{NotFound}` 闭合；恢复遇 open intent 按记录的 `replay` / `provenance` 处理（#3）。
- **服务在运行中崩溃**：不自动重连（[mcp.md](mcp.md) §7.3）；stdio 在途调用记 `Failed{Transport}`。

## 9. 失败隔离与状态报告

```rust
pub enum ServerStatus {
    Loaded { tools: Vec<ToolName> },
    Failed { stage: LoadStage, message: String },
}

pub enum LoadStage { Config, Sandbox, Spawn, Handshake, Discovery, Registration }

pub struct McpError { pub server: ServerId, pub stage: LoadStage, pub message: String }
```

- `required: false` 的失败只记状态；`required: true` 的失败使 `load` 返回 `McpError`。
- 失败服务不贡献任何工具；`message` 不含秘密值。不新增内核事件。

## 10. 出界

- 动态发现、运行期安装/卸载、热替换。
- WASM / dylib 插件、registry、OAuth 与凭据管理。
- MCP 之外的协议（ACP 若实现，是应用 / 会话接口，不复用本工具来源，见 [mcp.md](mcp.md) §9）。
- 部分工具登记；实现指纹恢复校验。

## 11. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| P1 | 两个配置使用同一 `id` | 两个都 `Failed{Config}`；任一 `required` 则 `McpError`；不加载任何工具 |
| P2 | 同一服务两个工具 sanitize 后同名，或合成名超 64 | 该服务失败，其他服务不受影响 |
| P3 | 握手版本不在 `protocol_versions` | `Failed{Handshake}`；required 返回 `McpError` |
| P4 | `required` 服务启动失败 | `McpError`；错误信息不含秘密值 |
| P5 | 一个服务提供多个工具 | 以 `<id>__<name>` 暴露；来源 `Provenance::External` |
| P6 | 重启后工具被删除，模型再次请求 | `ToolCallRequested` + `Failed{NotFound}`，不执行 |
| P7 | 配置含未知字段（含旧 `manifest_version` / `protocol` / `capabilities`） | 解析失败 |
| P9 | `env` / `headers` 含秘密 | `Debug` 不含秘密值；配置不进日志、trace、usage |
| P10 | provider 返回 `Partial` | `Failed{Sandbox}`，进程未启动 |
| P11 | `shutdown()` | stdio 进程已退出，即使引擎仍持有工具 |
