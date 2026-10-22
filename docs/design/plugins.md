# 插件清单、加载与版本兼容

> 来源：[#16](https://github.com/zibyn/asterpath/issues/16)，2026-10-22 已定。本文件定义外部插件的清单形状、加载规则、工具命名空间与冲突策略、版本协商、失败隔离，以及插件与进程内模块的边界。
>
> 对齐：[#2 模块契约](./module-contracts.md)、[#3 事件日志](./event-log-recovery.md)、[#7 工具契约](./tools.md)、[#20 外部工具协议 adapter](https://github.com/zibyn/asterpath/issues/20)、[#15 打包](https://github.com/zibyn/asterpath/issues/15)。
>
> 相关 ADR：[0013 外部插件用清单加载，进程内模块编译期组装](../adr/0013-plugin-boundary.md)。

## 1. 决策摘要

- **进程内模块与外部插件分开**：进程内模块随宿主编译，由组装根显式选择；没有清单、没有动态发现。只有外部插件（子进程 / MCP）有清单。
- **清单是 host 提供的数据，框架只定义形状**：`PluginManifest` 在 `asterpath-plugin-host`；框架不读清单文件、不做发现、不展开环境变量、不持久化清单。
- **加载是启动期一次性动作**：v1 不做动态发现、热替换、运行期安装/卸载；换插件 = 重启进程。
- **命名空间强制**：外部工具以 `<plugin_id>__<tool_name>` 暴露；`__` 保留给外部命名空间；碰撞或超长 = 整插件加载失败，绝不 last-write-wins。
- **版本协商是精确白名单**：`protocol_versions` 列出接受的协议版本；握手结果必须命中；插件自身 `version` 只用于诊断。
- **失败按插件隔离**：可选插件失败不阻止宿主启动；`required: true` 的插件失败中止启动；没有部分能力降级。
- **秘密不进日志**：manifest 的 `env` 等敏感字段 `Debug` 脱敏，manifest 永不进事件日志。
- **未知工具是正常终态**：模型请求不存在的工具时仍写意图，随后以 `ToolResult{Failed, NotFound}` 闭合；恢复按请求记录里的 `replay` 与 `provenance` 处理。

## 2. 范围与术语

| 词 | 含义 |
| --- | --- |
| 插件（Plugin） | 经子进程或 MCP 接入的外部扩展；对内核只表现为叶子能力 |
| 插件清单（Plugin Manifest） | host 提供的外部插件描述：标识、协议、传输、能力、超时、是否必需 |
| 插件实例（Plugin Instance） | 加载集合中一个 `PluginId` 对应的外部插件运行体；同一实现可有多个实例，但标识必须不同 |
| 插件宿主（Plugin Host） | 进程内模块 `asterpath-plugin-host`，负责启动/连接、握手、发现工具并注册为 `Provenance::External` |
| 工具命名空间（Tool Namespace） | 外部工具对模型暴露时的 `<PluginId>__<名称>` 前缀约定；`__` 保留 |

边界：

- 进程内模块（provider、storage、executor、agent loop 等）不是插件，没有 manifest；它们由组装根选择（§3.1）。
- 插件是叶子能力：v1 只提供工具，不提供 provider、storage、executor 等核心能力（#2）。
- 插件之间没有依赖关系；加载顺序不影响可用工具集。

## 3. 加载形态

### 3.1 进程内模块

- 随宿主编译，经 Cargo 依赖进入二进制；组装根显式构造并注册。
- "可选模块" = 独立 crate + 组装根的依赖/构造选择。
- contract crate 与 module crate 一律不带 feature（ADR-0001）；facade / 参考 CLI 可用 feature 裁剪，具体布局归 [#15](https://github.com/zibyn/asterpath/issues/15)。
- 没有 manifest，没有运行期发现，没有热替换。

### 3.2 外部插件

- host 构造一组 `PluginManifest`，交给 `asterpath-plugin-host`。
- 插件宿主启动或连接插件、完成握手与能力发现，把每个工具注册进 `ToolRegistry`，来源为 `Provenance::External { transport }`。
- 一个插件可以提供多个工具。
- 插件宿主自己不发现插件；清单从哪来（文件、代码、数据库）由 host 决定。

## 4. 插件清单

```rust
pub struct PluginManifest {                  // host 构造或反序列化；字段集受 manifest_version 约束
    pub manifest_version: u32,               // 只接受 1
    pub id: PluginId,                        // [a-z0-9-]{1,32}
    pub name: Option<String>,                // 缺省 = id
    pub version: Option<String>,             // 插件自身版本；仅诊断，不参与门禁
    pub required: bool,                      // 缺省 false
    pub protocol: PluginProtocol,            // Mcp | Acp | CustomStdio
    pub protocol_versions: Vec<String>,      // 非空；精确匹配，不做范围
    pub transport: PluginTransport,
    pub capabilities: Vec<PluginCapability>, // 缺省 [Tools]
    pub timeouts: PluginTimeouts,            // 缺省值见下
}

#[non_exhaustive]
pub enum PluginProtocol {
    Mcp,
    Acp,
    CustomStdio,
}

#[non_exhaustive]
pub enum PluginTransport {                   // serde 内部标签 kind
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,       // 追加到继承环境；同键覆盖
        cwd: Option<PathBuf>,                // 缺省为宿主进程 cwd
    },
    Http { url: String },                    // 认证头等字段归 #20，随 manifest_version 扩展
}

pub enum PluginCapability {
    Tools,                                   // v1 唯一支持的能力
}

pub struct PluginTimeouts {
    pub startup: Option<Duration>,           // None -> 30s
    pub call: Option<Duration>,              // None -> #7 external 默认 120s
}
```

序列化规则：

- `transport` 用内部标签 `kind`（`stdio` / `http`）；`protocol`、`capabilities` 用小写字符串。
- 超时在清单里用毫秒（`startup_ms` / `call_ms`），Rust 类型用 `Duration`；非正值是清单错误。
- 未知字段忽略（与 #4 一致）；未知 enum 值、缺失必填字段、未知 `manifest_version`、声明未实现的能力，都是清单错误。
- `version` 是自由字符串，不按 semver 校验，不参与任何兼容判断。
- `capabilities` 缺省 `["tools"]`；声明 `tools` 之外的能力 = 加载失败，不静默忽略。

示例：

```json
{
  "manifest_version": 1,
  "id": "weather",
  "name": "Weather MCP",
  "version": "0.3.1",
  "required": false,
  "protocol": "mcp",
  "protocol_versions": ["2025-06-18"],
  "transport": {
    "kind": "stdio",
    "command": "weather-mcp",
    "args": ["--stdio"],
    "env": { "WEATHER_API_KEY": "<secret>" }
  },
  "capabilities": ["tools"],
  "timeouts": { "startup_ms": 30000, "call_ms": 120000 }
}
```

### 4.1 传输与环境

- `command` 直接交给操作系统执行，**不经过 shell**；`args` 原样传递，不做变量替换。
- `env` 追加到宿主进程的环境；同键覆盖。框架不做 `${VAR}` 展开，host 在构造 manifest 前解析。
- 秘密可以放在 `env`（以及 #20 后续加入的认证字段）里；`Debug` 必须脱敏，错误信息只报字段名，不报值。
- 插件进程**继承宿主进程的环境变量**；需要最小权限的宿主应在自己的进程中先净化环境。manifest 不是沙箱，进程隔离与文件/网络边界由宿主或扩展模块负责（#21）。
- manifest 本身不进入事件日志、trace 或 usage；它只是启动期配置。

## 5. 加载规则

每个插件的加载阶段固定为：

```text
清单校验 -> 启动/连接（transport） -> 握手与版本协商（protocol） -> 能力发现 -> 名称合成与冲突校验 -> 原子注册
```

- **清单校验**在启动任何进程之前完成：`manifest_version`、`id` 字符集、`protocol_versions` 非空、能力受支持、超时为正。
- **重复 `id`**：加载集合里共享同一 `id` 的所有 manifest 都失败；其中任一 `required` 则宿主启动失败。绝不"先到先得"。
- **握手与版本协商**由协议 adapter 执行（#20）；`protocol_versions` 是宿主接受的精确白名单，adapter 的协商结果必须命中，否则该插件失败。
- **能力发现**（例如 MCP `tools/list`）由 adapter 完成；发现到的工具必须都能被命名空间规则合法暴露，否则整插件失败。
- **原子注册**：先校验该插件的全部合成名称，再一次性注册；任何一个名称非法、超长或碰撞，都不注册该插件的任何工具。
- 插件之间无依赖；宿主可以串行或并行加载，结果不依赖顺序。
- 加载失败不产生半截状态：没有工具、没有连接、没有注册残留。

## 6. 工具命名空间与冲突

### 6.1 名称规则

- **进程内工具**：裸名，字符集 `[A-Za-z0-9_-]`、长度 1–64；`__` 保留，出现在进程内工具名里是注册错误。
- **外部工具**：`<plugin_id>__<sanitized_tool_name>`；模型与 `ToolRegistry` 只看到合成名。
- **sanitize**：把 `[A-Za-z0-9_-]` 之外的每个字符替换为 `_`，保留大小写；不做截断。
- 合成名长度必须 1–64；超长或 sanitize 后为空 = 该插件加载失败。
- 合成名在 `ToolRegistry` 中全局唯一；重复注册是错误，不是覆盖。

### 6.2 冲突判定

- 不同插件的 `id` 不同，而 `id` 不含 `_`，因此合成名天然带不同前缀；跨插件碰撞不可能发生。
- 碰撞只可能发生在同一插件内部（两个原始工具名 sanitize 后相同）。此时整插件失败，不保留其中任何一个。
- 同一插件多实例允许，但必须使用不同 `id`；同 `id` 多实例已在 §5 拒绝。
- `spawn_agent` 等内置工具使用裸名，不会与外部工具的 `__` 命名空间相撞。

### 6.3 映射

插件宿主维护 `合成名 -> (plugin_id, 原始工具名)` 的映射：模型看到合成名，adapter 用原始名调用插件。`ToolCallRequested` 记录合成名，恢复与追踪都以合成名为准。

## 7. 版本兼容

### 7.1 合约 crate

- 整个 workspace 锁同一版本；`asterpath-core` / `asterpath-protocol` 在 major 内保证源码兼容。
- 1.0 之前 breaking 用 minor 版本，并写迁移说明；嵌入方用 caret。
- module crate 不承诺独立兼容：必须与合约 crate 同一 workspace 版本。没有 ABI 承诺。
- 公共稳定面最终由 [#15](https://github.com/zibyn/asterpath/issues/15) 收敛。

### 7.2 外部插件协议

- `protocol_versions` 是**精确字符串白名单**，不是 semver 范围；空列表是清单错误。
- adapter 握手（例如 MCP `initialize`）返回的协议版本必须命中白名单；否则该插件加载失败。
- 插件自身 `version` 只用于诊断展示，不参与兼容判断。
- 不降级、不禁用部分能力、不尝试旧协议回退。
- MCP 版本字符串与常量归 [#20](https://github.com/zibyn/asterpath/issues/20)；自定义 stdio 协议版本是 adapter 定义的 opaque 字符串，同样精确匹配。
- `PluginProtocol::Acp` 是清单能识别的值；v1 是否实现 ACP 由 [#20](https://github.com/zibyn/asterpath/issues/20) 决定。识别值不等于实现，未实现的协议值使插件加载失败。

### 7.3 Provider、model alias 与 raw

- Provider 只有进程内路径（#2）：没有 manifest、没有动态加载、没有运行期版本协商；兼容性由合约 crate semver + 同 workspace 版本保证。
- model alias 解析归 [#15](https://github.com/zibyn/asterpath/issues/15)；插件不提供 alias。
- `RawProviderData.kind` 由 adapter 拥有、原样持久化；跨版本不翻译、不重新解释。旧 raw 若新 adapter 不能回放，按 #11 视为不可回放并记 trace；upcast 归 #4。

### 7.4 清单版本与日志版本

`manifest_version` 与 `LOG_SCHEMA_VERSION`（#4）相互独立：前者管插件清单形状，后者管事件日志。插件升级不需要日志迁移；清单形状变化只影响启动期解析。

## 8. 升级、恢复与在途调用

- **没有热替换**：升级插件 = 重启宿主进程；运行中的会话靠日志恢复，不做运行期迁移。
- **工具消失**（删除或改名）：
  - 模型新请求该工具：pre-execute 照常运行；若未被拒绝，写 `ToolCallRequested`（`provenance = Unknown`，`replay = Unsafe`），跳过审批与执行，写 `ToolResult{Failed, NotFound}`。若 pre-execute 拒绝，写 `Denied`。
  - 崩溃恢复遇到 open intent：`provenance = Unknown` 直接写 `Failed{NotFound}`；否则按记录里的 `replay` 处理——`Safe` 且工具不存在写 `Failed{NotFound}`，`Unsafe` 写 `Interrupted`（#3）。
- **同名工具实现变更**：v1 不在 `ToolCallRequested` 里记录实现指纹；宿主/插件契约要求升级时保持同名工具的 replay 语义，否则插件应把该工具声明为 `Unsafe`。这是有意的 v1 边界。
- **插件 server 在宿主运行中崩溃**：连接与重连策略归 #20；在途调用按 #7 记 `Failed{Transport}`，取消宽限到期记 `Interrupted`。manifest 在 v1 不定义重启策略。

## 9. 失败隔离与状态报告

- **配置错误**（清单非法、重复 `id`）：相关 manifest 失败；若其中任一 `required`，宿主启动失败。
- **运行期失败**（启动、握手、发现、注册）：`required: true` → 宿主启动失败；`required: false` → 插件不可用，宿主继续。
- **没有隐式降级**：失败插件不贡献任何工具；模型看不到它；宿主不会得到"部分可用"的插件。
- 加载结果按插件返回，供 host 记录/展示：

```rust
pub enum PluginStatus {
    Loaded { tools: Vec<ToolName> },
    Failed { stage: PluginLoadStage, message: String },
    Skipped { reason: String },              // host 策略显式跳过；框架不做发现
}

pub enum PluginLoadStage {
    Manifest,
    Spawn,
    Handshake,
    Discovery,
    Registration,
}
```

- v1 不新增内核事件；状态与诊断由 host 消费。`message` 不得包含秘密值。
- 可选插件失败后，宿主可以在自己的 UI/日志里展示状态；框架不提供插件管理面。

## 10. 与相邻票的边界

| 主题 | 归属 |
| --- | --- |
| manifest 形状、加载选择、`id`/命名空间/冲突、版本策略、安装边界 | **本票（#16）** |
| 传输实现、`initialize`、`tools/list`、`tools/call`、取消、断连/重连、协议版本常量、协议错误映射 | [#20](https://github.com/zibyn/asterpath/issues/20) |
| crate 打包、feature 布局、CLI `plugin list`、公共稳定面、`ToolName` 精确类型 | [#15](https://github.com/zibyn/asterpath/issues/15) |
| 安装、更新、市场/registry、凭据/OAuth、环境变量展开、策略与脱敏 | host 或生态工具 |
| provider / storage / executor / agent loop 等核心能力 | 进程内模块，不是插件（#2） |

## 11. v1 出界

- 动态发现（扫目录、自动注册）与运行期安装/卸载。
- 热替换、热重载、运行期升级。
- WASM 与原生 dylib 插件。
- registry / marketplace / 更新服务。
- OAuth 与凭据管理；框架只接收 host 解析好的 manifest。
- 插件提供 provider、storage、executor 等核心能力。
- 插件提供 command / context 贡献（合约尚不存在）。
- 部分工具注册：一个插件要么全部工具可用，要么整插件失败。
- 实现指纹 / descriptor hash 恢复校验。

## 12. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| P1 | 两个 manifest 使用同一 `id` | 两个都失败；任一 `required` 则宿主启动失败；不注册任何工具 |
| P2 | 同一插件的两个工具 sanitize 后同名（如 `a.b` 与 `a_b`），或合成名超 64 | 整插件失败，两个工具都不注册；其他插件不受影响 |
| P3 | 握手返回的协议版本不在 `protocol_versions` | 插件 `Failed{Handshake}`；可选插件宿主继续，required 宿主失败 |
| P4 | `required: true` 的插件启动失败 | 宿主启动失败；错误信息不含秘密值 |
| P5 | 一个插件提供两个工具 | 注册为 `<id>__<name>`；模型看到合成名；adapter 用原始名调用；来源是 `Provenance::External` |
| P6 | 重启后工具被删除，模型再次请求 | 写 `ToolCallRequested` 与 `ToolResult{Failed, NotFound}`，不执行、不审批；恢复场景按 §8 处理 |
| P7 | manifest 带未知字段 / `manifest_version: 2` | 未知字段正常加载；未知版本 `Failed{Manifest}` |
| P8 | 进程内模块 | 由 Cargo 依赖 + 组装根选择，无 manifest、无动态发现；工具名含 `__` 是注册错误 |
| P9 | manifest 的 `env` 含秘密 | `Debug` 输出不含秘密值；manifest 不出现在事件日志、trace、usage |

## 13. 留给下游

- **#7**：`ToolErrorKind::NotFound`、未知工具管线路径、`Provenance::Unknown`、命名空间规则已回写 [tools.md](./tools.md)。
- **#20**：传输变体与认证字段（HTTP headers 等）随 `manifest_version` 扩展；MCP 版本常量；MCP "未知工具"错误映射到 `NotFound` 而不是 `Transport`；重连/重启语义。
- **#15**：`PluginManifest` / `PluginId` 的最终打包位置；CLI `plugin list`；feature 布局；`ToolName` 的精确类型与校验。
- **#4**：`Provenance` 新增 `Unknown` 值；日志 schema 在 v1 发布前完成该字段。
- **#18**：manifest 不持久化，保留/归档无需处理插件清单。
