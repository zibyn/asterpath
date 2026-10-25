# opencode 架构调研（内核/宿主边界参考）

> 完成日期：2026-10-25。用途：审计 Asterpath 的内核/宿主边界（「web、desktop、cli 只是内核的不同展现形态」「内核嵌入 web 平台单设备服务上千用户」「CLI 与桌面端数据互通」），对照 opencode 的分层、存储、多进程与多用户做法。
>
> 一手来源：
> - 仓库已从 `sst/opencode` 迁到 **`anomalyco/opencode`**（`gh api repos/sst/opencode` 返回 `anomalyco/opencode`），默认分支 `dev`。
> - **dev 分支，提交 `32b57015`（2026-10-25）**，各包版本 `2.1.4`，下称「v2」。读了 `AGENTS.md`、`specs/v2/*.md`、`V2_HTTP_API_AUDIT.md`、`packages/{schema,core,protocol,server,client,sdk,cli,plugin,desktop}` 的源码。
> - **最新发布 `v1.19.4`（提交 `148842ef`，2026-10-22）**，下称「v1」。这一版同时带着旧运行时 `packages/opencode` 和新的 v2 包；读了 `packages/opencode/src`（server、session、share、sync、storage、cli/cmd/tui.ts）和 `packages/plugin/src/index.ts`。
> - 官方文档 opencode.ai/docs 的 server、sdk、permissions、plugins、agents、web 页（用抓取工具摘要后阅读，没有逐字核对）。**文档站描述的仍是 v1 API**（路径如 `/session/:id`），和 v2 源码（`/api/session/:sessionID`）对不上；`/docs/share` 已 404。
>
> 本机没有安装 opencode，没有运行过。文中「v1」「v2」指上面两个版本；没有在源码或文档中找到依据的条目标「未核实」。

## 0. 结论摘要

- **所有界面都是同一个 HTTP server 的客户端**。TUI、web、desktop（Electron）、IDE 插件、ACP、SDK 都走同一套 HTTP API 和 SSE 事件流。v1 中每个 TUI 进程在 Worker 线程里各起一个 server；v2 改成**每个用户（每个数据库）一个「托管后台服务」**，靠绑定固定端口选出唯一一个，所有客户端都连它。
- **一个 server 承载多个目录**：每个请求用 `?location[directory]=` 或 `x-opencode-directory` 头指定目录，server 按目录缓存一组「Location 服务」（配置、工具、权限、插件、文件系统）。会话执行在进程内全局调度，执行时再进入会话所属的 Location。
- **v2 把分层写成了包和依赖规则**：Schema（公开形状与持久事件）→ Core（运行时与持久化）、Protocol（HTTP 端点定义）→ Server（HTTP 适配）；Client 只依赖 Schema 和 Protocol；**SDK 在进程内执行 Server 的路由器，不开端口**，也能跑在 Cloudflare Durable Object 里。
- **存储**：v1 和 v2 都用 SQLite（`~/.local/share/opencode/opencode.db`，WAL，`busy_timeout=5000`）。v2 是事件溯源：`event` 表按聚合（会话）递增 seq，各表都是投影。多个进程可以打开同一个库，但**执行所有权是进程内的**，v2 明确不做跨进程的归属和隔离，靠「全用户一个服务进程」回避。
- **审批**：`permission.asked` / `permission.replied` 是**易失事件**，待审批项只存在内存的 Deferred 里，进程关闭时全部按拒绝处理。回复只有 `once` / `always` / `reject`。v2 的 `always` 按项目写进数据库。
- **多用户**：没有用户身份。认证只有一个服务器口令（HTTP Basic，用户名固定为 `opencode`），加上配对码换取的签名会话 cookie。多租户要靠部署单元隔离（每个 Durable Object 一个宿主）。SDK 的 `instances` 明确声明「不是授权或存储隔离边界」。

## 1. 客户端/服务端分层

### 1.1 所有界面都是 server 的客户端

- 文档：「the TUI is the client that talks to the server」；`opencode serve` 起无头服务，默认 `127.0.0.1:4096`，OpenAPI 3.1 规范在 `/doc`，SDK 由它生成；IDE 插件通过 `/tui/*` 端点遥控 TUI。（opencode.ai/docs/server）
- `opencode web` 起 server 并打开浏览器；可以另开终端 attach TUI，「sharing the same sessions and state」。（opencode.ai/docs/web）
- v1 的 TUI（`packages/opencode/src/cli/cmd/tui.ts`）用 `new Worker(...)` 在同一进程的 Worker 线程里起 server。不带 `--port` 时，fetch 和事件都经 Worker RPC 走（`url: "http://opencode.internal"`），**不开端口**；带 `--port` 或 `--hostname` 时才对外监听。
- v2 的 CLI 连接逻辑（`packages/cli/src/services/server-connection.ts`）分三种：
  - `--server URL`：连远端；
  - `--standalone`：起一个私有子进程 server，用 stdin 管道作为租约，管道 EOF 时 server 退出（`services/standalone.ts`）；
  - 默认：`Service.ensure()` 找到或拉起**托管后台服务** `opencode serve --service`。
- v2 desktop（`packages/desktop/src/main/service/background-service.ts`）同样调用 `Service.ensure` 连同一个后台服务，渲染进程是 `packages/app`（与 web 共用）。

### 1.2 托管后台服务：靠固定端口选出唯一进程

- 注册文件 `$XDG_STATE_HOME/opencode/service.json`（权限 0600）写着 url、pid、version、口令，是客户端发现服务的全部依据（`packages/client/src/effect/service.ts` 注释）。
- 选举靠**固定端口**：默认端口是 `0xC0DE`（49374），在 Windows 上会改到其他端口（`services/service-config.ts`）。desktop 源码直接写明了原因：「A fixed port makes a second contender fail to bind and back off; port 0 never collides, so two services could boot against the same database.」
- 绑定失败（EADDRINUSE）的候选进程会探测现任服务，确认后退出（`packages/cli/src/server-process.ts`）。服务每 5 秒检查注册文件仍属于自己，被替换就关机（`services/service-registration.ts`）。客户端版本与服务不一致时，可以选择替换、忽略或报错。
- 结论：v2 实现「CLI、桌面、web 数据互通」的方式是**一个用户只有一个服务进程**，不是多个进程共写一个库。

### 1.3 协议与事件流

- HTTP 端点由 `packages/protocol` 用 Effect `HttpApi` 定义，`packages/server` 组装并实现，`packages/client` 生成 Promise 和 Effect 两种客户端。审计时共 139 个端点（`V2_HTTP_API_AUDIT.md`），覆盖会话、inbox、权限、表单、文件系统、worktree、VCS、PTY、MCP、集成、凭据、插件 RPC 等，远不止会话内核。
- 两类事件流（`specs/v2/session.md`、`specs/v2/event-stream-architecture.md`）：
  - `GET /api/event`：**实例级易失流**，不重放，跨所有 Location。设计文档说明这是有意的：「The global cross-location event stream is intentional … The TUI uses event location metadata」。
  - `GET /api/experimental/session/:id/log?after=&follow=`：**会话级持久日志**，先订阅再重放；重放到捕获的水位时发一个 `log.synced` 标记，之后转为实时。文本、推理、工具输入等增量事件只在实时流里，不进重放。
- 分工（event-stream ADR）：「Core owns event meaning, publication, persistence, typed observation, durable logs, replay … Server owns public event selection, wire encoding, bounded connection delivery」。每个事件只编码一次，每个连接有独立的丢弃队列，容量 4096；某个连接溢出时只断开这一个连接，不阻塞发布者。

### 1.4 一个进程承载多个目录

- 每个请求按 `?location[directory]=`、`x-opencode-directory` 头、进程 cwd 的顺序确定目录（v2 `packages/server/src/location.ts` 的 `requestRef`；v1 `workspace-routing.ts:87` 用 `?directory=`）。v1 `serve.ts` 注释：「Server loads instances per-request via x-opencode-directory header」。
- Location = 目录 + 可选的 workspaceID。`LocationServiceMap` 是按 Location 键控的 `LayerMap`，每个 Location 有一套服务：配置、工具、权限、插件、文件系统。缓存设置为 `idleTimeToLive: Duration.infinity`（`core/src/location-services.ts:56`），**Location 实例从不淘汰**。
- `SessionExecution` 是进程全局的，只以会话 ID 为键；开始执行时再进入会话所在的 Location（`specs/v2/session.md`「Execution Is Process-Local」）。会话可以 `move` 到别的 Location。
- 事件流端点不经过 `LocationMiddleware`，所以一个 SSE 连接能看到所有目录的事件。

## 2. 会话/消息数据模型与存储

### 2.1 位置与引擎

- v1 和 v2 都把数据放在 SQLite `Global.data/opencode.db`（XDG data 目录，即 `~/.local/share/opencode/`）。可以用 `OPENCODE_DB` 指定路径；非正式渠道的版本使用 `opencode-<channel>.db`（`cli/src/database-path.ts`）。更早的版本把数据存成 JSON 文件（`storage/<kind>/…json`），v1 的 `storage/storage.ts` 还保留着把它迁到 SQLite 的迁移代码。
- 连接参数：`journal_mode=WAL`、`synchronous=NORMAL`、`busy_timeout=5000`，文件权限收紧到 0600（`core/src/database/database.ts`）。同一进程内按文件路径加信号量，防止重复执行迁移。
- 存储后端有三种实现：Bun、Node、workerd（Durable Object 存储，不支持调参用的 pragma）。

### 2.2 v1 模型：session / message / part

v1.19.4 的 core 中有 `SessionTable`、`MessageTable`、`PartTable`、`TodoTable`，还有已开始过渡到 v2 的 `SessionMessageTable`、`SessionInputTable`、`SessionContextEpochTable`（经 `gh api` 读 `packages/core/src/session/sql.ts@v1.19.4`）。

v1 的 `sync/README.md` 引入了事件溯源。原文：「allow for one device to control and modify the session, and allow multiple other devices to 'sync' session data … Because only one device is allowed to write … total ordering with a simple sequence id」；事件「sent _before_ the mutation happens, and 'projectors' handle the effects」。

### 2.3 v2 模型：事件日志 + 投影

- `event(id, aggregate_id, seq, type, data)` 在 `(aggregate_id, seq)` 上有唯一约束；`event_sequence(aggregate_id, seq, owner_id)`（`core/src/event/sql.ts`）。`owner_id` 用于**重放导入**时校验归属（`bus.ts` 的 `replay(…, {ownerID, strictOwner})`），不是执行锁。
- 投影表：
  - `session_v2`：包含 `parent_id`、fork 信息、directory、usage 统计、revert、permission 规则集、agent/model、`time_suspended`（执行认领时间戳）、`resume_attempts`；
  - `session_message`：按会话 seq 排序，`data` 存 JSON；
  - `session_inbox`、`session_pending`：未投递的输入；
  - `instruction_blob`、`instruction_state`：指令内容按哈希寻址；
  - 另有 `permission`（已保存的批准）、`project`、`workspace`、`credential`、`kv` 等表。
- 写入路径是「发布持久事件 → 在同一事务里由投影器写表」（`bus.publish(…, { commit })`）。

### 2.4 多进程语义

- **SQLite 层**：WAL 加 5 秒 busy_timeout，多个进程可以同时打开同一个库。
- **实时事件只在进程内**：Bus 的 live 通道是进程内的 `PubSub`（`core/src/bus.ts`）。别的进程写入的数据不会推给本进程的客户端。这一条由代码结构推断，没有看到文档明说。
- **执行归属只在进程内**：原文「Event replay ownership is separate from Session execution ownership. Local execution remains process-owned until clustering introduces an explicit placement and fencing protocol.」（`specs/v2/session.md`）。`SessionRunCoordinator` 只管本进程：同一会话的 resume 合并成一次，不同会话并发执行。
- **崩溃恢复**：开始执行时写入「write-ahead claim」（`time_suspended`）。成功、失败或用户中断时释放；关机或崩溃时保留。下次启动时恢复有认领的顶层会话，追加一条续跑指令，并计数恢复次数。还在运行状态的工具调用先判为失败，不重放副作用。设计文档说明：「does not guarantee exactly-once provider requests or tool effects」。
- **两个 opencode 同时运行**：
  - v1：每个 TUI 各有一个进程内 server，共用 `opencode.db`。互相看不到对方的实时事件，没有防止两进程同时驱动同一会话的机制（未核实是否有其他保护）。
  - v2 默认：所有 TUI 和 desktop 连同一个托管服务，不存在这个问题。
  - v2 `--standalone`：私有 server 仍用同一个数据库（`databasePath(global.data)`），与托管服务并存时退回到 v1 的情形。

## 3. 权限审批、中断、排队输入

### 3.1 审批往返

- 规则配置为 `allow` / `ask` / `deny`，支持通配，**最后一条匹配的规则生效**；agent 的规则覆盖全局规则；`--auto` 把 ask 视为 allow，但 deny 仍然生效。（opencode.ai/docs/permissions）
- v2 的 Schema（`schema/src/permission.ts`）：
  - `Request{ id, sessionID, action, resources[], save?[], metadata?, source?{type:"tool", messageID, id}, message? }`；
  - `Reply = "once" | "always" | "reject"`；
  - 事件 `permission.asked`、`permission.replied` 用 `ephemeral(...)` 定义，**不进持久日志**。
- 端点：`GET /api/permission/request`（所有待审批项）、`GET|POST /api/session/:id/permission`、`POST /api/session/:id/permission/:requestID/reply`，另有已保存批准的 CRUD（`/api/permission/saved`）。
- 实现（`core/src/permission.ts`）：
  - 待审批项是内存里的 `Map<ID, {request, Deferred}>`，Location 关闭时全部发出 `replied: reject`；
  - `reject` 时同一会话的其他待审批项**一并拒绝**；如果附带 `message`，以 `CorrectedError` 返回给模型，模型据此继续，而不是结束这一步；
  - `always` 时把 `request.save` 里的模式按 **项目** 写进 `permission` 表，然后重新评估其他待审批项，变成允许的自动放行。
- v1 文档说 `always` 只在「the rest of the current OpenCode session」内有效；v2 改为按项目持久化。
- v1 插件有 `permission.ask` 钩子，可以改写审批结果（`plugin/src/index.ts:261`）。
- v2 还有通用的「表单」通道（`/api/session/:id/form`，`question` 工具使用），和审批分开。

### 3.2 中断

`POST /api/session/:id/interrupt`：会话不存在时返回 `SessionNotFoundError`；会话空闲，或者不归本进程执行时是 no-op。中断不删除待投递的输入，并且释放执行认领。（`specs/v2/session.md`）

### 3.3 排队输入（inbox）

`prompt` 先发布持久事件 `session.inbox.enqueued`，写入 `session_inbox`，然后才决定是否唤醒执行（`resume:false` 只记录、不唤醒）。投递方式由 `delivery` 指定：

- `steer`（默认）：在下一个安全的步骤边界按顺序注入；
- `queue`：空闲时一次投递一条。

手动压缩和会话迁移也作为控制项进入 inbox，形成投递边界。输入 ID 重复时幂等（先到者生效）。inbox 支持列出、改投递方式、取消。（`specs/v2/session.md`）

## 4. 子 agent、压缩、分享、回滚

### 4.1 子 agent

- 文档（opencode.ai/docs/agents）把 agent 分两类：
  - primary：Build、Plan；
  - subagent：General、Explore；
  - 另有隐藏的 compaction、title、summary agent。
  主 agent 按描述自动调用子 agent，用户也可以 `@general` 手动调用。子会话可以在 TUI 中导航。
- v2 的 `subagent` 工具（`core/src/tool/plugin/subagent.ts`）：
  - 创建 `parentID = 当前会话` 的子会话，权限沿父链检查；
  - 支持 `background: true`：立即返回子会话 ID，完成后通过 `SubagentJob` 以合成输入通知父会话（`session/subagent-job.ts`）；
  - 子 agent 作为 Job 有恢复信息（recovery），可以续跑。

### 4.2 压缩

每一步之前估算请求大小，超出上下文窗口时，换成结构化的滚动摘要加最近若干条上下文；完整记录仍然保留在持久存储里。provider 报上下文溢出且还没有持久输出时，最多再压缩一次，重建同一步（`specs/v2/session.md`「Compaction Rebuilds Active History」）。压缩的触发和执行都在 Core 里。插件钩子 `experimental.session.compacting` 可以追加或替换压缩提示词。

### 4.3 分享

- v1 文档（`share.mdx`）：会话同步到 opncd.ai 生成公开链接，有 manual、auto、disabled 三种模式；企业版可以禁用、限定 SSO 或自托管。
- **但 v1.19.4 的代码已停用分享**：「Session sharing was discontinued. The share endpoints stay for API compatibility: share fails and unshare only clears the local link.」（`packages/opencode/src/share/session.ts`）
- v2 没有分享端点，只在 `session_v2.share_url` 保留了列。线上文档 `/docs/share` 返回 404。

### 4.4 revert / undo

- 快照是**影子 git 仓库**：以项目工作树为 worktree，git 目录放在 `Global.data/snapshot/<projectID>/<hash(worktree)>`，不碰用户自己的 `.git`（`core/src/snapshot.ts`）。只在 VCS 为 git 时启用。
- v2 回滚分三步：`revert/stage`（按消息边界恢复文件，并在会话上记录 revert 状态）、`revert/commit`、`DELETE revert`（撤销暂存，恢复原样）（`core/src/session/revert.ts`）。另有 `fork`，按边界复制消息。

## 5. 插件/扩展机制

### 5.1 v1 插件

- 位置：项目 `.opencode/plugins/`、全局 `~/.config/opencode/plugins/`，或配置中 `plugin: [npm 包]`；启动时用 Bun 安装依赖。（opencode.ai/docs/plugins）
- 插件拿到的上下文 `PluginInput{ client(SDK), project, directory, worktree, serverUrl, $(Bun shell), experimental_workspace.register }`（`plugin/src/index.ts:56`）。
- 钩子：
  - `event`、`config`、`tool`、`auth`、`provider`；
  - `chat.message`、`chat.params`、`chat.headers`；
  - `permission.ask`；
  - `command.execute.before`、`tool.execute.before`、`tool.execute.after`；
  - `shell.env`、`tool.definition`；
  - 若干 `experimental.*`（消息/系统提示词变换、压缩）。
- 插件运行在进程内，权力很大：可以改写审批、改写提示词，同名工具覆盖内置工具。

### 5.2 v2 插件

- 插件运行在进程内，同样有 Promise 和 Effect 两种 API（`packages/plugin/src/README.md`），两类能力：
  - **transform**：编辑有状态的目录，可编辑的领域有 `agent`、`command`、`integration`、`mcp`、`model`、`provider`、`reference`、`skill`、`tool`、`vcs`、`websearch`。按注册顺序可重放，可以 `reload`；
  - **runtime hook**：拦截运行中的操作，例如 `aisdk` 的 `sdk`/`language`，以及在发往 provider 前改写上下文（含工具集）的 `session.hook("context")`。
- **内置工具本身也是插件**（`core/src/tool/plugin/*`：read、write、edit、patch、shell、grep、glob、webfetch、websearch、subagent、question、todo、skill…）。MCP 工具也通过同一个 `Tool.Service.transform` 注册（`core/src/tool/AGENTS.md`）。
- Effect 版插件的每个领域都必须扩展 `@opencode/client/effect/api` 中对应的客户端接口（`packages/plugin/AGENTS.md`），也就是说**插件 API 等于 HTTP 客户端 API 加少量插件专用操作**。

### 5.3 放在内核外的东西

- **宿主适配器**：
  - `WorkspaceDriver`：远端工作区的 create / connect / suspend，绑定数据由 Core 不透明保存（`core/src/workspace/driver.ts`）；
  - `EnvironmentDriver`：提供 spawner 和文件实现，工具在这上面执行，相当于沙箱接缝。
- **策略**：`experimental.policies` 用 `provider.use`、`tool.use`、`integration.use` 的允许/拒绝语句控制，可以由 Console 下发并拥有最终决定权，由一个「terminal plugin」执行（`specs/v2/provider-policy.md`）。
- **CLI 进程管理**：服务注册、远程隧道（OpenTunnel）、web UI 资源、自动更新都在 `packages/cli`，不在 server 或 core 里。

## 6. 多用户与远程部署

- **认证只在 Server 层，而且只有单一口令**：
  - 设了 `OPENCODE_SERVER_PASSWORD`（v2 也认 `OPENCODE_PASSWORD`）就启用 HTTP Basic，用户名固定为 `opencode`（`server/src/auth.ts`）；
  - 也可以用 `?auth_token=`，或配对码（5 分钟有效、一次性）换取 30 天的 HMAC 会话 cookie，**改口令即全部吊销**；
  - 没设口令时，`opencode web` 和 `opencode serve` 不鉴权（docs/web 的警告）；托管服务和 standalone 模式总会生成随机口令。
- **没有用户身份**：请求上下文里只有 Location，没有用户；会话、权限、凭据都没有用户维度（各表都没有 user 列）。一个 server 就是一个用户的全部数据。
- **远程访问**：服务可以绑定 `0.0.0.0` 或开启 mDNS，可以配 `config.remote` 经 OpenTunnel 暴露到随机子域名（`cli/src/services/remote-tunnel.ts`）。这些都是「同一用户远程连自己的服务」。
- **嵌入式与多租户**：
  - SDK 的 `OpenCode.create({ database: { path } })` 在进程内执行路由器，默认使用内存数据库；
  - `OpenCodeWorkerd.create({ storage: state.storage })` 让每个 Durable Object 持有一个宿主（`packages/sdk/README.md`）。云上的多租户隔离靠**每个 DO 一个宿主、一个存储**，不是在一个宿主里区分用户（由 README 推断，未核实他们的云产品确实这样部署）；
  - SDK 的 `instances.key(session)` 可以按会话元数据给同目录的会话挂不同插件，但 README 明确写着：「Instance selection is not an authorization or storage-isolation boundary.」

## 7. 对 Asterpath 的启示

### 7.1 「内核库」与「内核服务协议」应该分开；协议是独立层，不归任何一个宿主

opencode v2 的依赖规则（根 `AGENTS.md`）：「Schema → Core and Protocol → Server；Client may depend on Schema and Protocol but never Core or Server；sdk composes Client, Core, and Server」。由此得出几点：

- **公开的数据形状和持久事件（Schema）是最底层**，内核和协议都依赖它；协议不定义自己的一套 DTO。Asterpath 已有的 `protocol` crate 若定位为「事件和记录的形状」，应该对应 Schema，而不是线协议。
- **线协议（Protocol + Server）是 Core 之上的一层适配**，由项目维护，不属于 CLI、desktop 或 web 中任何一个。所有宿主共享它，嵌入方也可以不用它：SDK 在进程内调用同一个路由器，不开端口。
- 对照 ADR-0035「命令按 thread/turn/item 语义命名，以后可抽成独立 app-server，不现在设计线协议」：opencode 的路线说明这一步可以推迟，但**宿主命令的语义应该从现在起就对齐将来的协议**。opencode v1 是「每个宿主一个进程内 server」，v2 才收敛到「一个服务 + 多个客户端」，中间经历了 v1/v2 双运行时并存的长期迁移（v1.19.4 同时带两套运行时）。

建议的形态：内核 crate（库）⊂ 可选的「内核服务」crate（会话、事件、审批、inbox 的线协议和适配器），宿主二选一：链接内核库，或者作为内核服务的客户端。服务协议放在内核仓库里维护，但不进内核库。

### 7.2 协议应该只覆盖内核，产品功能不要灌进去

opencode 的 139 个端点里，会话内核只占一部分，其余是 PTY、文件系统、VCS、凭据、OAuth、MCP 客户端、语音、插件 RPC 等产品功能。插件 API 又被规定等于客户端 API，结果**协议面就是产品面**。Asterpath 如果要让 web 平台嵌入，内核服务协议应只覆盖：

- 会话生命周期；
- 输入（inbox）；
- 中断；
- 审批或等待的应答；
- 持久日志的订阅/重放；
- 目录/投影查询。

文件浏览、终端、凭据管理等由宿主自己提供接口。

### 7.3 值得借鉴

- **区分持久日志和易失增量**：`session.log(after, follow)` 先订阅再重放，在水位处发 `synced` 标记；文本增量不进重放。客户端靠「投影 + 日志尾」恢复状态。这和 ADR-0035「视图重挂载先渲染 `projection()` 再回放缓存」一致，可以直接作为将来协议的订阅语义。
- **由 Server 负责事件编码和背压**：每个事件只编码一次，每个连接独立有界，慢连接被单独踢掉，不阻塞内核发布。上千连接的场景必须这样做，而这是服务层的责任，不应进内核。
- **输入先持久化再执行**：inbox 区分 steer 和 queue，ID 幂等，`resume:false` 只记录不唤醒，压缩和迁移也作为控制项走 inbox。对比 ADR-0035「运行中输入即引导，不另设队列」：opencode 最终同时支持两种投递方式，并为此给了持久化和幂等语义。web 场景下客户端重试很常见，值得考虑。
- **执行认领与崩溃恢复写清了边界**：write-ahead claim、有上限的恢复次数、悬空工具调用判为失败而不重放、明说不保证 exactly-once。和 ADR-0036 的方向一致。
- **多目录靠请求携带 Location，执行时按会话进入 Location**，不改进程 cwd；ADR-0035「按会话路由工作根」与此同构。
- **审批的「拒绝并附说明」以纠正反馈的形式回给模型**，而不是直接终止这一步；同一会话的其他待审批项一并处理。

### 7.4 应当避免或注意

- **用「全用户一个进程」换数据互通，规避了多写者问题，但不适合 web 多租户**。opencode 的执行归属只在进程内，v2 明确「until clustering introduces an explicit placement and fencing protocol」，所以只能靠固定端口选出单一服务。Asterpath 的目标（web 单设备上千用户、多 worker、滚动重启）正是 opencode 没做的部分。ADR-0036「按会话树认领 + 属主纪元 + 存储拒绝过期写」在这一点上走得比 opencode 远，方向应当保持。opencode 的 `event_sequence.owner_id` 只用于重放导入，不能当作执行围栏的先例。
- **Location 实例永不淘汰**（`idleTimeToLive: infinity`），面向单用户可以接受，上千用户会无限增长。Asterpath 的按工作根缓存要从一开始就有淘汰和资源上限。
- **认证没有用户维度**（单一口令，用户名固定）。opencode 的多租户靠部署单元隔离（每个 Durable Object 一个宿主）。Asterpath 若要在一个进程内服务多个用户，**租户/用户标识必须由宿主放进会话元数据并做过滤**，内核只提供不透明元数据（ADR-0032）和按元数据查询的门面。认证、授权留在服务层或宿主，不进内核。这与 framework-scope-boundary 的原则一致。
- **「always」放在哪里**：opencode v2 让 Core 把 always 按项目写进库。Asterpath 把「本会话允许」放在宿主内存（ADR-0035）。从机制/策略的划分看，保存范围（会话、项目、用户）是策略，属于宿主，但跨进程、跨重启生效需要持久化的存放位置；可以考虑由内核提供「宿主可写的会话级/目录级元数据」作为机制，而不是内核内建一张 permission 表。
- **待审批项只在内存里，进程退出即拒绝**。这和 ADR-0028 的进程内待审批相同；opencode 用单服务规避了跨进程审批，Asterpath 走多进程就必须有 ADR-0036 的「审批写日志 + 属主轮询」。
- **v1 双运行时迁移的代价**：文档站至今描述 v1 API，分享功能代码已停用而文档仍在，v1.19.4 同时带两套运行时。这说明线协议一旦被多种客户端依赖，就很难更改。在 Asterpath 定下线协议前，宜先把内核门面（会话、inbox、日志订阅、认领）稳定下来。

## 附：来源索引

- 依赖方向：`AGENTS.md`（dev@32b57015）
- 会话语义：`specs/v2/session.md`
- 事件流：`specs/v2/event-stream-architecture.md`
- 策略：`specs/v2/provider-policy.md`
- 端点清单：`V2_HTTP_API_AUDIT.md`
- 托管服务：`packages/client/src/effect/service.ts`、`packages/client/src/service-contender.ts`、`packages/cli/src/server-process.ts`、`packages/cli/src/services/{server-connection,standalone,service-registration,service-config,remote-tunnel}.ts`、`packages/desktop/src/main/service/background-service.ts`
- 存储：`packages/core/src/database/database.ts`、`packages/cli/src/database-path.ts`、`packages/core/src/event/sql.ts`、`packages/core/src/session/sql.ts`、`packages/core/src/bus.ts`
- 请求路由与认证：`packages/server/src/location.ts`、`packages/server/src/auth.ts`、`packages/server/src/middleware/authorization.ts`、`packages/server/src/pairing.ts`
- 审批：`packages/schema/src/permission.ts`、`packages/core/src/permission.ts`、`packages/core/src/permission/sql.ts`
- 子 agent、快照、回滚：`packages/core/src/tool/plugin/subagent.ts`、`packages/core/src/session/subagent-job.ts`、`packages/core/src/snapshot.ts`、`packages/core/src/session/revert.ts`
- 插件：`packages/plugin/src/README.md`、`packages/plugin/AGENTS.md`、`packages/core/src/tool/AGENTS.md`
- 宿主驱动：`packages/core/src/workspace/driver.ts`、`packages/core/src/environment/driver.ts`
- SDK：`packages/sdk/README.md`
- v1（v1.19.4@148842ef）：`packages/opencode/src/cli/cmd/tui.ts`、`packages/opencode/src/cli/cmd/serve.ts`、`packages/opencode/src/server/routes/instance/httpapi/middleware/workspace-routing.ts`、`packages/opencode/src/sync/README.md`、`packages/opencode/src/share/session.ts`、`packages/opencode/src/storage/storage.ts`、`packages/plugin/src/index.ts`
- 文档：opencode.ai/docs/{server,sdk,permissions,plugins,agents,web}；`share.mdx`@v1.19.4（raw）
