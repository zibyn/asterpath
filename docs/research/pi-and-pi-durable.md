# pi 与 Pi Durable 的内核/宿主边界调研

> 完成日期：2026-10-25。用途：为 Asterpath 的内核/宿主边界审计（ADR-0023、ADR-0035、ADR-0036）提供参照，重点看「同一内核、多种外壳」「会话数据互通」「一个进程服务多会话/多用户」这三件事 pi 是怎么做的。
>
> 一手来源：
> - `earendil-works/pi` 仓库（原 `badlogic/pi-mono`，2026 年 4 月随 pi 被 Earendil 收购迁移），main 分支提交 `d5b3de2`（2026-10-25）。通过 `gh api` 读取，未本地构建：
>   - 各包 `package.json`（版本均为 1.4.0）、根 `README.md`、`packages/agent/README.md`；
>   - `packages/coding-agent/docs/`：`how-pi-works.md`、`sessions.md`、`session-format.md`、`sdk.md`、`rpc.md`、`extensions.md`、`security.md`、`containerization.md`、`mcp.md`；
>   - `packages/coding-agent/src/core/session-manager.ts`（写盘路径）；
>   - `packages/durable/README.md` 与 `packages/durable/docs/spec.md`（自称“normative”的规范，5700 余行）；
>   - 已删除包的历史：`packages/mom` 与 `packages/pods` 在提交 `0ed0d43`（2026-04-30）删除，`packages/web-ui` 在 `b141e1f`（2026-05-20）删除；`mom` 的 README 取自删除前的版本。
> - 官方文章：[Pi Durable](https://earendil.com/posts/pi-durable/)、[Pi 1.0](https://earendil.com/posts/pi-1-0/)、[pi.dev 更新日志](https://pi.dev/news)。这三页由抓取工具摘要后阅读，没有逐字核对。
> - 二手：36kr 转载的 InfoQ 访谈（[英文](https://eu.36kr.com/en/p/4025329229631363)、[德文](https://eu.36kr.com/de/p/4025329229631363)）。只用来交叉印证。
>
> 本机没有安装 pi（`which pi` 无输出），因此没有运行 `--help`，也没有实际跑过任何模式。

## 0. 结论摘要

- **「pi durable」已确认**：指 `@earendil-works/pi-durable`，位于同一仓库的 `packages/durable`，2026-10-01 与 Pi 1.0 一起发布，状态是 experimental。它是**另一个独立的智能体内核**：不依赖 `pi-agent-core`，coding agent 也不依赖它。官方说法是：为了不把 pi 本体做胖，长时运行、多端访问这类需求被单独放进了这个包。
- **pi（coding agent）的分层**：`pi-ai`（provider 抽象）→ `pi-agent-core`（无持久化的 agent loop）→ `pi-coding-agent`（会话存储、工具、扩展、资源发现，**同时也是 SDK**）→ 四种外壳：交互 TUI、print、JSON 事件流、RPC（stdin/stdout JSONL）。外壳和内核同在一个进程，RPC 也只是“子进程 + 管道”，**没有服务器模式**。实验性的 server/client 栈在 1.2.0 被删掉（未发布过）。
- **pi 的会话**是 `~/.pi/agent/sessions/<cwd编码>/<时间>_<id>.jsonl`，append-only，用 `id/parentId` 构成树；压缩、分支摘要、上下文编辑都是追加条目，原条目保留。**没有任何文件锁或多进程语义**。各种外壳共享同一个目录，所以数据互通靠「同一种文件格式」。
- **Pi Durable** 用一个 Session/Harness 统管多个 conversation：每次提交在 conversation、entry、task、submission、document 上原子生效；任务按阶段做检查点（“effect sandwich”：先提交意图，再执行副作用，再提交结果）；崩溃后重开存储、自动续跑。后端有 memory、SQLite、JSONL、IndexedDB、Cloudflare Durable Object SQLite。**一个存储同一时刻只能有一个属主进程，不做跨进程锁**；其他进程只能 `readOnly` 打开。传输层不提供，由宿主自己做。
- **对 Asterpath 的主要启示**：pi 本体的“单用户、单进程、无锁 JSONL”正是 Asterpath 要避开的；Pi Durable 的“一个进程持有一个存储、内部承载多个 conversation、其他客户端经 watch 挂载、只读观察者”与 ADR-0036 的方向一致，可以拿它的不变量对照。详见 §6。

## 1. 分层：内核包与外壳

### 1.1 当前包（main，1.4.0）

| 包 | 角色 | 依赖（仅本仓库） | 出处 |
|---|---|---|---|
| `pi-ai` | 统一的多 provider LLM API、模型目录 | `pi-telemetry` | `packages/ai/package.json`、根 README |
| `pi-agent-core` | 有状态的 agent loop：工具执行、事件流、steer/followUp 队列、`beforeToolCall`/`afterToolCall` 钩子；**不负责持久化** | `pi-ai` | `packages/agent/README.md` |
| `pi-coding-agent` | 编码智能体 CLI，**同时导出 SDK**（`createAgentSession`、`SessionManager`、`AgentSessionRuntime`） | `pi-agent-core`、`pi-ai`、`pi-codemode`、`pi-mcp`、`pi-tui` | `package.json`、`docs/sdk.md` |
| `pi-tui` | 差分渲染的终端 UI 库 | 无 | `package.json` |
| `pi-mcp` | 独立的 MCP 客户端 | 无 | `package.json` |
| `pi-codemode` | 沙箱化的 JS 执行，唯一能力是调用注入的工具 | 无 | `package.json` |
| `chord` | 类型化宿主运行时：插件、契约、复制状态、基于 JSON 链路的 RPC | `pi-telemetry` | `package.json` |
| `pi-durable` | 持久化的 conversation/task/document 运行时 | `chord`、`pi-ai`、`pi-codemode`、`pi-telemetry`（**不依赖 `pi-agent-core`**） | `package.json` |
| `pi-env` | 给 Pi Durable 用的远程执行环境：经 SSH 部署的小守护进程 + `ExecutionEnv` 客户端 | `chord`、`pi-durable` | `package.json` |

已删除的外壳：`mom`（Slack 机器人）和 `pods`（GPU pod 上的 vLLM 管理）在 2026-04-30 删除，`web-ui` 在 2026-05-20 删除（仓库提交历史）。删除前的 `mom` 每个 Slack 频道维护自己的 `log.jsonl`（完整频道历史）和 `context.jsonl`（模型看到的上下文），工具可以跑在 Docker 沙箱里（旧 `packages/mom/README.md`）。也就是说，旧 mom 是一个进程管多个频道会话，但用的是自己的文件布局，没有复用 coding agent 的会话格式。

### 1.2 外壳如何嵌入内核

`how-pi-works.md`：“All interfaces use the same agent and session mechanisms.” 四种模式：

| 形态 | 进程边界 | 说明 | 出处 |
|---|---|---|---|
| 交互 TUI | 进程内 | 渲染会话与 agent 事件 | `how-pi-works.md` |
| print / JSON | 进程内 | 跑一个 prompt；JSON 模式把 agent 事件写成 JSONL | 同上 |
| RPC | 子进程 | `pi --mode rpc`：stdin 收 JSONL 命令，stdout 输出 `response`、会话事件和扩展 UI 记录；命令带 `id` 关联；扩展的 UI 交互（确认、选择等）转发给客户端 | `rpc.md` |
| SDK | 进程内 | `createAgentSession()` 得到 `AgentSession`，一个 session 拥有一个对话、模型、工具、队列、压缩状态和扩展运行时 | `sdk.md` |

- **没有服务器形态。** 1.2.0 的更新日志写着 “Removed the unreleased experimental server, client, and plugin stack”（`pi.dev/news` 摘要）。
- SDK 的可替换边界都是注入式的：`modelRuntime`、`settingsManager`、`sessionManager`（可以用 `SessionManager.inMemory()`）、`resourceLoader`（宿主可以完全接管扩展、技能、模板的发现）、`tools/customTools`（`sdk.md`）。
- CLI 与 SDK 默认行为不同：CLI 把 codemode、tool_search、MCP 当作内置扩展加载，**SDK 默认不加载**，需要宿主显式加入（`sdk.md`）。

## 2. 会话持久化（pi coding agent）

| 方面 | 事实 | 出处 |
|---|---|---|
| 格式 | JSONL，每行一个带 `type` 的对象；首行是 `session` 头（`version: 3`、`id`、`cwd`、可选 `parentSession`） | `session-format.md` |
| 位置 | `~/.pi/agent/sessions/--<path>--/<timestamp>_<session-id>.jsonl`，按工作目录分组；可用 `--session-dir`、`PI_CODING_AGENT_SESSION_DIR` 或 `sessionDir` 设置改位置 | `session-format.md`、`sessions.md` |
| 条目类型 | `message`、`model_change`、`thinking_level_change`、`usage`、`compaction`、`context_edit`、`branch_summary`、`custom`（扩展状态，不进上下文）、`custom_message`（扩展注入，进上下文）、`label`、`session_info` | `session-format.md` |
| 系统提示与工具 | 没有单独的状态条目；首次请求写一条带全部 prompt section 和工具声明的 system message，之后的变化按 section 名打补丁，回放即得当前状态 | `session-format.md` |
| 树 | `id/parentId`；“leaf” 是当前位置；`/tree` 在同一文件里切分支，`/fork`、`/clone` 生成新文件并在头部记 `parentSession`；切走时可以让 LLM 总结被放弃的分支（`branch_summary`） | `sessions.md`、`session-format.md` |
| 压缩 | 追加 `compaction` 条目（`summary`、`firstKeptEntryId`、系统提示检查点），重建上下文时替换旧条目，原条目不删 | `session-format.md`、`compaction.md` |
| 上下文编辑 | `context_edit` 只改未来的模型上下文，按分支生效 | `session-format.md` |
| 写盘 | 首条 user/assistant 消息出现前不建文件；第一次用 `openSync(path, "wx")` 写入积压条目，之后每条 `appendFileSync`；加载时修复被截断的末行 | `session-manager.ts` L1170–1187、L650–668 |
| 并发/多进程 | **没有锁，文档也没有任何多进程语义**。两个 pi 进程同时续写同一个文件会交错追加（由代码推断，未实测） | 对 docs 与 `session-manager.ts` grep `lock` 无结果 |
| 迁移 | v1 线性 → v2 树 → v3（`hookMessage` 改名 `custom`），加载时自动迁移 | `session-format.md` |
| 外壳共享 | TUI、print、JSON、RPC、SDK 共用 `SessionManager` 和同一目录，所以都能互相 `--resume`；SDK 宿主也可以用内存会话，自己保存条目，再通过 “restorable in-memory sessions”（0.85.0）恢复 | `how-pi-works.md`、`sdk.md`、`pi.dev/news` |

## 3. 扩展机制，以及刻意放在内核之外的东西

### 3.1 形状

- **扩展**是进程内加载的 TypeScript 模块（用 jiti，无需编译），默认导出一个工厂函数，接收 `ExtensionAPI`，可以注册工具、命令、快捷键、provider、事件处理器、渲染器和终端 UI。扩展拥有与 pi 进程相同的权限（`extensions.md`）。
- **事件组合语义写得很清楚**：通知、原地修改、链式变换、闸门（如 `tool_call` 返回 `{ block: true }`）、可操作边界（`turn_end`）、最后覆盖。闸门类处理器出错时按 fail-closed 处理，`tool_call` 处理器失败会直接阻止该工具（`extensions.md` “Events and concurrency”）。
- 扩展状态通过 `custom` 条目写进会话；`custom_message` 可以注入上下文（`session-format.md`）。
- **pi-agent-core 层**只有 `beforeToolCall`/`afterToolCall`、`transformContext`、`convertToLlm`、`prepareRequest`、`finishTurn` 这类回调，扩展系统属于 coding agent 层（`packages/agent/README.md`）。
- 技能、prompt 模板、主题是声明式资源，通过 npm/git 的 “Pi packages” 分发（`how-pi-works.md`）。

### 3.2 刻意不放进内核的

| 能力 | 立场 | 出处 |
|---|---|---|
| 权限/审批 | 内核没有审批流程：“it does not ask for approval before every tool call”；要审批就写扩展（示例 `permission-gate.ts`） | `security.md`、`extensions.md` |
| 沙箱 | 不内置；安全边界来自操作系统或虚拟化（Docker、Docker Sandboxes、OpenShell、Gondolin 微虚拟机扩展），并说明“看 transcript、项目信任、审 diff 都不是安全边界” | `security.md`、`containerization.md` |
| 项目信任 | 有，但只管启动时加载哪些项目资源，不限制工具 | `security.md` |
| 子智能体、plan mode | 不内置：“skips features like sub-agents and plan mode. Ask Pi to build what you want” | 根 README |
| MCP | 立场有变化。早期 Mario 写过 “what if you don't need MCP”（旧 mom README 链接）；1.0 起原生支持，但是作为 CLI 的**内置可替换扩展**（stdio 与 streamable HTTP），并优先走 codemode/延迟加载；SDK 默认不带 | `mcp.md`、`sdk.md`、Pi 1.0 文章 |

## 4. Pi Durable：持久执行模型

### 4.1 它是什么

- npm 包 `@earendil-works/pi-durable`，代码在 `packages/durable`；“a framework for building any agentic application, coding agents included”；experimental，API 可能变化；目前只有 TypeScript（Pi Durable 文章、README）。
- 官方说明：它和 coding agent 共享 `pi-ai` 等代码，经验会回流给 coding agent，但 coding agent 本身不变（Pi Durable 文章）。依赖关系也印证了这一点：它不依赖 `pi-agent-core`，自带一套 loop（`pi.generation`/`pi.tool` 内置任务，见 spec §8）。

### 4.2 核心概念（README、spec §1）

- **Session / Harness**：一个打开的存储加上在其上运行智能体的机制。Session 拥有**一条变更线**（mutation line），所有提交原子生效，“只有已提交的状态可见”。
- **Conversation**：由不可变 entry 组成的 transcript 范围，可以 fork；一个 Harness 内有多个。
- **Document**：用 Chord 操作表示的类型化 JSON 状态，与 transcript 在同一次提交中变更；内置文档保存 agent 选择、provider 会话标识、实时生成/工具状态、收件箱和用量。
- **Task**：持久状态机，每一步都做检查点，并有属主（某个 conversation 或另一个 task）。子智能体就是被 task 拥有的 conversation。
- **Submission**：交给 conversation 的输入；对忙碌的 conversation 可以选 steer、follow-up 或 reject；`requestId` 保证幂等提交。

### 4.3 崩溃恢复

- **Effect sandwich**（spec §5.2）：`提交意图阶段 → 执行外部副作用 → 提交结果或下一阶段`。如果重开时停在意图阶段，就说明副作用可能已发生，阶段处理器要安全重试、轮询外部句柄，或记一次中断。外部副作用一律不在 Session 的变更事务里执行（不变量 4）。
- 工具只有声明 `replay: "safe"` 才会在重开后重跑，否则模型收到 `interrupted` 错误结果。replay-safe 的调用方重跑时，已发出的嵌套调用按 key 找回。被中断的模型请求会重发（README、Pi Durable 文章）。
- codemode 脚本在确定性沙箱里运行（虚拟时钟、固定种子、UTC），每个工具结果先写日志；重启后重跑脚本并按顺序回放结果，如果走了不同路径就报错（README L368）。
- 崩溃循环保护：每次被崩溃、驱逐或重置打断都会计数，达到 `restartLimit`（默认 5）后任务停住，等 `retryTask(id)`（README）。
- 流式损失窗口：部分输出约每 100 ms 提交一次，但作为 “deferred commit” 最迟约 1 s 后才落盘，崩溃最多丢这一段（README、spec §4.1）。
- 存储出错就让整个 Harness 以 `SessionFailed` 结束，需要重新打开（README、spec §10.1）。
- 36kr 说它“不用事件溯源，而用版本化文档”。spec §13 非目标里确有 “Session-kernel semantic event journal”，文档也有版本与迁移（§3.6），基本一致；但“版本化文档”这个说法不是官方原话。

### 4.4 谁调度、进程模型、存储后端

- **调度器在进程内**：串行预留可运行的任务，然后在 Session 变更线之外执行处理器（spec §5.4）。`close()` 停止所有调用但不写结果，下次打开时续跑。
- **可被驱逐的宿主自己负责唤醒**：在 Cloudflare Durable Object 这类环境，给 Harness 传 `onWake`，它不再自己起定时器，而是告诉宿主什么时候需要 `resume()`，宿主用 alarm 唤醒（README L166）。
- **单属主**：“One owning process per storage. There is no cross-process locking.” 其他进程用 `readOnly: true` 打开；写入、`resume()` 和跑任务都会以 `ReadOnlySession` 拒绝。浏览器的 IndexedDB 由宿主用 Web Lock 包住（README）。
- **没有 worker 池**，也不做多写者合并：非目标里明确列出 “CRDT/offline multi-writer merge”（spec §13）。

| 后端 | 语义 | 出处 |
|---|---|---|
| Memory | 参考语义，不持久化 | spec §11.1 |
| SQLite（Node） | 一个 SQL 事务对应一次提交；WAL、`synchronous=NORMAL`；进程崩溃不丢，断电可能丢最后一条；可用表前缀与宿主共库 | README、spec §11.2 |
| JSONL（Node） | `main.jsonl` + 每文档/每任务 sidecar，提交写 marker；可选 `fsync`；全部历史常驻内存；不压缩 `main.jsonl` | spec §11.3 |
| IndexedDB（浏览器） | 一个数据库同一时刻一个属主 | README |
| Cloudflare DO SQLite | 提交就是 DO 的存储事务；同一个存储对象上的多个 Pi 存储共享一个操作队列 | README |

SQLite/JSONL 的可移植核心不依赖 Node API，可以跑在 Bun 或 DO 上。文章称接口也能放在 KV 或 Postgres 上，**但没有发布 Postgres 后端**（Pi Durable 文章）。

## 5. 多用户与服务端部署

- **pi coding agent**：单用户、单进程。没有服务器；一个 `AgentSession` 对应一个对话，`AgentSessionRuntime` 切换会话时会替换整个 session 并重建服务（`sdk.md`）。理论上 SDK 宿主可以在一个进程里建多个 `AgentSession`，文档没有讨论这种用法（未核实）。安全文档默认“Pi 拥有启动它的那个账户的全部权限”，与多租户不相容（`security.md`）。
- **Pi Durable**：
  - 一个 Harness 承载多个 conversation，每个都有自己的 agent、provider 会话 ID、用量和收件箱（README “Multiple Conversations Per Process”）。
  - 多客户端：任意数量的客户端可以挂到一个 conversation 上；`watch()` 先给当前视图，再推每次提交的 Chord 操作；迟到或重连的客户端从当前视图开始，不回放历史；任何客户端都能 steer 或排队（Pi Durable 文章、README）。还有实验性的 `watchEvents()`（coding agent 风格事件，落后超过 100 批就发新快照）和 `watchTaskGraph()`。
  - **传输层不提供**：没有 HTTP/WebSocket，宿主把 `watch` 的操作自己发给远端（README 示例：`await send(ops)`）。
  - 多用户的组织方式由宿主决定。官方给出的形态是 Cloudflare Durable Object：一个 DO 持有一个 SQLite 存储，按 DO 粒度横向扩展。“一个 DO 对应一个用户”是从这一设计推出来的，官方没有这样写（未核实）。
  - 执行环境可以和 agent 进程分开：工具通过 `ExecutionEnv` 运行，`pi-env` 提供经 SSH 部署的远程守护进程；`edit`/`write` 按环境 `id` 和路径串行化（README L432–456）。

## 6. 对 Asterpath 的启示

### 值得借鉴

1. **外壳就是同一内核的不同调用方式。** pi 的 TUI、print、JSON、RPC 和 SDK 共用 `AgentSession` 与 `SessionManager`，数据互通靠同一种会话格式和同一个默认目录。这和“web、desktop、cli 都只是内核的不同展现形态”一致。RPC 用 stdin/stdout JSONL，加上扩展 UI 请求转发，是给非同语言宿主的最低成本接口。
2. **审批、沙箱、子智能体、plan mode 都放在内核外，但内核提供闸门。** pi 给出带 fail-closed 语义的 `tool_call` 闸门，沙箱交给 OS/容器。这与 ADR-0007、0019、0025 的方向一致。可以借鉴它把**处理器组合语义**（通知/变换/闸门/最后覆盖）写成一张表的做法。
3. **Pi Durable 的“单属主 + 只读观察者 + 宿主负责传输和唤醒”** 与 ADR-0036（显式认领、只读句柄、唤醒发现只返回无主会话、路由到属主是宿主的事）几乎同构。它的 `onWake` 把定时器交给宿主，适合 web 平台的进程驱逐和滚动重启，可以作为 Asterpath 唤醒接口的参照。
4. **effect sandwich + `replay: "safe"` + `requestId` 幂等** 是恢复语义的最小集合，可以拿来对照 Asterpath 的 `StreamResumed{Crash}` 和不安全工具中断规则（ADR-0006、0024）是否覆盖了“意图已提交、副作用可能已发生”这种情况。
5. **一个进程持有多个会话，用内存缓存加按空闲时间淘汰的文档**（`documentRetentionMs`），以及“已观察的文档不淘汰”，是单机承载大量会话的具体做法。
6. **watch = 当前视图 + 增量，迟到者不回放历史**，这是给 web/桌面多客户端的合适投影接口。它与“日志即真相、视图是折叠”（ADR-0023、0030）并不冲突。

### 应避免

1. **pi coding agent 的会话没有锁、没有属主。** CLI 与另一个外壳同时续写同一会话会交错追加。Asterpath 要求 CLI 与桌面端数据互通，就必须有 ADR-0036 那样的属主机制。不能照搬 pi “共享目录即互通”的做法。
2. **会话存储挂在“应用层”包里。** pi 的 `SessionManager` 在 `pi-coding-agent` 中，`pi-agent-core` 完全不持久化，结果 Pi Durable 不得不另起一套 loop，两个内核并存。Asterpath 已经把日志和存储放在内核（ADR-0002、0005），应该保持，不要退回到“loop 在内核、持久化在外壳”。
3. **按 `cwd` 编码目录、文件名即索引** 适合单机 CLI，不适合 web 多用户。Asterpath 用目录或宿主元数据（ADR-0032）更合适。
4. **CLI 与 SDK 的默认能力不同**（codemode、MCP 只在 CLI 默认加载）会让不同外壳的行为悄悄分叉。Asterpath 应让宿主显式装配，而且各外壳使用同一份装配。
5. **Pi Durable 的单属主是整库级的**（一个存储一个进程，DO 场景靠多个存储扩展）。Asterpath 的目标是单机一个进程服务上千用户、同时允许 CLI 与桌面端共用一个库，所以 ADR-0036 把粒度缩到会话树是必要的。Pi Durable 不能直接回答“同一个库多进程共存”这个问题。
6. **deferred commit 允许观察到未落盘的状态**（最多约 1 s）。如果 Asterpath 想做流式增量落盘，要像它那样明确写出丢失窗口，并限定只有哪些记录可以这样做；否则就保持“落盘后才可见”。
