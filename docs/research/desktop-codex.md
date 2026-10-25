# Codex 交互客户端设计调研（桌面端参考）

> 完成日期：2026-10-25。用途：为 Asterpath 的 Tauri 2 桌面客户端（ADR-0035）选取交互模型与核心功能子集，并找出内核门面的缺口。
>
> 一手来源：
> - 本机 `codex-cli 0.163.0`：`codex --help`、`codex app-server --help`、`codex app-server daemon --help`、`codex agents --help`、`codex remote-control --help`；用 `codex app-server generate-json-schema --out DIR` 导出的协议 JSON Schema（下称“Schema”）；`~/.codex` 的目录与文件结构（只看结构，没有抄录密钥和地址）。
> - `openai/codex` 仓库 main 分支，提交 `970111f`（2026-10-23）：`codex-rs/app-server/README.md`、`codex-rs/tui/src/slash_command.rs`、`codex-rs/tui/src/bottom_pane/*`（`approval_overlay.rs`、`footer.rs`、`chat_composer.rs`、状态栏项）。
> - OpenAI 官方文档（`developers.openai.com/codex/*` 已 308 跳转到 `learn.chatgpt.com/docs/*`）：[app-server](https://learn.chatgpt.com/docs/app-server)、[app](https://learn.chatgpt.com/docs/app)、[projects](https://learn.chatgpt.com/docs/projects)、[git-worktrees](https://learn.chatgpt.com/docs/environments/git-worktrees)、[permission-modes](https://learn.chatgpt.com/docs/permission-modes)、[code-review](https://learn.chatgpt.com/docs/code-review)、[CLI slash commands](https://learn.chatgpt.com/docs/cli/slash-commands)。文档页由抓取工具摘要后阅读，没有逐字核对。
>
> 没有实际打开桌面 App 操作过。只由二手报道支持的条目标“未核实”。

## 0. 结论摘要

- **Codex 的三种前端（TUI、VS Code 扩展、桌面 App）都是 app-server 的客户端**。app-server 是一个 JSON-RPC 2.0 双向协议服务：客户端发请求，服务端推通知，并通过**服务端发起的请求**向客户端要审批。一个 app-server 进程同时承载多个 thread，每个 thread 有自己的 `cwd`。
- 协议的核心对象只有三层：**Thread ⊃ Turn ⊃ Item**。Item 用 `item/started` → `item/*/delta` → `item/completed` 流式更新，`item/completed` 是最终状态。审批流程是：先发 `item/started`（待执行的命令或改动），再发服务端请求，客户端回复决定，服务端发 `serverRequest/resolved`，最后发 `item/completed`。
- 桌面 App 的组织方式是**项目（一个或多个本地文件夹）→ 会话**，多个项目可以并行运行。隔离靠 **Local / Worktree 两种模式**：Worktree 是 Codex 在 `$CODEX_HOME/worktrees` 下创建的 git worktree，可以和 Local 互相交接（handoff）。云端任务（Codex Cloud）是另一个产品面。
- 审阅以 **git 为准**：审阅面板显示整个仓库的 diff（未暂存、已暂存、提交、分支、上一回合），可以按整体、文件、hunk 暂存或还原，行内评论会作为反馈发回会话。`/review` 是独立的只读审阅器，审阅范围有四种。
- 对 Asterpath 的建议：桌面端第一版只做“单工作根 + 会话列表 + 对话流 + 审批卡片 + 中断 + 模型切换 + 用量 + git diff 面板”。主要门面缺口：事件只能由驱动者独享、没有回合内 steer 的明确语义、审批决定没有“本会话允许”、会话没有重命名或归档、没有按回合回退、事件里没有面向 UI 的 item 级“开始”与 diff 聚合。详见 §7。

## 1. 进程与架构

### 1.1 前端如何连接内核

| 前端 | 连接方式 | 出处 |
| --- | --- | --- |
| TUI | 默认连接**共享的本地 app-server 守护进程**；`--no-daemon` 改为不使用共享后台；`--remote ws://…|unix://…` 连接远程 app-server | `codex --help`（`--no-daemon`、`--remote`）；README 中有 “bundled, in-process TUI sessions (`codex-tui`)” |
| TUI 内部 | 审批 UI 直接使用协议类型 `CommandExecutionApprovalDecision`，可见 TUI 本身就是协议客户端 | `tui/src/bottom_pane/approval_overlay.rs` L836–899 |
| 桌面 App | 本地 stdio 会话，README 中称为 “local stdio desktop sessions (`Codex Desktop`)” | app-server README「User verification」节 |
| VS Code 扩展 | 启动 `codex app-server`；`--analytics-default-enabled` 的说明写着 “for first-party use cases like the VSCode IDE extension” | `codex app-server --help` |
| 其他 | `codex agents` 可以浏览“共享本地 app-server 守护进程上的全部 agent 会话”；`codex app-server proxy` 把 stdio 转发到运行中守护进程的控制 socket；`remote-control` 用于远程或手机配对 | 各子命令 `--help` |

本机观察（只看结构）：`~/.codex/app-server-control/app-server-control.sock` 是守护进程的控制 socket，`app-server-daemon/` 下有 updater 的 pid 和日志，`thread-writer-locks/` 按 thread 存放写者锁，`sessions/YYYY/MM/DD/rollout-*.jsonl` 是会话日志。另有 `session_index.jsonl`，每行是 `{id, thread_name, updated_at}`，同一个 id 改名时会追加新行。此外还有 `state_5.sqlite`、`thread_history_1.sqlite`、`queue_1.sqlite` 等 SQLite 状态库。

rollout 的行结构是 `{timestamp, ordinal, type, payload}`，`type` 的取值有 `session_meta`（含 `cwd`、`runtime_workspace_roots`、`originator`、`source`、`model_provider`、`history_mode`）、`turn_context`、`response_item`、`event_msg`（如 `task_started`、`item_completed`、`token_count`、`task_complete`）、`token_usage_record`。

### 1.2 传输与握手

- 传输方式：`--listen stdio://`（默认，按行分隔的 JSON）、`unix://[PATH]`、`ws://IP:PORT`、`off`。WebSocket 有 capability-token 和 JWT 两种认证（`codex app-server --help`）。官方文档写明线上省略 `"jsonrpc":"2.0"` 字段，WebSocket 标为 experimental。
- 握手：每个连接先发 `initialize` 请求，再发 `initialized` 通知（这是唯一的客户端通知，见 Schema `ClientNotification`）。重复 initialize 会报 `Already initialized`。可选 capability 包括 `experimentalApi` 和 `optOutNotificationMethods`（按方法名退订通知）。

### 1.3 方法与通知清单（Schema 统计：客户端请求 105 个，服务端通知 85 个，服务端请求 10 个）

与桌面客户端相关的子集如下：

- **thread**：`thread/start`、`thread/resume`、`thread/fork`、`thread/archive`、`thread/unarchive`、`thread/delete`、`thread/unsubscribe`、`thread/name/set`、`thread/list`、`thread/loaded/list`、`thread/read`、`thread/turns/list`、`thread/items/list`、`thread/revert`、`thread/compact/start`、`thread/shellCommand`、`thread/metadata/update`、`thread/goal/*`、`threadSection/*`（侧栏分组）、`thread/section/move`、`thread/attachment/*`（例如把 PR 关联到 thread）。
- **turn**：`turn/start`、`turn/steer`、`turn/interrupt`；**review**：`review/start`。
- **其他**：`model/list`、`permissionProfile/list`、`config/read`、`config/value/write`、`config/batchWrite`、`fuzzyFileSearch`（@ 文件补全）、`fs/*`（readFile、readDirectory、watch 等）、`command/exec*`（集成终端）、`skills/list`、`mcpServerStatus/list`、`account/rateLimits/read`、`account/usage/read`。
- **服务端通知**：`thread/started`、`thread/status/changed`、`thread/archived`、`thread/name/updated`、`thread/settings/updated`、`thread/tokenUsage/updated`、`thread/compacted`、`thread/closed`、`turn/started`、`turn/completed`、`turn/diff/updated`、`turn/plan/updated`、`item/started`、`item/completed`、`item/agentMessage/delta`、`item/reasoning/summaryTextDelta`、`item/reasoning/textDelta`、`item/commandExecution/outputDelta`、`item/fileChange/patchUpdated`、`item/mcpToolCall/progress`、`serverRequest/resolved`、`account/rateLimits/updated`、`error`、`warning`、`fs/changed`。
- **服务端请求**（需要客户端回复）：`item/commandExecution/requestApproval`、`item/fileChange/requestApproval`、`item/permissions/requestApproval`、`item/tool/requestUserInput`、`mcpServer/elicitation/request`、`item/tool/call`（客户端动态工具），以及旧版的 `execCommandApproval` 和 `applyPatchApproval`。

### 1.4 核心对象（Schema）

- **Thread**：`id`、`sessionId`、`name`、`preview`、`cwd`、`projectId`、`section`、`status`、`model`、`reasoningEffort`、`forkedFromId`、`parentThreadId`、`gitInfo{branch,sha,originUrl}`、`source`、`ephemeral`、`createdAt`、`updatedAt`、`recencyAt`、`turns[]`。
  - `ThreadStatus` 有四种：`notLoaded`、`idle`、`systemError`、`active{activeFlags}`；`activeFlags` 取 `waitingOnApproval` 或 `waitingOnUserInput`。**侧栏的“运行中 / 等你审批”标记就是这个字段。**
- **Turn**：`id`、`status`（`inProgress`、`completed`、`interrupted`、`failed`）、`items[]`、`error`、`startedAt`、`completedAt`、`durationMs`。
- **ThreadItem**（按 `type` 区分）：`userMessage`、`agentMessage`、`reasoning{summary,content}`、`plan`、`commandExecution{command,commandActions,cwd,aggregatedOutput,exitCode,status,durationMs}`、`fileChange{changes[{path,kind,diff}],status}`、`mcpToolCall{server,tool,arguments,result,error,status}`、`dynamicToolCall`、`collabAgentToolCall` 和 `subAgentActivity`（子 agent）、`webSearch`、`imageView`、`imageGeneration`、`enteredReviewMode` 和 `exitedReviewMode`、`contextCompaction`、`hookPrompt`、`sleep`。
- **UserInput**：`text`（带 `text_elements`）、`image`、`localImage{path}`、`audio`、`skill{name,path}`、`mention{name,path}`。
- **ThreadSettings**（随 `thread/settings/updated` 推送）：`cwd`、`model`、`modelProvider`、`effort`、`summary`、`approvalPolicy`、`approvalsReviewer`、`sandboxPolicy`、`activePermissionProfile`、`collaborationMode`。

### 1.5 事件流形态（官方文档）

- `turn/start` 的响应里 turn 为 `inProgress`。随后依次推送 `turn/started`、各 item 的 `item/started` / delta / `item/completed`，最后是 `turn/completed`（完成、中断或失败）。失败时先发 `error`，再发 `turn/completed{failed}`。
- `item/started` 携带完整 item，`item.id` 就是后续 delta 的 `itemId`。文档说明 `item/completed` 是权威状态。plan 的最终文本不一定等于各 delta 拼接的结果。
- `turn/diff/updated`：**整个回合聚合后的 unified diff**，每次文件改动都会重发全量。`turn/plan/updated`：步骤列表，每步状态为 `pending`、`inProgress` 或 `completed`。
- `thread/tokenUsage/updated`：`ThreadTokenUsage{total,last,modelContextWindow}`，其中分项为 `input`、`cachedInput`、`cacheWriteInput`、`output`、`reasoningOutput`、`total`（Schema）。

### 1.6 审批往返（官方文档 + Schema）

1. `item/started`：先给出待执行的 `commandExecution` 或 `fileChange` item。
2. 服务端请求 `item/commandExecution/requestApproval`，参数包括 `threadId`、`turnId`、`itemId`、`command`、`commandActions`（解析好、便于展示的动作）、`cwd`、`reason`、`networkApprovalContext`、`proposedExecpolicyAmendment`（建议的“以后同前缀命令免批”规则）、`proposedNetworkPolicyAmendments`。
3. 客户端回复 `decision`：
   - 命令执行：`accept`、`acceptForSession`、`acceptWithExecpolicyAmendment{…}`（持久规则）、`applyNetworkPolicyAmendment{…}`、`decline`、`cancel`。
   - 文件改动：`accept`、`acceptForSession`、`decline`（拒绝，回合继续）、`cancel`（拒绝并立即中断回合）。
   - 权限请求：`{permissions, scope: turn|session}`。
4. `serverRequest/resolved{threadId, requestId}`：请求已被回复，或被回合结束、中断清理。**多窗口时，其他窗口靠它撤下审批卡片。**
5. `item/completed`：`status` 为 `completed`、`failed` 或 `declined`。

TUI 的审批选项文案（`approval_overlay.rs`）：“Yes, proceed / Yes, just this once”“Yes, and don't ask again for commands that start with `<prefix>`”“Yes, and don't ask again for this command in this session”“Yes, and allow this host for this conversation”“No, and tell Codex what to do differently”。另有 `approvalsReviewer`：`user` 或 `auto_review`，后者由一个子 agent 代为审批（Schema `ApprovalsReviewer`）。

### 1.7 一个进程服务多少工作目录 / 会话

- **多个**。`thread/loaded/list` 返回当前进程已加载的多个 thread id。`thread/start`、`thread/resume`、`thread/fork` 都可以单独指定 `cwd`，`turn/start` 也能覆盖 `cwd`（Schema）。`thread/list` 的 `cwd` 过滤接受字符串或数组（`ThreadListCwdFilter`）。
- 订阅是**按连接**的：每个连接各自订阅 thread。`thread/unsubscribe` 之后，如果没有订阅者、也没有活动超过 30 分钟，thread 才卸载（官方文档）。所以关闭窗口不会中断后台任务。
- 存储由守护进程独占，`thread-writer-locks/` 是按 thread 的写者锁（本机结构观察）。CLI、TUI 和 App 通过连接**同一个守护进程**共享会话，而不是各自打开同一份存储。

## 2. 工作目录 / 项目模型

- **会话归属于 cwd**：`Thread.cwd` 和 `session_meta.cwd` 记录会话的工作目录，另有 `runtime_workspace_roots`、`--add-dir`（额外可写目录）、`/cd`（会话中途改 cwd）。项目信任按目录记录在 `config.toml` 的 `[projects."<path>"] trust_level`（本机结构）。没有项目根标记的目录不会持久化信任（README「Project trust」）。
- **桌面 App 的项目**：本地项目可以包含**一个或多个本地文件夹**，新会话从主文件夹开始，可以添加次文件夹；远程项目目前只支持一个文件夹。项目和会话都可以置顶，可以把整个项目的会话一起归档（[projects](https://learn.chatgpt.com/docs/projects)）。协议里有 `Thread.projectId` 和 `project/changed`、`thread/project/updated` 通知（Schema）。
- **多项目并行**：App 的定位是 “Run projects in parallel … from one desktop workspace”（[app](https://learn.chatgpt.com/docs/app)）。
- **git worktree 隔离**（[git-worktrees](https://learn.chatgpt.com/docs/environments/git-worktrees)）：
  - 新会话在输入框下方选择 Local 或 Worktree。Worktree 从所选分支的 HEAD 创建，处于分离头状态（不是具名分支），位于 `$CODEX_HOME/worktrees`。
  - **Handoff** 用来在 Local 和 worktree 之间搬运会话和代码。`.gitignore` 忽略的文件只有列在 `.worktreeinclude` 里才会带过去。点 “Create branch here” 会创建 `codex/` 前缀的分支。
  - 默认保留最近 15 个托管 worktree；置顶的、运行中的、永久的 worktree 不会自动删除。删除前会保存快照，重新打开会话时可以恢复。
  - CLI 也有 `--worktree` 和 `/worktree`（`codex --help`、slash_command.rs）。
- **本地与云端**：Codex Cloud 是独立的产品面（“parallel cloud environments”）。CLI 的 `codex cloud` 可以浏览云任务并把改动应用到本地，`codex apply` 把最近一次 diff 用 `git apply` 应用到本地。桌面 App 是本地交互式的入口（[app](https://learn.chatgpt.com/docs/app)、`codex --help`）。
- **发布时间**：macOS 版本于 2026-02-02 发布，宣传点为多 agent 并行、worktree、skills、automations（**未核实**，来源是二手报道，例如 [alternativeto](https://alternativeto.net/news/2026/2/openai-unveils-a-dedicated-codex-desktop-app-for-macos-and-anyone-can-try-it-for-free)）。现在官方文档把桌面端统称为 “ChatGPT desktop app”，支持 macOS、Windows 和 Linux。

## 3. 会话生命周期

| 动作 | 协议 | 语义要点 |
| --- | --- | --- |
| 新建 | `thread/start{cwd, model, sandbox, approvalPolicy, ephemeral, …}` | `ephemeral` 表示只存在内存中。返回 thread 和生效的设置 |
| 恢复 | `thread/resume{threadId, excludeTurns, 覆盖项…}` | 恢复本身不更新 `updatedAt`；可以同时覆盖 model、cwd、sandbox |
| 分叉 | `thread/fork{threadId, lastTurnId, ephemeral}` | 用 `lastTurnId` 截断到某个回合，不能截在进行中的回合。新 thread 带 `forkedFromId` |
| 回退 | `thread/revert{threadId, beforeTurnId}` | 用来取代已删除的 `thread/rollback`（README「Thread rollback」） |
| 归档 | `thread/archive` / `unarchive` | 把日志移到 archived 目录，并尝试一起归档子 thread；`thread/list` 默认不显示已归档的 |
| 删除 | `thread/delete` | 永久删除，连同所有后代 |
| 命名 | `thread/name/set` → `thread/name/updated` | 本机 `session_index.jsonl` 每次改名追加一行 |
| 读取 | `thread/read{includeTurns}`、`thread/turns/list`、`thread/items/list` | 历史分页，支持用 item 锚点作游标（README「Item history anchors」） |

- **列表与过滤**：`thread/list` 的参数有 `archived`、`cwd`（单个或多个）、`searchTerm`、`sourceKinds`（cli、vscode、exec、appServer、subAgent…）、`modelProviders`、`sectionId`、`excludedThreadIds`、`sortKey`（`created_at`、`updated_at`、`recency_at`、`section_position`）、`sortDirection`，并用游标分页（Schema）。另有实验性的 `readState{firstUnread, revision}`，用于未读标记（README「Thread read state」）。
- **后台任务的呈现**：侧栏读 `ThreadStatus.active.activeFlags` 判断“运行中、等审批、等输入”。`thread/status/changed` 实时推送状态。`codex agents` 是 TUI 里的“agent 指挥中心”，浏览守护进程上的全部会话。`/ps` 和 `/stop` 管理后台终端。App 还有 automations（定时任务）（[app](https://learn.chatgpt.com/docs/app)）。

## 4. 交互 UI

### 4.1 对话流 item 的呈现

- 消息：`agentMessage` 流式输出 Markdown。推理：`reasoning.summary` 和 `content`，有单独的 delta 通知，客户端可以折叠。
- 命令：`commandExecution` 带 `commandActions`（把命令解析成读文件、搜索、列目录等友好动作）、输出流、退出码、耗时。
- 文件改动：`fileChange.changes[]` 中每个文件带 `kind` 和 unified diff；回合级的聚合 diff 由 `turn/diff/updated` 给出。
- MCP：`mcpToolCall` 带参数、结果、错误和进度通知；可以内嵌 MCP App UI（`mcpAppUi`，inline 或 fullscreen）。
- 计划：`turn/plan/updated` 的步骤清单，以及 plan 模式（`/plan`）下的 `plan` item。
- 子 agent、网页搜索、图片、压缩、审阅模式的进入与退出各自对应独立的 item 类型（§1.4）。

### 4.2 审批 UX：审批策略 × 沙箱

- 两个独立的轴。沙箱 `-s`：`read-only`、`workspace-write`、`danger-full-access`。审批 `-a`：`untrusted`、`on-request`、`never`，以及 `granular`（Schema `AskForApproval`；CLI help 只列出了 `on-request` 和 `never`）。审批人 `approvalsReviewer` 可以是 `user` 或 `auto_review`。
- App 和 IDE 在输入框下方提供“权限模式”菜单，有 Ask for approval、Approve for me（`workspace-write` + `on-request` + 自动审阅）、Full access、Custom（取自 config.toml）四种；CLI 用 `/permissions`。文档原话：“Changing who reviews a request doesn't expand the sandbox”（[permission-modes](https://learn.chatgpt.com/docs/permission-modes)）。另有 `permissionProfile/list` 列出权限 profile（Schema）。
- 允许的粒度有四级：单次、本会话（`acceptForSession`）、持久前缀规则（execpolicy amendment）、持久网络规则（§1.6）。

### 4.3 输入框

- 斜杠命令（slash_command.rs，约 60 个；以下是与桌面端相关的）：`/model`、`/permissions`、`/new`、`/resume`、`/fork`、`/rename`、`/archive`、`/delete`、`/compact`、`/review`、`/diff`、`/mention`、`/cd`、`/status`、`/usage`、`/plan`、`/goal`、`/init`、`/mcp`、`/skills`、`/worktree`、`/app`（把当前会话转到桌面 App 继续）、`/side`和`/btw`（临时的旁支对话）、`/export`、`/copy`、`/ps`、`/stop`。
- `@` 文件引用：输入框以 `@` 为前缀触发文件补全（`chat_composer.rs` L2164、L2482、L2667），后端是 `fuzzyFileSearch` 请求及其 session 通知；结果作为 `UserInput.mention{name,path}` 提交。`$` 前缀插入 skill 或 app（slash-commands 文档 `/apps`）。
- 图片：`UserInput.image` 和 `localImage`，CLI 用 `-i/--image`。粘贴图片的具体交互**未核实**。
- **中断与 steer**：`turn/interrupt{threadId, turnId}` 让回合以 `interrupted` 结束。`turn/steer{threadId, expectedTurnId, input}` 把输入**追加到正在进行的回合**，不新开回合；`expectedTurnId` 不匹配或没有活动回合时失败（官方文档）。TUI 中运行时按 Tab 把消息排到下一回合（`footer.rs` 的 queue hint；文档：“type a slash command and press Tab to queue it”），协议里有对应的 `thread/queue/changed`。Esc 中断、Enter 发送即 steer 的键位**未核实**。

### 4.4 状态栏

TUI 的 `/statusline` 可选项（`StatusLineItem::*`）：ModelName、ModelWithReasoning、Reasoning、ApprovalMode、Permissions、ContextUsed、ContextRemaining、ContextWindowSize、UsedTokens、TotalInputTokens、TotalOutputTokens、FiveHourLimit、WeeklyLimit、EstimatedThreadCost、GitBranch、BranchChanges、PullRequestNumber、CurrentDir、ProjectRoot、SessionId、ThreadName、TaskProgress、FastMode、CodexVersion 等。选择结果持久化到 `tui.status_line`。速率限制的数据来自 `account/rateLimits/read` 和 `…/updated`，即 `RateLimitSnapshot{primary,secondary,credits,…}`（Schema）。

## 5. 配置

- 模型和推理档位：`model/list` 列出模型目录（含各模型支持的 effort），`/model` 同时选模型和档位。`turn/start{model, effort, summary}` 可以**按回合覆盖**，设置变化通过 `thread/settings/updated` 推送（Schema）。
- profiles：`-p/--profile <name>` 在基础配置之上叠加 `$CODEX_HOME/<name>.config.toml`（`codex --help`）。`-c key=value` 是通用的覆盖方式。
- 权限预设：§4.2 的权限模式菜单和 `permissionProfile/list`。
- UI 能改什么：协议提供 `config/read`、`config/value/write{keyPath, value, mergeStrategy, expectedVersion}`、`config/batchWrite`，可以直接写 `config.toml`，并用 `expectedVersion` 做乐观并发（Schema）。`/debug-config` 显示配置层来源，`/experimental` 切换特性开关，`/statusline`、`/theme` 等写回 TUI 偏好。

## 6. diff 与审阅

- **审阅面板**（[code-review](https://learn.chatgpt.com/docs/code-review)）：反映**整个 git 仓库的状态**，而不只是 Codex 改过的部分。视图有 Unstaged（默认）、Staged、Commit、Branch（相对基线分支）、Last turn。可以按整体、文件、hunk 三级执行暂存或还原（Stage all / Revert all）。在某一行点 “+” 可以写行内评论，评论汇总后作为消息发回会话，Codex 把它当作审阅意见。审阅面板要求项目在 git 仓库中。
- **`/review`**：一个独立的审阅器，只报告按优先级排序的问题，不改工作区。范围有四种：基线分支、未提交改动（含未跟踪文件）、某个提交、自定义指令；IDE 只支持前两种。协议是 `review/start{threadId, target, delivery: inline|detached}`，产出 `enteredReviewMode` 和 `exitedReviewMode` 两个 item。CLI 有无头的 `codex review`。
- **撤销**：文件层面靠 git 还原（审阅面板）；会话层面用 `thread/revert{beforeTurnId}`；CLI 还有 `codex apply` 把 diff 应用到工作区。
- worktree 删除前会保存快照，重新打开会话可以恢复（§2）。

## 7. 对 Asterpath 桌面端的启示

### 7.1 值得采用的核心子集

1. **前端只当客户端，所有状态来自内核**。Codex 的 TUI 同样是协议客户端。Tauri 的 Rust 侧持有 `Asterpath`，前端只接收序列化的 `AgentEvent` 和查询结果。这和 ADR-0035 的“前端手写 TS 类型、忽略未知 `type`”一致，也和 Codex 的“`item/completed` 为准、delta 只用于展示”是同一个原则：Asterpath 用 `projection()` 或日志重建，事件只做展示。
2. **三层模型 Session ⊃ Turn ⊃ 条目**。直接对应 `TurnStarted`、`ModelDelta`、`ToolStarted`、`ToolFinished`、`TurnCommitted`。UI 以“工具调用卡片”为单位，按工具名映射成命令、文件改动、MCP 等不同呈现。
3. **侧栏状态三态**：运行中、等审批、空闲。Codex 用 `activeFlags`，Asterpath 可以用 `facts().state()` 加上 `approval_gate().pending`。
4. **审批卡片**：显示工具、参数、副作用（`side_effect`）、来源（`provenance`），提供“允许”“拒绝并说明理由”两个按钮。拒绝理由回传给模型，对应 Codex 的 “No, and tell Codex what to do differently”。
5. **中断**用 `cancel()`。**新建、打开、分叉**用 `create_session`、`open_session`、`fork_session`。**会话列表**用 `sessions()`，按 `last_activity` 排序，标题来自宿主元数据。
6. **模型和推理档位切换**用 `update_config(ConfigPatch{model, request})`，与 Codex 的 `/model` 一样作用于下一步。**状态栏**显示模型、上下文占用、本会话 token 和费用（`usage()`）。
7. **diff 面板以 git 为准**。由宿主读工作区的 `git diff`，提供未暂存和上一回合两个视图，按文件或 hunk 还原。这完全是宿主功能，不需要内核参与。行内评论也只是拼成一条用户消息提交。
8. **@ 文件补全和图片输入**都是宿主侧的功能：在工作根内做模糊搜索，提交时转成 `Part` 文本或文件引用；图片转成图片 `Part`（ADR-0031 的文件输入）。

暂不采用（属于产品功能，或与 ADR-0035 冲突）：多项目并行和 worktree handoff、云任务、automations、共享守护进程或远程控制、旁支对话、goal、section 分组、未读状态、权限 profile 编辑器、插件市场、语音、宠物。

### 7.2 可能暴露的门面缺口（只记录，不在桌面端开后门）

| # | 缺口 | Codex 对应 | 说明 |
| --- | --- | --- | --- |
| G1 | **会话级工具根**：一个 `Asterpath` 只服务一个工作根 | `thread/start{cwd}`，一个进程承载多个 cwd | ADR-0035 已记录。这是多项目并行和 worktree 隔离的前提 |
| G2 | **事件扇出与重新订阅**：`drive()` 的事件流只给驱动者一个人；视图切走再切回时，正在运行的回合缺少“当前进行中”的快照 | 按连接订阅 thread，`thread/read` 加增量通知 | 宿主可以在后台任务里自己广播。不过“已提交的前缀 + 未提交的进行中部分”目前只能靠宿主缓存事件拼起来。需要核实 `projection()` 是否包含未提交的步骤 |
| G3 | **回合内 steer**：`submit_input` 在回合进行中提交时，究竟是并入下一步还是排到下一回合，门面文档没有写明 | `turn/steer{expectedTurnId}` 与排队（`thread/queue/changed`）是两种不同语义 | 需要核实，并写入文档或 ADR。UI 要据此决定运行中的输入框怎么表现 |
| G4 | **审批粒度**：`ApprovalDecision` 只有 `Allow` 和 `Deny{reason}` | `acceptForSession`、持久前缀规则、`cancel`（拒绝并中断） | “本会话允许”可以由宿主的审批策略记住，属于宿主策略，按 framework-scope 应归宿主。“拒绝并中断”可以组合 `approve(Deny)` 加 `cancel()`。结论：多半不是门面缺口，但需要在宿主层写明 |
| G5 | **会话改名、归档、删除** | `thread/name/set`、`archive`、`delete` | 元数据只在 `SessionCreated` 时记录（ADR-0032），之后不能修改。归档有 ADR-0016 的接缝，但门面上没有调用入口 |
| G6 | **按回合回退或从某回合分叉** | `thread/revert{beforeTurnId}`、`thread/fork{lastTurnId}` | `fork_session` 只能从最后一个已提交回合分叉（ADR-0033）。“回到第 N 轮重试”需要一个能指定位置的 fork |
| G7 | **会话列表的过滤与搜索** | `thread/list{searchTerm, archived, sortKey, cwd}` | `SessionQuery` 只支持 parent 过滤和游标。单工作根时够用；标题搜索可以由宿主在拿到的页上过滤 |
| G8 | **上下文窗口占用** | `ThreadTokenUsage.modelContextWindow` 与 `last` | 状态栏需要“已用 / 窗口大小”。窗口大小来自模型或 provider 目录，最近一次请求的输入 token 来自 `usage()`。需要核实门面能否取得窗口大小 |
| G9 | **面向 UI 的回合 diff 与计划** | `turn/diff/updated`、`turn/plan/updated` | Asterpath 不应该内建，由宿主用 git 或工具结果自行计算。列出这一项只是为了明确**不是**缺口 |
| G10 | **跨进程共享会话** | 守护进程 + 多客户端连接同一存储 | ADR-0035 选择了独立数据库，CLI 和桌面端互不可见。要合并只能走“一个写者进程加 IPC”，而这正是 Codex 的守护进程方案。现在只记下这条路径，不实施 |
