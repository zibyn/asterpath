# Claude Code 交互客户端设计调研（供桌面端参考）

> 完成日期：2026-10-25。用途：为 Asterpath 的 Tauri 2 桌面参考宿主（ADR-0035）取舍交互设计，并倒推门面缺口。
>
> 一手来源：
> - 本机 `Claude Code 2.1.310`：`claude --help`、`claude agents|attach|logs|mcp|plugin --help`；`~/.claude/` 目录结构与会话 JSONL 的记录类型和字段名（只看结构，未读内容，未读密钥）；`~/.local/share/claude/versions/2.1.310` 二进制里的字符串（只用于确认控制协议的消息名）。
> - 官方文档 code.claude.com/docs/en/：`desktop`、`interactive-mode`、`permission-modes`、`permissions`、`sessions`、`checkpointing`、`settings`、`sub-agents`、`commands`、`statusline`、`headless`、`agent-view`、`worktrees`、`claude-code-on-the-web`、`agent-sdk/overview`、`agent-sdk/typescript`、`agent-sdk/user-input`。下文用 `[页面名]` 标出处。
>
> 文档没写、由二进制字符串或本机目录推断的条目标“未核实”。

## 0. 结论摘要

- **内核只有一个**：终端 TUI、IDE 扩展、桌面 App 的 Code 标签页、Agent SDK 都在跑同一个 Claude Code 程序。SDK 和桌面端把它当子进程用，通过 stdio 上的 `stream-json` 收发消息，并用 `control_request` / `control_response` 做双向控制（审批、中断、切模式、切模型）。
- **一个进程只跑一个会话、一个主工作目录**。要并行就开多个进程：多开终端、`--bg` 交给 supervisor、桌面端用侧栏开多个会话。工作目录也决定会话存在哪：`~/.claude/projects/<路径转义>/<session>.jsonl`。
- 桌面 App 的形态是**侧栏会话列表 + 对话流 + 会话侧栏卡片（Changes / Context / Environment）+ 可拆分面板（diff、终端、文件、计划、任务）**。多个会话并行，worktree 隔离默认关闭、按会话可选；运行环境分本地、SSH、云端三类。
- **对 Asterpath 最值得采用的核心子集**：
  1. 会话列表按工作根过滤，显示名字或自动标题、最后活动时间；
  2. 对话流里工具调用默认折叠，Edit/Write 展示 diff，另有一个可以展开思考与全部步骤的视图模式；
  3. 审批卡片提供“允许一次 / 本会话允许 / 拒绝并附说明”；
  4. 运行中允许继续输入（steering），并提供停止按钮；
  5. 模型选择器、上下文占用环、手动 compact；
  6. 分叉会话。
- **门面缺口**主要在会话目录上：创建后不能改名或补标题，不能按元数据过滤，不能从指定回合分叉或回退。另外两项待核实：事件里能否拿到子智能体的归属，以及上下文占用的分母（窗口大小）从哪里来。审批、steering、取消、compact、改配置这些已经够用。

## 1. 进程与架构

| 端 | 与内核的关系 | 出处 |
| --- | --- | --- |
| 终端 TUI | `claude` 进程本身；无参数进入交互，`-p` 无头 | `claude --help` |
| Agent SDK（TS/Python） | “A library that runs the Claude Code binary”；SDK 拉起 CLI 子进程，可用 `spawnClaudeCodeProcess` 自定义拉起方式（`SpawnOptions{command,args,cwd,env,signal}`）；关闭时“first closes the process's stdin and waits about two seconds” | [agent-sdk/overview] [agent-sdk/typescript] |
| 桌面 App Code 标签页（本地会话） | “Desktop runs the same underlying engine with a graphical interface”；“the Code tab runs its own copy of Claude Code”，由桌面 App 下载和更新，版本可以与终端里的 `claude` 不同 | [desktop] |
| 桌面 App SSH 会话 | 远端装一个 helper（`~/.claude/remote/srv/`）和一份 Claude Code（`~/.claude/remote/ccd-cli/`）；桌面端经 SSH 连 helper 的 Unix socket `rpc.sock`，helper 再为每个会话启动 Claude Code 进程 | [desktop] |
| 云端会话（web / 桌面 Cloud / `--cloud`） | “each session runs in an isolated, Anthropic-managed VM”，在 VM 里新 clone 仓库，关掉笔记本也继续跑 | [claude-code-on-the-web] |
| 后台会话（`--bg`、agent view） | 一个 supervisor 后台服务，“Each session is its own Claude Code process under the supervisor”；状态在 `~/.claude/daemon/roster.json`、`~/.claude/jobs/<id>/state.json` | [agent-view]，本机目录 |

**stream-json 协议**（`-p --input-format stream-json --output-format stream-json`）：

- 输出是逐行 JSON，`type` 有 `system`（子类型 `init` / `compact_boundary` / `api_retry` / `permission_denied` / `task_started` / `task_progress` / `session_state_changed` 等）、`assistant`、`user`、`stream_event`（需 `--include-partial-messages`）、`result`。`result` 带 `total_cost_usd`、`usage`、`modelUsage`、`permission_denials`。[headless] [agent-sdk/typescript]
- `system/init` 带 `cwd`、`tools`、`model`、`permissionMode`、`slash_commands`、`mcp_servers`、`skills`、`capabilities`。`capabilities` 用于做特性探测，例如 `interrupt_receipt_v1`。[agent-sdk/typescript]
- 子智能体的消息也走主流，用 `parent_tool_use_id` 指向发起它的 Agent `tool_use`；主对话里这个字段是 `null`。[headless]
- `session_state_changed` 的 `state` 取 `idle` / `running` / `requires_action`，其中 `requires_action` 表示等待宿主回答，例如审批。需要设置 `CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS=1` 才发。[agent-sdk/typescript]

**控制协议**（未核实：公开文档没有给出线格式）：

- 二进制里有 `type:"control_request"` / `type:"control_response"`，请求子类型包括 `initialize`、`can_use_tool`、`interrupt`、`set_permission_mode`、`set_model`、`rewind_files`、`get_context_usage`、`stop_task`、`hook_callback`、`mcp_*`。应答形如 `{type:"control_response",response:{subtype:"success"|"error",request_id,...}}`。（本机二进制字符串）
- 这和 SDK 的 `Query` 方法一一对应：`interrupt()`、`setPermissionMode()`、`setModel()`、`rewindFiles()`、`getContextUsage()`、`stopTask()`、`reinitialize()`。`canUseTool` 回调返回 `null` 时，要求宿主“has already sent the `control_response` itself, echoing `requestId`”。这说明审批回调确实走同一条控制通道。[agent-sdk/typescript]
- 审批的方向是内核向宿主发 `can_use_tool`、宿主回应答，属于反向 RPC；中断、切模式则是宿主向内核发请求。

**一个进程服务多少会话**：一个进程同一时刻只有一个会话、一个主工作目录。

- `/resume`、`/clear`、`/branch` 会切换当前进程写入的会话：“`/branch` copies the transcript and switches the running Claude Code process to write to it”。[sessions]
- 主工作目录可以用 `/cd` 移动。会话的存储随目录一起迁到新目录的项目存储下。[permissions] [sessions]
- 并行靠多进程。同一会话被两个进程同时 resume 时，“messages from both interleave into one transcript”，没有写者锁。[sessions] [desktop]

## 2. 工作目录与项目模型

- **存储按目录分**：`~/.claude/projects/<project>/<session-id>.jsonl`，`<project>` 是 cwd 路径把非字母数字字符替换成 `-`。[sessions] 本机有 9 个项目目录，`-home-zibyn-Projects-asterpath` 下有 163 项。
- **会话旁边的附属目录**：
  - `<session>/tool-results/`：大工具结果落盘（本机目录）；
  - `<session>/subagents/agent-<id>.jsonl` 与 `.meta.json`（字段 `agentType`、`description`、`toolUseId`、`spawnDepth` 等）（本机目录，[sub-agents]）；
  - `~/.claude/file-history/<session>/<hash>@vN`：checkpoint 的文件快照（本机目录，未核实为其唯一用途）。
- **JSONL 是树结构**（本机观察）：每行带 `uuid` / `parentUuid`、`sessionId`、`cwd`、`gitBranch`、`isSidechain`、`timestamp`、`version`。记录类型有：
  - 对话：`user`、`assistant`（内容块为 `text` / `thinking` / `tool_use` / `tool_result`）；
  - 系统：`system`（`turn_duration`、`local_command`）；
  - 其他：`file-history-snapshot`、`attachment`、`ai-title`、`last-prompt`、`mode`。

  官方声明这个格式是内部格式，“changes between versions”。脚本应改用 `/export`、`-p --output-format json` 或 SDK 的 `listSessions` / `getSessionMessages`。[sessions]
- **会话选择器的范围**：默认是当前 worktree，`Ctrl+W` 扩到同仓库的所有 worktree，`Ctrl+A` 扩到全机，`Ctrl+B` 只看当前分支。选到别的项目的会话时，Claude 会把 `cd ... && claude --resume` 复制到剪贴板，不在当前进程打开。[sessions]
- **桌面端**：每个会话“has its own chat history and project folder”，用 prompt 框下方的控件选文件夹；侧栏 `+ New` 开新会话并行，可并排或单独开窗口。[desktop]
  - Worktree 隔离默认关，开启后放在 `<project-root>/.claude/worktrees/`。
  - 桌面端和 CLI 各有自己的会话列表，但读同一套 settings 和 CLAUDE.md。CLI 会话可以用 `/desktop` 或 `claude --desktop --resume <id>` 带进桌面端，“Desktop continues the same session rather than a copy”。[desktop]
- **本地 / SSH / WSL / 云端**由会话创建时选的环境决定。云端会话以“仓库”代替“文件夹”，结果推到分支或 PR 上；终端用 `--teleport` 拉回本地，“The terminal gets its own copy of the session”。[desktop] [claude-code-on-the-web]
- **CLI 的 worktree**：`-w/--worktree <name>` 在 `.claude/worktrees/<name>/` 建分支 `worktree-<name>`，退出时干净就自动删，有改动就问保留还是删除。子智能体可以在 frontmatter 写 `isolation: worktree`。[worktrees]

## 3. 会话生命周期

| 动作 | Claude Code | 出处 |
| --- | --- | --- |
| 新建 | `claude`；会话内 `/clear`（旧会话保留，可 resume） | [sessions] |
| 续最近 | `-c/--continue`，限当前目录；跳过 `-p` / SDK / `/loop` 起的会话 | [sessions] |
| 选择器 | `-r/--resume [id\|name\|搜索词]`、`/resume`。每行显示名字或 AI 标题或首条 prompt、距最后活动时间、git 分支、文件大小；`Space` 预览，`Ctrl+R` 改名 | [sessions] |
| 命名 | `-n`、`/rename`；未命名时用小模型根据首条 prompt 生成标题，接受 plan 时改用 plan 标题 | [sessions] |
| 分叉 | `/branch [name]`（同进程，保留“本会话允许”的授权）；`--fork-session`（新进程，授权不带过去）；`/fork` 分叉成后台会话 | [sessions] [agent-view] |
| 回退 | `/rewind` 或空输入时按两次 `Esc`。按用户 prompt 列出 checkpoint，可选：恢复代码和对话 / 只恢复对话 / 只恢复代码 / 从这里开始摘要 / 摘要到这里为止 | [checkpointing] |
| checkpoint 范围 | 每条开启回合的 prompt 一个；只跟踪文件编辑工具的改动，不跟踪 Bash 的改动（除非文件已被编辑工具改过）；子智能体的改动通常不恢复；保留最近 100 个；回合中途插入的消息没有自己的 checkpoint | [checkpointing] |
| 恢复内容 | 对话、被中断的工具调用（标为 cut off，不重跑）、模型、permission mode（有规则）。后台 bash 不恢复 | [sessions] |
| 后台 bash | `Ctrl+B` 把运行中的 Bash 或子智能体转后台；超时自动转后台；输出写文件，可用 Read 读；进程退出时清理 | [interactive-mode] |
| 后台会话 | `--bg`、`claude agents`（分组：Ready for review / Needs input / Working / Completed），`attach`、`logs`、`stop`、`rm`；需要审批时显示 Needs input，必须 attach 进去回答 | `claude --help` [agent-view] |
| 删除 | `claude purge [path]` 清整个项目；默认按 `cleanupPeriodDays`（30 天）自动清理 | `claude --help` [sessions] |

## 4. 交互 UI

**消息流**

- 终端：工具调用默认折叠，例如 MCP 调用合并成“`Called slack 3 times`”。`Ctrl+O` 打开 transcript viewer，展开全部工具细节，并显示每条消息的时间戳和模型。[interactive-mode]
- 桌面：三种 view mode，用 `Ctrl+O` 循环切换。[desktop]
  - Normal：“Tool calls collapsed into summaries, with full text responses”；
  - Thinking：在 Normal 基础上显示思考；
  - Verbose：显示每次工具调用和每个中间步骤。
- 子智能体调用在流里显示为 `name(任务描述)` 一行。[sub-agents]

**diff**

- 桌面：Changes 卡片显示 `+12 -1`，点开是逐文件 diff，可以逐行评论后用 `Cmd/Ctrl+Enter` 一次提交给 Claude。Manual 模式下审批编辑时也先展示 diff。[desktop]
- 终端：`/diff` 面板列出文件和增删行数，可按回合（`T1`、`T2`…）只看那一回合的编辑，`Current` 看工作树。回合视图“come from Claude's file edits rather than from git”。[interactive-mode]

**todo 列表**：`Ctrl+T` 开关，最多显示 5 项，状态分 pending / in progress / complete；跨 compact 保留。它和 `/tasks`（后台 shell 与子智能体）是两回事。[interactive-mode]

**思考**：终端 `Alt+T` 开关扩展思考（新模型总是开着）；桌面在 Thinking 或 Verbose 视图里显示。[interactive-mode] [desktop]

**审批对话框**

- 终端 Bash 审批的选项是：`Yes` / `Yes, and don't ask again for: npm test *` / `Yes, and switch to auto mode` / `No`，底部提示 `Esc` 取消、`Tab` 附加说明。[permissions]
- 在 Yes 或 No 上按 `Tab` 可以附评论：选 Yes 时，评论在工具结果之后发给 Claude；选 No 时，评论作为拒绝理由，Claude 继续工作。选 No 又不写评论，会停止当前回合。[permissions]
- “don't ask again”的落盘按工具类型分。[permissions]

  | 工具类型 | 授权范围 | 落在哪里 |
  | --- | --- | --- |
  | Bash、WebFetch 域名 | 永久，按仓库 | 写入仓库根的 `.claude/settings.local.json` |
  | 文件修改 | 到会话结束 | 不落盘 |

- 只有当提示能完整展示一条规则会放行什么时，才提供“记住”选项；否则只能允许一次。[permissions]
- 桌面：外部站点审批卡是 `Allow once` / `Always allow` / `Deny`，应用访问是 `Allow for this session` / `Deny`。[desktop]
- SDK 视角：`canUseTool` 返回 `allow`（可带 `updatedInput` 改参数、`updatedPermissions` 记规则），或返回 `deny`（带 `message`，可选 `interrupt`）。规则可写到 `userSettings` / `projectSettings` / `localSettings` / `session` / `cliArg`。`AskUserQuestion`（问用户选择题）也走同一个回调。[agent-sdk/user-input] [agent-sdk/typescript]

**权限模式**

| 模式 | 不问就执行的范围 |
| --- | --- |
| `default`（UI 叫 Manual） | 只有读 |
| `acceptEdits` | 读、文件编辑、常见文件系统命令 |
| `plan` | 只读探索并写 plan；批准前不改源码 |
| `auto` | 由分类器审查后执行 |
| `dontAsk` | 凡是会提示的一律拒绝 |
| `bypassPermissions` | 全部，只建议在容器里用 |

- deny 规则在任何模式下都生效。[permission-modes]
- `Shift+Tab` 的循环顺序是 `default → acceptEdits → plan`，然后（若启用）`bypassPermissions`、`auto`；`dontAsk` 不在循环里。[permission-modes]
- 状态栏会显示当前模式，例如 `⏸ plan mode on`。[permission-modes]
- 桌面端的模式选择器在 prompt 框下方，按文件夹记住所选模式；只有 plan 例外，只对当前会话有效。[desktop]
- plan 完成时给三个选项：批准并进入 auto（或 acceptEdits）、批准但手动审编辑、继续规划。[permission-modes]

**输入**

- `@` 触发文件路径补全；`Ctrl+V` 粘贴图片，插入 `[Image #N]`。[interactive-mode]
- 桌面还支持附件（图片、PDF）、拖放、右键路径选“Attach as context”。[desktop]
- `!` 进入 shell 模式。[interactive-mode]

**中断与排队（steering）**

- `Esc` 中断当前回复或工具调用，“Claude keeps the work done so far”。[interactive-mode]
- 运行中按 Enter，消息进入排队，在对话里以灰色显示：[interactive-mode]
  - 普通消息“as soon as those tool calls finish, within the same turn”送达；
  - 命令则等回合结束再执行；
  - `Ctrl+Enter` 立即发送；
  - 在输入框首行按 `Up` 可以把排队的消息撤回到输入框。
- 桌面端：停止按钮立即中断；也可以直接输入纠正，“Claude reads the correction as soon as the current action completes”。[desktop]

**状态栏 / 用量**

- 终端可以用 `statusLine` 脚本自定义状态栏，输入 JSON 包括：[statusline]
  - `model.display_name`；
  - `workspace.current_dir` / `project_dir` / `added_dirs`；
  - `cost.total_cost_usd`、`cost.total_lines_added` / `cost.total_lines_removed`；
  - `context_window.used_percentage` 与 `context_window.context_window_size`；
  - `session_name`、`transcript_path`、`worktree.*`、`rate_limits.*`。
- 桌面端在模型选择器旁放一个上下文占用环，Context 卡片按类别展开。[desktop]
- 相关命令：[commands]
  - `/context`：彩色网格展示上下文占用；
  - `/compact [instructions]`；
  - `/cost`：`/usage` 的别名。

**斜杠命令**：内置约 100 个，大量是产品功能。与会话内核相关的核心集合是：`/clear`、`/resume`、`/rename`、`/branch`、`/rewind`、`/compact`、`/context`、`/usage`、`/model`、`/effort`、`/permissions`、`/plan`、`/config`、`/status`、`/diff`、`/tasks`、`/add-dir`、`/cd`、`/export`、`/init`、`/memory`、`/mcp`、`/hooks`、`/agents`。桌面端里，没有参数形式的终端对话框命令（如 `/permissions`）回复“isn't available in this environment”，`/config` 打开设置页。[commands] [desktop]

## 5. 配置

- **分层**：user（`~/.claude/settings.json`）、shared project（`.claude/settings.json`）、project local（`.claude/settings.local.json`）、managed。[settings]
  - 优先级从高到低：managed > 命令行 `--settings` > local > project > user；
  - 列表型的键（如 `permissions.allow`）跨层合并，不互相覆盖；
  - 设置文件改动后热重载，但 `model` 例外，只在启动时读一次。
  - 另有 `~/.claude.json` 存账户、MCP、按项目的信任状态，以及 `/config` 写的全局键。
- **本机 user settings 的键**（只看键名）：`env`、`permissions.defaultMode`、`statusLine`、`enabledPlugins`、`extraKnownMarketplaces`、`theme`、`autoMode`、`includeCoAuthoredBy`。
- **规则语法**是 `Tool` 或 `Tool(specifier)`。[permissions]
  - 例子：`Bash(npm run *)`、`Read(./.env)`、`WebFetch(domain:example.com)`、`mcp__server__tool`；
  - `*` 匹配任意文本（含空格），`Bash(git log *)` 只放行 `git log`；
  - 求值顺序 deny → ask → allow，先匹配者胜，与具体程度无关；任一层的 deny 都不能被其他层的 allow 覆盖；
  - deny 一个裸工具名，会把这个工具从模型可见的工具列表里移除。
- **模型切换**：`/model` 切换并存为新会话默认值，在 picker 里按 `s` 只切当前会话；`--model`；桌面端用 prompt 框下方的模型选择器，会话中途也能换。[settings] [desktop]
- **`/config`** 只列一小组个人选项（主题、编辑器模式、verbose 等），大多存进 user settings，少数存 local；它不是 settings.json 的完整视图。[settings]

## 6. 子智能体与任务的呈现

- **定义与隔离**：`.claude/agents/*.md`（frontmatter 有 `tools`、`model`、`permissionMode`、`background`、`isolation`）。每个子智能体有独立的上下文窗口，只把摘要返回主对话。[sub-agents]
- **前台与后台**：
  - 前台会阻塞主对话，审批直接透传给用户；
  - 后台子智能体“surface every permission prompt in your main session”，提示里写明是哪个子智能体在问；`Esc` 只拒绝这一次调用，不停止子智能体；
  - 用户选了会话级授权，就对整个会话生效，包括主对话。[sub-agents]
- **UI**：
  - 后台子智能体列在输入框下方的面板里，嵌套的显示成树，有后代的行带 `(+N)`；
  - `/tasks` 列出运行中和刚完成的后台工作，`Enter` 打开该子智能体的 transcript，`x` 清除；
  - 桌面端有 Background tasks 面板（子智能体、后台 shell、workflow），可以打开输出或停止。[sub-agents] [desktop]
- **存储**：`<session>/subagents/agent-<id>.jsonl`，与主对话分开，不受主对话 compact 影响；可以用 `SendMessage` 恢复一个子智能体继续工作。默认最多嵌套 3 层、并发 20 个。[sub-agents]
- **流协议**：`system/task_started` 带 `task_id`、`tool_use_id`、`task_type`（`local_bash` / `local_agent` / `remote_agent`）、`spawn_depth`、`parent_task_id`；`system/task_progress` 带 `usage{total_tokens,tool_uses,duration_ms}`、`last_tool_name`、`summary`。[agent-sdk/typescript]

## 7. 对 Asterpath 桌面端的启示

### 7.1 值得采用的核心子集

ADR-0035 已定的约束是：一个窗口一次一个工作根，独立数据库，工作根内的会话由后台任务驱动到空闲。下表只列这个约束下有价值的部分。

| 采用 | 对应 Claude Code | Asterpath 落点 |
| --- | --- | --- |
| 侧栏会话列表：只看当前工作根，按最后活动时间排序，显示名字 / 标题 / 首条输入摘要；新建、分叉 | 桌面侧栏、`/resume` 选择器、`/branch` | `sessions(SessionQuery)` + `CatalogEntry.metadata`（ADR-0032）；`create_session`、`fork_session` |
| 会话状态徽标：运行中 / 等待审批 / 空闲 | `session_state_changed`、agent view 分组 | `AgentEvent::TurnStarted` / `ApprovalRequested` / `Idle`；冷启动时用 `state()` / `facts()` |
| 对话流：文本流式；工具调用默认一行摘要、可展开；Edit/Write 渲染 diff；思考块可折叠；“普通 / 详细”两档视图 | 桌面 Normal / Thinking / Verbose | `ModelDelta`（`PartDelta`）、`ToolStarted{args}`、`ToolFinished{output}`；diff 由前端根据工具参数计算 |
| 审批卡片：允许一次 / 本会话允许同类 / 拒绝并附说明 | 终端审批框、桌面 `Allow once / Always allow / Deny` | `approve(call, Allow \| Deny{reason})`；“本会话允许”的规则存在宿主的 `ApprovalPolicy` 内存里，“永久允许”写进宿主配置，都不进内核 |
| 运行中可继续输入，并有停止按钮 | 排队消息、`Esc`、桌面停止按钮 | `submit_input` 在回合中途记录，下一个 step 读到（ADR-0006 steering）；`cancel()` |
| 模型选择器、上下文占用环、手动 compact、用量 | 模型 picker、占用环、`/compact`、`/usage` | `update_config(ConfigPatch{model})`、`compact_now()`、`usage(scope)`、`projection()` |
| `@` 文件引用、图片粘贴与附件 | `@`、`Ctrl+V`、桌面附件 | `UserInput.parts`（ADR-0031 文件输入） |
| 子智能体：工具行显示 `name(描述)`，可点开看子会话 | 子智能体行、`/tasks` 打开 transcript | 子会话是带 `parent` 的会话，用 `sessions(ParentFilter)` 列出并 `open_session` 只读查看 |
| 追踪 / 调试视图 | transcript viewer（`Ctrl+O`） | `trace(session)`，这是 Asterpath 特有的 |

**不采用**（属于产品功能，或违反内核约束）：

- auto 分类器、`dontAsk` / `bypass` 档位：权限模式可以先只做 Manual，加一个可选的“自动接受编辑”，由宿主策略实现；
- 后台会话 supervisor、云端 / SSH / teleport、Remote Control、Dispatch；
- 每个会话一个 worktree、worktree 隔离：ADR-0035 是单工作根；
- checkpoint 文件快照：代码层面的回退交给 git，不在内核里做；
- hooks / 插件 / 技能管理 UI、状态栏脚本、斜杠命令大全、side chat（`/btw`）、session recap。

### 7.2 可能暴露的门面缺口

“现状”一栏按 `crates/asterpath/src/lib.rs` 与 protocol 类型核对过；“缺口”一栏是待设计确认的初稿。

| 桌面需求 | 现状 | 缺口 |
| --- | --- | --- |
| 改名、自动生成标题（首条 prompt 之后才有） | `metadata` 只在 `SessionCreated` 时记录，门面没有改元数据的 API | 需要“追加会话元数据”的记录与 API；否则宿主只能自建索引，失去“列表来自目录”的单一来源 |
| 按工作根过滤会话列表 | `SessionQuery` 只有 `parent` / `after` / `limit` | 缺按元数据键值过滤；ADR-0035 用独立数据库可以绕开，但 CLI 与桌面将来合库时需要 |
| 从某个回合分叉 / “恢复对话到这里” | `fork_session` 只取源会话最后一个已提交回合的末尾 | 缺按 `TurnId` 截断的分叉（ADR-0033 的快照点能否参数化）；回退可以用“从旧回合分叉”来表达，不需要改写日志 |
| 删除 / 归档会话 | 门面没有 | ADR-0016 规定保留策略归宿主，但宿主拿不到删除或归档的入口；至少需要一个“退役会话”的接缝 |
| 撤回已排队、尚未被消费的输入 | `submit_input` 只追加，不能撤回 | Claude Code 用 `Up` 撤回排队消息；Asterpath 可以暂不做，前端在提交前本地排队即可（等回合结束再 `submit_input`），代价是失去回合内 steering |
| 审批时修改参数 | `ApprovalDecision` 只有 `Allow` / `Deny{reason}` | SDK 有 `updatedInput`；第一版不需要，记作非缺口 |
| 上下文占用百分比 | `TurnCommitted.usage` 是整个回合的合计；有 `projection()` | 需要“最近一次模型调用的输入 token”和模型窗口上限；是否每个 step 都有 usage 事件、窗口上限从哪里取，未核实 |
| 子智能体进度挂在父会话的工具行下 | 子会话有 `parent`；`ToolStarted` 只有工具名和参数 | 父会话的事件流里能否拿到“这个工具调用对应哪个子会话 id”，未核实；没有的话，前端只能用 `sessions(parent=…)` 轮询 |
| 多个工作根并行 | ADR-0035 已记录：工具在构造时绑定根目录，进程 cwd 是全局状态 | 会话级工具根；已知缺口，另开 issue |
| 当前模式（plan / 自动接受编辑）跨重启保留 | 审批策略是宿主代码 | 宿主可以存进会话元数据，但同样受“元数据不可变”的限制，归到第一行 |

**判断**：交互层面需要的内核机制（事件流、审批、steering、取消、compact、改配置、分叉、用量、追踪）门面已经基本具备。真正缺的是**会话目录的可变性**（改名、标题、退役）和**分叉点可选**。这两项都属于“机制”，按框架边界应当进内核；模式、规则、diff、视图档位都是宿主策略，留在桌面端。
