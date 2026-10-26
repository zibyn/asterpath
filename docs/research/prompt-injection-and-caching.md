# 编码智能体的提示词注入与 prompt cache 调研

> 完成日期：2026-10-26。用途：对照 Codex CLI、Claude Code、pi、opencode、GitHub Copilot 五个编码智能体，看它们怎样拼装模型请求（system、项目指令、环境信息、运行中提醒、子智能体），以及为提高 prompt cache 命中率做了什么；为 Asterpath 的 `new_session_spec`、Anthropic/OpenAI adapter 渲染、ADR-0030 与压缩设计找可借鉴之处。
>
> 一手来源（源码均为浅克隆或 codeload 打包下载，只读，没有构建或运行）：
> - **Codex CLI**：`openai/codex` main，提交 `970111f`（2026-10-23）。读了 `codex-rs/core/src/{client.rs, agents_md.rs, compact.rs, thread_manager.rs, codex_delegate.rs}`、`core/src/context/*`（含 `world_state/`）、`core/src/session/{mod.rs, time_reminder.rs}`、`codex-api/src/common.rs`、`models-manager/{models.json, prompt.md, src/model_info.rs}`、测试 `core/tests/suite/{prompt_caching.rs, prompt_cache_key.rs, scenarios_strict_3p_cache.rs}`。GitHub Releases `rust-v0.163.0`（2026-10-15）、`rust-v0.164.0`（2026-10-23）的发布说明（`gh api`）。
> - **Claude Code**（闭源）：`anthropics/claude-code` 的 `CHANGELOG.md`，提交 `2eff7a8`（2026-10-25，最新条目 2.1.310）；官方文档 [prompt-caching](https://code.claude.com/docs/en/prompt-caching)、[memory](https://code.claude.com/docs/en/memory)、[context-window](https://code.claude.com/docs/en/context-window)、[Agent SDK: Modifying system prompts](https://code.claude.com/docs/en/agent-sdk/modifying-system-prompts)；官方博客 [Lessons from building Claude Code: Prompt caching is everything](https://claude.dev/blog/lessons-from-building-claude-code-prompt-caching-is-everything/)（Thariq Shihipar，2026-04-30）；API 文档 [Prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)。没有反编译发布包。
> - **pi**：`earendil-works/pi`（原 `badlogic/pi-mono`）main，提交 `d5b3de2`（2026-10-25），各包 1.4.0。读了 `packages/coding-agent/src/core/{system-prompt.ts, resource-loader.ts, cache-warmer.ts, cache-stats.ts}`、`packages/ai/src/api/{anthropic-messages.ts, openai-responses.ts, openai-responses-shared.ts, openai-codex-responses.ts}`、`packages/ai/src/utils/transcript.ts`、`coding-agent/docs/{session-format.md, settings.md}`、`coding-agent/CHANGELOG.md`。
> - **opencode**：`sst/opencode` dev，提交 `5b41e1f`（2026-10-26），各包 2.1.4（即 [opencode.md](opencode.md) 所说的 v2）。读了 `packages/core/src/session/{system-prompt.ts, model-request.ts, instructions.ts, instruction-state.ts, affinity.ts, projector.ts, runner/to-llm-message.ts, runner/prompt/system.txt}`、`core/src/{instruction-discovery.ts, instructions/builtins.ts, config/plugin/instruction.ts}`、`core/src/plugin/{optimize.ts, plan.ts, warming.ts, provider/prompt-cache-key.ts}`、`packages/ai/src/{cache-policy.ts, protocols/anthropic-messages.ts, protocols/shared.ts}`、`specs/v2/tools.md`。
> - **GitHub Copilot（VS Code agent 模式）**：`microsoft/vscode-copilot-chat` 已于 2026-05-20 归档（提交 `5863f5a`，README 写明迁入主仓库）。现行代码读的是 `microsoft/vscode` main 提交 `4f012f7`（2026-10-26）的 `extensions/copilot/`：`src/extension/intents/node/cacheBreakpoints.ts`、`src/extension/prompts/node/agent/{agentPrompt.tsx, promptRegistry.ts, summarizedConversationHistory.tsx}`、`src/platform/endpoint/node/{messagesApi.ts, responsesApi.ts}`、`src/platform/networking/common/anthropic.ts`；`chatEndpoint.ts`、`customInstructions.tsx`、`configurationService.ts` 取自归档仓库（未与现行版本逐行对比）。`extensions/copilot/CHANGELOG.md`（0.27，2025-05-07 条目）。官方文档 [Custom instructions](https://code.visualstudio.com/docs/copilot/customization/custom-instructions)。
> - **Copilot CLI**（闭源）：`github/copilot-cli` 的 `changelog.md`（最新 1.0.103，2026-10-22）。
>
> 文档页由抓取工具摘要后阅读，引文没有逐字核对。只由推断支持的条目标“推断”，找不到一手出处的标“未核实”。

## 0. 结论摘要

- **五家都把“会变的东西”移出 system，放进对话历史**：Claude Code 的 CLAUDE.md、环境、git 状态在第一条 user 消息，变化走 `<system-reminder>`；Codex 的 AGENTS.md 和 `<environment_context>` 是 user 消息，权限、模式等是 developer 消息，变化按“世界状态”差量追加；opencode 和 pi 把日期、环境、指令文件做成带键的状态，变化时追加 delta；Copilot 把首轮环境冻结在第一条 user 消息里，以后的变化放进最新 user 消息。日期留在 system 里会每天打掉缓存，pi（0.80.7）和 Copilot 的注释都明确写了这一点。
- **会话中途改指令、工具、档位，都不改前缀，改为追加**。这和 Asterpath 的 ADR-0030 是同一个方向。Anthropic 侧走 `role: "system"` 消息加 `tool_addition`/`tool_removal`（pi、opencode），或 `<system-reminder>`（Claude Code）；工具多时用 `defer_loading` 加 tool search（Claude Code、Copilot）；Claude Code 的 plan mode 用工具和消息实现，工具列表从不变。OpenAI 侧用 developer 消息、`additional_tools` 项（Codex responses-lite、pi）和 `configuration_update` 项（pi，改 reasoning 档位）。
- **压缩后要么沿用父请求的前缀，要么重建基线**。Claude Code 的摘要请求用和主对话完全相同的 system、工具和历史，只在末尾追加摘要指令（“cache-safe forking”），压缩后从磁盘重新注入 CLAUDE.md。Codex 压缩后完整重渲染一次初始上下文，作为新的比较基线。opencode 在 `Compaction.Ended` 时推进 epoch，把当前指令值并为新基线。pi 的 `compaction` 条目带一份 system 检查点。**Asterpath 的摘要请求不带 system 和工具，和会话缓存没有任何共享前缀**，是本次发现的最大差距。
- **OpenAI 侧五家（凡是有 OpenAI 路径的）都用 `store: false`**，靠 `prompt_cache_key` 做缓存亲和。键取会话 ID，子会话和 fork 继承父会话的键（Codex、opencode、Copilot 是 `conversationId:family`）。`previous_response_id` 只在 WebSocket 连接内使用（Codex、pi、Copilot），并且在模型、工具等字段变化时放弃。Asterpath 的 OpenAI adapter 也是 `store: false`，但没有发 `prompt_cache_key`。
- **缓存要靠度量和测试守住**。Claude Code 对命中率设告警、按事故处理，`/usage` 会给出“可能原因”。Codex 有 `prompt_caching.rs` 测试，断言跨回合前缀与键不变。Copilot 对每个模型族做缓存断点快照测试，并上报 `promptCacheTokenCount` 遥测。pi 有缓存未命中提示和按成本计算的缓存预热。

## 1. Codex CLI

### 1.1 注入方式

| 方面 | 做法 | 出处 |
| --- | --- | --- |
| 基础提示词 | **按模型切换**：模型目录每项带 `model_messages.instructions_template`，目录没有时退回内置 `prompt.md`（“You are a coding agent running in the Codex CLI…”），配置 `base_instructions` 可覆盖 | `models-manager/models.json`、`models-manager/src/model_info.rs:16,46` |
| 基础提示词的位置 | 请求**没有顶层 `instructions` 字段**；基础指令渲染成 `input` 开头的一条 developer 消息，ID 由线程 ID 加文本哈希的 UUIDv5 生成，重试和恢复时不变 | `codex-api/src/common.rs:279`（`ResponsesApiRequest` 字段表）；`core/src/client.rs:1007-1041` |
| 项目指令发现 | 从 cwd 向上找到项目根（默认标记 `.git`，可配 `project_root_markers`），收集根到 cwd 之间每一级的 `AGENTS.md`（或 `AGENTS.override.md`、可配的后备文件名），按从根到 cwd 的顺序拼接；不越过项目根。用户级指令与项目文档用 `--- project-doc ---` 分隔 | `core/src/agents_md.rs:1-52` |
| 项目指令的位置 | **user 角色**消息：`# AGENTS.md instructions for <dir>\n\n<INSTRUCTIONS>…</INSTRUCTIONS>` | `core/src/context/user_instructions.rs:22-40` |
| 环境信息 | `<environment_context>`：cwd、shell、`current_date`、时区、网络、文件系统权限，作为世界状态的一节，**变化时只追加差量** | `core/src/context/world_state/environment.rs:115-140`；测试 `send_user_turn_with_changes_sends_environment_context` |
| 运行时提醒 | “世界状态”（`WorldState`）由 AGENTS.md、环境、权限、协作模式、模型指令、工具、插件等若干节组成。每节保存“上次模型看到的快照”，按 RFC 7386 merge patch 持久化进 rollout；每一步只渲染与快照不同的节，追加为 developer 或 user 消息。AGENTS.md 变化时追加“替换”通知或“移除”通知 | `core/src/context/world_state/mod.rs:292-496`、`world_state/agents_md.rs:44-74` |
| 时间提醒 | `<current_time_reminder>` developer 消息，只在 user 回合或工具输出边界之后发 | `core/src/context/current_time_reminder.rs`、`core/src/session/time_reminder.rs` |
| 换模型 | 追加 developer 消息 `<model_switch>`，内容是新模型的指令（“The user was previously using a different model…”），放在 developer 段最前 | `core/src/context/model_switch_instructions.rs`；`session/mod.rs` 中 `build_initial_context_with_world_state` |
| 技能、插件、apps | 用法说明作为 developer 片段，是否附带由模型目录的 `include_skills_usage_instructions` 等开关决定（细节未逐一核对） | `models.json` 字段 |
| 子智能体 | 角色指令 `MultiAgentRoleInstructions` 是单独一条 developer 消息；子线程继承父线程的指令、环境和缓存键 | `session/mod.rs`；`thread_manager.rs:2060-2073`；`codex_delegate.rs:100-115` |

### 1.2 缓存措施

| 措施 | 做法 | 出处 |
| --- | --- | --- |
| 前缀稳定 | 基础指令和 responses-lite 的工具块（`AdditionalTools` 项）都用确定性 ID 拼进 `input` 开头；环境、AGENTS.md 只发差量 | `client.rs:1007-1041` |
| 工具 | 普通模型发顶层 `tools`；`use_responses_lite` 模型把工具作为 input 里的 `additional_tools` 项发送，可以增量追加 | `client.rs:1015-1030`；`world_state/top_level_tools.rs` |
| `prompt_cache_key` | 策略 `PromptCacheKey::{SessionDerived, Custom, Omit}`。默认取会话 ID；内部会话取 `source:parent_thread_id`；子线程继承父线程的策略。0.164.0 起允许调用方脱离会话 ID 自行指定（#55587） | `thread_manager.rs:270-290`、`client.rs:655-676`；Release `rust-v0.164.0` |
| 服务端状态 | `store: false`。`previous_response_id` 只用于 WebSocket 复用。复用前逐字段比较上一请求，模型、工具、`tool_choice`、reasoning、`prompt_cache_key` 等任一变化即放弃复用，并记下原因（如 `tools_changed`） | `client.rs:1120`、`client.rs:395-455`；文件头注释（WebSocket 预热 `generate=false`） |
| 显式断点 | 不发 `prompt_cache_options`（测试断言为空），依赖 OpenAI 隐式前缀缓存 | `core/tests/suite/client_websockets.rs:707` |
| 压缩 | 压缩后完整重渲染一次初始上下文，插在最后一条真实 user 消息之前，得到的快照作为新的比较基线；压缩请求超窗时**从头部**删历史以保留前缀 | `compact.rs:83-115, 636-691, 360` |
| fork | 临时 fork 共享缓存路由但保留会话身份；0.163.0 加入完整 fork 的“历史前缀保留”（#52535） | 测试 `ephemeral_fork_shares_cache_routing_but_keeps_session_identity`；Release `rust-v0.163.0` |
| 回归测试 | `prompt_caching.rs`：`prompt_tools_are_consistent_across_requests`、`prefixes_context_and_instructions_once_and_consistently_across_requests`、`overrides_turn_context_but_keeps_cached_prefix_and_key_constant`、`per_turn_overrides_keep_cached_prefix_and_key_constant`；`scenarios_strict_3p_cache.rs`（MCP 刷新不破缓存）；0.163.0 加入 app-server 前缀兼容性测试（#51857） | 测试文件；Release `rust-v0.163.0` |
| 度量 | 记录 `cached_input_tokens`、`cache_write_input_tokens` | `compact.rs:461-528` |

## 2. Claude Code

### 2.1 注入方式

| 方面 | 做法 | 出处 |
| --- | --- | --- |
| 基础提示词 | `claude_code` preset，含工具说明与安全规则；有完整和精简两种（`CLAUDE_CODE_SIMPLE_SYSTEM_PROMPT`）。是否按模型切换未核实 | SDK 文档 Modifying system prompts |
| system 里有什么 | 核心指令和工具定义，另外嵌入**自动记忆目录的路径**（因此缓存实际按机器和目录隔离）；`--append-system-prompt` 和 SDK 的 `append` 接在 preset 后面 | prompt-caching「Cache scope」；SDK 文档 |
| 项目指令发现 | 启动时加载 cwd 及其**每一级父目录**的 `CLAUDE.md`、`.claude/CLAUDE.md`、`CLAUDE.local.md`，按从根到 cwd 的顺序拼接；另有用户级 `~/.claude/CLAUDE.md`、组织 managed、`.claude/rules/`。**子目录**里的同名文件在读、写、编辑该子目录下文件时按需加载。没有 CLAUDE.md 时读 `AGENTS.md`（v2.1.277 起） | memory 文档「How CLAUDE.md files load」「AGENTS.md」 |
| 项目指令的位置 | “CLAUDE.md content is delivered as a user message after the system prompt, not as part of the system prompt itself.” | memory 文档 |
| 环境信息 | “the conversation opens with an announcement of the working directory, platform, shell, and OS version”；启动时有一份 git 状态快照（分支、最近提交），压缩后会重新读取 | prompt-caching「Cache scope」；context-window；CHANGELOG 2.1.269 |
| 运行时提醒 | `<system-reminder>`，放在下一条 user 消息或工具结果里（博客）；有些模型改用单独的 `system` 角色消息（SDK 文档）。具体包括：文件已变更、任务列表提醒、技能与子智能体列表、hook 的 `additionalContext`、输出风格、commit/PR 署名、后台任务通知（2.1.234）。Sonnet 5 不使用 mid-conversation system 角色（2.1.201） | SDK 文档「Reminders Claude Code adds」；博客；CHANGELOG |
| 模式切换 | plan mode 用 `EnterPlanMode`/`ExitPlanMode` 工具实现，进入时追加一条约束消息，工具列表不变；切换输出风格以消息形式发送（2.1.251 起）；技能与命令在调用处作为 user 消息注入 | 博客；prompt-caching「Actions that keep the cache」 |
| 中途改 system | 会话首个请求时记录 system prompt，之后一直复用，直到压缩；SDK 建议用下一条消息或 hook 的 `additionalContext` 传达新指令 | SDK 文档「Change the prompt of an existing session」 |
| 子智能体 | 有自己的 system prompt 和工具集，第一请求读不到父会话的缓存；fork（`subagent_type: "fork"`，2.1.232 起默认开启）完整继承父会话的 system、工具和历史，因此能读到父缓存 | prompt-caching「Subagents and the cache」 |

### 2.2 缓存措施

| 措施 | 做法 | 出处 |
| --- | --- | --- |
| 分层顺序 | system prompt（核心指令加工具定义，跨会话共享）→ 项目上下文（CLAUDE.md、自动记忆、无作用域规则）→ 对话 | prompt-caching 层次表；博客 |
| 断点 | 公开文档没有给出断点位置和数量。已知的两点：一是对话中途追加的系统上下文块“marks that block for caching”；二是 `__SYSTEM_PROMPT_DYNAMIC_BOUNDARY__` 把 system 拆成静态与动态两块，**各自带断点**（2.1.275） | prompt-caching「Where the cache lives」；SDK 文档；CHANGELOG 2.1.275 |
| 跨用户共享 | SDK 的 `excludeDynamicSections` / CLI 的 `--exclude-dynamic-system-prompt-sections` 把自动记忆路径等按用户变化的内容移到第一条 user 消息，让 system 在不同机器间完全一致 | SDK 文档 |
| 工具 | 工具顺序必须确定（博客提到曾因“shuffling tool order definitions non-deterministically”破缓存）。开启 tool search 时，工具列表**固定为会话首次请求时的列表**，之后连上的 MCP 服务器以 `defer_loading` 存根加入；bare deny 规则不改动工具列表（2.1.267 修复多处中途改工具列表的问题） | 博客；prompt-caching「Connecting or removing an MCP server」；CHANGELOG 2.1.267 |
| 换模型和档位 | 换模型必然整段未命中；缓存仍热时 `/model` 会先要求确认。博客建议改用子智能体交接。Opus 5.5、Sonnet 5.5、Haiku 5.5、Fable 5.1 上调整 effort 不破缓存（2.1.260） | prompt-caching「Switching models」「Changing effort level」 |
| CLAUDE.md 修改 | 会话中改 CLAUDE.md 不破缓存，**但也不生效**，要等 `/clear`、`/compact` 或重启 | prompt-caching「Editing CLAUDE.md mid-session」 |
| 压缩 | 摘要请求重复主对话的 system、工具和待摘要历史，只在末尾追加摘要指令（“cache-safe forking”），缓存热时成本只是一小部分；压缩后从磁盘重新注入 CLAUDE.md、自动记忆、技能描述、git 状态和最近读过的最多 5 个文件；system 层一般仍然命中 | prompt-caching「Compacting the conversation」；context-window；博客 |
| TTL | 订阅用户在套餐额度内，主对话用 1h，其余（子智能体、压缩、标题等）用 5m；API key 和云厂商默认都是 5m。可分别用 `promptCacheTtl` 和 `subagentPromptCacheTtl` 设置（2.1.242）。有优先级链：`FORCE_PROMPT_CACHING_5M` > 环境变量 > 设置 > agent frontmatter `experimental.cacheTtl` > `ENABLE_PROMPT_CACHING_1H` > 默认 | prompt-caching「Which TTL each request gets」 |
| 闲置 | 1h TTL 下，200K 以上的对话闲置约 54 分钟会在过期前自动压缩（2.1.299）；`/loop` 唤醒避开 1h 边界（2.1.304） | CHANGELOG |
| 图片 | 超过上限时**成批**删除最老的图片，每批只破一次缓存 | prompt-caching「Accumulating many images」 |
| 恢复会话 | 恢复时复用记录下来的 system 和工具，不重新渲染（2.1.267 等多条修复：附件、MCP 定义、子智能体工具列表、OAuth 刷新后重渲染工具等） | CHANGELOG 2.1.267、2.1.280、2.1.290 等 |
| 度量 | `/usage` 的 `Prompt cache (main)` 行：命中率、未命中次数、冷热状态（2.1.251），以及可能原因，如 “tool definitions changed”（2.1.260）；状态栏 `prompt_cache` 对象；OTel 导出；团队对命中率设告警，“declare SEVs if they're too low” | prompt-caching「Check cache performance」；博客 |

## 3. pi

### 3.1 注入方式

| 方面 | 做法 | 出处 |
| --- | --- | --- |
| 基础提示词 | 一份通用 preamble（“You are an expert coding assistant operating inside pi…”），**不按模型切换**；可用 `SYSTEM.md`（项目 `.pi/` 或全局）替换，用 `APPEND_SYSTEM.md` 追加。用 OAuth 走 Anthropic 时，system 首块固定为 Claude Code 身份句 | `system-prompt.ts`；`resource-loader.ts:1249-1268`；`anthropic-messages.ts:1222-1238` |
| system 结构 | 按名字分节，顺序是 `preamble`、`tools`、`rules`、`docs`、`addendum`、`project_context`、`skills`、`cwd`、扩展自定义节；除 preamble 外每节用 `<name>…</name>` 包裹，以便后续按节替换 | `system-prompt.ts` `buildSystemPromptSections` |
| 项目指令发现 | 先全局目录，再从 cwd **一直向上到文件系统根**，每级取第一个存在的 `AGENTS.override.md`、`AGENTS.md`、`AGENTS.MD`、`CLAUDE.md` 或 `CLAUDE.MD`，从远到近排列；嵌套 worktree 里与主仓库同一份的上下文文件去重 | `resource-loader.ts:211-300` |
| 项目指令的位置 | system 的 `<project_context>` 节，每个文件包成 `<project_instructions path="…">` | `system-prompt.ts` `renderProjectContext` |
| 环境信息 | 只有 `<cwd>` 节；**日期已从默认提示词删除**，原因是它让 system 缓存跨天失效（0.80.7，#6621） | `system-prompt.ts`；CHANGELOG 0.80.7 |
| 运行时变化 | 首个 system 消息带全部节和 `toolsAdded`；之后按节 diff，生成 `SystemMessage.sections` 补丁并写进会话记录。支持的模型原地发送 mid-conversation system 消息，不支持的折回开头 | `system-prompt.ts` `diffSystemPromptSections`；`openai-responses-shared.ts:129`；CHANGELOG 0.86.0（#9548） |
| 子智能体 | 不内置 | 见 [pi-and-pi-durable.md](pi-and-pi-durable.md) |

### 3.2 缓存措施

| 措施 | 做法 | 出处 |
| --- | --- | --- |
| Anthropic 断点 | system 块打一个（OAuth 时身份块和正文各打一个）、最后一个工具打一个、最后一条 user 或 system 消息的最后一块打一个 | `anthropic-messages.ts:1220-1245, 1529-1555, 1659` |
| TTL | `cacheRetention: none/short/long`，`long` 且模型支持时用 `ttl: "1h"`；环境变量 `PI_CACHE_RETENTION` | `anthropic-messages.ts:70-97` |
| 中途改工具 | Anthropic：请求级工具列表保持不变，之后的工具用 `tool_addition` 块按值定义，同名重定义也不破缓存（1.0.1）。OpenAI Responses：`additional_tools` 项；除第一批外每批前面附说明，提示还有哪些工具不可用（注释称与 Codex 做法相同） | `anthropic-messages.ts:1175-1190`；`openai-responses-shared.ts:166-185`；CHANGELOG 0.80.7（#6474）、1.0.1 |
| 中途改档位 | Anthropic 用 mid-convo effort 消息；OpenAI GPT-6 用 `configuration_update` 项代替修改 `reasoning.effort`（1.3.0） | `anthropic-messages.ts:1196-1210`；`openai-responses-shared.ts:134-140`；CHANGELOG 1.3.0 |
| OpenAI 缓存键 | `prompt_cache_key` 取会话 ID（截到 64 字符），`store: false`；有长保留选项（GPT-5.6+ 用 `prompt_cache_options.ttl: "30m"`）。Codex 后端经 WebSocket 用连接内的 `previous_response_id` | `openai-responses.ts:367-370`；`openai-codex-responses.ts:1536-1551`；CHANGELOG 0.85.1 |
| 压缩 | `compaction` 条目带 `systemMessage`，即压缩边界处回放得到的完整 prompt 节和工具声明；重建时它成为开头的 system，保留区间内的 system 消息被它取代 | `docs/session-format.md`「CompactionEntry」 |
| 缓存预热 | 长工具运行期间（默认 `streaming`）或闲置时（`idle`），在 TTL 的 90% 处用 1 token 回放请求续期；只有预计节省不少于 $0.05 时才发；闲置续期假设的“下一请求在过期前到来”概率为 0.15 | `cache-warmer.ts`；`docs/settings.md`；CHANGELOG 0.86.0（#9668） |
| 度量 | 页脚显示最近一次缓存命中率（0.79.0）；`showCacheMissNotices` 在记录里提示显著的未命中（0.80.4）；`cache-stats.ts` 按“空闲超过 TTL”和“换了模型”归因，1024 token 以下视为噪声 | CHANGELOG；`cache-stats.ts` |
| 回归测试 | `ai/test` 下 `cache-retention`、`openai-responses-cache-affinity-e2e`、`openai-codex-cache-affinity-e2e`、`openai-completions-prompt-cache` 等 | 测试目录 |

## 4. opencode（v2，dev 分支）

### 4.1 注入方式

| 方面 | 做法 | 出处 |
| --- | --- | --- |
| 基础提示词 | 默认 `system.txt`（15 行，含 “`<system-reminder>` blocks are harness instructions”），按工具集插入工具使用指导；**插件按模型切换**：gpt（含 astra 变体）、kimi、trinity、muse（meta）替换，claude 追加。agent 自带 `system` 时替换默认 | `session/runner/prompt/system.txt`；`session/system-prompt.ts`；`plugin/optimize.ts` |
| system 结构 | `[agent.system 或默认提示词, initial]` 两段，`initial` 是指令基线的初始渲染 | `session/model-request.ts` `baseTranscript` |
| 项目指令发现 | 全局 `<config>/AGENTS.md`；cwd 向上到项目根（cwd 在 home 下时到 home）的每级 `AGENTS.md`；配置里的额外路径和 glob、远程 URL。**文件被监视**，变化即触发重算 | `config/plugin/instruction.ts:30-110` |
| 嵌套指令 | read 工具读到某目录下的文件时，加载该目录的 `AGENTS.md`，作为 `synthetic` 消息（user 角色）注入，并按路径去重；压缩或回滚把它挤出历史后会重新注入 | `session/instructions.ts`；`tool/plugin/read.ts:16` |
| 环境信息 | 内置两条“指令”：`core/date`（初始渲染为 `Today's date: …`，变化后渲染为 `Today's date is now: …`）和 `core/environment`（`<env>` 中有 cwd、项目根、是否 git 仓库、平台、临时目录） | `instructions/builtins.ts` |
| 运行时变化 | 指令是带键的状态（`InstructionState`）：首次观察作为基线，之后的变化发布 `InstructionsUpdated`，正文是 delta 文本，例如“The instructions from X changed. Here's the diff”（diff 比全文短时用 diff），持久事件里冻结渲染后的文本 | `session/instruction-state.ts`；`instruction-discovery.ts` `renderUpdate` |
| 模式切换 | plan 插件在请求末尾、user 提示之前插入 `<system-reminder>` 的进入或离开文本，并持久化为 synthetic；按请求核对，被压缩剥掉后会补回。注释写明“appending near the tail so the cached prefix stays warm” | `plugin/plan.ts` |
| 中途 system 消息 | Anthropic 新模型（Opus 4.8、Claude 5 及以后）原生 `role: "system"`；其他情况包成 `<system-update>` 追加到 user 回合；只接受纯文本，不放检索或工具内容 | `ai/src/protocols/anthropic-messages.ts:851-900`；`protocols/shared.ts:197-224` |
| 子智能体 | 子会话的缓存亲和取父会话 ID，fork 取源会话 ID | `session/affinity.ts` |

### 4.2 缓存措施

| 措施 | 做法 | 出处 |
| --- | --- | --- |
| 断点策略 | 默认 `auto`：最后一个工具、**第一个和最后一个**不同的 system 块、对话尾部 1 处，上限 4 个；手动放置的 `CacheHint` 计入预算。只对认 inline 标记的协议生效；Gemini 只认最后一个断点，尾部断点每步都写新缓存，所以经网关时 Gemini 不打；Qwen 不打工具断点 | `ai/src/cache-policy.ts` |
| TTL | `CacheHint.ttlSeconds` 映射为 `5m` 或 `1h` | `anthropic-messages.ts:537-546` |
| 缓存键 | `promptCacheKey` 取会话亲和 ID（去掉 `ses_` 前缀），同时发 `x-session-affinity` 等头；对没有专用包的若干 provider 声明支持 `prompt_cache_key`。代码 TODO 写着嵌套 fork 尚未复用根会话的键 | `session/model-request.ts:275-292`；`plugin/provider/prompt-cache-key.ts` |
| 最后一步 | 最后一个 Step 仍保留工具定义，用 `toolChoice: "none"`，“so the cached prompt prefix survives” | `specs/v2/tools.md:144` |
| 压缩 | `Compaction.Ended` 时 `advanceEpoch`：把当前指令值设为新的 `initial_values`（重新定基线）；摘要以 `<conversation-checkpoint>` 形式进入一条 user 消息 | `session/projector.ts:778-782`；`instruction-state.ts:139-154`；`to-llm-message.ts:293-307` |
| 回滚 | 只有回滚删掉了更新或 epoch 起点时才清空折叠，“keeping its baseline keeps the prompt cache” | `instruction-state.ts:156-159` |
| 图片 | 有迟滞：图片总量超过 25 MiB 时，从最老的开始删到 15 MiB，这样每进来一张新图不必都移动切点 | `session/model-request.ts:186-200` |
| 预热 | 可选 `warming` 插件：每 4 分钟发一次保活请求，最长 30 分钟 | `plugin/warming.ts` |

## 5. GitHub Copilot

### 5.1 VS Code agent 模式：注入方式

| 方面 | 做法 | 出处 |
| --- | --- | --- |
| 基础提示词 | **按模型族切换**：`PromptRegistry` 按 family 前缀注册 Anthropic、Gemini、xAI、Kimi、Minimax、ZAI 及 OpenAI 多版（gpt-5、5.1、5.2、5.3-codex、5.4…）等提示词；首块固定为 “You are an expert AI programming assistant, working with a user in the VS Code editor” 加身份与安全规则 | `prompts/node/agent/promptRegistry.ts`、`anthropicPrompts.tsx:541-646`、`agentPrompt.tsx` `render` |
| 消息顺序 | system（基础与模型提示词）→ 自定义指令（默认放在 **system**，`chat.customInstructionsInSystemMessage` 默认 true）→ 模式指令（`<modeInstructions>`）→ 全局上下文 user 消息 → 历史 → 当前 user 消息 → 本回合工具轮次 | `agentPrompt.tsx`；`cacheBreakpoints.ts` 注释 |
| 项目指令发现 | `.github/copilot-instructions.md`、仓库根 `AGENTS.md`（`chat.useAgentsMdFile` 默认开）、`CLAUDE.md`；嵌套 `AGENTS.md` 需打开实验设置 `chat.useNestedAgentsMdFiles`，只列出位置，由 agent 自行加载；`*.instructions.md` 按 `applyTo` 匹配或按描述按需加载；用户级 `~/.copilot/…`、`~/.claude/CLAUDE.md`。父仓库搜索需打开 `chat.useCustomizationsInParentRepositories` | VS Code 文档 Custom instructions；`customInstructions.tsx` |
| 环境信息 | 首轮渲染“全局上下文”user 消息：`<environment_info>`（OS）、`<workspace_info>`（任务、目录、工作区结构）、用户偏好；**按会话第一回合缓存在元数据里，工作区文件夹不变就原样复用** | `agentPrompt.tsx` `getOrCreateGlobalAgentContextContent`、`GlobalAgentContext`；`conversation.ts:460` |
| 每回合上下文 | 当前 user 消息里有 `<context>`（当前日期、编辑过的文件、终端状态、todo、hook 附加上下文）、当前编辑器、`<reminderInstructions>`、`<userRequest>`。**渲染后冻结**（`RenderedUserMessageMetadata`），以后回合原样回放 | `agentPrompt.tsx` `AgentUserMessage`、`FrozenContentUserMessage` |
| 技能、agent、指令列表 | 第一回合把“customizations index”快照进 system；以后列表变化时，只在最新 user 消息里给出 `<customizationsUpdate>`，以免打破 system 缓存 | `agentPrompt.tsx` `getOrFreezeCustomizationsIndex` |
| 子智能体 | 搜索、执行子智能体有专用提示词；子智能体请求不用 1h TTL | `searchSubagentPrompt.tsx`、`executionSubagentPrompt.tsx`；`messagesApi.ts:256-266` |

### 5.2 VS Code agent 模式：缓存措施

| 措施 | 做法 | 出处 |
| --- | --- | --- |
| Chat Completions 断点 | 共 4 个：提示词自带 1–2 个（全局上下文、当前 user 消息），其余从尾部分配，当前 user 消息以下放在每轮最后一个工具结果上，以上放在没有工具调用的 assistant 消息上；还有剩余就给 system 或指令消息 | `intents/node/cacheBreakpoints.ts` |
| Anthropic Messages 断点 | 先清空，再放：**最近两条**可缓存消息各 1 个（第二个是 tail 未命中时的后备）、最后一个非 deferred 工具 1 个、system 最后一块 1 个，合计 4 个；工具排序时非 deferred 在前 | `messagesApi.ts:174, 271-273, 605-660` |
| Anthropic TTL | 实验开关：Agent 位置、非子智能体时，tools 和 system 用 1h；消息断点的 1h 是子开关，必须先开父开关，因为“longer-TTL breakpoints [must] appear before shorter ones”；注释说消息断点每回合轮换，1h 的双倍写入费只在闲置超过 5 分钟时划算 | `networking/common/anthropic.ts:198-242` |
| Responses 断点 | `prompt_cache_options: {mode: explicit / implicit}`；explicit 时在 system 末尾、全局上下文末尾放锚点，再加最近 20 个 user 或工具批次边界（`prompt_cache_breakpoint`） | `responsesApi.ts:134-158, 629-645`；`cacheBreakpoints.ts` `addResponsesCacheBreakpoints` |
| OpenAI 缓存键 | `prompt_cache_key = conversationId:family`（实验开关 `chat.responsesApi.promptCacheKey.enabled`，默认关） | `responsesApi.ts:186-189`；`configurationService.ts:893`（归档版） |
| 服务端状态 | `store: false`；`previous_response_id` 只在 WebSocket 活跃连接上使用，服务端报 `InvalidStatefulMarker` 时退回全量重发；模式切换（`modeChanged`）时清掉 | `responsesApi.ts:156, 336-345`；`chatEndpoint.ts:388-410`（归档版） |
| 压缩 | 0.27 起用摘要取代 FIFO 截断，“This keeps the prompt prefix stable”；Full 模式摘要请求带上原工具并设 `tool_choice: 'none'`（是否为了共享前缀属推断）；摘要请求去掉断点；摘要文本生成时一次性追加 transcript 指针，以后原样回放，“preserving Anthropic prompt cache hits” | CHANGELOG 0.27；`summarizedConversationHistory.tsx:714-770, 955-985` |
| 日期 | 日期在每回合 user 消息里；模拟测试中不输出，注释说日期会让缓存“invalidated every day” | `agentPrompt.tsx` `CurrentDatePrompt` |
| 测试与度量 | `cacheBreakpoints.spec.ts`；各模型族的 `cache_BPs*.spec.snap` 快照；`messagesApi.spec.ts`、`responsesApi.spec.ts`；摘要遥测 `promptCacheTokenCount`；按角色和字符数记录的 `promptTypes`，作为命中率代理指标 | 测试目录；`summarizedConversationHistory.tsx:54-67` |

### 5.3 Copilot CLI

只有 changelog 可查：恢复会话时复现原来附加的文件引用，即使文件已经改动，“avoiding prompt-cache resets”（1.0.64）；`/usage` 显示缓存写入 token（1.0.60）；读或编辑匹配文件时自动应用 `.github/instructions` 中的路径指令（1.0.100）；支持 `.claude/rules`（1.0.89）；自定义 agent 可用 `include-custom-instructions: true` 引入仓库指令（1.0.86）；AGENTS.md 等文件支持 `@` 导入。**system 组成、断点、TTL、缓存键均未核实。**

## 6. 横向对比

| 维度 | Codex CLI | Claude Code | pi | opencode v2 | Copilot（VS Code） | Asterpath（现状） |
| --- | --- | --- | --- | --- | --- | --- |
| 基础提示词来源 | 模型目录逐模型模板，退回 `prompt.md` | `claude_code` preset（完整或精简） | 单一 preamble，可用 SYSTEM.md 替换 | 默认 `system.txt` 加按模型的插件替换或追加 | `PromptRegistry` 按模型族 | **无内置**，由应用给出 |
| 基础提示词位置 | input 首条 developer 消息（确定性 ID） | 顶层 system | 顶层 system | 顶层 system | system 消息 | 顶层 system |
| 项目指令位置 | user 消息 | 第一条 user 消息 | system 的 `<project_context>` 节 | system 第二段（基线） | system（默认） | system |
| 发现规则 | 项目根→cwd 每级；用户级；可配后备文件名 | cwd 及所有父目录、用户级、组织级、rules；子目录按需 | 全局，加 cwd→文件系统根每级 | 全局，加 cwd→项目根每级，被监视；子目录读取时按需 | 根目录 AGENTS.md / copilot-instructions / CLAUDE.md；嵌套为实验；`applyTo` 按需 | 只有 `<root>/AGENTS.md` |
| 环境信息位置 | user `<environment_context>`，含日期，只发差量 | 第一条 user 消息，加 git 快照 | system `<cwd>`，无日期 | system 基线，日期变化发 delta | 首轮全局 user 消息冻结；日期在每回合 user 消息 | 无 |
| 运行时提醒机制 | 世界状态差量（developer 或 user） | `<system-reminder>` 或 system 角色消息 | 按节补丁的 system 消息 | `InstructionsUpdated` delta、`<system-reminder>`、`<system-update>` | 最新 user 消息（`<context>`、`<customizationsUpdate>`、提醒） | `Instruction` 条目原位渲染（Anthropic system 消息、OpenAI developer 消息） |
| 缓存断点策略 | 无显式断点（OpenAI 隐式） | 未公开；动态边界两块各带断点；中途系统块带断点 | system、最后工具、最后消息 | 最后工具、首尾 system、尾部 1 处 | 4 个：工具、system、最近两条消息（Messages）；Responses 显式 2 个前缀锚点加 20 个 | system 末块、最后工具、最后消息（3 个，5m） |
| 中途变更处理 | 差量消息；`additional_tools`；`<model_switch>` | 记录下的 system 一直沿用；工具冻结加 deferred；plan mode 用工具实现 | `tool_addition`；mid-convo system 与 effort；`configuration_update` | 原生 system 消息或 `<system-update>`；inline tools 未细查 | 冻结首轮内容，漂移进最新 user 消息；deferred 工具 | ADR-0030：追加指令、inline tools；参数变化改顶层 |
| 压缩后前缀 | 重渲染完整初始上下文，作为新基线 | 摘要请求复用父前缀；压缩后重新注入 CLAUDE.md 等 | 检查点 system 消息 | `advanceEpoch` 重定基线 | 摘要冻结后原样回放 | 重定基线；**摘要请求无 system 和工具** |
| OpenAI 缓存键 | 会话派生，子线程继承，可自定义或省略；`store:false`；WebSocket 上用 `previous_response_id` | 不适用 | 会话 ID；`store:false`；Codex 后端 WebSocket 链式 | 会话亲和 ID（子会话用父的） | `conversationId:family`（实验）；`store:false`；WebSocket 链式 | `store:false`，**不发键** |
| TTL | 不适用 | 主对话与其他请求分桶；1h 或 5m 可配 | short / long（1h） | 按 hint 取 5m 或 1h | tools 和 system 1h（实验），消息为子开关 | 只有 5m |
| 度量与测试 | `prompt_caching.rs` 等 | `/usage` 命中率与原因，告警 | 页脚命中率、未命中提示、预热 | 无专门测试（未查） | 断点快照测试、遥测 | 记录 `cache_read/cache_write`，无前缀测试 |

## 7. 对 Asterpath 的启示（只是建议）

### 7.1 已经对齐的部分

- **ADR-0030 的方向与业界一致**：变更作为历史中的一个位置，压缩时重定基线。pi（system 检查点）、opencode（`advanceEpoch`）、Codex（压缩后以新快照为基线）都是同一结构。Anthropic adapter 的 `inline_changes`（system 消息加 `tool_addition`/`tool_removal`）和 pi、opencode 的渲染几乎一样。
- **“同一日志渲染出同一字节”**（`request.rs` 头注释）相当于 Copilot 的“冻结已渲染内容”和 Claude Code 的“记录 system 与工具，不重新渲染”。后两者在 CHANGELOG 里修过大量“恢复后重新渲染导致未命中”的问题，Asterpath 的日志化设计从根上避开了这一类问题。
- **system 里没有日期和 cwd**（`new_session_spec` 只放 AGENTS.md 原文和技能索引），不存在 pi 0.80.7 修掉的那种跨天失效。
- OpenAI 侧 `store: false`、不使用 `previous_response_id`（`render.rs:33`），与 Codex、pi、Copilot 的 HTTP 路径一致。

### 7.2 差距与建议

| # | 差距 | 建议 | 参照 |
| --- | --- | --- | --- |
| 1 | `DefaultCompactor` 的摘要请求不带 system 和工具，摘要指令放在**最前面**（`compaction/mod.rs:155-173`），与会话缓存零共享，每次压缩都按全价读取被摘要的区间 | 提供 cache-safe 的摘要路径：沿用会话的基线 system、工具（`tool_choice: none`）和历史前缀，把摘要指令作为最后一条 user 消息；被摘要的区间本来就是会话前缀，可以直接命中。这和 compaction 模块头注释“never inherits the session's request, tools”冲突，需要修订 ADR-0009 或补一条 ADR（reasoning 回放门控仍然适用） | Claude Code 博客第 6 条与 prompt-caching 文档；Copilot Full 模式 |
| 2 | OpenAI adapter 不发 `prompt_cache_key` | 由框架提供默认的会话派生键：取**树根会话 ID**，子会话与 fork 共享，并允许应用覆盖为自定义或省略（按 ADR-0048，机制归框架，策略归应用；也适合 web 多租户按租户分键）。可以放在 `SessionContext`（ADR-0039 已经带上树根） | Codex `PromptCacheKey`；opencode `SessionAffinity` |
| 3 | Anthropic 只有 5m TTL，3 个断点 | ① 每个请求可选 TTL，并分“主会话”和“其余”（子会话、压缩）两个桶，在断点上满足“1h 在 5m 之前”的顺序约束；② 用上第 4 个断点：倒数第二条消息作为后备锚点，应对一个 step 新增块数超过 20 块回看窗口（并行工具结果较多）或尾部未命中的情况 | Claude Code TTL 分桶；Copilot `addMessagesApiCacheControl`、`isExtendedCacheTtlMessagesEnabled`；API 文档 20 块回看 |
| 4 | ADR-0030 的 system 只能追加，不能按键替换；同一份指令（AGENTS.md、技能索引）刷新多次会在历史里叠加多份全文，压缩时也是全部并入基线 | 给 `Instruction` 增加可选的**节键**：同键的新值在历史中渲染为“替换通知加新文本”（或 diff），压缩时基线里只保留每个键的最新值。这样应用可以安全地做 AGENTS.md 监视刷新、日期或环境更新 | pi 按节补丁；opencode `Instructions`（initial/changed/removed 三种渲染）；Codex 世界状态 |
| 5 | 环境信息完全交给应用，但没有说明应该放在哪里 | 在 `docs/design` 或应用指南里写明：易变内容（日期、cwd、git 状态）不要进 `SessionSpec.system`，作为首条 user 消息或带键指令放入，变化时追加 delta | 五家一致 |
| 6 | `agents_md` 只读 `<root>/AGENTS.md` | 属于应用策略。若要扩展，可选：项目根→cwd 逐级、用户级、子目录读取时按需注入（作为历史条目，不改基线） | Codex、Claude Code、opencode |
| 7 | 中途改 reasoning 档位或请求参数会改顶层字段 | adapter 在模型支持时把档位变化渲染为原位项（Anthropic mid-conversation `output_config`，OpenAI `configuration_update`），与 inline tools 同样处理 | pi 1.3.0；opencode `mid-conversation-output-config-2026-07-01` beta |
| 8 | 没有缓存回归测试 | 加一条验收测试：相邻两次请求的渲染字节（去掉断点标记后），前一次必须是后一次的前缀；覆盖追加指令、增减工具、换参数、压缩后第二次请求这几种情况 | Codex `prompt_caching.rs`；Copilot 快照测试 |
| 9 | 有 `cache_read/cache_write` 数据，但没有未命中归因 | 在 `usage` 或 `trace` 视图里标出“上一请求写入，这一请求没有读到”的未命中，并给出可能原因（超过 TTL、换模型、工具变化、`prefix_epoch` 变化） | Claude Code `/usage`；pi `cache-stats.ts` |
| 10 | 低优先级 | 缓存预热（pi 按成本决定、opencode 定时）属于应用策略；图片裁剪要做迟滞或成批删除（等 Asterpath 有图片裁剪时再考虑） | pi、opencode、Claude Code |

### 7.3 未核实与需要确认

- Claude Code 的断点数量与位置、基础提示词是否按模型切换、日期的具体位置：公开文档没有写，**未核实**。
- Copilot CLI 的提示词组成与缓存策略：**未核实**（只有 changelog）。
- opencode 的 Anthropic inline tools 渲染没有细读；Copilot 中 `modeInstructions` 放在 system 里，换模式是否破坏 system 缓存属**推断**。
- Copilot 的 `chatEndpoint.ts`、`customInstructions.tsx`、`configurationService.ts` 取自 2026-05-20 的归档仓库，现行版本可能已有变化。
- Asterpath 工具描述的顺序是否严格确定（取决于 `ConfigSnapshot.tools` 的来源顺序）没有核查，建议由 §7.2 第 8 条的测试覆盖。
