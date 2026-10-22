# 参考实现研究摘要

> 2026-10-22 建图时的五份研究子智能体报告摘要。原始结论均来自一手来源；标 **[未核实]** 的条目未在报告中确认。票与 spec 引用本文件，不重复粘贴。

## 1. Pi / Pi Durable

**身份**：`earendil-works/pi`（原 `badlogic/pi-mono`），TypeScript monorepo；Pi Durable 是同一仓库的 `packages/durable`（`@earendil-works/pi-durable`），不是独立项目。

**会话/事件模型**：消息为 `AgentMessage[]`（user / assistant / toolResult + 扩展自定义）；turn = 一次 LLM 调用 + 工具执行，run = `agent_start` → turns → `agent_end`；事件有 `message_start/update/end`、`tool_execution_*`、`turn_*`。持久化是 JSONL 会话树：header + 带 `id`/`parentId` 的条目（message、model_change、compaction、context_edit、branch_summary、custom、label 等），从叶到根重建上下文。

**Pi Durable**：不可变 transcript 条目；原子 `commit`；task 是持久状态机；submission 是可等待输入。**意图先提交、执行后发生**；每次工具调用是持久化子任务；崩溃后只有声明 `replay: "safe"` 的调用重跑，否则模型收到 `interrupted` 错误并带着已提交的部分输出。`requestId` 去重；`resume()` 重排未完成工作；timer 存 deadline；重启上限默认 5。存储：memory / IndexedDB / Node SQLite / JSONL / Cloudflare DO SQLite。

**扩展**：进程内 TS 模块；`pi.on()` 生命周期 hook；`registerTool/Command/Shortcut/Flag/Provider/VirtualModel/McpServer`；`appendEntry` 写不进模型上下文的持久状态。限制：无沙箱、同 OS 权限、工具不能真正注销。Durable 的扩展是 `defineExtension({name, tools, sections, hooks, wraps, tasks})`，按 conversation `configure()`。

**子智能体**：Pi 核心没有；Pi Durable 把子会话建模为 task 拥有的 conversation，子继承 owner 的 agent，取消自底向上，父可通过 `allSettled`/`failFast` 等待。

**Provider**：`pi-ai` 统一 `stream/complete`，覆盖 OpenAI Completions/Responses、Anthropic、Google、Bedrock、DeepSeek 等；流事件 `start/text_*/thinking_*/toolcall_*/done/error`；跨 provider 交接会丢外来 thinking 块。

**限制**：Durable experimental；单进程独占存储；存储错误会让整个 harness 失败；SQLite WAL + `synchronous=NORMAL` 可能丢最新提交；事件流不可续传；无工具审批。

来源：https://github.com/earendil-works/pi · https://github.com/earendil-works/pi/tree/main/packages/durable

## 2. DeepSeek Harness

**身份**：官方 `deepseek-ai/deepseek-harness`（`dsh`），MIT，Node/TS；构建在 Cordis 插件框架上；README 是 "Everything is a Plugin"。

**插件模型**：插件是函数或 `Service` 子类，向共享 `ctx` 添加服务、类型化事件和可逆副作用；声明稳定 key（`ctx.tools`、`ctx.llm`、`ctx.sessions`）与 `inject` 依赖；通过 `ctx.effect()`/`ctx.on()` 注册，卸载时全部回卷。**没有可打补丁的特权内核**——模型 adapter、工具注册表、会话日志、agent loop 本身都是插件。事件五种派发：emit、waterfall（`next()` 环绕中间件）、parallel、serial、bail。

**扩展 seam**：LLM adapter、tools、shell、terminal、human commands、background jobs、webhook、filesystem provider/policy、sandbox、agent interception、context injection、UI、session event types、session persistence、per-agent scoping、subagents、agent presets、compaction、skills、schedules、storage、credentials、telemetry、approval/interaction。

**Loop**：turn = 零或多个 step；step = 一次模型请求 + 其工具调用。管线：claim input → 组装 prompt/tools → `agent/pre-step` → `step/start` → `agent/request` waterfall → commit → 从日志派生 frozen history → `llm/stream` → 追加 assistant 消息 → 派发工具 → `step/end`。工具管线：先记 `tool/call` → `tools/pre-execute`（allow/deny/ask）→ guards → approval → `tools/execute`（timeout/retry/metrics）→ `projectContent` → `tools/post-execute` → `tools/finalizeContent` → `tools/result`。并行/独占分类 + 滚动池；取消用 `AbortSignal`；失败尝试也记录，不伪造历史。

**会话**：append-only `SessionEvent` 是唯一真相，`deriveMessages()` 投影模型历史；不变量 **"模型可见即已记录"**。持久化是 seam，带单写者所有权；JSONL 后端有校验和、zstd 帧、每批 fsync、原子 materialize、torn-tail 截断、版本化 generation 与迁移。`agents.resume()` 恢复；在 turn 边界 fork；崩溃恢复时合成 `interrupted` 收尾。

**协议**：`LlmAdapter` 只需 `stream()`；adapter-owned replay state 只回传给同一 adapter；工具参数全程保持原始 JSON 字符串。支持 MCP、ACP stdio、SDK JSON-RPC、hook 协议桥。

**限制**：developer preview、会破坏性变更；无安全审计；插件在进程内执行宿主代码，sandbox/approval 不是安全边界；同进程工具无法硬杀。

来源：https://github.com/deepseek-ai/deepseek-harness · https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/architecture.md

## 3. Duroxide / Temporal

**Duroxide 身份**：`microsoft/duroxide`，MIT，Rust 可嵌入持久执行运行时，preview v0.1.32。

**核心**：orchestration 是确定性 async fn + `OrchestrationContext`；activity 是副作用工作 + `ActivityContext`；每个 instance+execution 一条 append-only history。`schedule_activity`、`schedule_timer`、`schedule_wait`、`dequeue_event`、子编排、`join`/`select2` 发出 action，每个 action 绑定持久事件 ID；恢复时从头重放，按顺序比对 action；不匹配即 nondeterminism failure。完成事件带 `source_event_id`，不带内存 token。

**持久化**：`Provider` trait（read、append_with_execution、worker/orchestration 队列 enqueue/fetch/ack、锁续约）；内置 `SqliteProvider`，`duroxide-pg` 独立仓库。SQLite 表：instances、executions、history、orchestrator_queue、worker_queue、sessions；peek-lock 租约；turn 在实例锁下原子提交；timer 是延迟 `visible_at` 行。**没有显式 history-schema/replay 版本字段**，只拒绝未知事件类型。

**确定性**：编排代码禁用 rand、`SystemTime::now`、文件/网络 IO、`tokio::time::sleep`、`select!`/`join!`、无序 HashMap；改用 `ctx.new_guid()`、`ctx.utc_now()`、activity、`ctx.select2/3`、`ctx.join`、有序集合。activity tag 是持久事件身份，改名会破坏重放。

**失败/定时/事件**：`RetryPolicy` 支持 per-attempt timeout 与 None/Fixed/Linear/Exponential backoff；activity 仍需幂等（执行后 ack 前崩溃会重派）。`schedule_wait(name)` 一次性、位置相关、需要预先订阅、不跨 continue-as-new；`dequeue_event(queue)` 持久 FIFO、可缓冲、跨 continue-as-new。取消是协作式，默认 30s 宽限。`continue_as_new` 开新 execution、空 history，版本升级只能在这里发生。

**Temporal 概念**：服务持久化 dispatch task、可变执行状态、append-only history、namespace、visibility；worker 生成 Command，服务记录 Event，重放比对 Command；不匹配即非确定性错误。确定性约束只覆盖产生 Command 的调用。activity 在重放路径外、at-least-once、幂等由用户负责。`SideEffect`/`MutableSideEffect` 把非确定值记入 history。timers 持久化；signals 单向记录；queries 只读；updates 有 validator 并返回结果。child workflow 有独立 history；history 上限 51,200 events / 50 MB；continue-as-new 保留 Workflow ID、新 Run ID、新 history。

**关键结论**：崩溃恢复一次 agent turn，**完整确定性重放是可选的，检查点更便宜**；跨天 HITL 才需要 durable timer、缓冲外部事件、查询、history 上限、显式版本化、取消与审计。两者共同边界：**非确定工作必须是已记录步骤，绝不能在可恢复协调器内直接做**。

**风险**：Duroxide preview、缺 replay-schema 版本、无 activity 版本化、默认 history 上限 1024、SQLite provider 无保留/清理文档，不适合作为无保留的生产依赖。

来源：https://github.com/microsoft/duroxide · https://docs.temporal.io/

## 4. Codex CLI / Claude Code

**Loop**：Codex `run_turn` 采样 → 在流内执行工具 → 只要 `model_needs_follow_up || has_pending_input` 就继续；没有固定外部迭代上限，靠压缩与取消兜底；重试受 provider `stream_max_retries` 限制；turn 前/中/后自动压缩。Claude Code 文档描述同样的循环，可选 `maxTurns` 与 `maxBudgetUsd`，默认关闭。

**工具**：Codex 的 `ToolSpec` 支持 Function / Freeform / ToolSearch / WebSearch / namespaced；注册表带策略检查；调度器解析审批与沙箱、执行、允许一次升级重试。并行调用共享 `RwLock` 读锁，独占调用拿写锁，各自 Tokio task，结果后排序。Claude Code 只读工具并发、Edit/Write/Bash 串行；自定义工具需 `readOnlyHint`。

**审批/沙箱**：Codex 两层独立——sandbox（read-only / workspace-write / danger-full-access）+ approval policy（on-request / never + 细分类别）；macOS Seatbelt、Linux bwrap+seccomp、Windows MXC；跨边界触发审批；auto_review 交给 reviewer agent 且 fail closed。Claude Code 权限规则 deny → ask → allow，首个匹配胜出；OS 沙箱 opt-in 且只覆盖 Bash/PowerShell；文件工具、MCP、hooks 在沙箱外；存在绕过沙箱的重试后门。

**会话/恢复**：Codex 用 JSONL rollout，可 resume/fork；重建时选最新可用 `Compacted` 条目、重放 replacement history、再扫 turns；处理不完整尾段与 `TurnAborted`。Claude Code transcript 是 JSONL，`--continue`/`--resume`/`--fork-session`；**崩溃时仍在运行的工具不重跑**，标记为 cut off，告诉模型核实副作用；checkpoint 只覆盖文件工具。

**子智能体**：Codex TOML 定义，异步 spawn，子默认全新 thread、可请求全历史 fork；sandbox/approval/permissions/provider/MCP 父级作用域；并发 `agents.max_concurrent_threads_per_session`。Claude Code Markdown+YAML 定义；普通子智能体全新上下文、fork 继承全历史；只返回最终总结；`SendMessage` 可续聊；默认深度 3、并发 20。

**Hooks**：Codex 有 SessionStart/End、UserPromptSubmit、PreToolUse、PermissionRequest、PostToolUse、Pre/PostCompact、SubagentStart/Stop、Stop、Interrupt；PreToolUse 可拒绝/改写输入/加上下文。Claude Code hook 面更大，handler 可为 command/HTTP/MCP tool/prompt/agent，并行执行；exit 2 阻断，JSON 可 allow/deny/ask/rewrite。

**取消/steering**：Codex 取消产出 `TurnAborted`；运行中新输入排队，在 step 边界 drain。Claude Code Esc 取消当前工具并等待；输入在当前工具结束后、同一 turn 内读取。

来源：https://github.com/openai/codex · https://code.claude.com/docs/

## 5. Anthropic Messages / OpenAI Responses

**请求**：Anthropic `POST /v1/messages`，top-level `system`，`messages[{role, content}]`，`tools[{name, description, input_schema}]`，`tool_choice` auto/any/tool/none + `disable_parallel_tool_use`，`max_tokens` 必填，`thinking`；2026-10-21 版移除 `temperature`/`top_p`/`top_k`。OpenAI `POST /v1/responses`，`instructions`，`input` 字符串或 item 列表，`tools`，`tool_choice` none/auto/required/function/allowed_tools，`max_output_tokens`，`previous_response_id`/`conversation`/`store`，`reasoning{effort,summary,context}`，`text.format`，`truncation`。

**响应**：Anthropic `content[]` 块（text / tool_use / thinking / redacted_thinking / server_tool_use + result），`stop_reason` 含 `tool_use`、`pause_turn`、`refusal`、`model_context_window_exceeded`。OpenAI `output[]` item（message/function_call/function_call_output/reasoning/builtin call/compaction），`status` queued/in_progress/completed/failed/cancelled/incomplete，`incomplete_details.reason`。

**Usage**：Anthropic `input_tokens` 不含缓存，另有 `cache_creation_input_tokens`、`cache_read_input_tokens`、`output_tokens_details.thinking_tokens`；总输入 = 三者相加。OpenAI `input_tokens` 含缓存，另有 `input_tokens_details.{cached_tokens,cache_write_tokens}`、`output_tokens_details.reasoning_tokens`、`total_tokens`。**必须显式归一化 inclusive/exclusive。**

**流式**：Anthropic 命名 SSE：`message_start` → 每块 `content_block_start` → delta（text/input_json/thinking/signature）→ `content_block_stop` → `message_delta` → `message_stop`，另有 ping/error；块按 `index` 定位。OpenAI：`response.created` → `in_progress` → `output_item.added` → `content_part.added` → deltas → 匹配 `...done` → `output_item.done` → `completed|incomplete|failed`；事件带 `sequence_number`；**[未核实] 完整规范顺序**，adapter 应容忍 item 事件交错。

**工具**：Anthropic `tool_use{id, name, input: object}`，结果 `tool_result{tool_use_id, content, is_error?}` 放在下一条 **user** 消息里，结果排在文本前；并行调用是多块，结果同一条 user 消息。OpenAI `function_call{id, call_id, name, arguments: JSON string}`，结果 `function_call_output{call_id, output}` 是顶层 item，无错误标志；`parallel_tool_calls:false` 限制为 ≤1。差异：object vs JSON 字符串、错误信号、块 vs 顶层 item。

**状态**：Anthropic 无状态，客户端每次回传 system、tools、全部消息与每个 thinking 块。OpenAI 可选服务端状态：`store:true`（默认，保留 30 天）、`previous_response_id`、`conversation{id}`；无状态使用时必须回放全部 output item，包括 reasoning item。

**Reasoning**：Anthropic `thinking{thinking, signature}`，signature 是加密的完整推理，必须原样回传（包括空文本 `display:"omitted"` 块）；绑定生产模型与前缀。OpenAI `reasoning{id, summary, content?, encrypted_content?, status}`，必须回放进 `input`；`store:false` 时请求 `include:["reasoning.encrypted_content"]`。**两者的不透明形式都不可跨供应商翻译。**

**缓存**：Anthropic 显式 `cache_control:{type:"ephemeral", ttl?}`，最多 4 个断点，最小 512–4096 tokens，前缀顺序 tools→system→messages，5m/1h。OpenAI 默认自动前缀缓存，可选显式断点，GPT-5.6+ 最小 1024 tokens，30m/12h；模型/工具/配置变化会使缓存失效。

**真正的不兼容**：1) 角色消息 + 内容块树 vs 扁平 item 列表；2) Anthropic 因 `tool_use` 结束回合，OpenAI 工具调用只是 item、响应仍 completed；3) reasoning 契约不同且不可翻译；4) 服务端状态无 Anthropic 对应物；5) sampling 在 Anthropic 已移除；6) 工具参数 object vs JSON 字符串；7) usage 缓存口径相反；8) 消息 item 内含 parts vs 块在顶层。

**内部事件模型应保留**：有序 item 日志（Message / Reasoning / ToolCall / ToolResult / ServerToolCall/Result / Compaction / Refusal），同时保留 raw provider JSON；把 `TurnEnd` 与 item 事件分开，不从传输层完成推断；流式事件带单调序号；usage 显式区分 cached-read / cache-write / reasoning 与 inclusive/exclusive；能力标志（server_state、forced_tool_choice、sampling、reasoning_roundtrip、citations、builtin_tools）；跨 provider 不合成不透明 reasoning，丢弃并打标记。

来源：https://platform.claude.com/docs/en/api/messages · https://developers.openai.com/api/reference/resources/responses/methods/create
