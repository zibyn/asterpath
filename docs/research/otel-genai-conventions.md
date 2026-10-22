# OpenTelemetry GenAI 语义约定研究

> 研究票：zibyn/asterpath#14 · 完成日期：2026-10-22
>
> 一手来源：`open-telemetry/semantic-conventions-genai`，commit [`9ffc7e0`](https://github.com/open-telemetry/semantic-conventions-genai/commit/9ffc7e00c639be7712e544e3e54331e885dccf84)（2026-10-21T19:06:47Z）。该 commit 的 [`model/manifest.yaml`](https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/manifest.yaml) 声明 `schema_url: https://opentelemetry.io/schemas/gen-ai-dev/1.42.0-dev`、`stability: development`，并依赖核心约定 `v1.44.0`。
>
> 本文只做约定研究与 Asterpath 建议；未引入 OTel SDK，未修改其他文件。未能在上述一手来源中确认的内容标为“未核实”。

## 0. 结论摘要

- 截至 2026-10-22，GenAI 约定整体是 **Development（预稳定，等同 experimental）**，不是 Stable，也不是 Deprecated。仓库没有正式 release/tag，只能按 `1.42.0-dev` schema URL 加 commit 引用。
- 当前约定覆盖 model call、agent invocation、workflow、plan、tool execution、token usage、content、exceptions、metrics；**不覆盖** session/turn/step 的 span 层级、subagent span 类型、成本/总 token 属性、缓存断点/TTL、重放语义。
- 最值得 Asterpath 借用的是 **token 归一化口径**（input 含缓存、output 含 reasoning、cache/reasoning 是子集、billed 优先）和 **错误/状态语义**（`error.type` + 成功不设状态、失败 `Error`）。
- 最需要拒绝的是把 OTel 的 agent span 模型当成 Asterpath 的内部层级，以及把 prompt/completion 默认写进 trace。Asterpath 的 append-only 事件日志才是内容与重放的真相源。

## 1. 位置、版本与稳定性

### 1.1 文档已经迁出核心仓库

- `opentelemetry.io` 的 GenAI 规范页现在只是迁移提示：“GenAI semantic conventions have moved to the OpenTelemetry GenAI semantic conventions repository”，并声明 “This page has moved and is no longer maintained in this repository.”（[OTEL-MOVED]）
- 核心 `semantic-conventions` 仓库通过 commit `c9e48b1`（2026-05-05，“Move GenAI semantic conventions to its own dedicated repository (#3696)”）完成迁移。（[MOVE]）
- 因此旧版 `opentelemetry.io/docs/specs/semconv/gen-ai/*` 不能再作为规范正文引用；本文全部规范事实来自新仓库的固定 commit。

### 1.2 稳定性

`docs/gen-ai/README.md` 写的是 **`Status: Development`**；各分页（client inference、agent spans、events、exceptions、metrics、token metrics、Anthropic、OpenAI）同样标为 Development。（[README], [CI], [AGENT], [EVENTS], [EXC], [METRICS], [TOKM], [ANTH], [OPENAI]）

OTel 的成熟度定义中，Development 意味着“SHOULD NOT be used in production”，组件可能在没有预先通知的情况下被移除；这与 Stable 的 “ready for general availability” 相对。（[MATURITY]）本文把 Development 视为 experimental，但保留 OTel 的原文状态名。

| 范围 | 当前状态 | 证据 |
| --- | --- | --- |
| 全部 GenAI 文档 | Development | [README] |
| 绝大多数 `gen_ai.*` span/属性/指标 | Development | [CI], [AGENT], [TOKM] |
| `error.type` | Stable | [CI], [AGENT], [REC-ERR] |
| `server.address`, `server.port` | Stable | [CI], [AGENT] |
| `process.executable.name`, `process.executable.path`, `process.exit.code` | Release Candidate | [AGENT] |
| turn/step/session span | not defined | 见 §8 |
| `gen_ai.usage.total_tokens`、成本/价格属性 | not defined | 见 §6、§8 |
| 缓存断点、TTL、`cache_control` | not defined | 见 §7.3 |
| 进入 Stable 的时间表 | **未核实** | 仓库没有 roadmap/release，tag 为空 |

版本引用建议：`semantic-conventions-genai@9ffc7e0` + schema URL `gen-ai-dev/1.42.0-dev`。不要写“OTel GenAI v1.x stable”，当前不存在这样的版本。

## 2. Span 总览

GenAI 约定把 span 定义为“caller 观察到的逻辑操作”；span 覆盖从发起到完整响应、错误或取消；自动重试应折叠进一个逻辑 span。（[SPANS]）

| 对象 | span type / `gen_ai.operation.name` | span name | kind | 主要属性 |
| --- | --- | --- | --- | --- |
| 推理/模型调用 | `gen_ai.client.inference` / `chat`, `generate_content`, `text_completion` | `{gen_ai.operation.name} {gen_ai.request.model}` | `CLIENT`（同进程可 `INTERNAL`） | operation、provider、request/response model、finish reasons、usage、content（opt-in） |
| Embeddings | `gen_ai.embeddings.client` / `embeddings` | `{operation} {model}` | `CLIENT` | model、usage.input_tokens、dimension |
| Retrieval | `gen_ai.retrieval.client` / `retrieval` | `{operation} {data_source.id}` | `CLIENT` | data_source.id、top_k、documents（opt-in） |
| Fetch response | `gen_ai.fetch_response.client` / `fetch_response` | `fetch_response` | `CLIENT` | response.id、response.status；**不报 token usage** |
| Memory | `gen_ai.memory.client` / `create_memory`, `search_memory` 等 | `{operation}` | `CLIENT`（同进程可 `INTERNAL`） | memory store/record id |
| 工具执行 | `gen_ai.execute_tool.internal` / `execute_tool` | `execute_tool {gen_ai.tool.name}` | `INTERNAL` | tool.name、tool.call.id、tool.type |
| 创建 agent | `gen_ai.create_agent.client` / `create_agent` | `create_agent {gen_ai.agent.name}` | `CLIENT` | agent.id/name/description/version |
| 调用 agent（远程） | `gen_ai.invoke_agent.client` / `invoke_agent` | `invoke_agent {gen_ai.agent.name}` | `CLIENT` | agent 身份、conversation.id、usage |
| 调用 agent（同进程） | `gen_ai.invoke_agent.internal` / `invoke_agent` | 同上 | `INTERNAL` | 同上，但无 provider/server 字段 |
| 工作流 | `gen_ai.invoke_workflow.internal` / `invoke_workflow` | `invoke_workflow {gen_ai.workflow.name}` | `INTERNAL` | workflow.name、conversation.id |
| 规划 | `gen_ai.plan.internal` / `plan` | `plan {gen_ai.agent.name}` | `INTERNAL` | agent.name |

（[CI], [SPANS], [AGENT]）

`gen_ai.operation.name` 的 well-known 值（全部 Development）：`chat`, `create_agent`, `create_memory`, `create_memory_store`, `delete_memory`, `delete_memory_store`, `embeddings`, `execute_tool`, `fetch_response`, `generate_content`, `invoke_agent`, `invoke_workflow`, `plan`, `retrieval`, `search_memory`, `text_completion`, `update_memory`, `upsert_memory`。（[CI], [AGENT]）

`gen_ai.provider.name` 的 well-known 值（全部 Development）包括 `anthropic`, `openai`, `aws.bedrock`, `azure.ai.inference`, `azure.ai.openai`, `deepseek`, `gcp.gemini`, `gcp.gen_ai`, `gcp.vertex_ai`, `mistral`, `x_ai` 等。（[CI], [AGENT]）

## 3. Model / LLM call

### 3.1 Span

- Span 类型：`gen_ai.client.inference`；span name SHOULD 为 `"{gen_ai.operation.name} {gen_ai.request.model}"`。
- Span kind SHOULD 为 `CLIENT`；同进程模型 MAY 为 `INTERNAL`。推荐在模型通常运行在另一进程、或调用经过 HTTP 等被插桩协议时使用 `CLIENT`。
- Requirement level：Recommended。
- 文档描述：代表 “a client call to Generative AI model or service that generates a response or requests a tool call based on the input prompt”。（[CI]）

**Required 属性**：`gen_ai.operation.name`、`gen_ai.provider.name`。

**Conditionally Required**：`error.type`、`gen_ai.conversation.id`、`gen_ai.output.type`、`gen_ai.prompt.name`、`gen_ai.prompt.version`、`gen_ai.request.choice.count`、`gen_ai.request.model`、`gen_ai.request.seed`、`gen_ai.request.stream`、`gen_ai.request.top_k`、`server.port`。

**Recommended**：`gen_ai.conversation.compacted`、`gen_ai.request.frequency_penalty`、`gen_ai.request.max_tokens`、`gen_ai.request.presence_penalty`、`gen_ai.request.previous_response.id`、`gen_ai.request.reasoning.level`、`gen_ai.request.stop_sequences`、`gen_ai.request.temperature`、`gen_ai.request.top_p`、`gen_ai.response.finish_reasons`、`gen_ai.response.id`、`gen_ai.response.model`、`gen_ai.response.time_to_first_chunk`、所有 `gen_ai.usage.*`、`server.address`。

**Opt-In**：`gen_ai.input.messages`、`gen_ai.output.messages`、`gen_ai.prompt.variable.*`、`gen_ai.system_instructions`、`gen_ai.tool.definitions`。（[CI]）

`gen_ai.response.finish_reasons` 是按返回的 choice/candidate 顺序排列的字符串数组；如果某个位置因失败、取消或流提前结束而没有预期的 finish reason，该位置用 `error`，不能省略。（[AGENT]）

### 3.2 事件

- `gen_ai.client.inference.operation.details`：描述一次推理请求（含 chat history 和参数），可在 trace 之外独立保存输入/输出；SHOULD 用 `DEBUG`（severity number 5）；属性集与 inference span 相同。（[CI]）
- `gen_ai.client.operation.exception`：GenAI client 操作异常事件，SHOULD 用 `WARN`（severity number 13）；属性为 `exception.message`、`exception.type`、`exception.stacktrace`；`exception.message` 和 `exception.type` 至少有一个。（[EXC]）
- `gen_ai.evaluation.result`：评估结果事件，SHOULD 挂到被评估的 GenAI operation span；没有 span id 时用 `gen_ai.response.id` 关联；属性包括 `gen_ai.evaluation.name`、`gen_ai.evaluation.score.label`、`gen_ai.evaluation.score.value`、`gen_ai.evaluation.explanation`、`gen_ai.response.id`。（[EVENTS]）

### 3.3 状态码

GenAI 文档不定义自己的状态码枚举，只要求遵循 OTel 的 Recording Errors：（[CI], [AGENT], [REC-ERR]）

- 成功：span status code **MUST be left unset**（不是 `Ok`）。
- 失败：SHOULD 设为 `Error`，并设置 `error.type`；可选 status description 写入非敏感信息。
- `error.type` 必须是低基数标识：provider/client error code、异常规范名或类似值；well-known fallback 是 `_OTHER`。
- 不要重复把状态码或 `error.type` 写进 status description。
- 被重试或已被优雅处理的错误 SHOULD NOT 记录到描述该操作的 span/metric 上。
- 自动重试应折叠进一个逻辑 span，覆盖所有 retry 的总时长。

## 4. Agent run / turn / workflow

### 4.1 已定义的 span

- `create_agent {gen_ai.agent.name}`（CLIENT）：创建一个 agent 资源，例如 hosted agent。这不是“定义一个本地 agent 配置”的通用操作。（[AGENT]）
- `invoke_agent {gen_ai.agent.name}`：一次 agent 调用。远程 agent 用 CLIENT；同进程 agent 用 INTERNAL。属性包含 `gen_ai.agent.id`、`gen_ai.agent.name`、`gen_ai.agent.description`、`gen_ai.agent.version`、`gen_ai.conversation.id`，以及模型、usage、content（opt-in）。（[AGENT]）
- `invoke_workflow {gen_ai.workflow.name}`（INTERNAL）：协调多个 agent 或其他 GenAI 操作的工作流。只有用户可见的 workflow 入口才上报；如果 workflow 只是另一个 operation 的内部实现细节，不应单独上报。嵌套的应用定义 workflow 仍应上报。（[AGENT]）
- `plan {gen_ai.agent.name}`（INTERNAL）：agent 的规划/任务分解阶段。只有能可靠区分“规划”和普通推理时才上报。显式层级规则：产生计划的 LLM call 是 plan span 的子 span；由计划产生的 tool/task span 通常是同一 `invoke_agent` 下的兄弟节点。（[AGENT]）

### 4.2 没有 turn / step / session / subagent span

- **turn**：未定义。现行 `docs/gen-ai` 中没有名为 turn 的 span、属性或事件。
- **step**：未定义。没有 step span，也没有 step 级属性。
- **session**：未定义 session span。唯一相关的是 `gen_ai.conversation.id` 属性；文档把它描述为 “The unique identifier for a conversation (session, thread)”。（[CI], [AGENT]）
- **subagent**：未定义独立的 subagent span 类型。最接近的建模是嵌套的 `invoke_agent`（子 agent 的调用作为父 step/tool 的子 span）。
- **agent run**：OTel 没有 “run” 这个词；最接近的是 `invoke_agent` 所代表的一次 agent invocation。它是否对应 Asterpath 的 session 或 turn，需要 Asterpath 自己决定（见 §9.1、§11）。

### 4.3 conversation.id 规则

`gen_ai.conversation.id` 只在已经存在标识符、或应用通过 OpenTelemetry context / library 机制提供时设置。文档明确禁止用新 UUID、trace ID 或请求内容哈希来合成 fallback。（[AGENT]）

这条规则对 Asterpath 有利：Asterpath 本来就有真实 session id，可以直接作为 `gen_ai.conversation.id` 导出，不需要合成。

### 4.4 agent 身份规则

- `gen_ai.agent.id`：稳定的唯一标识；hosted agent 用 provider 分配的 resource id；不要记录 transient in-memory instance id。
- `gen_ai.agent.name`：人类可读名称。
- `gen_ai.agent.description`：自由文本描述。
- `gen_ai.agent.version`：agent 版本。
- `gen_ai.request.model`：只有在被插桩库保证“一个 agent 只有一个模型”时才填；多模型或动态选择时应省略。（[AGENT]）

## 5. Tool call

- Span name：`execute_tool {gen_ai.tool.name}`；kind `INTERNAL`；operation 为 `execute_tool`；Requirement level Recommended。（[SPANS]）
- Required：`gen_ai.operation.name`、`gen_ai.tool.name`。
- Conditionally Required：`error.type`、`gen_ai.agent.name`、`gen_ai.conversation.id`、`gen_ai.tool.call.id`。
- Recommended：`gen_ai.tool.description`、`gen_ai.tool.type`。
- Opt-In：`gen_ai.tool.call.arguments`、`gen_ai.tool.call.result`。
- `gen_ai.tool.type` 的 well-known 值：`function`（client 侧执行模型生成的参数）、`extension`（agent 侧直接调用外部 API）、`datastore`（结构化/非结构化数据访问）。（[SPANS]）

语义要点：

- 工具执行应该被追踪，除非另一个 instrumentation 已经可靠覆盖所有工具类型；MCP 工具可能由 MCP instrumentation 覆盖；应用自己的工具应手工插桩。
- 不要为一次调用记录两个不同的 span。
- 参数和结果预期是对象；序列化字符串应尽量反序列化；可能含敏感信息；在 event 上必须结构化，在 span 上优先结构化、结构化不支持时才允许 JSON 字符串。（[SPANS]）
- OTel 的 `execute_tool` 描述的是**工具执行**，不是模型发出 tool call 的决策。模型侧的 tool call 以 message part `tool_call` 表示（见 §7.1），并以 `gen_ai.tool.call.id` 关联。

### 5.1 Agent Skills 的 refinements

`docs/gen-ai/gen-ai-agent-spans.md` 还为 agent skills 定义了 execute_tool 的细化形式：

- Load skill span：`execute_tool {tool.name} {skill.name}`，属性 `gen_ai.skill.name`、`gen_ai.skill.description`、`gen_ai.skill.source.uri`。
- Read skill resource span：`execute_tool {tool.name} {skill.name} {skill.resource.name}`。
- Command execution span：可追加 `gen_ai.skill.resource.name`、`process.executable.name`、`process.executable.path`、`process.exit.code`；`process.*` 是 Release Candidate。（[AGENT]）

这些是 Asterpath 当前不需要的可选 refinement（见 §11）。

## 6. Token usage

### 6.1 Span 属性

| 属性 | 类型 | 语义 |
| --- | --- | --- |
| `gen_ai.usage.input_tokens` | int | 输入 token 总数，**包含** cached tokens。provider 给总量就用总量，否则汇总解析出的类别。billed 与 model-consumed 同时存在时，报 billed。 |
| `gen_ai.usage.output_tokens` | int | 输出 token 总数，**包含** reasoning tokens。 |
| `gen_ai.usage.cache_read.input_tokens` | int | 从 provider-managed cache 读取的输入 token，**是 input_tokens 的子集**。 |
| `gen_ai.usage.cache_write.input_tokens` | int | 写入 provider-managed cache 的输入 token，**是 input_tokens 的子集**。 |
| `gen_ai.usage.reasoning.output_tokens` | int | 用于 reasoning/chain-of-thought/extended thinking 的输出 token，**是 output_tokens 的子集**。 |
| modality 子集：`gen_ai.usage.text.input_tokens`、`gen_ai.usage.image.input_tokens`、`gen_ai.usage.audio.input_tokens`，以及对应的 `output_tokens`、`cache_read.input_tokens` | int | 按 modality 拆分的子集；各自也包含在对应的 aggregate 中。 |

（[CI], [AGENT], [ANTH], [OPENAI]）

关键不变量：

- `input_tokens` 是总输入；所有 modality input 以及 cache_read/cache_write 都**已经 inside** 这个总数，不能再加一次。
- `output_tokens` 是总输出；所有 modality output 以及 reasoning 都**已经 inside** 这个总数，不能再加一次。
- 文档给的例子：100 text tokens（40 cached）+ 200 image tokens → total input 300、total cache-read 40、text input 100、text cache-read 40、image input 200。（[CI]）
- 对于 token metrics，每个 measurement 必须是已知且严格为正的 modality contribution；未知 modality 报 `unknown`。当前 HEAD 的 commit message 就是 “Record only positive inference usage contributions (#602)”。（[TOKM]）

### 6.2 Provider 归一化

Anthropic：

```text
gen_ai.usage.input_tokens = input_tokens + cache_read_input_tokens + cache_write_input_tokens
gen_ai.usage.cache_read.input_tokens = cache_read_input_tokens
gen_ai.usage.cache_write.input_tokens = cache_write_input_tokens
gen_ai.usage.output_tokens = output_tokens
gen_ai.usage.reasoning.output_tokens = Anthropic 的 thinking token 计数
```

Anthropic 的 `input_tokens` **不含** cached tokens，所以必须相加。OTel 的 Anthropic 页没有给出 thinking token 的具体 usage 字段名（字段名见协议研究，此处 **未核实**）；cache_read/cache_write 字段名则由该页明确给出。（[ANTH]）

OpenAI：

```text
gen_ai.usage.input_tokens = input_tokens
gen_ai.usage.cache_read.input_tokens = input_tokens_details.cached_tokens
gen_ai.usage.cache_write.input_tokens = adapter 原始 usage 的 cache-write 字段
gen_ai.usage.output_tokens = output_tokens
gen_ai.usage.reasoning.output_tokens = output_tokens_details.reasoning_tokens
```

OpenAI 的 `input_tokens` 已包含 cached tokens，不能再次相加。OTel 的 OpenAI 页只规定 cache-write 值应计入 `input_tokens`，没有给出具体字段名（具体字段名 **未核实**）。（[OPENAI]）

两者都遵循 “billed 优先于 model-consumed”。（[ANTH], [OPENAI]）

### 6.3 Metrics

所有 GenAI token metrics 都是 Development / Recommended：

| Metric | Instrument | Unit |
| --- | --- | --- |
| `gen_ai.client.inference.usage.input_tokens` | Counter | `{token}` |
| `gen_ai.client.inference.usage.output_tokens` | Counter | `{token}` |
| `gen_ai.client.inference.usage.cache_read.input_tokens` | Counter | `{token}` |
| `gen_ai.client.inference.usage.cache_write.input_tokens` | Counter | `{token}` |
| `gen_ai.client.inference.usage.reasoning.output_tokens` | Counter | `{token}` |
| `gen_ai.client.inference.operation.input_tokens` | Histogram | `{token}` |
| `gen_ai.client.inference.operation.output_tokens` | Histogram | `{token}` |
| `gen_ai.client.inference.duration` | Histogram | `s` |
| `gen_ai.client.inference.time_to_first_chunk` | Histogram | `s` |
| `gen_ai.client.inference.time_per_output_chunk` | Histogram | `s` |

（[TOKM], [CI]）

- 五个 usage counter 必需属性：`gen_ai.operation.name`、`gen_ai.provider.name`、`gen_ai.token.modality`（`text`/`image`/`audio`/`unknown`）；`gen_ai.request.model` 条件必需；`server.port` 在设置 `server.address` 时条件必需；`gen_ai.response.model`、`server.address` 推荐。
- 两个 operation histogram **没有** `gen_ai.token.modality`，因为跨 modality 的百分位不可相加。
- operation histogram 的 bucket 边界：`[1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864]`。
- usage counter 用于总量/成本近似；operation histogram 只用于单次操作的分布，不能用于总量或成本。
- 成功和失败的推理操作都应记录贡献；usage counter 不带 `error.type`，operation histogram 带条件必需的 `error.type`。（[TOKM], [METRICS]）

### 6.4 没有 total / cost

现行 GenAI 约定没有 `gen_ai.usage.total_tokens`，也没有成本、价格、币种或缓存折扣属性。total 必须由 `input_tokens + output_tokens` 自己算；成本属于 Asterpath 的本地计价层。（在 `model/gen-ai/registry.yaml` 的 `gen_ai.usage.*` 列表中未出现 total；docs 中也没有 cost 属性。）

## 7. 内容、reasoning、缓存、错误

### 7.1 Prompt / completion 内容

默认行为是**不记录** instructions、inputs、outputs。可选方案：完全不记录；记录到 span 属性；或存到外部存储并在 span 上留引用。（[SPANS]）

四个 opt-in 内容属性（全部 Development，类型 `any`）：

| 属性 | 语义 | 备注 |
| --- | --- | --- |
| `gen_ai.system_instructions` | provider/API 单独传入的 system instructions | 嵌在 chat history 里的 instructions 应放进 `gen_ai.input.messages` |
| `gen_ai.input.messages` | 按发送给模型的顺序记录的消息 | 必须遵循 input-messages JSON schema |
| `gen_ai.output.messages` | 每个 choice/candidate 一条消息，不能跨消息拆分 choice | 必须遵循 output-messages JSON schema；出错时只有产生了输出（含部分流式输出）才记录，不能合成 |
| `gen_ai.tool.definitions` | 工具定义 | 可能很大且敏感；不建议默认填可选属性 |

（[CI], [SPANS], [INPUT], [OUTPUT]）

消息 schema 的结构是 `{role, parts[]}`（input 还允许 `name`；output 允许 `name`、`finish_reason`，但 `finish_reason` 在 schema 中已标 deprecated，要求改用 `gen_ai.response.finish_reasons`）。`role` 的 well-known 值是 `system`/`user`/`assistant`/`tool`。（[INPUT], [OUTPUT]）

part 的 `type` well-known 值：`text`、`reasoning`、`tool_call`、`tool_call_response`、`server_tool_call`、`server_tool_call_response`、`blob`、`file`、`uri`、`compaction`，以及允许 provider 自定义类型的 generic part。（[INPUT], [OUTPUT]）

`gen_ai.prompt.variable.*` 是动态 key：变量名成为属性后缀，例如 `user_name = Alice` → `gen_ai.prompt.variable.user_name = "Alice"`；文档警告可能含敏感信息。（[CI]）

### 7.2 Reasoning

- 请求侧：`gen_ai.request.reasoning.level`（string，Development），记录 provider 的原始 reasoning/thinking effort 值。Anthropic 对应 `output_config.effort`；OpenAI 对应 `reasoning.effort`（示例 `low`/`medium`/`high`）。（[ANTH], [OPENAI]）
- 用量侧：`gen_ai.usage.reasoning.output_tokens`（int，Development，子集）。
- 内容侧：**没有单独的 reasoning 内容属性**。reasoning 作为 output message 的 `reasoning` part（`{type:"reasoning", content}`）表示；Anthropic 的 `redacted_thinking` 在非规范测试用例中通过 generic part 承载（`{type:"redacted_thinking", data:...}`），不是正式的 well-known part 类型。（[OUTPUT], [OUTPUT-TEST]）
- OTel 不定义跨 provider 的 reasoning 翻译规则；Anthropic 的 signature/redacted 形式不能被翻译成 OpenAI 的 encrypted reasoning，反之亦然（这与 #12 参考笔记的协议研究一致）。

### 7.3 Caching

现行 GenAI 约定只用 token 计数表示 provider-managed cache：

- `gen_ai.usage.cache_read.input_tokens`
- `gen_ai.usage.cache_write.input_tokens`
- 以及对应的 modality 子集。

（[CI], [ANTH], [OPENAI]）

**没有** `cache_control`、cache breakpoint、TTL、min-token 阈值或缓存命中内容的属性。（在 `docs/gen-ai/*.md` 与 `model/gen-ai/registry.yaml` 中检索 `cache_control`/TTL 无结果。）Asterpath 若需要这些语义，必须作为原生字段自行定义，不能声称是 OTel 约定。

### 7.4 Errors

- `error.type`（string，Stable）：低基数错误类；SHOULD 匹配 provider/client error code、canonical exception name 或类似值；well-known fallback `_OTHER`。GenAI 文档没有专门的 error value 枚举。（[CI], [AGENT], [EXC]）
- `gen_ai.client.operation.exception` 事件记录异常，WARN；`exception.message`、`exception.type`、`exception.stacktrace`。（[EXC]）
- Span status：成功不设（Unset），失败设 `Error`；不要重复状态码/`error.type` 到 description；不要记录被重试或已处理的错误。（[REC-ERR]）
- 缺失预期输出时，`gen_ai.response.finish_reasons` 对应位置用 `error`，而不是省略。（[AGENT]）
- `gen_ai.execute_tool` 的失败同样走 `error.type` + Recording Errors；没有单独的 tool error 状态枚举。（[SPANS]）

## 8. 未定义 / 不适用清单

| 概念 | 状态 | 说明 |
| --- | --- | --- |
| session span | not defined | 只有 `gen_ai.conversation.id` 属性 |
| turn span / turn id | not defined | 现行 GenAI 文档没有 turn 概念 |
| step span / step id | not defined | 没有 step 概念 |
| subagent span 类型 | not defined | 用嵌套 `invoke_agent` 近似 |
| agent run 术语 | not defined | 最接近的是 `invoke_agent` |
| `gen_ai.usage.total_tokens` | not defined | 自行用 input + output 计算 |
| 成本/价格/币种/折扣 | not defined | 不属于 GenAI 约定 |
| 缓存断点/TTL/`cache_control` | not defined | 只有 cache token 计数 |
| retry count 属性 | not defined | 自动重试折叠进一个逻辑 span |
| replay / attempt / dedupe 语义 | not defined | 没有 replay 标记或幂等键约定 |
| sampling 策略 | not defined | 只建议哪些属性在 span 创建时可用 |
| redaction 规则 | not defined | 只有敏感信息警告，没有规范性脱敏规则 |
| reasoning 内容属性 | not defined | reasoning 放在 message part 中 |
| 按消息类型拆分的旧事件（如 `gen_ai.system.message`） | not defined in current docs | 现行 `docs/gen-ai` 未定义；是否曾正式 deprecated **未核实**，不采用 |
| 进入 Stable 的日期/路线图 | **未核实** | 仓库无 release/tag/roadmap |

## 9. 对 Asterpath 原生追踪的直接影响

前提：Asterpath 的 append-only 事件日志是持久化与重放的真相源；trace 是事件日志的投影。OTel 约定只影响**命名、字段口径和可选导出**，不应反向决定内部层级。

### 9.1 Span hierarchy

建议保留原生 `session → turn → step → model call / tool call / subagent / workflow`，并按下面的方式映射到 OTel 形状：

| Asterpath 层级 | 是否保留为原生 span | OTel 导出映射 |
| --- | --- | --- |
| session | 是（根） | 不导出 OTel session span；在所有导出 span 上带 `gen_ai.conversation.id = session_id` |
| turn | 是 | OTel 无对应物。导出为 `INTERNAL` 的原生命名 span，或只在原生 trace 中保留 |
| step | 是 | OTel 无对应物。导出为 `INTERNAL` 的原生命名 span |
| model call | 是 | 按 `gen_ai.client.inference` 语义：`{operation} {model}`、`CLIENT`、模型/usage/status 属性 |
| tool call | 是 | `execute_tool {tool.name}`、`INTERNAL` |
| subagent | 是 | 嵌套 `invoke_agent {agent.name}`、`INTERNAL` |
| workflow | 是 | `invoke_workflow {workflow.name}`、`INTERNAL` |
| plan | 可选 | 只有能可靠区分规划时才导出 `plan {agent.name}` |

具体规则：

- **不要把 `invoke_agent` 当成“每个 turn 一个 span”。** OTel 的 `invoke_agent` 是 agent invocation，不是 turn；按 turn 生成会混淆语义。建议把 subagent 的一次运行映射为嵌套 `invoke_agent`；顶层 session 是否也映射为 `invoke_agent` 属于待定项（§11）。
- **保留 turn/step 作为原生 INTERNAL span。** 不要为了 OTel 兼容把它们压平成 model call 的兄弟节点；否则 Asterpath 的重放、token 聚合和取消边界会丢失。
- **父关系**：model call 和 tool call 的父是 step；subagent 的父是发起它的 step；subagent 内部的 model/tool call 的父是 subagent 的 `invoke_agent`；workflow 的 activities 的父是 workflow。OTel 唯一明确的层级规则是 plan：产生计划的 LLM call 是 plan 的子，计划产生的 tool/task 与 plan 同级挂在 `invoke_agent` 下；这条可以借用。
- **completion 内容不要默认进 trace**：session/turn/step span 默认只带结构字段和事件引用；prompt/completion 留在事件日志，按需导出。
- **create_agent 不适用**：Asterpath 的 agent 是本地配置/类型，不是 hosted agent 资源；不要为构造 agent 生成本地 span 去凑 `create_agent`。

### 9.2 Token accounting fields

建议原生 `TokenUsage` 采用与 OTel 相同的语义，但保持 Rust 类型：

```rust
struct TokenUsage {
    input_tokens: u64,              // inclusive: uncached + cache_read + cache_write
    output_tokens: u64,             // inclusive: visible + reasoning
    cache_read_input_tokens: u64,   // subset of input_tokens
    cache_write_input_tokens: u64,  // subset of input_tokens
    reasoning_output_tokens: u64,   // subset of output_tokens
    // total_tokens = input_tokens + output_tokens，计算得出，不落库
    estimated: bool,                // 本地 tokenizer 估算时为 true
    source: UsageSource,            // Provider / LocalTokenizer
    // 可选：modality: HashMap<Modality, ModalityUsage>，不是核心必需字段
}
```

规则：

- **Adapter 边界归一化**：Anthropic 的 `input_tokens` 不含 cache，必须加 cache_read + cache_write；OpenAI 的 `input_tokens` 已含 cache，不能再加。两边都优先报 billed，而不是 model-consumed。（[ANTH], [OPENAI]）
- **子集不叠加**：`cache_read`、`cache_write`、`reasoning` 已包含在 `input`/`output` 中；聚合时只加 `input + output`，不能再把子集加进去。（[CI], [TOKM]）
- **聚合层级**：model call → step → turn → session。subagent 的 usage 归到 subagent 自己的子树；父级 rollup 用显式的 `include_descendants` 标志，默认不重复计数。OTel 的 `gen_ai.invoke_agent.inference_calls` / `tool_calls` 也明确把 subagent 的调用记在 subagent 自己的 invocation 上，每个 call 在整棵树里只计一次；这条聚合不变量可以借用。（[METRICS]）
- **失败/取消**：只要有可用的 provider usage 就记录；usage counter 只记录严格为正的贡献；不要因为失败就不记，也不要伪造用量。（[TOKM]）
- **重试**：一个逻辑 model call 的 usage 只记一次，不按 transport attempt 重复累计；attempt 作为事件/原生字段记录。
- **不要引入** `total_tokens` 作为必填字段或成本字段；成本在 Asterpath 的计价层计算，输入是归一化后的 token 字段 + 价格表。（[TOKM]）

### 9.3 Replay traces

OTel GenAI 完全没有 replay 语义：没有 replay 标记、attempt 字段、幂等键或去重规则。Asterpath 必须自己定义：

- 原生 span/事件带 `replay: bool`、`replay_of: Option<SpanId | EventId>`、`attempt: u32`、`log_seq: u64`（或 event id）。
- **trace identity**：建议从事件 id 确定性派生 span id，使重放重建同一条 trace；如果选择新建 trace，则必须设置 `replay=true` 并链接原 trace。OTel 不规定这一点，属于 Asterpath 决策。
- **token 去重**：重放时按 `request_id` / `tool_call_id` 去重，不能再次累加 usage。`gen_ai.response.id` 和 `gen_ai.tool.call.id` 可以作为关联键，但 OTel 不定义去重语义。（[CI], [SPANS]）
- **retry**：借用 OTel 的“一个逻辑 span 覆盖所有 retry”，但把每次 attempt 作为 span event 或原生 attempt 记录。这样既保持 trace 可读，也保留诊断信息；OTel 没有 retry count 属性，不要自造 `gen_ai.retry.count` 并声称合规。
- **compaction**：借用 `gen_ai.conversation.compacted`（boolean）作为原生的 `compacted` 字段，标记本次 model call 使用的是压缩后的 conversation 视图。（[CI]）
- **replay 不是 OTel 概念**：不要把 replay 字段命名为 `gen_ai.*`，以免被误认为符合 OTel 约定；在导出层可以忽略这些字段，或放到 native resource attributes。

### 9.4 Export boundary

- **核心不依赖 OTel SDK/OTLP。** 原生 Rust 类型（`Span`, `Trace`, `TokenUsage`, `ModelCall`, `ToolCall` 等）是唯一内部模型；不引入 `opentelemetry` crate。
- **唯一知道 `gen_ai.*` 字符串的地方是可选 exporter**：一个 feature/crate 把原生 span/事件映射成 OTel 形状的 struct/JSON。核心域类型不 stringly-typed，属性名只在序列化边界出现。
- **固定版本**：exporter 固定 `semantic-conventions-genai@9ffc7e0` + `gen-ai-dev/1.42.0-dev`，并写 mapping tests。因为整体是 Development，升级时按 commit diff 审查，不要用 semver 假设。
- **内容边界**：默认导出不含 prompt/completion；内容留在事件日志。启用时用显式 `capture_content` 开关，并在序列化前跑脱敏 hook；优先导出事件 id/内容哈希引用，而不是全文。OTel 的内容属性本身是 opt-in，且明确警告 sensitive/PII；借用这个默认值。（[CI], [SPANS]）
- **provider 特定属性**：`openai.api.type`、`openai.request.service_tier`、`openai.response.system_fingerprint` 等只放在 OpenAI adapter/exporter 中，不进入核心模型。（[OPENAI]）
- **不要导出** `gen_ai.prompt.variable.*`：动态 key 既增加基数又容易泄露变量内容；Asterpath 用显式的 prompt template id/version 即可。（[CI]）
- **sampling**：GenAI 文档不定义采样策略，只建议 sampling-relevant 属性。Asterpath 保留原生策略（错误、慢请求强制保留）；不要因为引入 exporter 就换成 OTel sampler。（[CI], [AGENT]）

## 10. 来源

以下 URL 全部固定到本文引用的 commit（除非另有说明）：

- CI — Client inference: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/client-inference.md>
- SPANS — GenAI spans（embeddings/retrieval/fetch_response/memory/execute_tool）: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-spans.md>
- AGENT — Agent spans: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-agent-spans.md>
- EVENTS — Events: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-events.md>
- EXC — Exceptions: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-exceptions.md>
- METRICS — General metrics: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-metrics.md>
- TOKM — Inference token metrics: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-token-metrics.md>
- ANTH — Anthropic: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/anthropic.md>
- OPENAI — OpenAI: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/openai.md>
- README — GenAI index: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/README.md>
- MANIFEST — schema URL / stability: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/manifest.yaml>
- INPUT — input messages JSON schema: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/gen-ai/gen-ai-input-messages.json>
- OUTPUT — output messages JSON schema: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/gen-ai/gen-ai-output-messages.json>
- OUTPUT-TEST — reasoning/redacted_thinking test case: <https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/non-normative/tests/cases/output-messages-reasoning-and-generic-parts.yaml>
- REC-ERR — Recording Errors（GenAI 文档链接的 v1.44.0 版本）: <https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/general/recording-errors.md>
- MATURITY — OTel maturity levels: <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/maturity-levels.md>
- MOVE — 迁移 commit: <https://github.com/open-telemetry/semantic-conventions/commit/c9e48b1d1af565454b73b9e14f0841bca4670ada>
- OTEL-MOVED — opentelemetry.io 迁移提示: <https://opentelemetry.io/docs/specs/semconv/gen-ai/>

## 11. 借用 / 拒绝 / 待定

| 借用 | 拒绝 | 待定 |
| --- | --- | --- |
| **模型调用命名与枚举**：span name `{operation} {model}`、`CLIENT`、`gen_ai.operation.name` 的 `chat`/`generate_content`/`text_completion`、`gen_ai.provider.name` 的 `anthropic`/`openai`、`gen_ai.request.model`/`gen_ai.response.model`、`gen_ai.response.finish_reasons`、`gen_ai.request.stream`。证据：这些是规范的 Recommended/Required 命名，导出时无需再造一套。([CI]) | **每次 transport attempt 一个 span**：OTel 明确要求自动重试折叠进一个逻辑 span；按 attempt 建 span 会重复计时和 usage。([SPANS]) | **内部 span 类型名**：是否把原生模型调用类型命名为 `ModelCall` 而不是 `gen_ai.client.inference`；建议内部用域类型，仅导出时用 OTel 名。([CI]) |
| **Agent 身份与 conversation id**：`gen_ai.agent.id`/`name`/`description`/`version`，`gen_ai.conversation.id`（用真实 session id，禁止合成 UUID/trace id/hash）。证据：Asterpath 有真实 id，可直接映射。([AGENT]) | **为每个 turn 生成 `invoke_agent`**：OTel 的 `invoke_agent` 是一次 agent invocation，不是 turn；按 turn 生成会误报 agent 调用次数。([AGENT]) | **顶层 session 是否导出为 `invoke_agent`**：取决于 Asterpath 是否把 session 定义为一次有界 agent run；若 session 跨多 turn，建议只保留原生 session span。([AGENT]) |
| **Subagent / workflow 形状**：subagent 用嵌套 `invoke_agent {agent.name}`（INTERNAL）；workflow 用 `invoke_workflow {workflow.name}`（INTERNAL）；父关系按调用树。证据：这是 OTel 唯一接近 Asterpath 子 agent/工作流的模型。([AGENT]) | **`create_agent` span**：Asterpath 的 agent 构造是本地配置，不是 hosted agent 资源；为构造生成本地 span 只是凑语义。([AGENT]) | **`plan` span**：只有能可靠区分规划与普通推理时才有价值；先不纳入核心 schema，等规划阶段成为一等概念再说。([AGENT]) |
| **Tool call 形状**：`execute_tool {tool.name}`、`INTERNAL`、`gen_ai.tool.name`、`gen_ai.tool.call.id`、`gen_ai.tool.type`（`function`/`extension`/`datastore`）、`gen_ai.agent.name`；参数/结果 opt-in。证据：直接覆盖 Asterpath 的工具执行 span。([SPANS]) | **重复 tool span**：OTel 明确“不要为一次调用记录两个 span”；不要同时生成 Asterpath tool span 和 MCP/tool wrapper span。([SPANS]) | **`gen_ai.tool.type` 的取值**：Asterpath 的工具可能既有本地函数又有远程扩展；是否只保留 `function`/`extension` 两值待定。([SPANS]) |
| **Token 归一化口径**：`input_tokens` 含 cache、`output_tokens` 含 reasoning、cache_read/cache_write/reasoning 是子集、billed 优先、严格正贡献、子集不重复相加。证据：直接解决 Anthropic exclusive 与 OpenAI inclusive 的差异，是 #13 的核心。([CI], [TOKM], [ANTH], [OPENAI]) | **`gen_ai.usage.total_tokens` 字段**：OTel 没有定义；total 由 input + output 计算，落库会引入两个真相源。([TOKM]) | **modality 拆分**：text/image/audio 子集目前不是 Asterpath 必需；先保留 aggregate + cache + reasoning，等多模态 adapter 出现再加。([CI]) |
| **Token 指标单位**：usage counter/histogram 用 `{token}`，duration 用 `s`；operation histogram 用规范 bucket；usage counter 带 `gen_ai.token.modality`。证据：若将来导出 metrics，可直接对齐。([TOKM], [METRICS]) | **把子集 counter 加进 total**：cache_read/cache_write/reasoning 已 inside aggregate；再加一次会重复计费。([TOKM]) | **是否原生发 metrics**：Asterpath 目前以事件日志 + span 为主；是否额外维护 counter/histogram 待 #13 决定。([TOKM]) |
| **内容默认关闭 + 结构化 part**：`gen_ai.input.messages`/`output.messages`/`system_instructions`/`tool.definitions` 全部 opt-in；part 类型 `text`/`reasoning`/`tool_call`/`tool_call_response`/`server_tool_call`/`compaction` 等可作为导出映射。证据：与 Asterpath 事件日志的 item 模型高度相似。([CI], [INPUT], [OUTPUT]) | **默认把 prompt/completion 写进 span**：OTel 默认不记录，且警告 PII；Asterpath 的 trace 默认只带事件引用。([SPANS]) | **是否采用 OTel 的完整 JSON schema**：Asterpath 内部有自己的 item 日志；导出时再做 schema 映射，还是直接复用 schema，待 #12 决定。([INPUT], [OUTPUT]) |
| **Reasoning 表示**：请求用 `gen_ai.request.reasoning.level`；用量用 `gen_ai.usage.reasoning.output_tokens`；内容作为 `reasoning` part；redacted thinking 用 generic part。证据：不引入单独的 reasoning 内容属性，避免把不透明思考块当普通文本。([ANTH], [OPENAI], [OUTPUT]) | **缓存断点/TTL 属性**：OTel 只定义 cache token 计数，没有 `cache_control`/TTL；不要把这些字段命名成 `gen_ai.*`。([CI]) | **跨 provider reasoning 翻译**：OTel 不定义；Anthropic signature/redacted 与 OpenAI encrypted reasoning 不可互译。Asterpath 应保留原始 provider 字段并禁止自动翻译。([OUTPUT], [OUTPUT-TEST]) |
| **错误/状态语义**：`error.type` 低基数、成功 Unset、失败 Error、自动重试折叠、`finish_reasons` 缺失位置用 `error`、不记录已处理错误。证据：这是 Stable 属性 + 明确的 Recording Errors 规则。([REC-ERR], [CI], [EXC]) | **把 HTTP status 当 `error.type`**：OTel 要求 provider/client error code 或 canonical exception name；HTTP 404 不一定算错误。([REC-ERR]) | **异常表示**：用 OTel 的 `gen_ai.client.operation.exception` 事件，还是原生 error 事件 + 导出映射；建议原生事件为真相源。([EXC]) |
| **导出边界**：可选 exporter 才认识 `gen_ai.*`；固定 commit/`1.42.0-dev`；默认不导内容；provider 特定属性留在 adapter。证据：满足“不引入 OTel SDK”的约束，同时保留互操作路径。([README], [MANIFEST]) | **OTel SDK/OTLP 作为核心依赖**：超出本票约束；核心不得依赖 `opentelemetry` crate 或第三方 agent 框架。([README]) | **导出格式与采样**：OTLP JSON、其他格式、以及是否导出 turn/step span，均待 #12 决定；GenAI 约定本身不定义采样。([CI], [AGENT]) |
| **重放所需的对齐点**：`gen_ai.response.id`、`gen_ai.tool.call.id`、`gen_ai.conversation.id`、`gen_ai.conversation.compacted` 可作为关联键或压缩标记。证据：语义明确，但均为 Development，导出时仍需固定 commit。([CI], [SPANS]) | **依赖 OTel 表达 replay/attempt/dedupe**：OTel 没有这些语义；Asterpath 必须原生定义 `replay`/`replay_of`/`attempt`/`log_seq`。([CI]) | **replay 的 trace identity**：重放重建同一 trace 还是新建 trace 并链接，取决于 #3 的恢复模型；OTel 不提供答案。([AGENT]) |

[CI]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/client-inference.md
[SPANS]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-spans.md
[AGENT]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-agent-spans.md
[EVENTS]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-events.md
[EXC]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-exceptions.md
[METRICS]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-metrics.md
[TOKM]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/gen-ai-token-metrics.md
[ANTH]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/anthropic.md
[OPENAI]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/openai.md
[README]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/README.md
[MANIFEST]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/manifest.yaml
[INPUT]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/gen-ai/gen-ai-input-messages.json
[OUTPUT]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/model/gen-ai/gen-ai-output-messages.json
[OUTPUT-TEST]: https://github.com/open-telemetry/semantic-conventions-genai/blob/9ffc7e00c639be7712e544e3e54331e885dccf84/docs/gen-ai/non-normative/tests/cases/output-messages-reasoning-and-generic-parts.yaml
[REC-ERR]: https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/general/recording-errors.md
[MATURITY]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/maturity-levels.md
[MOVE]: https://github.com/open-telemetry/semantic-conventions/commit/c9e48b1d1af565454b73b9e14f0841bca4670ada
[OTEL-MOVED]: https://opentelemetry.io/docs/specs/semconv/gen-ai/
