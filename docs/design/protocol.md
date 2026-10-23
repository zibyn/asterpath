# 规范化协议模型与 adapter 契约

> 来源：[#11](https://github.com/zibyn/asterpath/issues/11)，2026-10-22 已定。本文件定义规范条目模型、请求/流式事件、能力标志、usage 与错误归一化、原始 JSON 保留与回放、Anthropic Messages / OpenAI Responses 双向映射，以及 adapter 与 loop 的责任边界。
>
> 对齐：[#2 模块契约](./module-contracts.md)、[#3 事件日志](./event-log-recovery.md)、[#6 agent loop](./agent-loop.md)、[#7 工具契约](./tools.md)、[#9 压缩](./compaction.md)。
>
> 相关 ADR：[0010 规范协议模型](../adr/0010-canonical-protocol-model.md)。
>
> 本票对既有文档的修订（已同步）：
>
> - 引入 `Part`：`ToolOutput.content`、`ToolResult.output`、`InputRecorded.input`、`CompactionApplied.summary`、`SpawnRequest.task`、`Summary.items` 由 `Vec<Item>` 改为 `Vec<Part>`；`ContextForked.items`、`AssistantMessage.items`、`ModelCallAborted.partial` 仍是 `Vec<Item>`。
> - `Provider::capabilities` 接收 `&ModelSpec`。
> - `ModelRequest` 增加 `prefix_epoch` 与 `reasoning`；`Reasoning` 增加 `binding`。
>
> 2026-10-22 补齐（[#26](https://github.com/zibyn/asterpath/issues/26)）：
>
> - 新增 `RequestConfig`，作为 `ModelRequest` 五个请求参数的唯一来源（`SessionSpec.request` / `ConfigSnapshot.request`）。
> - `Provider` 增加 `id()`；新增 `ProviderCatalog` 值类型，按 `ProviderId` 解析并拒绝重复 id。
> - 能力匹配不在 `create_session` 预检，仍由 adapter 在发 HTTP 前返回 `Unsupported`。
>
> 2026-10-22 补齐（[#28](https://github.com/zibyn/asterpath/issues/28)）：
>
> - `ReasoningBinding` 增加 `tools_hash`；工具集变化的请求不回放旧 reasoning（§9）。
> - `tools` 从 `ConfigSnapshot.tools`（`Vec<ToolDescriptor>`）渲染，session 内固定（[packaging.md](packaging.md) §2.1）。

## 1. 决策摘要

- **规范历史是扁平有序 `Vec<Item>`**；`Item` 是顶层条目，`Part` 是内容片段。工具调用/结果是顶层 item，不是 message 的 part。
- **system 不进历史**：它是 `ModelRequest.system`，由 adapter 渲染为 Anthropic 顶层 `system` 或 OpenAI `instructions`。
- **供应商特有数据按 item 保留为原始 JSON 字符串**；同 provider + model + `prefix_epoch` 时逐字节回放，不 parse/re-serialize。
- **流式事件规范化**为 `ResponseStarted → ItemStarted / PartDelta / ItemCompleted → ResponseEnded`；只有 `ItemCompleted` 落日志，delta 是瞬态的。
- **能力标志按 `ModelSpec` 声明**；loop 只按标志分支，不认 provider 身份；请求需要而能力不支持时返回 `Unsupported`，不静默降级。
- **usage 统一为 exclusive 口径**：未缓存输入、缓存读、缓存写、输出、推理分开；原始 usage 留在 raw。聚合口径见 [usage.md](usage.md)（#13）。
- **错误由 adapter 分类**为 `ProviderError`；loop 只读 `retryable` 与 `ContextExceeded`，不映射 HTTP 状态。
- **跨 provider / 压缩后旧 reasoning 不回放**；规范可见部分保留，不把 summary 伪装成文本。trace 记录丢弃项。
- **`Provider` 只有 `id` + `capabilities` + `stream`**；adapter 管传输、格式、归一化、raw；loop 管重试、日志、工具、投影、压缩。
- **v1 不做服务端状态**：OpenAI 恒 `store:false`，始终发送完整历史；不使用 `previous_response_id` / `conversation`。

## 2. 分层与术语

| 词 | 含义 |
| --- | --- |
| 历史（history） | 投影出的有序 `Vec<Item>`，对应 `ModelRequest.items` |
| 条目（item） | 模型历史中的有序顶层单元；消息、推理、工具调用/结果、服务端工具、压缩摘要或不透明条目 |
| 片段（part） | 条目内部的内容单元；文本、图像、拒绝或不透明内容块 |
| 模型响应（response） | 一次 `Provider::stream` 调用产出的规范流式结果 |
| 模型尝试（attempt） | 一次 HTTP 请求；重试产生多个 attempt，只有成功 attempt 产出一条 `AssistantMessage` |
| 前缀代次（prefix epoch） | 会话中已发生的压缩次数；用于判断旧 reasoning 是否仍可回放 |
| 原始供应商数据（raw） | 随条目保存的供应商原始 JSON 文本；只能原样回放 |

`ProviderId`、`ModelId`、`ToolName`、`ModelAlias`、`JsonString` 是 protocol 里的 opaque newtype；`JsonString` 保存原始 JSON 文本，不保证与 `serde_json::Value` 往返一致。`JsonValue` 与 `JsonSchema` 是 `serde_json::Value` 的别名（任意 JSON 值 / JSON Schema 文档）；`ToolArgs = JsonString`。

日志信封的 `LogEntry` / `NewEntry` 定义在 core，payload 是不透明 `serde_json::Value`；protocol 提供 `decode_entry(&LogEntry) -> Result<SessionEvent, LogDecodeError>`，先按 `schema_version` 逐级 upcast 再解码（[ADR-0020](../adr/0020-log-envelope-core-typed-decoding-protocol.md)）。`LogDecodeError` 分 `UnsupportedVersion` / `Upcast` / `Decode`，均为硬错误。

## 3. 规范条目模型

```rust
pub enum Role {
    User,
    Assistant,
}

pub enum Item {
    Message {
        role: Role,
        parts: Vec<Part>,
    },
    Reasoning {
        summary: Vec<Part>,             // 模型可见的摘要/思考文本；可空
        binding: ReasoningBinding,
        raw: RawProviderData,           // 必须保留，含 signature / encrypted_content
    },
    ToolCall {
        call_id: String,                // 规范 id：Anthropic tool_use.id / OpenAI call_id
        provider_item_id: Option<String>, // OpenAI item id 等，仅同 provider 原始回放用
        name: ToolName,
        raw_args: JsonString,           // 权威参数文本
        raw: Option<RawProviderData>,
    },
    ToolResult {
        call_id: String,
        provider_item_id: Option<String>,
        parts: Vec<Part>,
        is_error: bool,
        raw: Option<RawProviderData>,
    },
    ServerToolCall {
        provider: ProviderId,
        name: String,
        call_id: Option<String>,
        raw: RawProviderData,
    },
    ServerToolResult {
        provider: ProviderId,
        call_id: Option<String>,
        parts: Vec<Part>,
        raw: RawProviderData,
    },
    Compaction {
        parts: Vec<Part>,
    },
    ProviderOpaque {
        provider: ProviderId,
        kind: String,
        raw: RawProviderData,
    },
}

pub enum Part {
    Text {
        text: String,
    },
    Image {
        media_type: String,
        source: ImageSource,
    },
    Refusal {
        text: String,
    },
    ProviderOpaque {
        provider: ProviderId,
        kind: String,
        raw: RawProviderData,
    },
}

pub enum ImageSource {
    Base64 { data: String },   // 不带 data: 前缀的 base64 载荷
    Url { url: String },
}

pub struct ReasoningBinding {
    pub provider: ProviderId,
    pub model: ModelId,
    pub prefix_epoch: u32,     // 该 reasoning 产出时已发生的压缩次数
    pub tools_hash: String,    // 产出时请求工具集的指纹；见 §9
}

pub struct RawProviderData {
    pub provider: ProviderId,
    pub model: Option<ModelId>,
    pub kind: String,          // adapter 自定义，例如 "anthropic.thinking"
    pub json: JsonString,      // 原始 JSON 文本，逐字节
}
```

`ToolCall.parsed_args` 不是序列化字段；消费方按需解析 `raw_args`，解析失败交给 #7 的 `InvalidArgs`，不在协议层报错。

### 3.1 字段规则

- `Message.role` 只有 `User | Assistant`；tool result 是顶层 item，不存在 `Tool` role。
- `Message.parts` 中 user 侧可含 `Text` / `Image` / `ProviderOpaque`；assistant 侧可含 `Text` / `Refusal` / `ProviderOpaque`。
- `Reasoning.summary` 只用于展示与 trace；同 provider 回放以 `raw` 为准，跨 provider 时整个 `Reasoning` 不进入请求。
- `Reasoning.binding` 由 adapter 按实际服务的请求填写：`provider` / `model` / `prefix_epoch` / `tools_hash`。`tools_hash` 覆盖请求里的模型可见工具集，fork 收窄工具集后旧 reasoning 因此失效（§9）。
- `ToolCall.raw_args` 是 pre-execute 之前的模型原始参数；工具执行使用 `ToolCallRequested.raw_args`（effective args，#7）。
- `ToolResult.parts` 是模型可见内容；`is_error` 由 #7 的 `status != Completed` 投影而来。
- `InputRecorded.input` 投影为一条 `Item::Message { role: User, parts }`；连续输入按 seq 各成一条，Anthropic adapter 可合并相邻同角色消息。
- `ToolResult` item 由 #7 的 session `ToolResult` 投影：`Completed` 用 `output`，`Failed` 用 `error.message`，`Interrupted` 用固定中断提示；`is_error = status != Completed`。
- `ServerToolCall` / `ServerToolResult` 是供应商内置工具，框架不执行、不审批；只在同 provider 请求中原样回放。
- `Compaction.parts` 通常是一条 text part；投影中只保留最新一条（#9）。
- `ProviderOpaque` 顶层 item 承载未知 output item；`Part::ProviderOpaque` 承载未知内容块。两者都只在同 provider 请求中回放。

### 3.2 不变量

| # | 不变量 |
| --- | --- |
| P1 | 历史有序；顺序就是模型看到内容的顺序。adapter 不得改变语义顺序。 |
| P2 | `Message.role` 只有 `User` / `Assistant`；system 不在历史中。 |
| P3 | `Part` 不嵌套 `Item`；`Message.parts`、`ToolResult.parts`、`Compaction.parts` 只含 `Part`。 |
| P4 | `ToolCall.call_id` 在投影历史内唯一；`ToolResult.call_id` 必须引用较早的 `ToolCall`，投影按 `ToolCall` 的 seq 排序。 |
| P5 | `ToolCall.raw_args` 是权威值；不存 `parsed_args`，不 re-serialize。 |
| P6 | `Reasoning.binding` 记录产出 provider / model / 前缀代次 / 工具集指纹；任一不匹配即不可回放。 |
| P7 | `raw.json` 是原始 provider JSON 的字符串片段；回放时不得 parse 后重新序列化。 |
| P8 | `ProviderOpaque`、`ServerToolCall`、`ServerToolResult` 只在同 provider 请求中回放。 |
| P9 | `Compaction` 是模型可见摘要；同一时刻投影只保留最新一条。 |
| P10 | `Part::Refusal` 是模型可见内容；`StopReason::Refusal` 与它并存，不互相替代。 |

## 4. 请求模型

```rust
pub struct ModelSpec {
    pub provider: ProviderId,
    pub model: ModelId,
    pub context_window: Option<u32>,
}

// 会话级请求配置；只用于回合内的模型调用，压缩摘要请求独立构造（compaction.md §4）。
pub struct RequestConfig {
    pub max_output_tokens: NonZeroU32,      // 默认 8192
    pub tool_choice: ToolChoice,            // 默认 Auto
    pub disable_parallel_tool_use: bool,    // 默认 false
    pub sampling: Option<SamplingParams>,   // 默认 None
    pub reasoning: Option<ReasoningParams>, // 默认 None
}

pub struct ModelRequest {
    pub spec: ModelSpec,
    pub prefix_epoch: u32,              // 当前压缩代次
    pub system: Vec<Part>,              // 只允许 Text
    pub items: Vec<Item>,               // 完整历史
    pub tools: Vec<ToolDescriptor>,
    pub tool_choice: ToolChoice,
    pub disable_parallel_tool_use: bool,
    pub sampling: Option<SamplingParams>,
    pub max_output_tokens: u32,         // 必填；Anthropic max_tokens
    pub reasoning: Option<ReasoningParams>,
}

pub enum ToolChoice {
    Auto,
    None,
    Required,
    Tool(ToolName),
}

pub struct SamplingParams {
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub top_k: Option<u32>,
    pub seed: Option<u64>,
}

pub struct ReasoningParams {
    pub effort: Option<ReasoningEffort>, // OpenAI reasoning.effort
    pub budget_tokens: Option<u32>,      // Anthropic thinking.budget_tokens
    pub summary: bool,                   // OpenAI reasoning.summary；Anthropic 忽略
}

pub enum ReasoningEffort {
    Low,
    Medium,
    High,
}
```

请求规则：

| # | 规则 |
| --- | --- |
| R1 | `items` 始终是完整投影历史；不使用服务端状态，不发 `previous_response_id` / `conversation`。 |
| R2 | `system` 只允许 `Part::Text`；其他 part 返回 `InvalidRequest`。 |
| R3 | `tools` 由 `ConfigSnapshot.tools`（`Vec<ToolDescriptor>`）渲染，session 内固定；Anthropic 用 `input_schema`，OpenAI 用 `parameters`。 |
| R4 | `max_output_tokens` 必填；Anthropic 映射 `max_tokens`，OpenAI 映射 `max_output_tokens`。 |
| R5 | `tool_choice` 的 `Required` / `Tool(_)` 需要 `forced_tool_choice` 能力；否则 `Unsupported`。 |
| R6 | `sampling` 非空时需要 `sampling` 能力；不支持返回 `Unsupported`，不静默丢弃字段。 |
| R7 | `disable_parallel_tool_use = true` 需要 `parallel_tool_calls` 能力；否则 `Unsupported`。 |
| R8 | `reasoning` 非空时按 provider 映射；不支持的字段（如 Anthropic 的 `effort`、OpenAI 的 `budget_tokens`）返回 `Unsupported`。 |
| R9 | OpenAI adapter 固定设置 `store:false` 与 `include:["reasoning.encrypted_content"]`。 |
| R10 | 任何无法渲染的规范内容都返回 `Unsupported` 或 `InvalidRequest`；不静默改写历史。 |

- `RequestConfig` 在 `create_session` 时随 `SessionSpec` 写入 `ConfigSnapshot`；loop 每次都从快照读取，恢复后重建出同一请求。
- `create_session` 只做结构校验（provider 已注册、`system` 全为 `Part::Text`、工具名可解析）；`sampling` / `reasoning` / `Required` / `disable_parallel_tool_use` 的能力不匹配不在创建时预检，由 adapter 按 R5–R8 返回 `Unsupported`。

`ReasoningParams` 映射：

| 字段 | Anthropic | OpenAI |
| --- | --- | --- |
| `budget_tokens` | `thinking: {type:"enabled", budget_tokens}` | 不支持；非空 → `Unsupported` |
| `effort` | 不支持；非空 → `Unsupported` | `reasoning.effort` |
| `summary` | 忽略（thinking 文本照常返回） | `reasoning.summary = "auto"`；false 时不请求摘要 |

## 5. 能力标志

```rust
pub struct ProviderCapabilities {
    pub server_state: bool,          // v1 恒 false
    pub forced_tool_choice: bool,    // Required / Tool(_)
    pub sampling: bool,
    pub reasoning_roundtrip: bool,   // 返回可回放的不透明 reasoning
    pub citations: bool,             // informational；v1 不暴露 typed citations
    pub builtin_tools: bool,         // 可能产出 ServerToolCall/Result
    pub cache_control: bool,         // informational；v1 无规范断点
    pub image_input: bool,
    pub parallel_tool_calls: bool,   // 可控制是否并行
}
```

- `Provider::capabilities(&ModelSpec)` 按模型返回；同一 provider 的不同模型可以不同。
- 默认值保守：未知即 `false`。能力标志是 adapter 的声明，不是运行时探测。
- loop 只按标志分支，不按 provider 身份分支；需要的能力为 `false` 时，adapter 在发 HTTP 前返回 `Unsupported`。
- `server_state` 在 v1 恒为 `false`；即使供应商支持，也不使用。
- `citations` / `cache_control` 在 v1 仅用于告知宿主；typed citations 与宿主指定断点出界，供应商数据仍保留在 `raw`。
- `builtin_tools` 为 `true` 时，`ServerToolCall` / `ServerToolResult` 是普通历史 item；loop 不执行它们。
- `image_input = false` 时，请求中的 user/assistant `Part::Image` 返回 `Unsupported`；工具结果图像按 §10.4 渲染为占位。
- 能力匹配不在 `create_session` 预检；adapter 是唯一权威（#26）。

## 6. Provider 与 Compactor 契约

```rust
pub trait Provider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities;
    fn stream(&self, request: ModelRequest)
        -> BoxStream<'_, Result<StreamEvent, ProviderError>>;
}

// 静态 provider 集合：组装根构造，loop 与 compactor 按 id 解析。
pub struct ProviderCatalog(BTreeMap<ProviderId, Arc<dyn Provider>>);
impl ProviderCatalog {
    pub fn new(providers: Vec<Arc<dyn Provider>>) -> Result<Self, DuplicateProviderId>;
    pub fn get(&self, id: &ProviderId) -> Option<Arc<dyn Provider>>;
}

pub enum StreamEvent {
    ResponseStarted {
        sequence: u64,
        request_id: Option<String>,
    },
    ItemStarted {
        sequence: u64,
        output_index: u32,
    },
    PartDelta {
        sequence: u64,
        output_index: u32,
        part_index: u32,
        delta: PartDelta,
    },
    ItemCompleted {
        sequence: u64,
        output_index: u32,
        item: Item,
    },
    ResponseEnded {
        sequence: u64,
        stop_reason: StopReason,
        usage: Usage,
    },
}

pub enum PartDelta {
    Text { text: String },
    JsonArgs { fragment: String },
    ReasoningSummary { text: String },
}

pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    PauseTurn,
    ContextWindowExceeded,
    Refusal,
    Other { provider_label: String },
}
```

- `ProviderId` 实现 `Ord` 与 `Hash`（opaque newtype，仅用于索引，不暴露内部表示）。
- `ProviderCatalog::new` 的重复 id 是组装错误；`create_session` / `spawn` 查不到 provider 时是配置错误。
- `ProviderCatalog` 由组装根构造并注入 agent loop 与 compactor；模块不自行发现 provider。

终止原因映射：

| 规范 | Anthropic `stop_reason` | OpenAI `status` / `incomplete_details` |
| --- | --- | --- |
| `EndTurn` | `end_turn` | `completed`（无 function_call） |
| `ToolUse` | `tool_use` | `completed`（有 function_call） |
| `MaxTokens` | `max_tokens` | `incomplete` / `max_output_tokens` |
| `StopSequence` | `stop_sequence` | 不出现 |
| `PauseTurn` | `pause_turn` | 不出现 |
| `ContextWindowExceeded` | `model_context_window_exceeded` | 通常走错误路径 |
| `Refusal` | `refusal` | `incomplete` / `content_filter` |
| `Other` | 未知值 | 未知状态 |

- `Other` 在 loop 中按 `EndTurn` 处理；`provider_label` 只用于诊断。
- provider 的原始 status 保留在 `AssistantMessage.raw`；不单列 `provider_status` 字段。
- `StopReason` 是模型响应的终止原因，不是 session 的 `TurnEndReason`；两者由 #6 映射。

流式不变量：

| # | 不变量 |
| --- | --- |
| S1 | `ResponseStarted` 是第一个事件，恰好一次。 |
| S2 | `ResponseEnded` 是最后一个事件，恰好一次；之后不再有事件。 |
| S3 | 每个 `output_index` 先有 `ItemStarted`，后有 delta 与 `ItemCompleted`。 |
| S4 | 每个 `output_index` 的 `ItemCompleted` 恰好一次。 |
| S5 | `output_index` 在一条响应内唯一；规范 item 顺序按 `output_index`，不是到达顺序。 |
| S6 | `sequence` 在一条响应内严格递增，由 adapter 分配。 |
| S7 | delta 是瞬态的；只有 `ItemCompleted.item` 进入日志。 |
| S8 | 流以 `Err(ProviderError)` 终止时，不会再有 `ResponseEnded`；loop 不写 `AssistantMessage`。 |
| S9 | 已 `ItemCompleted` 的 item 可以用于 `ModelCallAborted.partial` 的诊断；delta 不投影。 |
| S10 | `ResponseStarted` / `ResponseEnded` 命名属于模型响应，不是 session 的 `TurnStarted` / `TurnCommitted`。 |

`ItemCompleted` 可以按 provider 的完成顺序到达；loop 按 `output_index` 排序后写入 `AssistantMessage.items`。adapter 负责把供应商的交错事件归一化；顺序不变量违反时返回 `StreamProtocol`。

### 6.1 Compactor 契约（#27）

压缩是**可选能力模块**：`asterpath-agent` 只依赖本节的 trait，默认实现 `asterpath-compaction` 由组装根注入；`None` 表示压缩不可用。摘要调用独立构造请求，不继承 `RequestConfig`（[compaction.md](compaction.md) §4）。

```rust
pub trait Compactor: Send + Sync {
    /// 纯函数：按配置与投影决定是否压缩、切点在哪；不读日志、不调用模型。
    fn plan(&self, input: &CompactionInput) -> CompactPlan;

    /// 只为 `CompactPlan::Summarize` 调用；执行摘要调用与压后校验。
    fn compact<'a>(&'a self, job: CompactionJob)
        -> BoxFuture<'a, Result<CompactSummary, CompactionError>>;
}

pub struct CompactionInput {
    pub trigger: CompactionTrigger,
    pub model: ModelSpec,                 // 下一次请求的模型；usable window 的来源
    pub config: CompactionConfig,
    pub turn: TurnConfig,                 // 摘要调用的 attempt / 退避沿用回合配置
    pub history: Vec<ProjectedEntry>,     // 模型可见投影，按序
    pub step_starts: Vec<LogId>,          // 可切割边界的 StepStarted LogId（已提交 step），升序
    pub last_usage: Option<UsageAnchor>,  // 最近一次 AssistantMessage
}

pub struct UsageAnchor {
    pub origin: LogId,                    // 该 AssistantMessage 的 LogId
    pub usage: Usage,
}

pub struct ProjectedEntry {
    pub origin: ProjectedOrigin,
    pub items: Vec<Item>,
}

#[non_exhaustive]
pub enum ProjectedOrigin {
    Log(LogId),
    Summary(LogIdRange),
}

#[non_exhaustive]
pub enum CompactPlan {
    NotNeeded,                            // 仅 Threshold 触发可能返回
    NothingToDo,
    NotConfigured,                        // 未配置 ModelSpec.context_window
    Summarize(CompactionJob),
}

pub struct CompactionJob {
    pub first_kept: LogId,                // 某个已提交 step 的 StepStarted LogId
    pub replaces: LogIdRange,
    pub model: ModelSpec,                 // 摘要模型
    pub summary_input: Vec<Item>,         // 已剥 reasoning/raw/非文本的文本视图
    pub max_summary_tokens: u32,
    pub usable_window: u32,               // 由会话模型算出，不是摘要模型
    pub retained_tokens: u64,             // first_kept 之后的估算
}

#[non_exhaustive]
pub enum CompactSummary {
    Applied(CompactionOutcome),
    Insufficient,
}

pub struct CompactionOutcome {
    pub first_kept: LogId,
    pub summary: Vec<Part>,
    pub replaces: LogIdRange,
    pub model: ModelSpec,
    pub usage: Usage,
    pub raw: Option<RawProviderData>,
}

#[non_exhaustive]
pub enum CompactResult {
    NotNeeded,
    NothingToDo,
    NotConfigured,
    Applied(CompactionOutcome),
    Insufficient,
}

#[non_exhaustive]
pub enum CompactionError {
    Provider(ProviderError),
}
```

规则：

- **单一写入者**：`Compactor` 只读、返回结果；agent 在 `plan` 返回 `Summarize` 后写 `CompactionStarted { trigger, replaces_from, caused_by, turn }`（`turn` 为所属回合，Idle 为 `None`；形状见 [compaction.md](compaction.md) §5），再用 `compact` 的结果写终态。
- `ProviderCatalog` 在构造 Compactor 时注入；`CompactionInput` 不携带 provider。
- `CompactionOutcome.model` 是摘要调用实际使用的模型；`CompactionApplied.model` 直接取它（#13 的用量归因）。
- `plan` 不产生 I/O：读入投影、算切点与估算；`compact` 只做摘要调用与压后校验。`compact` 只返回 `Applied` / `Insufficient`。
- `first_kept` 必须是 `step_starts` 中的某个 `LogId`；`replaces.first` = `history` 首条的 origin 起点；`replaces.last` = `first_kept` 之前**最后一个模型可见 origin**（不是原始日志的前一条）。见 [compaction.md](compaction.md) §4。
- 估算：`estimated_input = last_usage.usage.input_total() + bytes/3(history 中 origin > last_usage.origin 的条目)`；无 `last_usage` 时不做自动压缩。
- `CompactResult` 是 `SessionHandle::compact_now` 与 loop 的公开展示；`NotConfigured` / `NothingToDo` / `Insufficient` 是状态，不是错误。
- 类型归属：本节 trait 与值类型在 `asterpath-protocol`；`LogIdRange` 在 `core`；`CompactionConfig` 见 [compaction.md](compaction.md) §8。

## 7. Usage 归一化

```rust
pub struct Usage {
    pub input: InputTokens,
    pub output: OutputTokens,
}

pub struct InputTokens {
    pub uncached: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

pub struct OutputTokens {
    pub total: u64,
    pub reasoning: u64,
}

impl Usage {
    pub fn input_total(&self) -> u64 {
        self.input.uncached + self.input.cache_read + self.input.cache_write
    }

    pub fn total(&self) -> u64 {
        self.input_total() + self.output.total
    }
}
```

| 规范字段 | Anthropic | OpenAI Responses |
| --- | --- | --- |
| `input.uncached` | `input_tokens` | `input_tokens - cached_tokens - cache_write_tokens`（饱和减到 0） |
| `input.cache_read` | `cache_read_input_tokens` | `input_tokens_details.cached_tokens` |
| `input.cache_write` | `cache_creation_input_tokens` | `input_tokens_details.cache_write_tokens` |
| `output.total` | `output_tokens` | `output_tokens` |
| `output.reasoning` | `output_tokens_details.thinking_tokens` | `output_tokens_details.reasoning_tokens` |
| 总 token | 派生 | 派生；忽略供应商的 `total_tokens` |

- 统一为 **exclusive** 口径：`input_total = uncached + cache_read + cache_write`；OpenAI 的 inclusive `input_tokens` 必须减去缓存部分。
- 缺失字段按 0 处理；原始 usage 留在 `raw`，不伪造。
- `Usage` 只描述一次模型响应；`AssistantMessage.usage` 与 `CompactionApplied.usage` 使用它；失败 attempt 的 usage 是 best-effort `Option<Usage>`。
- 聚合、桶与 partial usage 见 [usage.md](usage.md)（#13）。承载 usage 的日志事件（`AssistantMessage` / `CompactionApplied` / `ModelCallAborted`）各带 `model: ModelSpec`，用于按模型聚合；`raw` 只作原始回放，不参与归因。
- 记录到用量桶的判定是 protocol 的 `usage_of(event, compaction_turn) -> Option<UsageEntry>`，见 [usage.md](usage.md) §4。
- 失败 attempt 的已知用量经 `ProviderError.partial_usage` 交给 loop；它只作诊断，不进总量。

## 8. 错误归一化

```rust
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
    pub request_id: Option<String>,
    pub message: String,
    pub partial_usage: Option<Usage>,   // 流已给出的已知用量；仅诊断
    pub raw: Option<RawProviderData>,
}

pub enum ProviderErrorKind {
    Unsupported,
    Auth,
    InvalidRequest,
    ContextExceeded,
    RateLimited,
    Overloaded,
    Transport,
    Timeout,
    Server,
    StreamProtocol,
}
```

映射规则：

| 条件 | `kind` | 默认可重试 |
| --- | --- | --- |
| 请求字段/能力不支持 | `Unsupported` | 否 |
| 400 / 422 | `InvalidRequest` | 否 |
| 401 / 403 | `Auth` | 否 |
| 404 | `InvalidRequest` | 否 |
| 408 | `Timeout` | 是 |
| 429 | `RateLimited` | 是；解析 `Retry-After` |
| 500 / 502 / 504 | `Server` | 是 |
| 503 / 529 | `Overloaded` | 是；解析 `Retry-After` |
| 连接/DNS/TLS/重置 | `Transport` | 是 |
| 请求超时 | `Timeout` | 是 |
| 上下文超限错误码/消息 | `ContextExceeded` | 否 |
| SSE 截断 / 无终态事件 | `Transport` | 是 |
| SSE 事件顺序/结构错误 | `StreamProtocol` | 否 |
| 其他未知 4xx | `InvalidRequest` | 否 |
| 其他未知 5xx | `Server` | 是 |

- HTTP 状态、SSE `error`、`Retry-After`、`request-id` / `x-request-id`、上下文超限识别全部在 adapter。
- loop 只读 `retryable` 与 `kind == ContextExceeded`；重试次数、退避、取消归 #6。
- `request_id` 来自响应头或响应 id；`raw` 保存错误 body 的原始文本。
- `partial_usage` 是 adapter 的 best-effort：只填 provider 已明确给出的部分（例如 Anthropic `message_start` 的输入侧），缺的部分按 0，不估算。loop 原样写入 `ModelCallAborted.usage`（#13）。
- 流中途失败前已 `ItemCompleted` 的 item 由 loop 组成 `ModelCallAborted.partial`；不写 `AssistantMessage`（#6 I9）。

## 9. 原始 JSON 保留与回放

| 位置 | 是否必须 | 说明 |
| --- | --- | --- |
| `Reasoning.raw` | 必须 | thinking signature / encrypted_content 所在 |
| `ServerToolCall.raw` / `ServerToolResult.raw` | 必须 | 供应商内置工具无规范等价物 |
| `ProviderOpaque.raw` | 必须 | 未知条目/内容块 |
| `ToolCall.raw` / `ToolResult.raw` | 可选 | 供应商有未建模字段时保留 |
| `AssistantMessage.raw` | 可选 | 响应级元数据：response id、status、headers |
| `CompactionApplied.raw` | 可选 | 摘要响应的元数据 |

回放规则：

1. **同 provider + 同 model + `binding.prefix_epoch == request.prefix_epoch` + `binding.tools_hash == tools_hash(request.tools)`**、且 item 未被改写：adapter 直接 splice `raw.json`，不 parse、不重新序列化。
2. **跨 provider**：`Reasoning`、`ServerToolCall/Result`、`ProviderOpaque` 不进入请求；规范可见的 `Message` / `ToolCall` / `ToolResult` / `Compaction` 正常渲染。`Reasoning.summary` 不转成文本。
3. **epoch 不匹配**（压缩改变了前缀）：`Reasoning` 不进入请求；其他 item 不受影响。
4. **model 变化但 provider 相同**：视为不匹配，`Reasoning` 不进入请求。
5. **工具集指纹不匹配**（fork 收窄工具集，或将来任何工具集变化）：`Reasoning` 不进入请求；其他 item 不受影响。
6. 丢弃项在 trace 记 `protocol.dropped_items`，附 provider / kind / reason；不影响日志与投影。
7. `raw.json` 是给同一 adapter 的私有格式；框架不解释其内容，也不保证跨 adapter 版本兼容（版本兼容由 adapter 自己负责；跨版本 raw 的边界见 [plugins.md](plugins.md) §7.3）。

`tools_hash` 的定义：把 `ModelRequest.tools` 的模型可见字段（`name` / `description` / `schema`）按 `name` 排序，序列化为紧凑 JSON 数组 `[{"name":…,"description":…,"schema":…}, …]`（对象键按字典序递归排序，RFC 8785 可作实现参考），取 SHA-256，小写十六进制。空工具集为 `[]` 的哈希。实现是 `asterpath_protocol::tools_hash`；adapter 在产出 `Reasoning` 时用所服务请求的同一函数计算并写入 `binding`；请求前对每条 `Reasoning` 重算比较。

`prefix_epoch` 的定义：会话中截至请求水位已发生的 `CompactionApplied` 数量。`Reasoning.binding.prefix_epoch` 在产出时写入（取自 `request.prefix_epoch`）；压缩后旧 reasoning 自然失效，压缩后新产出的 reasoning 使用新代次。

## 10. 双向映射表

### 10.1 请求

| 规范 | Anthropic Messages | OpenAI Responses |
| --- | --- | --- |
| `system: Vec<Part>` | 顶层 `system`（text blocks） | `instructions`（文本拼接） |
| `items: Vec<Item>` | `messages[]`（按 §10.2 分组） | `input[]`（扁平 item 列表） |
| `tools: Vec<ToolDescriptor>` | `tools[{name, description, input_schema}]` | `tools[{type:"function", name, description, parameters}]` |
| `tool_choice` | `auto` / `any` / `tool` / `none` | `auto` / `required` / `function` / `none` |
| `disable_parallel_tool_use` | `tool_choice.disable_parallel_tool_use` | `parallel_tool_calls: false` |
| `sampling` | `temperature` / `top_p` / `top_k` | `temperature` / `top_p` / `seed` |
| `max_output_tokens` | `max_tokens` | `max_output_tokens` |
| `reasoning` | `thinking` | `reasoning` |
| `prefix_epoch` | adapter 内部使用，不发送 | adapter 内部使用，不发送 |
| 完整历史 | 每次发送 `messages[]` | 每次发送 `input[]`；`store:false` |

### 10.2 Anthropic content block → 规范 Item

| Anthropic | 规范 |
| --- | --- |
| `text` | `Item::Message { role: Assistant, parts: [Text] }` |
| `thinking` | `Item::Reasoning { summary: [Text], binding, raw }` |
| `redacted_thinking` | `Item::Reasoning { summary: [], binding, raw }` |
| `tool_use` | `Item::ToolCall`；`call_id = tool_use.id` |
| `server_tool_use` | `Item::ServerToolCall` |
| `*_tool_result`（内置工具结果） | `Item::ServerToolResult` |
| 未知块 | `Part::ProviderOpaque` |

- 同一 assistant 消息里的多个块按原顺序投影为连续 item。
- `stop_reason = "refusal"` → `StopReason::Refusal`；若有文本内容，按 `Part::Text` 保留；若供应商给独立 refusal 内容，按 `Part::Refusal`。
- `stop_reason = "tool_use"` → `StopReason::ToolUse`；`pause_turn` → `StopReason::PauseTurn`；`model_context_window_exceeded` → `StopReason::ContextWindowExceeded`。

### 10.3 OpenAI output item → 规范 Item

| OpenAI | 规范 |
| --- | --- |
| `message` + `output_text` | `Item::Message { role: Assistant, parts: [Text] }` |
| `message` + `refusal` | `Part::Refusal` |
| `function_call` | `Item::ToolCall`；`call_id = call_id`，`provider_item_id = id` |
| `reasoning` | `Item::Reasoning`；`summary` 来自 summary 文本，`raw` 含 `encrypted_content` |
| 内置工具调用 | `Item::ServerToolCall` |
| 内置工具结果 | `Item::ServerToolResult` |
| 原生 compaction item | v1 不解释；保留为 `Item::ProviderOpaque` |
| 未知 item | `Item::ProviderOpaque` |

- `response.status = completed` 有 function_call 时 `StopReason::ToolUse`，否则 `EndTurn`；`incomplete` 的 `max_output_tokens` → `MaxTokens`，`content_filter` → `Refusal`；`failed` → 错误路径。
- `response.output` 中的 item 顺序即 `output_index`；adapter 容忍事件交错，最终按 `output_index` 归一。

### 10.4 工具调用与结果

| 规范 | Anthropic | OpenAI |
| --- | --- | --- |
| `ToolCall{call_id, name, raw_args}` | `tool_use{id, name, input}`；`input` 由 `raw_args` 解析为 object | `function_call{call_id, name, arguments}`；`arguments` 原样使用 `raw_args` 文本 |
| `ToolResult{call_id, parts, is_error}` | 下一条 user 消息的 `tool_result{tool_use_id, content, is_error}` | `function_call_output{call_id, output}` |
| 连续多个 `ToolResult` | 合并进同一条 user 消息，`tool_result` 块排在文本之前 | 每个结果一个顶层 item |
| `Part::Text` | `text` block | 文本 |
| `Part::Image` | `image` block | 文本占位 `[image omitted: <media_type>]`（v1） |
| `Part::Refusal` | 按 text block 渲染 | `refusal` content part |
| `is_error = true` | 原生 `is_error: true` | 首个文本前缀 `[tool error] `（adapter 渲染） |

- 工具结果在规范投影中按 `ToolCall` 顺序排列；日志完成顺序不影响投影（#7）。
- OpenAI 的 `output` v1 以文本承载；非文本 part 使用占位并在 trace 标记 `protocol.rendered_as_text`。
- `raw_args` 原样传给 Anthropic 的 `input` 解析与 OpenAI 的 `arguments`，不做 re-serialize。

### 10.5 Reasoning

| 规范 | Anthropic | OpenAI |
| --- | --- | --- |
| `Reasoning.summary` | `thinking` 文本（`redacted_thinking` 为空） | summary 文本 |
| `Reasoning.raw` | 完整 `thinking` / `redacted_thinking` 块 | 完整 `reasoning` item，含 `encrypted_content` |
| 同 provider 回放 | 原样拼回 thinking block（含 `signature`） | 原样放入 `input`（含 `encrypted_content`） |
| 跨 provider / epoch 不符 | 丢弃整个 item | 丢弃整个 item |
| `summary` 跨 provider | 不转文本 | 不转文本 |

- OpenAI `store:false` 时必须请求 `include:["reasoning.encrypted_content"]`，否则无法在无服务端状态时回放。
- Anthropic `display:"omitted"` 的空文本 thinking 仍然是一个 `Reasoning` item；没有它，signature 无法回放。

### 10.6 Server tools / compaction / refusal

| 规范 | Anthropic | OpenAI |
| --- | --- | --- |
| `ServerToolCall` | `server_tool_use` block | 内置工具调用 item |
| `ServerToolResult` | `*_tool_result` block | 内置工具结果 item |
| `Compaction` | user text block | 普通 input 文本消息（v1 不用原生 compaction item） |
| `Part::Refusal` | text block + `StopReason::Refusal` | `refusal` part |
| `ProviderOpaque` | 同 provider 原样回放 | 同 provider 原样回放 |

### 10.7 流式事件映射

**Anthropic**

| Anthropic SSE | 规范事件 |
| --- | --- |
| `message_start` | `ResponseStarted`（request id；输入 usage 暂存） |
| `content_block_start` | `ItemStarted { output_index = block index }` |
| `content_block_delta` / `text_delta` | `PartDelta::Text` |
| `content_block_delta` / `input_json_delta` | `PartDelta::JsonArgs` |
| `content_block_delta` / `thinking_delta` | `PartDelta::ReasoningSummary` |
| `content_block_delta` / `signature_delta` | adapter 内部，不暴露 |
| `content_block_stop` | `ItemCompleted` |
| `message_delta` | 暂存 `stop_reason` / 输出 usage |
| `message_stop` | `ResponseEnded` |
| `ping` | 忽略 |
| `error` | `ProviderError` |

**OpenAI Responses**

| OpenAI SSE | 规范事件 |
| --- | --- |
| `response.created` / `response.in_progress` | `ResponseStarted`（一次） |
| `response.output_item.added` | `ItemStarted { output_index }` |
| `response.content_part.added` | 不单独暴露；part 起始由 delta 隐含 |
| `response.output_text.delta` | `PartDelta::Text` |
| `response.function_call_arguments.delta` | `PartDelta::JsonArgs` |
| `response.reasoning_summary_text.delta` | `PartDelta::ReasoningSummary` |
| `response.reasoning_text.delta` / encrypted 片段 | adapter 内部，不暴露 |
| `response.output_item.done` | `ItemCompleted` |
| `response.completed` / `response.incomplete` | `ResponseEnded` |
| `response.failed` | `ProviderError` |

- provider 的 `sequence_number` 只用于 adapter 内部校验；规范 `sequence` 由 adapter 重新分配。
- OpenAI item 事件可能交错；adapter 按 `output_index` 归一，`ItemCompleted` 可按完成顺序发出，loop 最终按 `output_index` 排序。

## 11. 跨供应商与失效规则

| 情况 | 行为 |
| --- | --- |
| 同 provider / model / epoch | `Reasoning`、`ServerTool*`、`ProviderOpaque` 原样回放 |
| 跨 provider | `Reasoning`、`ServerTool*`、`ProviderOpaque` 不进入请求；`summary` 不转文本 |
| 压缩后 epoch 不符 | `Reasoning` 不进入请求；`ServerTool*` / `ProviderOpaque` 仍按 provider 匹配回放 |
| 同 provider 换模型 | `Reasoning` 不进入请求；其他 item 正常 |
| `ToolCall` / `ToolResult` 跨 provider | 用规范字段重新渲染；`raw` 忽略 |
| `Message` / `Compaction` 跨 provider | 正常渲染 |

- 丢弃是**投影/渲染层**行为，不修改日志；原始 item 仍可扫描，切回原 provider 后仍可回放。
- 丢弃项在 trace 记 `protocol.dropped_items`；不向模型注入「某段推理被丢弃」的合成文本。
- 供应商不透明 reasoning 不做跨供应商翻译（地图已出界）。

## 12. adapter 与 loop 的责任边界

| 关注点 | adapter | loop |
| --- | --- | --- |
| 传输、鉴权、base URL、代理 | 是 | 否 |
| HTTP/SSE 解析、事件顺序归一化 | 是 | 否 |
| item 组装、delta 拼装 | 是 | 否 |
| 原始 JSON 保留与同 provider 回放 | 是 | 否 |
| usage / 错误归一化 | 是 | 否 |
| 能力声明 | 是 | 消费 |
| 请求渲染（system / tools / items） | 是 | 否 |
| 重试次数、退避、取消 | 否 | 是 |
| 日志 append / dedup / 检查点 | 否 | 是 |
| 工具执行、审批、截断 | 否 | 是 |
| 投影、压缩、steering | 否 | 是 |
| 发起取消 | 响应 stream drop | 发起 |

- adapter 不写日志、不执行工具、不跨请求保存状态。
- loop 不解析供应商 JSON，不按 provider 名分支。
- 一次 `Provider::stream` 对应一次 attempt；重试由 loop 重新调用 `stream`，adapter 不内建重试。

## 13. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| A1 | Anthropic thinking 回放 | 请求中的 `thinking` block 与记录逐字节一致；signature 不变 |
| A2 | OpenAI encrypted reasoning（`store:false`） | 请求含 `include:["reasoning.encrypted_content"]`；无 `previous_response_id`；回放成功 |
| A3 | 跨 provider 切换 | `Reasoning` / `ServerTool*` / `ProviderOpaque` 不进入请求；规范历史仍可投影；trace 有 `dropped_items`；不报错 |
| A4 | 压缩后旧 reasoning | `binding.prefix_epoch` 不匹配 → 丢弃；压缩后新 reasoning 使用新代次并可回放 |
| A17 | 工具集变化（fork 收窄） | `binding.tools_hash` 不匹配 → `Reasoning` 丢弃并记 `dropped_items`；规范历史与工具调用仍完整 |
| A18 | 重启后同名工具 schema 变化 | 请求 `tools` 仍由 `ConfigSnapshot.tools` 渲染，与崩溃前一致；注册表新 schema 只影响新 session |
| A5 | 工具调用/结果双向映射 | Anthropic 合并 user 消息且 `tool_result` 在文本前；OpenAI `function_call` / `function_call_output` 成对 |
| A6 | OpenAI 事件交错 | 按 `output_index` 归一；item 不丢不重；`ItemCompleted` 顺序不影响最终历史 |
| A7 | usage 归一化 | Anthropic 三项相加；OpenAI inclusive 减缓存；`total` 由派生方法得到 |
| A8 | 错误映射 | 429 → `RateLimited` + `retry_after`；401 → `Auth` 不可重试；上下文超限 → `ContextExceeded` |
| A9 | 能力不支持 | 未发 HTTP 即返回 `Unsupported`；不静默丢弃请求字段 |
| A10 | 流协议违规 | 缺 `ResponseEnded` / 顺序错 / `sequence` 回退 → `StreamProtocol`；无 `AssistantMessage` |
| A11 | 工具结果图像跨协议 | Anthropic 原样渲染；OpenAI 渲染占位并记 trace |
| A12 | `Part::Refusal` | 两边渲染正确；`StopReason::Refusal` 并存；不丢内容 |
| A13 | 系统提示不入历史 | `system` 单独渲染；`items` 不变；请求中无 system role message |
| A14 | 原始字节保真 | `raw.json` 的键序、空白、转义在回放后不变 |
| A15 | `Compaction` 跨协议 | Anthropic user text block；OpenAI 普通 input 文本消息 |
| A16 | `ProviderOpaque` 回放 | 同 provider 原样；跨 provider 丢弃并记 trace |

## 14. 出界与非目标

- **typed citations**：v1 只保留在 raw；`Part::Text` 不带 citations 字段。
- **宿主指定 cache breakpoint**：v1 无规范 API；adapter 可自行设置自动断点。
- **OpenAI 原生 compaction item**：v1 两边都渲染为文本；原生 item 只作为 `ProviderOpaque` 保留。
- **`allowed_tools` / 更细的 tool_choice**：v1 只有 `Auto / None / Required / Tool`。
- **OpenAI 服务端状态**：`previous_response_id` / `conversation` 不使用；始终完整历史 + `store:false`。
- **跨供应商不透明 reasoning 翻译**：技术上不可行；丢弃并记 trace。
- **非流式 `complete()`**：调用方 drain `stream` 即可。
- **provider 内建重试**：重试归 loop；adapter 只做错误分类。
- **供应商特定请求字段的透传**：v1 不开放 raw request override。

## 15. 留给下游

- **#12**：`StreamEvent`、`ResponseStarted` / `ResponseEnded`、`protocol.dropped_items` 到 trace span 的命名与属性。
- **#13**：已定。`Usage` 的聚合、桶、partial usage 与 session 树 rollup 见 [usage.md](usage.md)；`ProviderError.partial_usage` 与三个事件上的 `model` 字段已回写本文件与 #3。
- **#15**：已定。`ProviderId` / `ModelId` / `ToolName` / `JsonString` / `ModelAlias` 是 opaque newtype；alias 集合由宿主配置（[packaging.md](packaging.md) §3.2）。#26 已定 `Provider::id()` + `ProviderCatalog` 与 `RequestConfig`。
- **#16**：已定。Provider 只有进程内路径，无清单、无动态加载，兼容性由合约 crate semver 保证；model alias 归 #15；`RawProviderData.kind` 原样持久化、跨版本不翻译，不可回放时按本文件规则丢弃并记 trace。见 [plugins.md](plugins.md) §7。
- **#19**：流中断、错误路径与跨 provider 切换的确定性测试。
- **#4**：`Item` / `Part` / `Usage` 持久化后的 schema 版本与 upcast 规则（首次定义，暂无 upcast）。
- **#8**：fork 原样复制规范 item 与 raw，不做过滤；跨 provider 丢弃由 adapter 渲染请求时执行。
