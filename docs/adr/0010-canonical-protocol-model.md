# 规范协议模型采用扁平 item 历史、逐字节原始回放与能力标志

**Status**: accepted (2026-10-22)
**来源**: [#11](https://github.com/zibyn/asterpath/issues/11)
**修订**: 2026-10-22（[#26](https://github.com/zibyn/asterpath/issues/26)）— `Provider` 增加 `id()`；新增 `ProviderCatalog`；请求参数由 `RequestConfig` 提供（`SessionSpec` / `ConfigSnapshot`），能力匹配仍由 adapter 判定。2026-10-22（[#28](https://github.com/zibyn/asterpath/issues/28)）— `ReasoningBinding` 增加 `tools_hash`；工具集变化使旧 reasoning 不可回放。

## Context

Anthropic Messages 与 OpenAI Responses 的结构不兼容：前者是角色消息 + 内容块树、tool_use 结束回合、thinking signature 必须原样回传；后者是扁平 output item、工具调用只是 item、`store:false` 时必须回放 encrypted reasoning。框架又要求本地事件日志是唯一真相、双协议都是一等公民、供应商特有信息不静默丢弃。需要一个既能投影模型历史、又能逐字节回放供应商不透明数据、还能让 loop 不认识具体供应商的规范模型。

## Decision

- **规范历史是扁平有序 `Vec<Item>`，内容片段是 `Part`**。工具调用/结果、推理、服务端工具、压缩摘要都是顶层 item；消息内部只放内容片段。system 是请求字段，不进历史。
- **供应商原始 JSON 按 item 保存为字符串，同 provider + model + `prefix_epoch` + `tools_hash` 时原样回放**，不 parse 后重新序列化。`ReasoningBinding.prefix_epoch` 让压缩后旧 reasoning 自动失效，`tools_hash` 让工具集变化（例如 fork 收窄）后的旧 reasoning 失效；跨 provider 丢弃整个不透明 item，不把 summary 伪装成文本。
- **流式结果规范化为 `ResponseStarted → ItemStarted / PartDelta / ItemCompleted → ResponseEnded`**，adapter 负责供应商事件交错与顺序归一，只有 `ItemCompleted` 落日志。
- **能力标志按 `ModelSpec` 声明**；loop 只按标志分支，不按 provider 身份分支；请求需要而能力不支持时返回 `Unsupported`，不静默降级。
- **usage 统一为 exclusive 口径**（未缓存输入 + 缓存读 + 缓存写），原始 usage 留在 raw；错误由 adapter 归一化为带 `retryable` / `retry_after` / `request_id` 的 `ProviderError`，loop 只消费规范分类。
- **`Provider` 只有 `id` + `capabilities` + `stream`**；adapter 管传输、格式、归一化与 raw，loop 管重试、日志、工具、投影与压缩。

## Consequences

- 日志体积增加：每个模型产出的关键 item 携带原始 JSON；换来同 provider 的逐字节回放与未知字段保真。
- adapter 复杂度上升：SSE 交错、usage inclusive/exclusive、错误分类、能力映射都在 adapter；loop 保持供应商无关。
- v1 放弃服务端状态：OpenAI 恒 `store:false`，每次发送完整历史；换来本地日志的唯一真相。
- 跨 provider 切换会丢失不透明 reasoning（不可翻译），但规范可见历史与工具调用仍完整；切回原 provider 后原始 item 仍可回放。
- typed citations、宿主 cache breakpoint、OpenAI 原生 compaction item、`allowed_tools` 明确出界；需要时作为向后兼容新增。

## Considered alternatives

- **Anthropic 式角色消息 + parts**：更贴合 Anthropic，但 OpenAI 的扁平 output item 与交错事件需要额外降维，且工具调用/结果会嵌套在消息里，不利于按 seq 投影。
- **把所有供应商字段 typed 化**：规范类型会随供应商 API 变化而膨胀，未知字段仍会丢；原始 JSON 回放更小、更保真。
- **每次响应只存一整块 raw JSON**：日志更小，但 item 级回放、压缩摘要与跨 provider 投影都要从大块里重新定位，耦合更高。
- **翻译不透明 reasoning**：技术上不可行；伪造推理会污染模型历史。
- **loop 内按 provider 身份分支**：把供应商差异扩散到 loop；能力标志把差异限制在 adapter。
- **使用 OpenAI 服务端状态**：与「本地日志是唯一真相」冲突，也让恢复依赖远端状态。
