# 规范协议模型：扁平 item 历史、不透明数据原样回放、能力标志

**Status**: accepted (2026-10-22)；2026-10-24 整理
**来源**: [#11](https://github.com/zibyn/asterpath/issues/11)、[#26](https://github.com/zibyn/asterpath/issues/26)、[#28](https://github.com/zibyn/asterpath/issues/28)、[#96](https://github.com/zibyn/asterpath/issues/96)
**规范**: [protocol.md](../design/protocol.md)

## Context

Anthropic Messages 与 OpenAI Responses 结构不兼容：前者是角色消息 + 内容块树、thinking signature 必须原样回传；后者是扁平 output item，`store:false` 时必须回放 encrypted reasoning。框架要求本地日志是唯一真相、两种协议都是一等公民、供应商特有信息不静默丢弃，同时 loop 不认识具体供应商。

## Decision

- **规范历史是扁平有序 `Vec<Item>`，内容片段是 `Part`**。工具调用/结果、推理、服务端工具、压缩摘要都是顶层 item；system 是请求字段，不进历史。
- **不透明 item（reasoning、server tool、opaque）保存供应商原始 JSON，按回放绑定原样回放**，不 parse 后重新序列化。绑定不匹配（不同 provider 实例、模型，或绑定声明的失效条件）时丢弃整个 item，不翻译、不伪装成文本。绑定字段见 protocol.md。
- **message 按规范 parts 渲染**，只在同一 provider 实例时从 raw 合并少数已知字段；hook 或压缩改写过的文本不能被旧 raw 覆盖。
- **流式结果规范化为 `ResponseStarted → ItemStarted / PartDelta / ItemCompleted → ResponseEnded`**，只有完成的 item 落日志。
- **能力标志按模型声明**；loop 只按标志分支，不按 provider 身份分支；请求需要而能力不支持时返回 `Unsupported`，不静默降级。
- **usage 统一为 exclusive 口径**（未缓存输入 + 缓存读 + 缓存写），错误归一化为带可重试性与 request id 的 `ProviderError`。
- **`Provider` 只有身份、能力与 `stream`**：adapter 管传输、格式、归一化与 raw；loop 管重试、日志、工具、投影与压缩。provider 身份是实例（[ADR-0029](0029-provider-instance-is-a-vendor-endpoint.md)）。

## Consequences

- 日志体积增加，换来同 provider 回放与未知字段保真。
- adapter 复杂度集中在 adapter，loop 保持供应商无关。
- 不使用服务端状态（OpenAI `store:false`），每次发送完整历史。
- 跨 provider 切换会丢失不透明 reasoning，规范可见历史与工具调用仍完整。

## Considered alternatives

- **Anthropic 式角色消息 + parts**：OpenAI 的扁平 item 需要额外降维，工具调用嵌在消息里不利于按 seq 投影。
- **所有供应商字段 typed 化**：规范类型随供应商 API 膨胀，未知字段仍会丢。
- **每次响应只存一整块 raw**：item 级回放与投影要从大块里重新定位。
- **翻译不透明 reasoning**：不可行，伪造推理会污染历史。
- **loop 按 provider 身份分支**：供应商差异扩散到 loop。
- **使用 OpenAI 服务端状态**：恢复依赖远端，与本地日志唯一真相冲突。
