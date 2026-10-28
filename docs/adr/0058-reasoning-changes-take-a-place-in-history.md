# reasoning 变更也是历史中的一个位置，支持时原位渲染

**Status**: accepted (2026-10-28)；扩展 [ADR-0030](0030-session-config-is-a-fold-of-log-records.md)（「变更是历史中的一个位置」从 system 与工具集扩到 reasoning）
**来源**: [#180](https://github.com/zibyn/asterpath/issues/180)（[#174](https://github.com/zibyn/asterpath/issues/174)）

## Context

ADR-0030 让 system 与工具集的中途变更成为历史中带位置的条目，顶层字段保持基线；请求参数（`ConfigPatch.request`）的变更则直接改顶层字段。会话中途改 reasoning 档位是最常见的请求参数变更，而改顶层 reasoning 在部分供应商上会使前缀缓存失效。

两家供应商都提供了原位改档位的方式（2026-10-28 核对官方文档）：Anthropic 在 `role: "system"` 消息上带 `output_config.effort`（beta `mid-conversation-output-config-2026-07-01`），OpenAI 用 `configuration_update` input 项（GPT-6 系列）。两者都只能改 effort；thinking 开关、`budget_tokens`、`summary` 只在顶层。pi 与 opencode 已这样做（[调研](../research/prompt-injection-and-caching.md) §3.2、§4.2）。

## Decision

- **reasoning 变更投影为条目**：`ConfigChanged` 改了 `request.reasoning` 时，投影生成 `Item::ReasoningChanged`，落位规则同指令条目与工具变更条目。`ModelRequest` 同时携带基线（`baseline_reasoning`：创建时或最近一次压缩时的值）与生效值（`reasoning`）。
- **原位还是顶层由 adapter 按模型决定**：模型 profile 声明支持（`inline_effort`），且基线与每条变更都能原位表达（只差 effort，档位为模型接受）时，顶层渲染基线、变更渲染为原位项；否则顶层渲染生效值、变更不渲染，与现状相同。判定是整个请求一次，不按条目混用。
- **压缩重定基线**：被替换区间内的 reasoning 变更并入 `baseline_reasoning`，与 system、工具集一致。
- 请求参数里 reasoning 以外的字段仍只改顶层：它们没有原位形式。

## Consequences

- 支持的模型上，中途改档位不改请求前缀，缓存继续命中。
- 原位变更的生效时机由供应商决定：Anthropic 在工具结果之后的变更从下一条用户输入起生效，而不是下一个 step。
- 一个请求里只要有一条变更不能原位表达（如关掉 thinking），整个请求退回顶层生效值，此前的原位变更也不渲染。
- 支持的模型列表写在各 adapter 的模型表里，应用可按端点覆盖（ADR-0029）。

## Considered alternatives

- **逐条决定原位或顶层**：顶层只有一个 reasoning，混用时顶层与原位项的先后关系无法表达。
- **把 reasoning 变更渲染为指令文本**：模型不会因此改变推理档位。
