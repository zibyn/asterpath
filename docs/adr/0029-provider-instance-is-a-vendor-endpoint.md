# provider 实例 = 供应商端点，模型规则由 adapter 给默认值、宿主可注入

**Status**: accepted (2026-10-24)
**来源**: [#106](https://github.com/zibyn/asterpath/issues/106)
**修订**: [ADR-0010](0010-canonical-protocol-model.md) 的 provider 身份（`id()` 不再等于协议名）
**规范**: [protocol.md](../design/protocol.md)；参考 CLI 的 `[providers.<id>]` 配置见 [packaging.md](../design/packaging.md)

## Context

DeepSeek、GLM、MiniMax、Qwen 等供应商提供 Anthropic Messages 或 OpenAI Responses 兼容端点。原先 adapter 的 `id()` 固定为协议名，按模型分支的规则表（Anthropic `rules_for`、OpenAI `efforts_for`）是 crate 私有的，未命中的 id 一律落到 `LATEST`。于是同一进程不能同时接真 Anthropic 和 DeepSeek；DeepSeek 只能借用 `anthropic` 的 id 与 key，它的 reasoning 还会被当成同 provider 数据回放给真 Anthropic；`deepseek-flash` 实测接受 `thinking: {type: "disabled"}`，却因 `LATEST` 不允许 `Off` 在本地被拒。

规则取决于（端点，模型）：同一供应商的不同模型能力不同，同一模型经不同端点时行为和 id 格式也可能不同。只按全局模型前缀不够，只按供应商也不够。参考 pydantic-ai（2026-10 读源码）：`OpenAIChatModel` 是协议，`DeepSeekProvider` 等是供应商，由供应商按模型名给出 `ModelProfile`。

## Decision

- **provider 实例 = 协议 + 端点 + key + 规则。** 每个 adapter 可用 `with_id(...)` 实例化多次，默认 id 仍为 `anthropic` / `openai`。raw / reasoning 的同 provider 回放门控（ADR-0010）按实例 id 生效，不同供应商的数据互不回放。
- **规则是公开值类型**：`AnthropicModelProfile`（`thinking`、`efforts`、`thinking_default_on`、`forced_tool_choice`、`sampling`）与 `OpenAiModelProfile`（`efforts`、`forced_tool_choice`、`sampling`）。两者 `#[non_exhaustive]`，从 `latest()` 起用 `with_*` 设置。
- **查找顺序：实例的模型前缀条目（`with_model_profile`，按注册顺序先命中者生效）→ 实例默认（`with_default_profile`）→ 内置表 → `LATEST`。** 宿主条目可以覆盖官方 id。adapter 内所有按模型分支的地方（渲染、预检、`capabilities()`）都走这一查找。
- **内置表只收官方模型**，来源注释与 `LATEST` 语义不变；第三方供应商与模型的规则是宿主策略，由宿主注入。
- **仍然不静默降级**：profile 未列出的档位、能力关闭的字段照旧在发出 HTTP 前返回 `Unsupported`。

## Consequences

- 同一进程可以同时配真 Anthropic 与任意 Anthropic / OpenAI 兼容供应商，各自独立的 key、端点与回放身份。
- 会话快照记录的是实例 id；恢复时宿主必须以同一 id 注册 provider。
- 供应商的模型能力变化由宿主跟进，框架不随第三方发版。

## Considered alternatives

- **内置 DeepSeek 等供应商预设**：框架要追踪第三方的模型与能力变化，与「机制归框架、策略归宿主」冲突；拒绝。
- **全局模型前缀表，可由宿主追加**：同一模型经不同端点规则可能不同，全局表无法区分；拒绝。
- **只按供应商给一个 profile**：同一供应商的模型能力不同（DeepSeek `/models` 的 `api_capabilities`）；拒绝。
- **未知模型降级（去掉不支持的字段）并告警**：违反 ADR-0010 的不静默降级；拒绝。
- **模型条目字段级继承供应商默认**：合并语义难以从配置一眼看出；先用完整 profile，需要时再加。
- **从 models.dev 或供应商 `/models` 自动拉取规则**：引入网络依赖与外部数据的信任问题；出界。
