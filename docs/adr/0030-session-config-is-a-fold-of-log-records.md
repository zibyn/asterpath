# 会话配置是日志记录的折叠，变更是历史中的一个位置

**Status**: accepted (2026-10-24)；「system 只能追加」部分修订 by [ADR-0057](0057-instruction-sections-are-replaced-by-key.md)（带键的指令节可按键替换或删除）；reasoning 变更的位置由 [ADR-0058](0058-reasoning-changes-take-a-place-in-history.md) 扩展
**来源**: [#108](https://github.com/zibyn/asterpath/issues/108)
**修订**: [ADR-0010](0010-canonical-protocol-model.md) 的请求模型（system 与工具集不再只来自创建时快照）

## Context

会话配置只在创建时写进 `SessionCreated.config`，之后无法改变。主流 agent 都支持会话中途换模型或 reasoning 档位（Claude Code `/model`、Codex 按回合覆盖、opencode 按消息选模型）、增减工具（MCP `tools/list_changed`、宿主按需启用），以及刷新指令（技能索引、AGENTS.md、工作目录）。宿主目前唯一的办法是新开会话，丢掉历史与 reasoning。

两家供应商对「变更」的约束不同（2026-10-24 核对官方文档，见 #108）：

- **Anthropic**（Fable 5.1 / Opus 5.5 / Sonnet 5.5 / Haiku 5.5）：thinking block 只在顶层 `system`、`tools` 与它之前的消息都不变时有效，否则 400 或被丢弃。官方做法是不改这两个顶层字段，而是在 `messages` 中追加 `role: "system"` 消息；工具变更用其中的 `tool_addition` / `tool_removal` 块。
- **OpenAI**：reasoning 不与工具集绑定，可以直接改 `tools`；中途指令可以用 `developer` 消息。

已有的基础：每条模型记录自带实际使用的 `model`；不透明 reasoning 按绑定门控回放；工具意图自带 `replay` / `provenance`；子会话在 spawn 时刻快照继承。

## Decision

- **生效配置是折叠**：`SessionCreated.config` 依次应用之后的每条 `ConfigChanged`。引擎只有一个折叠入口，恢复、模型请求、调度、trace 都用它，不再各自读取 `SessionCreated`。
- **可变的是模型、请求参数、工具集与 system**。`turn` / `compaction` / `subagents` 参数保持创建时的值。
- **`ConfigChanged` 是字段补丁**：只携带变化的部分。模型与请求参数按字段覆盖；工具集记录 `added`（完整描述符）与 `removed`（名字）；system 只能追加指令，不能替换。
- **只有宿主能改**：经 `SessionHandle` 写入，写入前校验（provider 已注册、移除的工具存在、新增的工具名不冲突、system 只含文本）。校验失败不落日志。让模型自己换模型或开工具是宿主策略，经普通工具实现。
- **在下一个 `StepStarted` 生效**，与 steering 同构：记录随时追加；模型请求按 `StepStarted.seq` 水位折叠配置，因此重试与恢复渲染出同一个请求。进行中的 step 不受影响。
- **变更是历史中的一个位置，不是改写开头**：
  - 每个请求都有一条基线：会话创建时的 system 与工具集，或者最近一次压缩时的生效值。基线之后的变更作为带位置的条目投影进模型历史。
  - adapter 按各自协议渲染这些条目：Anthropic 渲染为 `role: "system"` 消息与 inline tools；OpenAI 渲染为 `developer` 消息，顶层 `tools` 直接用生效集合。供应商不支持中途指令或 inline tools 时，adapter 退回改写顶层字段，依赖既有的回放门控丢弃失效的 reasoning。
  - 压缩重新定基线：被替换区间内的变更并入基线，不进入摘要，追加的指令因此不会被概括掉。压缩本来就让旧 reasoning 失效（`prefix_epoch`），重定基线不额外付出代价。
- **子会话继承父会话在 spawn 所在 step 水位的生效配置**；之后父的变更不影响子，子也可以有自己的变更。
- **审批策略与沙箱策略不进入配置**（[ADR-0007](0007-approval-and-sandbox-layers.md)、[ADR-0019](0019-sandbox-seam-and-policy.md)），本决定不改变它们。

规范条目的形状、`ModelRequest` 如何同时携带基线与生效集合、各 adapter 的渲染细节，见 protocol.md 与 agent-loop.md（随实现补齐）。

## Consequences

- 宿主可以在一个会话里换模型、换档位、增减工具、追加指令，保留全部历史；工具变更可以直接接 MCP `list_changed` 与技能刷新（各自另开票）。
- 恢复仍然确定：配置变化在日志里，按水位折叠。
- 回合中途换模型时，本回合已有的 reasoning 由绑定门控丢弃；换回原模型后可以再次回放。
- 在 Anthropic 新模型上，工具与指令变更不会使旧 thinking 失效，也不会打破前缀缓存；退回改写顶层字段的路径会失效，宿主需要接受。
- 被移除的工具在历史中的调用保持原样；它已记录的意图照常收尾（执行走活注册表，恢复按记录的 `replay`）。
- 上下文窗口变小（换成小窗口模型）在下一个 step 的阈值检查里自然触发压缩，不需要专门路径。
- 「配置快照」只指创建时的初始值，词表随之修订。
- 新增一个会话事件变体与若干规范条目；发布前改动，`LOG_SCHEMA_VERSION` 不变。

## Considered alternatives

- **每次变更写全量快照**：折叠最简单，但每条都复制含 schema 的整个工具集，也表达不出「增/删」，adapter 无法渲染 inline tools。
- **替换顶层 system 与工具集**：语义直观，但在 Anthropic 新模型上使此前所有 thinking 失效（新账号默认 400）并打破缓存。
- **在下一个回合生效**：回合内配置恒定，但长工具循环中途无法换模型或增减工具。
- **按回合的临时覆盖、不落日志**（类似请求参数覆盖）：恢复时无法重现当时的请求，违反「模型可见即已记录」。
- **新开会话并复制历史**：宿主现在就能做，但丢失会话身份、reasoning 与子会话树。
- **允许改 `turn` / `compaction` / `subagents` 参数**：没有使用场景，恢复与子会话继承的语义会变多。
