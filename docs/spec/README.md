# Asterpath v1 spec

> 入口。设计决策来自 [设计地图 #1](https://github.com/zibyn/asterpath/issues/1)；本目录是 repo 侧 spec，tracker 侧单份 spec 由 `/to-spec #1` 生成，两者不互相替代。

## 目的地

一份可交给实现智能体独立开工的 **Asterpath v1 设计 spec + 关键 ADR**：可嵌入的 Rust 库 + 参考 CLI；内核优先（模块契约、session 持久执行与恢复、子智能体调度与恢复、Anthropic + OpenAI Responses 双协议、原生追踪与 token 统计）。**不做通用工作流引擎。**

完成判据：一个没参与设计的实现智能体拿到 spec 后无需追问即可做出 walking skeleton；关键决策有 ADR；每条不变量有可测的验收场景。

## 状态

- 内核、打包、保留/归档与测试工具链已定稿（[#2](https://github.com/zibyn/asterpath/issues/2)–[#19](https://github.com/zibyn/asterpath/issues/19)）。
- 扩展 seam：[#21](https://github.com/zibyn/asterpath/issues/21) 沙箱已定（[sandbox.md](../design/sandbox.md)）；[#20](https://github.com/zibyn/asterpath/issues/20) MCP 映射已定（ACP 出界）。这些不影响 M0–M4 开工。
- `asterpath-mcp`（#80 前为 `asterpath-plugin-host`）、MCP adapter 与沙箱归 **M5（post-v1）**，不进 v1 workspace 与 v1 验收；插件分发、ACP 与 WASM 出界（见 [packaging.md §5](../design/packaging.md#5-交付清单与里程碑)）。

## 阅读顺序

1. [打包、公共 API 与参考 CLI](../design/packaging.md)：crate 图、稳定面、CLI、交付清单、验证协议。
2. [模块边界、内核契约与插件宿主](../design/module-contracts.md)：内核契约、扩展钩子（`Hooks`）、生命周期。
3. [事件日志与检查点恢复语义](../design/event-log-recovery.md)：日志拓扑、提交单位、恢复与等待。
4. [Agent loop](../design/agent-loop.md)：turn/step、取消、steering、失败重试。
5. [工具契约、执行管线与审批边界](../design/tools.md)：管线、审批、并行、超时。
6. [子智能体模型](../design/subagents.md)：spawn、fork、取消、孤儿恢复。
7. [上下文压缩](../design/compaction.md)、[规范化协议](../design/protocol.md)、[追踪](../design/tracing.md)、[用量](../design/usage.md)、[存储](../design/storage.md)、[保留/归档](../design/retention.md)、[测试工具链](../design/testing.md)。
8. [验收场景](acceptance.md)：三段验收的精确断言。
9. [ADR 目录](../adr/) 与 [GLOSSARY](../../GLOSSARY.md)。

**M5（post-v1）**：[插件](../design/plugins.md)、[MCP adapter](../design/mcp.md)、[沙箱](../design/sandbox.md)。设计已定，不是 v1 的实现依据。

**M6（post-v1）**：[技能](../design/skills.md)。设计已定，不是 v1 的实现依据。

## 规范与历史

- `docs/design/*.md` 与 `docs/spec/*.md` 是**规范**；`docs/adr/*.md` 是需要长期保留的决策与理由。
- GitHub issue 是**历史**：每张票的提问与 resolution 记录在那里，不作为实现依据。
- 文档冲突时，以被链接为"最终"的章节为准；实现前发现的冲突属于 spec 缺陷，先修文档。

## 交付清单

见 [packaging.md §5](../design/packaging.md#5-交付清单与里程碑)。M0–M4 是验收闸门，不是实现计划。

## 验证协议

G1 计划闸与 G2 骨架闸的 prompt、通过条件与回灌流程见 [packaging.md §6](../design/packaging.md#6-验证协议spec-是否可开工)。

## 未决问题

- 内核与打包：无。
- 扩展 seam：保留/归档的退役接缝（`SessionStore::retire`）属 v1（[retention.md](../design/retention.md)）；测试工具链已定（[testing.md](../design/testing.md)）；MCP adapter、沙箱与插件宿主已定但归 M5（[mcp.md](../design/mcp.md)、[sandbox.md](../design/sandbox.md)、[plugins.md](../design/plugins.md)），都不属于 v1 交付闸门。
- 已知解释：验收 (b) 验证 adapter 等价，不验证 turn 内切换 provider；v1 的 provider/model 在 session 创建时快照。

## 出界

完整列表见 [设计地图 #1](https://github.com/zibyn/asterpath/issues/1) 的 **Out of scope**。要点：不做通用工作流引擎、不做多进程调度、不做 WASM 插件、不做跨供应商不透明 reasoning 翻译、不做全局跨 session 用量聚合。
