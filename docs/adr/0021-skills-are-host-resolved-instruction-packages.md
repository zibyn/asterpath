# 技能是宿主解析的指令包，没有权限，正文按需加载

**Status**: accepted (2026-10-23)；2026-10-24 整理
**来源**: [#63](https://github.com/zibyn/asterpath/issues/63)
**规范**: [skills.md](../design/skills.md)

## Context

Claude Code、Codex、OpenCode、Pi、DeepSeek Harness 都把技能实现为「目录 + `SKILL.md` + YAML frontmatter」，并做渐进披露：先给模型名称与描述，正文按需加载。差异集中在调用面、权限字段与分发。内核不读文件，读文件与发现是宿主职责。

## Decision

- **技能是宿主解析的指令包，不是内核能力**：对模型只表现为可选加载的指令文本。内核不新增 seam、事件或配置字段。
- **格式取主流实现的交集**：`description` 必填，`name` 缺省为目录名；未知字段忽略。
- **发现是宿主策略**：扫描哪些根、优先级与冲突规则由宿主决定，参考 CLI 的规则见 skills.md。
- **渐进披露**：名称 + 描述的索引进入会话的 system，正文由模型调用普通工具 `load_skill` 读取；加载结果作为工具结果入日志，恢复以日志为准。
- **技能没有权限**：不能改工具集、审批、沙箱或 system；`allowed-tools` 等字段不执行，资源与脚本不读取、不执行。
- **实现是可选适配器 crate `asterpath-skills`**，只依赖 protocol。

## Consequences

- 技能是不可信输入：提示注入仍可能，但影响面限于「模型看到什么文本」。
- 正文读当前磁盘：会话中技能文件被改或删，加载会看到新内容或 `NotFound`；已加载的正文由工具结果保留。
- 索引何时刷新随会话配置模型决定，不由本 ADR 固定。
- 用户调用、资源读取、脚本执行、打包与远程 registry 都可以后加，不需要改本决策。

## Considered alternatives

- **创建时全量注入正文**：技能一多就吃满上下文。
- **内核 context-contribution seam**：把「读哪些目录」这种宿主策略塞进内核。
- **把技能映射为 MCP prompts/resources 或插件能力**：耦合两个正交的扩展机制。
- **`allowed-tools` 作为限制或预批准**：让不可信仓库文件进入审批边界。
- **只改 CLI、不建 crate**：嵌入者要各自重写解析与索引。
