# 指令节：同一份指令按键刷新，不再叠加

**Status**: accepted (2026-10-28)；部分修订 [ADR-0030](0030-session-config-is-a-fold-of-log-records.md)「system 只能追加指令，不能替换」
**来源**: [#179](https://github.com/zibyn/asterpath/issues/179)（[#174](https://github.com/zibyn/asterpath/issues/174)）

## Context

ADR-0030 让会话中途的 system 变更只能追加：每条追加是历史中的一个指令条目，压缩时按日志顺序并入基线 system。同一份指令（AGENTS.md、技能索引、日期、环境）刷新几次，历史里就有几份全文，压缩后基线里也是几份全文。应用因此不敢在会话中途刷新 AGENTS.md 或日期。

改写顶层 system 会打破前缀缓存，在 Anthropic 新模型上还会使已有 thinking 失效（ADR-0030 已否决）。pi 的按节补丁、opencode 的 initial / changed / removed 指令、Codex 的世界状态差量都是按键替换，同时不改前缀（[调研](../research/prompt-injection-and-caching.md) §1.1、§3.1、§4.1）。

## Decision

- **system 分两部分**：无键的 `system`，与按键区分的**指令节**（`system_sections`，每个键至多一节）。创建时两者都可给（`SessionSpec.system_sections`），请求渲染时节排在无键 system 之后。
- **按键刷新仍是历史中的一个位置**：`ConfigPatch.system_sections` 里的每一节投影为一条带键的指令条目，落位规则与追加指令相同，不改顶层 system。条目渲染为「某节被替换」的通知加新文本；`parts` 为空表示删除该节，渲染为删除通知。
- **压缩按键重定基线**：被替换区间内的带键条目按键并入基线节（已有的键原位替换、新键排在最后、删除即移除），所以同一节刷新多少次，基线里都只有最新值。无键指令保持 ADR-0030 的行为。
- **子会话**：fresh 子会话继承父生效配置的节；fork 子会话继承父在 fork 点的基线节；换成档案或环境提供的 system 时不带父的节。
- 日志 schema 只新增可缺省字段，发布前不改 `LOG_SCHEMA_VERSION`（ADR-0004 已退役）。

## Consequences

- 应用可以监视 AGENTS.md、在跨日时刷新日期，而不让历史与基线无界增长，也不打破前缀缓存。
- 压缩前，同一节的旧值仍在历史原位，模型靠通知文本知道它已被取代；压缩后旧值消失。
- 易变内容（日期、cwd、git 状态）应放进带键指令节，而不是无键 `system`（指南见 [packaging.md](../design/packaging.md) §2.1）。

## Considered alternatives

- **替换顶层 system 的某一节**：语义最直接，但每次刷新都改前缀，与 ADR-0030 否决「替换顶层 system」的理由相同。
- **把键放进 `append_system` 的每个片段**：改动现有字段的形状，所有调用方都要改；无键追加是主要用法。
- **压缩前也在历史里折叠旧值**：会改写已发送过的历史，打破前缀。
