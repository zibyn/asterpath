# 用量是事件日志的确定性投影，内核不内置成本

**Status**: accepted (2026-10-22)
**来源**: [#13](https://github.com/zibyn/asterpath/issues/13)

## Context

Asterpath 要求原生 token 统计：不依赖第三方 agent/追踪框架，跨崩溃与重放不重复计数。两家协议对 usage 的缓存口径相反（Anthropic `input_tokens` 不含缓存，OpenAI 含缓存），流式响应只在结束时可靠给出用量，失败 attempt 的用量通常只有输入侧。与此同时，成本、价格表、币种、预算与告警是宿主策略，不应随内核演化。

用量还必须在子智能体场景下正确归因：子会话是独立日志、独立恢复单位，父子日志之间没有事务。

## Decision

- **只有 provider 返回值进账**。`Usage` 只来自协议响应；本地 tokenizer 与 `bytes/3` 估算永不进入用量记录、总量或成本事件。估算只用于 #9 的窗口判断。
- **沿用 #11 的 exclusive 归一化**：输入拆成未缓存 / 缓存读 / 缓存写，输出含推理子集；供应商 `total_tokens` 忽略；原始 usage 留在 `raw`。
- **三个桶**：`model_calls`、`compaction`、`partial`。总量 = 前两者；`partial` 是失败/中断 attempt 的 best-effort 下界，只作诊断。供应商传输层通过 `ProviderError.partial_usage` 提供它。
- **用量不新增日志事件**：`AssistantMessage` / `CompactionApplied` / `ModelCallAborted` 各自携带 `model` 与 `usage`。聚合是日志的纯折叠，精确一次由既有 dedup key 保证。
- **归因按会话树**：子 usage 只写子流；`Session` 与 `SessionSubtree` 查询分开返回 `own` / `descendants`，不提供含糊的单一父总量。树边是 `SessionCreated.parent`，经 core 的 `SessionCatalog` 读取；fork 不继承父用量。
- **查询纯折叠**：`UsageQuery`（protocol，`asterpath-usage` 实现）流式折叠 `EventLog`，v1 不建 usage 表；实时通知是 append 成功后由唯一写入者发出的 `UsageRecorded`。
- **成本不在内核**：成本模块订阅 `UsageRecorded`、自持价格表，用 #6 的 pre-step gate 返回 `Stopped`；最多超支一个 step。内核不提供 cost/currency/budget 类型。
- **全局跨 session 聚合出界**：v1 查询面限于单会话及其子树。

## Consequences

- 失败 attempt 的用量默认不计入总量；报告同时给出 `partial`，不会把已知与未知混成一个数。
- 两个崩溃窗口的代价是明确的：`ResponseEnded` 后、`AssistantMessage` 落盘前的用量永久丢失；压缩在 append 前崩溃会重复消耗摘要 token 且只记最终一次。
- 查询成本与日志长度线性；大规模报表需要宿主自己物化，或等后续 storage 扩展，而不是现在引入第二真相源。
- 三处事件各增加一个 `model: ModelSpec` 字段；`ProviderError` 增加 `partial_usage`；core 增加 `SessionCatalog`。日志 schema 在 v1 发布前完成这些字段。
- trace 与 usage 是同一日志的两条投影，必须对账（U12）；tracing 不拥有统计，usage 不读 span。
- 后端适配器只负责把已知 usage 填进 `ProviderError`；不推断、不补偿。

## Considered alternatives

- **本地 tokenizer 估算进账**：不引入 tokenizer 是 #9 的既定取舍；估算混入用量会让成本与审计不可信。
- **把 `partial` 计入总量**：失败 attempt 的输出通常不可知，混入会让“总量”变成半真半假；单列更诚实。
- **父日志复制子 usage**：会造成父子双计，也破坏“子会话是独立恢复单位”。
- **独立的 usage 日志事件**：多一份可重建的记录与迁移负担；用量已经挂在终态记录上。
- **usage 派生表 / 物化视图**：查询更快，但引入可重建数据的维护与一致性窗口；与 tracing 的“纯折叠”保持一致，v1 不做。
- **`UsageScope::All` + `SessionCatalog::roots()`**：全局报表很诱人，但 v1 的参考 CLI 与成本模块都不需要；留给宿主注册表。
- **把成本/币种放进内核**：价格与预算随宿主变化，属于地图明确划出的扩展层。
