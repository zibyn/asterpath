# facade 接受宿主模块，注册表与事件总线对嵌入者开放

**Status**: accepted (2026-10-23)
**来源**: [#72](https://github.com/zibyn/asterpath/issues/72)（架构审计候选 7）
**前置**: [ADR-0001](0001-dependency-direction.md)、[ADR-0007](0007-approval-and-sandbox-layers.md)、[ADR-0013](0013-plugin-boundary.md)、[ADR-0015](0015-packaging-and-stability.md)

## Context

内核的扩展机制是模块宿主、服务注册表与类型化事件总线（ADR-0001）；ADR-0007 把 post-execute waterfall 定为宿主做脱敏、审计改写的接缝，脱敏、成本、审计等能力按范围原则应以扩展模块接入。但 facade 没有注册模块的入口：

- `ModuleHost` 与事件总线是 facade 私有的，`PreExecuteEvent` / `PostExecuteEvent` / `PreStepGateEvent` 经 facade 无人能接。
- 注册表 9 处 `provide`、仅 1 处 `require`；facade 自己用具体 getter，注入后端时连事件日志、存储、会话目录都没有被 provide。
- `packaging.md` §3.2 让需要自定义模块的嵌入者“直接使用 `ModuleHost`”，而模块几乎不经注册表取依赖，这等于复制整段 facade 组装代码。

## Decision

- **facade 接受宿主模块**：`AsterpathBuilder` / `AsterpathConfig` 提供注册宿主模块的入口。宿主模块在内置模块之后、`init` 之前进入 facade 的 `ModuleHost`，按声明依赖拓扑初始化，关停逆序回卷；`init` 失败使 `build()` 失败并回卷已注册 effect。
- **只新增，不替换**：宿主模块 provide 已有服务是 `DuplicateProvider` 启动错误。替换内置实现仍只经现有配置字段（`agent`、`compactor`、`event_log` / `storage`、`approval` 等），不开第二条路径。
- **facade 保证的服务清单**是公开约定，与后端路径无关：事件日志、存储、会话目录、持久执行器、用量查询、工具注册表、子智能体 spawner；会话退役按后端可选。
- **顺序**：同一 waterfall 与 emit 按注册顺序，内置模块在前，宿主模块按 builder 注册顺序。
- **不出现第二条 compactor 注入路径**：删除 `CompactionModule` 与 `COMPACTOR_SERVICE`，compactor 只经 `AsterpathConfig.compactor`。
- 参考 CLI 不使用该入口（ADR-0013：进程内模块编译期组装）。

## Consequences

- facade（Tier 1）新增公开面：注册入口、服务清单与顺序语义进入 semver 承诺；新增或移除清单中的服务是兼容性变更。
- 嵌入者无需复制组装代码即可接入脱敏、成本、审计等扩展；“直接使用 `ModuleHost`”只剩替换多个内置模块的场景。
- 宿主模块与内置模块共享同一事件总线：宿主 waterfall 的错误按 waterfall 语义传播，可能使工具调用或回合失败；这是宿主代码的责任。

## Considered alternatives

- **收缩注册表**：删去无人 `require` 的 provide，文档写明拦截点只对自建组装根可用。实现最小，但扩展原则与 ADR-0007 的 post-execute 接缝在 facade 上不成立；拒绝。
- **维持现状**：拒绝，理由同上，且注入后端时服务缺失。
- **允许宿主模块替换内置模块**：灵活，但与配置字段形成两条替换路径，且替换 agent / 存储涉及组装顺序与环依赖；拒绝。
