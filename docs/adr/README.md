# ADR

ADR 记录**难以逆转、跨模块、有真实权衡**的决定及其理由。规范细节在 `docs/design/` 与 `docs/spec/`，范围与排期在 GitHub issue。

## 写什么

写进 ADR 的：数据模型与恢复语义、模块边界与公开扩展面、框架与宿主的职责划分、fail-closed 这类安全默认。

不写进 ADR、放进设计文档或 issue 的：

- 可调参数与默认值（超时、重试次数、并发上限、阈值、预算）；
- 单个适配器内部的实现选择（SQLite pragma、事务粒度、传输细节、协议版本常量）；
- 范围裁剪（「v1 不做 X」）——这是排期，不是对 X 的否决，不要写成 Considered alternatives 里的「拒绝」；
- CLI 命令、配置文件形状、CI 时间预算、crate 数量。

## 怎么写

- Context 自足：不依赖 issue 正文或不变量代号才能读懂；issue 只作「来源」链接。
- accepted 之后**不在原文打修订补丁**。决定被推翻或扩展时写新 ADR，旧 ADR 标 `superseded by` 并保留不动。只是细节变化就改设计文档，不碰 ADR。
- 2026-10-24 做过一次发布前整理：合并重叠的 ADR，把参数与实现细节移进设计文档，退役的编号保留为指向新位置的短桩，原文在 git 历史里（`8b79295`）。

## 冲突时

实现与 ADR 的 **Decision** 段冲突时，按 `docs/agents/domain.md` 显式提出。与设计文档里的参数或细节不一致，修设计文档即可，不算 ADR 冲突。

## 现行 ADR

0048 之前的 ADR 里「宿主」按「应用」读，「宿主元数据」即会话元数据。

| ADR | 决定 |
| --- | --- |
| [0002](0002-log-topology-and-commit-semantics.md) | 每个会话一条 append-only 日志，只提交终态，不做工作流引擎 |
| [0006](0006-agent-loop-execution-model.md) | Agent loop 以 step 为检查点，steering 不打断模型流 |
| [0007](0007-approval-and-sandbox-layers.md) | 审批是框架管线，隔离与内容安全是扩展责任 |
| [0008](0008-subagent-model.md) | 子智能体是工具调用拥有的独立子会话（「不能替换 system」由 0053 取代） |
| [0010](0010-canonical-protocol-model.md) | 规范协议模型：扁平 item、不透明数据原样回放、能力标志 |
| [0011](0011-native-tracing.md) | 追踪与用量是日志的确定性投影，成本不在内核 |
| [0019](0019-sandbox-seam-and-policy.md) | 沙箱是进程启动点的变换，策略归宿主，fail closed |
| [0021](0021-skills-are-host-resolved-instruction-packages.md) | 技能是宿主解析的指令包，没有权限 |
| [0023](0023-storage-holds-log-and-catalog-wake-by-fold.md) | 存储契约只有日志、目录与退役；唤醒由引擎折叠（单写者部分由 0036 取代） |
| [0025](0025-extension-points-are-typed-hooks.md) | 扩展点是固定的类型化钩子 |
| [0026](0026-engine-is-one-crate-only-adapter-traits-public.md) | 引擎是一个 crate，只有适配器 trait 与钩子公开，宿主显式驱动 |
| [0027](0027-mcp-is-a-tool-source-not-a-plugin-host.md) | MCP 是工具来源；ACP 不属于工具层 |
| [0029](0029-provider-instance-is-a-vendor-endpoint.md) | provider 实例 = 供应商端点，模型规则可由宿主注入 |
| [0030](0030-session-config-is-a-fold-of-log-records.md) | 会话配置是日志记录的折叠，变更是历史中的一个位置 |
| [0031](0031-server-tools-structured-output-and-file-input.md) | 服务端工具按 provider 声明，文件引用绑定 provider，结构化输出是请求约束 |
| [0032](0032-sessions-carry-opaque-host-metadata.md) | 会话携带创建时写入、不可变、框架不解释的宿主元数据 |
| [0033](0033-session-fork-reuses-context-fork-snapshot.md) | 会话分叉是宿主创建的独立会话，复用上下文分叉的快照 |
| [0034](0034-structured-output-modes-and-output-tool.md) | 结构化输出有显式选择的输出模式，工具模式由引擎以输出工具实现 |
| [0035](0035-desktop-reference-host.md) | 桌面参考宿主一个进程托管所有项目，与 CLI 共用配置与数据库（crate 组织由 0049 取代） |
| [0036](0036-per-session-tree-ownership.md) | 写者独占缩小到会话树：显式认领、进程租约与属主纪元，多进程共用存储（跨进程送达由 0047 取代） |
| [0037](0037-session-environment.md) | 会话环境由宿主适配器按会话解析，子进程默认不继承进程环境 |
| [0038](0038-watch-separates-observing-from-driving.md) | 观察与驱动分离：已提交日志可订阅，流式增量在属主进程内扇出 |
| [0039](0039-multi-user-embedding-mechanisms.md) | 多用户内嵌：用户是宿主元数据，内核只补按会话的机制（前提由 0047 修正） |
| [0044](0044-host-metadata-is-patchable.md) | 宿主元数据可按键修改：外部写入的 patch，目录行同事务合并（proposed） |
| [0045](0045-fork-at-any-committed-turn.md) | 会话分叉可指定任一已提交回合为水位（proposed） |
| [0046](0046-local-hosts-and-web-sdk-do-not-share.md) | 本地应用与 web 集成分开，互不共享数据（`asterpath-host` 部分由 0048 取代） |
| [0047](0047-multi-user-and-multi-node-belong-to-the-platform.md) | 多用户与多机部署归集成平台；内核只保证会话树单写者与 SQLite、Postgres 两种后端 |
| [0048](0048-no-host-tier-workspace-and-config-are-facade-features.md) | 没有宿主这一层；「宿主」改称「应用」（并入门面 feature 部分由 0049 取代） |
| [0049](0049-cli-and-desktop-are-one-application-crate.md) | CLI 与桌面端是一个应用 crate 的两个入口；组装与配置是它的私有模块 |
| [0050](0050-user-config-holds-api-keys-and-is-created-on-first-run.md) | 用户配置在首次运行时创建，API key 可以存在里面（文件须只有属主可读） |
| [0051](0051-agent-profiles-are-application-owned-prompt-files.md) | 智能体档案是应用的命名提示词文件，项目级优先、会话创建时解析一次，不能放宽审批与沙箱 |
| [0052](0052-output-styles-are-orthogonal-appended-prompt-files.md) | 输出风格是与档案正交选择、追加在档案正文之后的提示词文件，只有正文与描述（子会话部分由 0054 取代） |
| [0053](0053-subagents-select-application-agent-profiles.md) | 子智能体可按名字选用应用提供的智能体档案，档案列表属于会话环境，只能收窄工具，不能放宽审批与沙箱 |
| [0054](0054-output-styles-do-not-reach-subagents.md) | 输出风格不进入子会话：应用在会话环境里提供不带风格的基础 system，fork 子会话例外 |
| [0055](0055-sandbox-is-the-boundary-for-spawned-commands.md) | 起进程的工具以沙箱为边界，审批只问能否越出沙箱；审批模式只收紧沙箱，命令规则只改审批结论（模式切换部分由 0056 修正） |
| [0056](0056-read-only-cap-sticks-to-the-calls-it-allowed.md) | 因只读上限而放行的 `bash` 调用启动时保留 `ReadOnly`，记录只在内存中 |

其余编号（0001、0003–0005、0009、0012–0018、0020、0022、0024、0028）是 superseded / merged / retired 的短桩；0040 已被 0048 取代；0041–0043 已否决（见 0047）。
