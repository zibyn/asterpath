# 打包：一模块一 crate，显式驱动，稳定面收窄

**Status**: accepted (2026-10-22)
**来源**: [#15](https://github.com/zibyn/asterpath/issues/15)
**修订**: 2026-10-22（[#26](https://github.com/zibyn/asterpath/issues/26)）— `SessionSpec` / `ConfigSnapshot` 增加 `request`；`providers` 在 `build()` 经 `ProviderCatalog` 去重；facade 暴露 `approval_gate()`。2026-10-22（[#28](https://github.com/zibyn/asterpath/issues/28)）— `ConfigSnapshot.tools` 存解析后的 `Vec<ToolDescriptor>`，模型请求只从快照渲染。2026-10-23（[#72](https://github.com/zibyn/asterpath/issues/72)）— facade 接受宿主模块并公开保证的服务清单，取代“自定义模块就直接使用 `ModuleHost`”的口径，见 [ADR-0022](0022-facade-accepts-host-modules.md)。

## Context

[#2](0002-log-topology-and-commit-semantics.md) 与 [ADR-0001](0001-dependency-direction.md) 固定了依赖方向，但没有定最终 crate 列表、版本与稳定承诺、feature 组织、公共驱动 API 与参考 CLI 的行为边界。没有这些，实现智能体无法判断"哪些 crate 要建、哪些 API 不能改、宿主怎么跑一个 session"。

同时，`AgentRunner::run` / `SessionHandle` 的签名被 [agent-loop.md](../design/agent-loop.md) §12 明确留给本票；验收场景需要 `kill -9` 级的进程级测试，而不是内存模拟。

## Decision

- **v1 共 16 个库/应用成员 + `xtask`，按里程碑增量创建**。crate 列表、职责、依赖边与引入里程碑见 [packaging.md](../design/packaging.md) §2；`runtime` 与 `scheduler` 不合并，`asterpath-testing` 只依赖合约 crate，验收用独立的 `asterpath-acceptance` 组装真实模块；`asterpath-plugin-host` 等扩展归 M5（post-v1）；M0 只建 7 个 crate + `xtask`。
- **依赖边由 `cargo xtask check-deps` 对照根目录 `deps-allowlist.toml` 校验**：模块之间零边、`core` / `protocol` 无 feature。白名单是机器可读文件，不是脚本文本解析。
- **lockstep `0.1.0`，稳定面收窄到 facade + core + protocol + 日志 schema**。模块 crate 是 0.x 实验面；v1 不发布 crates.io；顶层公开事件/错误枚举（如 `SessionEvent`、`ToolError`、`AsterError`）`#[non_exhaustive]`，内嵌值枚举（`AbortKind` 等）保持可穷举。
- **feature 只在 facade、CLI 与后端 crate**：默认 `anthropic` + `openai` + `sqlite`；关闭 sqlite 时由宿主注入 `EventLog` / `Storage`。
- **没有后台 driver**。宿主显式调用 `SessionHandle::drive()`；同一 session 单飞；`submit_input` 只 append 并通知在途回合；丢弃流不等于取消，取消必须显式调用。
- **`AgentRunner::run` 的流只表达实时事件**。回合失败是 `TurnCommitted { reason }`，不是流错误；`AgentError` 只表示无法运行。恢复位置完全由日志推导。
- **参考 CLI 六命令，退出码 `0/1/2`**；审批由 CLI 处理 `ApprovalRequested`，TTY 默认 `ask`、非 TTY 默认 `deny`，密钥只从环境变量读。
- **验收用 `testdrive` + 真 `SIGKILL`**。三段场景的断言在 [acceptance.md](../spec/acceptance.md)；#19 只负责注入点工具链，不改变断言。
- **v1 的 provider/model 在 session 创建时快照**，不支持 turn 内切换。验收 (b) 验证的是两个 adapter 在同一脚本上的规范化等价与业务代码零分支。

## Consequences

- **2026-10-22 #32 修订**：crate 按里程碑增量创建，M0 只建 7 个 crate + `xtask`；`asterpath-plugin-host` 与 MCP adapter 移出 v1 workspace，归 M5（post-v1）；`plugin-host` feature、`AsterpathConfig.plugins` 与 CLI `plugin list` 从 v1 删除（CLI 由七命令改为六命令）。
- crate 数量多、间接层多；换来可检查的边界与可替换性，符合项目的核心目标。
- 稳定承诺只覆盖少数 crate，模块 crate 可以在 0.x 内演进；嵌入者要替换模块时依赖的是 core/protocol 的 trait，而不是模块的公开结构。
- 显式驱动把调度责任交给宿主，框架不藏后台任务；崩溃与宿主放弃走同一条恢复路径，测试因此能用真 `SIGKILL`。
- 验收测试需要单独的 `testdrive` 组装与 mock HTTP server，CI 时间增加；但三段 charter 验收因此可自动判定。
- `ConfigSnapshot` 在 protocol 中定形（补齐 #11 的缺口）；`tools` 存创建时解析后的 `Vec<ToolDescriptor>`，session 内模型请求只从快照渲染（#28）；之后加入插件/MCP 配置必须按 log schema 规则 bump 版本。

## Considered alternatives

- **单 crate + 模块纪律**：构建简单，但依赖边界不可检查，模块替换退化为修改内核；与 ADR-0001 冲突。
- **合并 runtime + scheduler**：少一个 crate，但通用持久等待与子会话调度被绑在一起，无法独立替换。
- **把验收测试放进 `asterpath-testing`**：少一个 crate，但 `testing` 必须依赖 facade 与具体模块，破坏合约白名单；且跨 package 无法用 `CARGO_BIN_EXE_` 找到 `testdrive`。
- **facade 自带后台 driver**：调用方更省事，但框架开始 spawn 任务，与 core 不 spawn 的约束冲突，恢复与取消语义也会分叉。
- **prompt 串进 CLI 做验收驱动**：不需要 `testdrive`，但产品二进制会背上测试用 provider 与退出码语义。
- **v1 发布 crates.io**：会立刻产生版本与兼容负担；git/path 依赖已满足嵌入需求。
