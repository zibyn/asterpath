# 桌面参考宿主：进程内托管多项目，按会话路由工作根

**Status**: proposed (2026-10-25)
**依赖**: [ADR-0036](0036-per-session-tree-ownership.md)–[ADR-0040](0040-host-kit-shared-by-reference-hosts.md)；在它们落地后按其重写本文。

## Context

仓库要提供一个 Tauri 2 桌面客户端，作为第二个参考宿主：只经 `asterpath` 门面公开 API 写成，门面不够时记录缺口，不为它开内核后门。CLI 与桌面端只是形态不同，数据必须互通。

参考对象（`docs/research/desktop-codex.md`、`desktop-claude-code.md`）：Codex 的 TUI、IDE 扩展与桌面 App 都是一个 app-server 进程的客户端，一个进程承载多个 thread，各有 cwd；Claude Code 一个进程一个会话，桌面端靠多进程并行。

约束：
- 工作区工具在 `AsterpathBuilder` 上注册并在构造时绑定根目录；`SessionSpec` 只按名字从注册表选工具。工具调用的 `ToolContext` 带会话 id。
- CLI 已有配置解析与会话组装（工作区工具、`AGENTS.md`、技能、宿主元数据），都写在二进制里，并以进程当前目录为工作根。

## Decision

- **进程内托管**：Tauri 后端常驻一个 `Asterpath`，承载所有项目的会话；前端经 Tauri 命令与 Channel 通信。命令按 thread / turn / item 语义命名，以后可抽成独立 app-server，不现在设计线协议。
- **项目 = 工作根**：侧栏按工作根分组会话；每个会话的工具、沙箱与环境由会话环境适配器按元数据解析（ADR-0037）。
- **复用宿主共用层**：依赖 `asterpath-host`（ADR-0040），读同一份 `config.toml`、同一个数据库。
- **会话由后台任务认领并驱动到空闲**（ADR-0036）：视图经 `watch` 订阅（ADR-0038）；被 CLI 占用的会话只读显示。
- **运行中输入即引导**：不另设下一回合队列。
- **「本会话允许」是宿主审批策略的内存状态**，不落日志，重启失效。
- **前端不生成类型**：手写 TS 类型，忽略未知 `AgentEvent` 类型；protocol 不为桌面端加派生。

## Consequences

- 桌面端与 CLI 的会话互通，可同时运行。
- 会话改名、归档/删除、按回合分叉、按元数据查询、上下文窗口大小是门面缺口，单独记 issue。

## Considered alternatives

- **独立 app-server 守护进程 + JSON-RPC**（Codex）：要先设计线协议；进程内托管足以满足现在的需要。
- **宿主路由工具按会话转发到工作根**：子会话不继承元数据，沙箱不随之路由（见 ADR-0037）。
- **依赖 `asterpath-cli` 的 lib target**：带进终端依赖（见 ADR-0040）。
- **一个窗口一个工作根，切换时重建 `Asterpath`**：切换要停掉在跑的回合，也不能多项目并行。
- **桌面端独立数据库**：让 CLI 与桌面端数据不通，违背两者只是形态不同。
- **给 protocol 加 `ts-rs` / `specta` 派生**：为宿主改合约 crate，违背参考宿主的定位。
