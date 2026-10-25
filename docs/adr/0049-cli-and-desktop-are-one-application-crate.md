# CLI 与桌面端是一个应用 crate 的两个入口；组装与配置是它的私有模块

**Status**: accepted (2026-10-26)
**部分取代**: [ADR-0048](0048-no-host-tier-workspace-and-config-are-facade-features.md) 的「内容并入门面，作为 `workspace` / `config` feature」及其稳定面后果；[ADR-0035](0035-desktop-reference-host.md) 的「命令层与窗口分开构建：workspace 成员 `asterpath-desktop`」
**调研**: [pi-and-pi-durable.md](../research/pi-and-pi-durable.md)、[desktop-codex.md](../research/desktop-codex.md)、[desktop-claude-code.md](../research/desktop-claude-code.md)、[mastra.md](../research/mastra.md)

## Context

ADR-0048 去掉了宿主这一层，把 `asterpath-host` 的内容并入门面，作为 `workspace` 与 `config` feature。这些内容是：按 `cwd` 元数据定工作根、读工作根下的 `AGENTS.md`、`.asterpath/skills` 等技能根、按名字找会话、续会话的工具规则、`config.toml`。它们是本地编码智能体这个产品的约定，使用方只有 CLI 与桌面端；web 平台有自己的工作区、用户与配置（ADR-0046），不会用。放进门面，等于把两个本地应用的约定写进稳定面。

文件系统、沙箱、技能、MCP 这些通用能力已经是适配器：`asterpath-tools`、`asterpath-sandbox-linux`、`asterpath-skills`、`asterpath-mcp`。Mastra 的 `Workspace` 对应的是这一部分，不含 `cwd`、`AGENTS.md` 或配置文件约定。

参照产品里，这类约定都在产品自己的包里，GUI 去用它，而不是另抽一层：

- pi：`pi-coding-agent` 既是 CLI 又是 SDK，按 `cwd` 加载 `AGENTS.md` 与技能，TUI、print、JSON、RPC 都是同一个包的模式。
- Codex：桌面 App 与 IDE 扩展都是 CLI 内 app-server 的客户端。
- Claude Code：桌面端与 Agent SDK 拉起 CLI 子进程。

由 Tauri 二进制兼作 CLI 不行：Linux 上它动态链接 webkit2gtk 与 gtk，CLI 常在 SSH、服务器、CI、容器里运行，没有这些库就启动不了。

## Decision

- **CLI 与桌面端合并为一个应用 crate**：工作根组装、`AGENTS.md`、技能根、MCP 加载、会话环境适配器、`cwd` / `name` 元数据约定、`config.toml` 解析，以及桌面端的命令层，都是它的私有模块。
- **两个入口**：CLI 二进制不依赖 Tauri；桌面窗口二进制在非默认 feature `tauri` 之后。默认构建与测试不需要 GUI 开发包（ADR-0035 不变）。
- **门面不加 `workspace` / `config` feature**：门面与适配器保持通用；web 平台用门面加适配器，自己写会话环境适配器。
- **进程模型不变**：CLI 与桌面端仍各自内嵌门面、共用一个库，靠会话树属主防双写（ADR-0035、0036）。
- ADR-0048 的其余决定不变：删除 `asterpath-host`，依赖角色没有 host，「宿主」改称「应用」。

## Consequences

- 组装与配置不进稳定面，随应用一起改；`config.toml` 的形状是这个应用的约定，不是门面契约。
- 桌面端与 CLI 共用代码不再经过一个公开 crate；改动约定只需改一处。
- 以后要让桌面端连 CLI 的 app-server（Codex 式，#156），是给这个 crate 加一个模式；桌面命令层已按会话 / 回合 / 条目的语义命名（ADR-0035）。
- 其他想复用这套本地约定的应用要么依赖这个应用 crate，要么照着写；现在没有这样的使用方。

## Considered alternatives

- **并入门面 feature（ADR-0048）**：本地产品约定进了稳定面，web 用不上。
- **Tauri 单一二进制兼作 CLI**：无图形环境里 CLI 启动不了。
- **桌面端连 CLI 的 app-server（Codex 式）**：要先设计线协议与版本兼容、进程拉起与重连；能得到远程机器、IDE 扩展与进程隔离，现在不需要。上面的做法不妨碍以后再走这一步。
- **保留两个 crate，组装放在 CLI 的 lib 里、桌面端依赖它**：桌面端依赖一个以 CLI 命名、带 clap 的 crate，两份 crate 边界只为一份代码。
