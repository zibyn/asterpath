# 桌面参考宿主：一个进程托管所有项目，与 CLI 共用配置与数据库

**Status**: accepted (2026-10-25)
**依赖**: [ADR-0036](0036-per-session-tree-ownership.md)–[ADR-0040](0040-host-kit-shared-by-reference-hosts.md)
**调研**: [desktop-codex.md](../research/desktop-codex.md)、[desktop-claude-code.md](../research/desktop-claude-code.md)
**来源**: #140、#150

## Context

仓库要提供一个 Tauri 2 桌面客户端，作为第二个参考宿主：只经 `asterpath` 门面公开 API 写成，门面不够时记录缺口，不为它开内核后门。CLI 与桌面端只是形态不同，数据必须互通，并且可以同时运行。

参考对象：Codex 的 TUI、IDE 扩展与桌面 App 都是一个 app-server 进程的客户端，一个进程承载多个 thread，各有 cwd，侧栏按项目分组；Claude Code 一个进程一个会话，桌面端靠多进程并行。

已有的机制：
- 写者独占以会话树为单位，打开得到只读句柄，驱动前显式认领；多个进程共用一个库（ADR-0036）。
- 会话的工具、沙箱与子进程变量由宿主的会话环境适配器按宿主元数据解析，一个 `Asterpath` 可以承载不同工作根的会话（ADR-0037）。
- 只读句柄可以 `watch`：重放已提交的记录，再推送新提交；流式增量只在属主进程内扇出（ADR-0038）。
- 会话目录可按元数据过滤；审批端点按会话树注册（ADR-0039）。
- 按工作根组装、`config.toml` 解析与元数据键在 `asterpath-host`，CLI 已经改用它（ADR-0040）。

Tauri 窗口要链接 webkit2gtk / gtk，编译需要这些库的开发包；仓库的其他成员都不需要。

## Decision

- **进程内托管**：Tauri 后端常驻一个 `Asterpath`，承载所有项目的会话；前端经 Tauri 命令（以及以后的 Channel）通信。命令按会话 / 回合 / 条目的语义命名，以后可抽成独立 app-server，现在不设计线协议。
- **共用 `asterpath-host`**：按 CLI 的规则读同一份 `config.toml`、同一个数据库，用它的会话环境适配器与元数据键（`cwd`、`name`）。桌面端不依赖 CLI。
- **项目 = 工作根**：侧栏列出目录里根会话 `cwd` 出现过的工作根，加上用户用目录选择框添加的工作根。添加的列表是桌面端自己的状态文件，不进数据库；会话列表用目录的元数据过滤取一个工作根的根会话。CLI 在某目录建的会话因此直接出现在对应项目下。
- **会话由后台任务认领并驱动到空闲**（ADR-0036）：视图经 `watch` 订阅（ADR-0038），切换视图不中断驱动；被其他进程占用的会话只读显示。审批端点为本进程驱动的每棵会话树注册（ADR-0039）。
- **运行中输入即引导**：不另设下一回合队列。
- **「本会话允许」是宿主审批策略的内存状态**，不落日志，重启失效。
- **前端不生成类型**：手写 TS 类型，忽略未知 `AgentEvent` 类型；protocol 不为桌面端加派生。
- **命令层与窗口分开构建**：workspace 成员 `asterpath-desktop`（`apps/desktop/src-tauri`）的库是命令层，不依赖 Tauri，用 `asterpath-testing` 的假 provider 做集成测试；窗口二进制在非默认 feature `tauri` 之后。默认的 workspace 构建与测试不需要 GUI 开发包。只保证 Linux。

## Consequences

- 桌面端与 CLI 的会话互通，可同时运行，同一会话一端驱动、另一端只读观察。
- 窗口层很薄，只把命令转给命令层；命令层的行为在没有图形环境的机器上也能测试。窗口本身要在装了 webkit2gtk / gtk 开发包的机器上用 `--features tauri` 编译。
- 一个进程里只有一个会话环境适配器：
  - `WorkRoot.new` 是适配器级的开关，同一进程里新建与续接的会话不能各用各的工具规则；
  - MCP 服务按工作根加载，而 builder 上的工具是全局的，多工作根进程还没有按会话的 MCP。
  这两点要在桌面端建会话之前解决。
- 目录只能列会话，没有「不同工作根」的查询；列项目要读全部根会话。目录变化不可订阅，侧栏靠重新查询刷新。
- 会话改名、归档/删除、按回合分叉、上下文窗口大小是门面缺口，单独记 issue。

## Considered alternatives

- **独立 app-server 守护进程 + JSON-RPC**（Codex）：要先设计线协议；进程内托管足以满足现在的需要。
- **一个窗口一个工作根，切换时重建 `Asterpath`**：切换要停掉在跑的回合，也不能多项目并行。
- **桌面端独立数据库**：CLI 与桌面端数据不通，违背两者只是形态不同。
- **依赖 `asterpath-cli` 的 lib target**：带进终端依赖（见 ADR-0040）。
- **项目列表存进数据库**：目录只有会话；没有会话的项目是桌面端的界面状态，web 宿主有自己的项目概念。
- **只列添加过的项目**：CLI 建的会话要先手动添加目录才看得见。
- **Tauri 作为默认依赖**：每台跑 `cargo test --workspace` 的机器都要装 GUI 开发包。
- **桌面端放在独立的 cargo workspace**：锁文件分裂，依赖白名单检查覆盖不到它。
- **给 protocol 加 `ts-rs` / `specta` 派生**：为宿主改合约 crate，违背参考宿主的定位。
