# 沙箱是 spawn 点变换，策略归宿主，v1 只定接缝

**Status**: accepted (2026-10-22)
**来源**: [#21 沙箱扩展模块](https://github.com/zibyn/asterpath/issues/21)
**前置**: [ADR-0007](0007-approval-and-sandbox-layers.md)、[ADR-0013](0013-plugin-boundary.md)、[ADR-0015](0015-packaging-and-stability.md)、[ADR-0017](0017-fault-injection-and-test-tooling.md)

## Context

[#7](../design/tools.md) 已把审批定为框架管线，把隔离与内容安全留给扩展模块/宿主，并明确「内核只建模 `Provenance`，真正的安全边界是进程边界」。剩下的是接缝形状、策略模型与调用点。

三个事实约束答案：

- **工具是 Rust trait object**，`invoke` 是不透明 future。装饰器无法替工具内部的 `Command::spawn` 加隔离，只能整次拒绝或审计；纯进程内工具根本没有进程边界。
- **只有进程启动点能被 OS 约束**：plugin-host 启动 MCP stdio server / `CustomStdio`，宿主自带的 shell 类工具每次起子进程。MCP HTTP 没有本地进程。
- **manifest 不是沙箱**（[#16](../design/plugins.md)）：让插件清单声明自己的约束档位，等于让被约束方选择约束。

## Decision

- **接缝是 spawn 点的纯变换**：合约 crate `asterpath-sandbox` 提供 `SandboxProvider::confine(SpawnRequest) -> ConfinedSpawn`。输入完整程序/参数/cwd/env/已解析策略，输出实际执行的命令与环境。provider 无状态、无 session、不返回 `Command`。
- **合约与后端分离**：合约 crate 零依赖；Linux（bwrap）与 macOS（Seatbelt）后端各自独立。v1 只定接缝与策略，不建 crate、不写后端、不进 workspace 验收。
- **只有文件系统三档策略**：`ReadOnly` / `WorkspaceWrite { writable_roots }` / `FullAccess`。网络、进程可见性、读白名单不在 v1 schema；请求不存在的维度在类型上不可表达。文件系统沙箱不等于网络隔离。
- **策略由宿主解析**：调用点静态 > session > 部署默认；模型参数不能放宽；不落日志、不进 `ConfigSnapshot`，与审批策略同构。恢复使用当前宿主装配，需要粘性的宿主自行保存并校验。
- **默认 fail closed**：后端不能完整强制就返回 `Unsupported`，不启动进程；`Partial { unenforced }` 只能由宿主的 `allow_partial` 部署配置显式接受。启动前策略拒绝 → `ToolError::Denied`；后端故障 → `Internal`；运行期 OS 拒绝框架不分类。
- **审批与沙箱正交**：`Allow` 不放宽沙箱；v1 不做 escalation；`ApprovalRequest` 不变。
- **插件清单不声明沙箱**；plugin-host 每次进程启动前调用 provider。HTTP MCP 无进程，不在沙箱范围。
- **子会话继承父的已解析策略、只能收窄**；机制是宿主解析器按 `SessionCatalog` 父边查找。

## Consequences

- core 零改动；protocol 只增加 `ToolError::Denied`，让沙箱的启动前拒绝有模型可见分类。
- 进程内工具获得不了 OS 隔离，这是能力边界而不是缺陷；宿主若需要硬边界，必须用进程边界（插件/子进程）表达。
- 策略不落日志意味着框架侧没有 mode 的审计记录；宿主需要时自行持久化，并承担恢复时边界变化的判断。
- 受限策略下没有 provider = 启动/加载失败，不会静默裸跑。
- 文件系统约束不附带网络保证；`ReadOnly` 的工具仍可联网，宿主不能把沙箱当作网络策略。
- v1 只交付 spec 与 ADR；`asterpath-sandbox` 继续是预留名，实现与后端属于扩展工作。

## Considered alternatives

- **`Tool` 装饰器做隔离**：实现简单，但 `invoke` 不透明，装饰器无法约束工具内部的 spawn，只能拒绝或审计；而且纯进程内工具无法被 OS 沙箱。拒绝，装饰器只保留为宿主自己的审计/拒绝便利。
- **把沙箱类型放进 core/protocol**：少一个 contract crate，但违反「内核不引入沙箱类型」，并让 Tier 1 稳定面承担平台细节。拒绝。
- **单一 `asterpath-sandbox` crate 同时装合约与后端**：少一个 crate，但合约 crate 会带上平台依赖与 feature，违背 ADR-0001/0015 的合约无 feature 约定。拒绝。
- **策略进 `ConfigSnapshot` 或新 session 事件**：恢复时更粘，但内核要持久化宿主策略、schema 要跟着平台演进；审批策略同样不持久化，保持同构。拒绝，粘性归宿主。
- **v1 建模网络/进程可见性**：更接近 Codex 的完整沙箱，但参考后端在 v1 不实现，schema 会先于强制力存在，等于允许谎报。拒绝，出界。
- **plugin manifest 声明 sandbox 档位**：方便按插件定制，但让被约束方选择约束，且 manifest 只是宿主提供的数据。拒绝，per-plugin 覆盖只能是宿主配置。
- **自动 escalation（拒绝 → 审批 → 更宽策略重试）**：体验好，但引入跨层状态、重试语义与新的审批交互，v1 不需要。拒绝。
- **没有后端时降级为裸跑**：可用性高，但失败方向错误；受限策略必须 fail closed。
