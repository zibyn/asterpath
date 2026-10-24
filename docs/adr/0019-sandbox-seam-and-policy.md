# 沙箱是进程启动点的变换，策略归宿主，失败即关闭

**Status**: accepted (2026-10-22)；2026-10-24 整理
**来源**: [#21](https://github.com/zibyn/asterpath/issues/21)
**规范**: [sandbox.md](../design/sandbox.md)

## Context

审批管线不负责隔离（[ADR-0007](0007-approval-and-sandbox-layers.md)）。工具是 Rust trait object，`invoke` 是不透明 future：装饰器无法替工具内部的进程启动加隔离，纯进程内工具根本没有进程边界。只有进程启动点能被 OS 约束（MCP stdio server、宿主自带的 shell 类工具）。让被约束方（例如 MCP 配置）声明自己的约束档位，等于让它自己选约束。

## Decision

- **接缝是进程启动点的纯变换**：`SandboxProvider::confine` 输入完整的程序、参数、cwd、env 与已解析的策略，输出实际执行的命令与环境。provider 无状态、无会话。
- **合约在 protocol，平台后端各自是独立 crate**。
- **策略由宿主解析**：调用点、会话、部署默认三层，模型参数不能放宽；策略不落日志、不进会话配置快照，与审批策略同构。子会话只能收窄父的策略。
- **schema 不先于强制力**：策略类型只表达参考后端能真正强制的维度；请求不存在的维度在类型上不可表达。档位见 sandbox.md。
- **默认 fail closed**：后端不能完整强制就返回 `Unsupported`，不启动进程；部分强制只能由宿主部署配置显式接受。
- **审批与沙箱正交**：`Allow` 不放宽沙箱，不做自动 escalation。

## Consequences

- 进程内工具得不到 OS 隔离；需要硬边界的宿主必须用进程边界表达。
- 框架侧没有沙箱模式的审计记录；需要粘性的宿主自行持久化并承担恢复时边界变化的判断。
- 受限策略下没有 provider 就启动/加载失败，不会静默裸跑。
- 文件系统约束不附带网络保证。

## Considered alternatives

- **`Tool` 装饰器做隔离**：无法约束工具内部的进程启动。
- **单一 crate 同时装合约与后端**：合约带上平台依赖与 feature。
- **策略进会话快照或新事件**：内核要持久化宿主策略，schema 跟着平台演进。
- **由 MCP 配置声明沙箱档位**：被约束方选择约束。
- **没有后端时降级裸跑**：失败方向错误。
