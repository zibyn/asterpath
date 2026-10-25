# 本地宿主与 web 集成分开：`asterpath-host` 只服务 CLI 与桌面端

**Status**: accepted (2026-10-25)；「`asterpath-host` 是本地参考宿主的共用层」及其 Consequences 取代 by [ADR-0048](0048-no-host-tier-workspace-and-config-are-facade-features.md)
**部分取代**: [ADR-0040](0040-host-kit-shared-by-reference-hosts.md) 中「web 可以直接用组装部分」与「配置加载是可选 feature」
**相关**: [ADR-0035](0035-desktop-reference-host.md)、[ADR-0036](0036-per-session-tree-ownership.md)、[ADR-0039](0039-multi-user-embedding-mechanisms.md)

## Context

ADR-0035、0036、0040 的 Context 以「CLI、桌面与 web 是同一内核的不同形态，数据必须互通」为前提，ADR-0040 据此让 `asterpath-host` 同时照顾 web：组装部分只收普通值、web 可以直接用，配置加载拆成可选 feature，模块注释写成三种形态可以互相续接会话。

实际是两类场景：

- **本地**：CLI 与桌面端下发给用户，在用户本机使用。两者读同一份 `config.toml`、同一个数据库，会话互通，可以同时运行。
- **web**：平台把 `asterpath` 门面作为 SDK 集成进自己的服务，经 web 给用户使用。工作区、用户、凭据、存储都是平台的概念，不读本机配置，也不与本地会话互通。

web 至今不是 `asterpath-host` 的使用方；`config` feature 只有 CLI 与桌面端两个使用方，两者都开启。

## Decision

- **`asterpath-host` 是本地参考宿主的共用层**：只为 CLI 与桌面端服务，`config.toml` 与环境变量解析是它的常规部分，不再是可选 feature。
- **web 只依赖门面**：自己实现会话环境适配器（ADR-0037）、审批策略与元数据约定；`cwd`、`name` 等元数据键只是本地宿主之间的约定，web 不需要遵守。
- **跨形态互通只限本地**：CLI 与桌面端之间互通；web 与本地之间不互通。
- **内核里哪些多进程、多用户机制保留**另见 [ADR-0047](0047-multi-user-and-multi-node-belong-to-the-platform.md)：多用户与多机部署归集成平台，内核只保证会话树单写者。

## Consequences

- 删除 `asterpath-host` 的 `config` feature，serde 与 toml 改为常规依赖；模块注释不再提 web。
- `asterpath-host` 的接口可以按本地需要演进（本地路径、`HOME`、XDG 目录），不必为 web 保持「只收普通值」。
- web 集成方没有参考组装代码可复用；需要时另写示例，不进 `asterpath-host`。

## Considered alternatives

- **保留现状，让 web 可以选用组装部分**：为一个不存在的使用方保留 feature 开关与约束。
- **web 与本地共用元数据键以便互通**：两类场景的用户与工作区不是同一个东西，互通没有使用场景。
