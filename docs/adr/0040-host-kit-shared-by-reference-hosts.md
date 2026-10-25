# 宿主共用层：组装约定从 CLI 下沉为 `asterpath-host`

**Status**: accepted (2026-10-25)
**调研**: [kernel-host-boundary-audit.md](../research/kernel-host-boundary-audit.md)

## Context

CLI、桌面与 web 是同一内核的不同形态，会话要在它们之间互通。决定能否互通的约定——按工作根组装工具、`AGENTS.md`、技能、MCP 加载、宿主元数据键、续会话时的工具规则——今天只写在 CLI 二进制里（`crates/asterpath-cli/src/session.rs`）。桌面端若依赖 CLI 的 lib target，会把 clap、终端输出、退出码一起带进来；web 用不上配置文件与环境变量解析那一半。依赖规则允许 app 依赖任何成员，拦不住这种耦合。

## Decision

- **新增 `asterpath-host` crate**，不是内核：依赖门面与适配器，不被它们依赖。依赖白名单里登记为新角色，只允许 app 依赖它。
- **两部分**：
  - **组装**：入参都是普通的值，实现 ADR-0037 的会话环境适配器与元数据约定；web 可以直接用。
  - **配置加载**（可选 feature）：`config.toml` 与环境变量解析，只有 CLI 与桌面端用。
- **CLI 改用它**，不再自带组装；桌面端依赖它，不依赖 CLI。

## Consequences

- 元数据键与组装规则只有一份，各形态的会话互通由同一份代码保证。
- `asterpath-host` 不进稳定面，与参考宿主一起演进。
- web 宿主可以只用组装部分，也可以完全自己组装，只要遵守同样的元数据约定。

## Considered alternatives

- **桌面依赖 `asterpath-cli` 的 lib target**：带进终端相关依赖，web 不可用。
- **组装约定进内核**：工作根、`AGENTS.md`、技能根位置都是宿主策略。
- **每个宿主各写一份**：约定漂移后会话不再互通。
