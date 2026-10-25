# 没有宿主这一层：工作根组装与配置加载是门面的可选 feature

**Status**: accepted (2026-10-25)；「并入门面，作为 `workspace` / `config` feature」及其稳定面后果取代 by [ADR-0049](0049-cli-and-desktop-are-one-application-crate.md)
**取代**: [ADR-0040](0040-host-kit-shared-by-reference-hosts.md)；[ADR-0046](0046-local-hosts-and-web-sdk-do-not-share.md) 中「`asterpath-host` 是本地参考宿主的共用层」
**修订**: [ADR-0026](0026-engine-is-one-crate-only-adapter-traits-public.md) 的依赖角色
**调研**: [pi-and-pi-durable.md](../research/pi-and-pi-durable.md)、[mastra.md](../research/mastra.md)、[agno.md](../research/agno.md)、[llamaindex.md](../research/llamaindex.md)

## Context

仓库把「宿主」当作一层架构：依赖角色是 protocol / adapter / engine / host / app，`asterpath-host` 夹在门面与 CLI、桌面端之间，只许 app 依赖；文档里大量决定写成「内核给机制，宿主给策略」，并有宿主、参考宿主、宿主共用层、宿主元数据等术语。

参照框架都没有这一层，只区分框架与使用框架的应用：

- pi：`pi-coding-agent` 既是 CLI 也是 SDK，会话存储、工具、技能、`AGENTS.md`、设置文件都在里面，其他外壳直接用它。
- Mastra、Agno：工具、MCP、工作区、记忆在 core 包里，按需选用；server 层只放鉴权与 HTTP 路由。
- LlamaIndex：core 与 workflows 库之外是 server 与部署工具，没有夹在中间的组装层。

`asterpath-host` 的内容（按工作根组装工作区工具、`AGENTS.md`、技能索引与加载器、MCP 加载、会话环境适配器 `WorkRoot`、`config.toml` 解析）是编码智能体的通用能力，CLI、桌面端以及集成 SDK 的 web 平台都可以用；单设一层只多出一个依赖角色和一套术语。

## Decision

- **删除 `asterpath-host`**，内容并入门面 `asterpath`，作为可选 feature：
  - `workspace`：按工作根组装工作区工具、`AGENTS.md`、技能索引与加载器、MCP 加载、会话环境适配器、`cwd` / `name` 元数据约定；
  - `config`：`config.toml` 与环境变量解析，依赖 `workspace`。
  与 `anthropic`、`openai`、`sqlite` 一样由使用方选择。
- **依赖角色去掉 host**：protocol / adapter / engine / app / tool。门面可以依赖它 feature 化的适配器（tools、skills、mcp、sandbox 后端）。
- **「宿主」不再是架构概念**：使用框架的一方称「应用」；宿主元数据称「会话元数据」，参考宿主称「参考应用」。已有 ADR 中的「宿主」按「应用」读，不改原文。
- 框架与应用的分工原则不变：框架提供机制与可选的默认实现，应用决定用哪些、怎样配置（ADR-0007、0019、0047）。

## Consequences

- 门面的稳定面随之包含 `workspace` 与 `config` 的公开 API；`config.toml` 的形状成为门面的契约，改动按门面的兼容规则处理。
- CLI 与桌面端改为依赖门面并开启 `workspace`、`config`；web 平台可以只开 `workspace`，或都不开、自己实现会话环境适配器。
- `cargo xtask check-deps` 删去 host 角色与「只许 app 依赖」规则。
- 代码注释与类型里的 host 用语（「host metadata」、`HostError` 等）要随之改名。

## Considered alternatives

- **CLI 兼作 SDK（pi 式）**：桌面端与 web 依赖一个以 CLI 命名的 crate，clap 要藏在 feature 后面；能力本身不是 CLI 专属的。
- **拆回各适配器**：`WorkRoot` 进 tools、`AGENTS.md` 进 skills、配置解析留给各应用；组装约定散在几处，CLI 与桌面端仍要各自拼一遍。
- **保留 `asterpath-host`，只改名**：仍是夹在门面与应用之间的一层。
