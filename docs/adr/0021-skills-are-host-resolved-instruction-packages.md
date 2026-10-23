# 技能是宿主解析的指令包，索引冻结、正文按需加载

**Status**: accepted (2026-10-23)
**来源**: [#63 M6: Skill 加载与支持](https://github.com/zibyn/asterpath/issues/63)
**前置**: [ADR-0001](0001-dependency-direction.md)、[ADR-0004](0004-log-schema-version-and-upcasting.md)、[ADR-0006](0006-agent-loop-execution-model.md)、[ADR-0013](0013-plugin-boundary.md)、[ADR-0015](0015-packaging-and-stability.md)

## Context

Claude Code、Codex、OpenCode、Pi、DeepSeek Harness 在 2026-10-23 都已把技能实现为“目录 + `SKILL.md` + YAML frontmatter”，并且都做渐进披露：先给模型名称与描述，正文按需加载。差异集中在调用面、权限字段和分发，而 Asterpath 的约束把它们大多排除在外：

- `ModelRequest.system` 来自 session 创建时持久化的 `ConfigSnapshot`，每个 step 只是 clone（`crates/asterpath-agent/src/lib.rs:1926`）；没有 prompt/context 注入 hook。
- 工具集同样在创建时快照；运行期注册只影响新 session。
- 外部插件目前只有 `Tools` 能力，没有 command 或 context 贡献（`crates/asterpath-plugin-host/src/manifest.rs:62`）。
- 内核不读文件，也没有文件系统策略；读文件是宿主职责。

需要决定：技能是什么、由谁发现与解析、如何进入模型上下文、恢复时以什么为准，以及技能能不能改变权限。

## Decision

- **技能是宿主解析的指令包，不是内核能力，也不是插件**。一个技能是一个目录下的 `SKILL.md`；对模型只表现为“可选加载的指令文本”，不注册工具、不改变审批或沙箱。
- **格式取五家的交集**：`description` 必填；`name` 可选、缺省等于目录名、提供则必须匹配。未知字段忽略；`allowed-tools`、`model`、`hooks` 等字段不执行，`config check` 提示被忽略。
- **发现是宿主策略**：显式 `[skills] paths` > 项目 `.asterpath/skills` > 项目 `.agents/skills` > 用户 `~/.config/asterpath/skills` > 用户 `~/.agents/skills`；每层只扫一级，跨层高优先级胜出，同层重名两个都跳过。`.claude/skills` 不自动扫描。
- **渐进披露**：创建 session 时把“名称 + 描述”索引追加进 `system`（8,000 字符预算），正文由模型调用 `load_skill(name)` 读取。索引随 `ConfigSnapshot` 冻结；正文每次读当前磁盘；工具结果入日志，恢复以日志为准。
- **`load_skill` 是普通工具**：`ReadOnly` + `Safe` + `Parallel`，不审批、不经沙箱；未知或缺失返回 `ToolError::NotFound`。索引是发现面，不是访问控制列表。
- **技能没有权限**：不能改工具集、审批、沙箱或 system；资源与脚本不读取、不执行。
- **实现是一个新的可选模块 crate**：`asterpath-skills`（M6、Tier 2），只依赖 `asterpath-protocol` 与外部 `saphyr`；宿主（参考 CLI）负责发现、索引与工具注册。内核无新 seam，日志 schema 不变。

## Consequences

- 内核与日志不动：没有新的 context-contribution seam，没有 `ConfigSnapshot` 字段，没有新事件；嵌入者复用 crate，自行决定根与开关。
- 索引成本有上界：8,000 字符，超出时低优先级技能被省略并诊断；单条描述 ≤1024 字符，正文 ≤64 KiB。
- 正文是实时的：技能文件在会话创建后被改/删，加载会看到新内容或 `NotFound`；已加载过的正文由工具结果精确保留。
- 技能是不可信输入：提示注入仍然可能，但没有权限意味着影响面被限制在“模型看到什么文本”。
- M6 没有用户调用、热加载、资源读取、脚本执行、插件打包与远程 registry；这些都是可后加的增量，不需要改本决策。
- `.agents/skills` 跟随了跨产品约定；`.claude/skills` 需要显式列入 `paths`。
- 新增 `saphyr` 依赖只落在 `asterpath-skills`；`serde_yml` 已废弃，不使用。

## Considered alternatives

- **创建时全量注入正文**：实现最短，但技能一多就吃满上下文，也无法在会话内按需选择；拒绝。
- **框架模块 + 新的 context-contribution seam + `ConfigSnapshot` 字段**：每个 session 自动生效，但要扩内核、改日志 schema，并把“读哪些目录”这种宿主策略塞进内核；拒绝。
- **把技能变成插件能力或映射 MCP prompts/resources**：会把两个正交的扩展机制耦合，MCP prompts 又是服务端内容，无法覆盖本地 `SKILL.md`；拒绝。
- **把正文也冻结进日志**：恢复最精确，但要新事件/字段与额外体积；工具结果已经记录了实际加载的正文，收益不足；拒绝。
- **`allowed-tools` 作为限制或预批准**：Claude Code 的同类字段是预批准而非限制，已有越权问题；技能内容影响权限会让不可信仓库文件进入审批边界；拒绝。
- **热加载或每个 step 重扫索引**：会改变前缀、破坏 reasoning 回放与恢复的一致性；M6 不需要；拒绝。
- **只改 CLI、不建 crate**：最小，但嵌入者要各自重写解析、优先级与索引；拒绝。
- **自写 frontmatter 解析器**：能避免依赖，但 YAML 边界多、不可信输入下更难审查；改用维护中的 `saphyr`（YAML 1.2），并在解析前限制 frontmatter 大小。
