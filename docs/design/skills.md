# 技能：宿主解析的指令包

> 来源：[#63](https://github.com/zibyn/asterpath/issues/63)（2026-10-23 设计）。本文件定义技能包格式、发现与优先级、会话集成、`load_skill` 工具语义、安全边界与验收。
>
> 对齐：[#2 模块契约](./module-contracts.md)、[#3 事件日志](./event-log-recovery.md)、[#6 agent loop](./agent-loop.md)、[#7 工具契约](./tools.md)、[#9 压缩](./compaction.md)、[#15 打包](./packaging.md)、[#16 插件边界](./plugins.md)。
>
> 相关 ADR：[0021 技能是宿主解析的指令包](../adr/0021-skills-are-host-resolved-instruction-packages.md)。
>
> 参考实现（2026-10-23 核对）：Claude Code、Codex、OpenCode、Pi、DeepSeek Harness 都把技能实现为目录 + `SKILL.md` + YAML frontmatter，先列元数据、正文按需加载。本设计取五家的交集，不引入任何一家的私有执行语义。

## 1. 决策摘要

- **技能是指令包，不是能力**。它是一个目录下的 `SKILL.md`；对模型只表现为“可选加载的指令文本”，不注册工具、不改变审批或沙箱、不参与能力发现。
- **宿主拥有策略，crate 拥有机制**。宿主决定扫描根、启用与诊断；`asterpath-skills` 只提供解析、索引渲染与 `load_skill` 工具。内核没有新 seam。
- **渐进披露**。system prompt 只放索引（名称 + 描述）；正文由模型调用 `load_skill(name)` 读取。工具结果照常入日志，恢复按日志重放。
- **索引冻结、正文实时**。索引随 `SessionCreated.config.system` 冻结；正文每次调用读当前磁盘。文件变化不影响已有会话的索引；已加载的内容以日志为准。
- **技能不授予权限**。`allowed-tools`、`model`、`hooks` 等字段不执行；技能内容不能改 system、审批、沙箱或工具集。
- **无日志 schema 变更**。不新增事件、span 或 CLI 管理命令；索引是既有 `system` 的一部分，加载是普通工具调用。
- **M6、Tier 2**。新增 `asterpath-skills` crate 与参考 CLI 接入；日志 schema 与 v1 稳定面不变。

## 2. 术语与边界

| 词 | 含义 |
| --- | --- |
| 技能（Skill） | 一段按需加载给模型的指令文本，带名称与描述用于发现和选择；不构成能力，也不授予权限。 |
| 技能包（Skill Package） | 一个可被发现的技能单元：一个目录，包含 `SKILL.md` 与可选资源文件。 |
| 技能索引（Skill Index） | 会话开始时写入 system 的技能名称与描述清单；会话期间固定。 |
| 技能根（Skill Root） | 宿主配置的、包含若干技能包的搜索位置；优先级由宿主决定。 |

边界：

- **不是插件**。插件是外部能力来源，M5 只有 `Tools` 能力；技能是本地指令文本，不贡献能力，也不进入插件清单。
- **不是工具**。工具是可执行副作用入口；技能只影响模型看到什么文本。
- **不是 command**。M6 没有命令 seam；用户只能在 prompt 里点名，由模型决定是否加载。
- **不是 system prompt 的替代**。宿主自己的 system 仍然先写；技能索引追加在后。
- **不是子智能体的配置单位**。技能随会话的 system 与工具快照继承，不为子会话单独解析。

## 3. 技能包格式

目录形状：

```text
<root>/
  my-skill/
    SKILL.md
    references/...     # M6 不读取
    scripts/...        # M6 不执行
```

`SKILL.md` 由 frontmatter 与正文组成：

```markdown
---
name: my-skill          # 可选；缺省 = 目录名；提供时必须等于目录名
description: 一句话说明这个技能何时有用。  # 必填
---

# My Skill

正文……
```

规则：

- `description` 必填、非空（trim 后）、≤1024 字符。它是模型选择技能的唯一路由文本。
- `name` 可选；缺省等于目录名。提供时必须等于目录名，否则该技能无效。名称必须匹配 `^[a-z0-9]+(-[a-z0-9]+)*$`，长度 1–64。
- `name` 与 `description` 必须是 YAML 字符串；其他标量类型（数字、布尔、null）按无效处理。
- 未知 frontmatter 字段忽略。`allowed-tools`、`disallowed-tools`、`model`、`hooks`、`context`、`agent` 被识别但不执行，`config check` 会逐条提示“Asterpath 不执行该字段”。
- frontmatter 必须以文件开头的 `---` 开始；缺失 frontmatter、缺失 `description`、YAML 解析失败都是无效技能。
- 整个 `SKILL.md` ≤64 KiB（UTF-8 字节）；frontmatter 在解析前单独限制为 ≤16 KiB。超限的技能无效，避免交给 YAML 解析器一个无界文档。
- 正文原样返回给模型；索引里的 `description` 把换行和控制字符压平成一行。
- YAML 使用 `saphyr`（YAML 1.2，维护中）解析。`serde_yml` 已废弃，不使用。
- 无效技能只影响它自己：跳过并产生诊断，其他技能和会话继续。没有部分注册，也没有 last-write-wins。

## 4. 发现与优先级

| 优先级 | 根 | 说明 |
| --- | --- | --- |
| 1（最高） | `[skills] paths` 中的每个路径 | 显式；相对路径按进程 cwd 解析 |
| 2 | `<cwd>/.asterpath/skills` | 项目原生 |
| 3 | `<cwd>/.agents/skills` | 跨产品约定（Codex / OpenCode / Pi / DeepSeek） |
| 4 | `$XDG_CONFIG_HOME/asterpath/skills`（回退 `~/.config/asterpath/skills`） | 用户原生 |
| 5（最低） | `~/.agents/skills` | 跨产品约定 |

- **只扫一层**：技能是 `<root>/<name>/SKILL.md`；不递归 `**/SKILL.md`。根下的散落 `.md` 文件不视为技能。
- **跟随符号链接**；同一规范路径被多个根发现时按最高优先级去重。
- **跨层同名**：高优先级胜出，低优先级被遮蔽；`config check` 报告被遮蔽的技能。
- **同层同名**：两个都跳过并诊断，不做“先到先得”。
- **每个新 session 创建时扫描一次**。没有热加载：新技能对下一个新 session 生效；已有 session 的索引不变，但 `load_skill` 读当前磁盘。
- **开关**：`[skills] enabled = false` 或 `run --no-skills` 只影响新 session（不扫描、不渲染索引、不注册工具）。`resume` 总是注册 `load_skill`，索引来自 `SessionCreated.config.system`（见 §5）。
- **`.claude/skills` 不自动扫描**。需要时写进 `[skills] paths`；格式兼容不等于替别的产品做发现。
- **项目信任**：M6 不提供信任提示；项目技能默认加载，宿主用 `enabled` / `--no-skills` 退出。需要信任门的宿主应在组装 `system` 之前自行拦截。

## 5. 会话集成

### 5.1 索引

会话创建时，宿主把索引作为一个文本块追加到自己的 system parts 之后：

```text
## Skills

Call `load_skill` with a skill name when the task matches its description.

- code-review: Review a diff for correctness and style.
- deploy: Release the current branch to staging.
```

- 顺序确定：按根优先级，层内按 name 升序。
- 总量预算 8,000 字符。按优先级顺序填充；放不下的条目及所有更低优先级条目整条省略，并产生诊断。单条 `description` 仍受 1024 字符上限约束，不做二次截断。
- 没有技能时不追加任何块；宿主自己的 system 原样保留。
- 索引是普通 `Part::Text`，进入 `ConfigSnapshot.system`；不新增字段，也不改变日志 schema。
- `ConfigSnapshot.tools` 同时包含 `load_skill` 的 `ToolDescriptor`。

### 5.2 恢复与子会话

- **恢复**：`resume` 不重新扫描索引，使用日志里的 `ConfigSnapshot.system`；`load_skill` 仍按当前根解析。技能文件消失 → 工具返回 `NotFound`；文件变化 → 返回新内容并作为新的工具结果入日志。已加载过的正文以日志为准。
- **子会话**：fresh 子继承父的 system 与工具快照，因此索引与 `load_skill` 一起继承。模型通过 spawn 参数收窄工具集时可能留下索引却没有 `load_skill`；这是宿主选择收窄的后果，M6 不做特殊处理。
- **compaction**：索引在 system，不参与压缩；技能正文是普通工具结果，可能被摘要替换，模型需要时可以再次 `load_skill`。

## 6. `load_skill` 工具

```rust
pub const LOAD_SKILL_TOOL_NAME: &str = "load_skill";

// 描述：Load the full instructions for a skill by name.
// schema：{ "type": "object", "properties": { "name": { "type": "string" } },
//          "required": ["name"], "additionalProperties": false }
// parallelism: Parallel
// replay: Safe
// side_effect: ReadOnly
```

行为：

- 名称按 §3 的正则校验；非法 → `ToolError::InvalidArgs`。
- 按 §4 的根与优先级解析；不存在 → `ToolError::NotFound`。
- 读取 `<root>/<name>/SKILL.md`；超过 64 KiB 或不是 UTF-8 → `ToolError::Internal`。
- 返回正文原文；不加包装、不做摘要。
- `ReadOnly` + 进程内工具：默认审批策略直接允许，不打开审批端点；不经过沙箱。
- 工具按当前根解析任何合法名称，**不校验该名字是否在本 session 的索引里**。索引是发现面，不是访问控制列表。
- `Safe`：崩溃恢复时未闭合的调用可以重跑；已经写入的 `ToolResult` 直接重放，不再读盘。

日志与追踪：

- 使用既有的 `ToolCallRequested` / `ToolResult`；不新增事件。
- 追踪是普通 tool span；不新增 span kind。
- 不产生用量。

## 7. 安全边界

- **技能是不可信的仓库内容**。加载技能等于让模型读一段仓库里的文本；提示注入是固有风险。缓解手段是：技能没有任何权限，只能影响模型看到什么。
- **宿主仍是唯一权威**：工具集、审批、沙箱、system 只能由宿主改变。技能不能提权、不能绕过审批、不能放宽沙箱。
- **`allowed-tools` 等字段不执行**。不要把它们当作限制或预批准；Claude Code 的同类字段是“预批准”而非限制，已有越权问题（anthropics/claude-code#89195）。`config check` 提示这些字段被忽略。
- **资源与脚本不执行**：M6 不读 `references/`、不跑 `scripts/`。技能可以提到它们，但模型没有对应工具（除非宿主另外提供）。
- **路径**：名称正则排除了 `/` 与 `.`，只读 `<root>/<name>/SKILL.md`；根在发现时规范化。不存在任意路径读取。
- **YAML 解析是信任边界**：frontmatter 先限 16 KiB 再交给 `saphyr`；只读取顶层 `name` / `description` 标量；不执行标签、锚点或自定义构造。解析失败即跳过该技能。
- **没有签名、registry 或信任提示**；项目技能默认加载，`enabled = false` / `--no-skills` 是退出开关。需要更强信任模型的宿主应在组装前自行判断。
- **诊断不包含正文**：只报路径、技能名与原因，避免把技能内容里的秘密带进 stderr 或日志。

## 8. 宿主接口

### 8.1 crate

`asterpath-skills`（M6，Tier 2 module）：

- workspace 依赖只有 `asterpath-protocol`；外部依赖 `saphyr`。
- 不实现 `Module`，也不注册服务：宿主在组装期调用它，把工具放进 `AsterpathConfig.tools`。

形状（实现可按需调整，公开面保持最小）：

```rust
pub struct SkillRoot {
    pub path: PathBuf,
    pub label: SkillRootLabel,   // Explicit | ProjectNative | ProjectAgents | UserNative | UserAgents
}

pub struct SkillSummary {
    pub name: String,
    pub description: String,
}

pub struct SkillDiagnostic {
    pub root: PathBuf,
    pub skill: Option<String>,
    pub kind: SkillDiagnosticKind,
    pub message: String,
}

pub struct Discovery {
    pub skills: Vec<SkillSummary>,
    pub diagnostics: Vec<SkillDiagnostic>,
}

pub fn discover(roots: &[SkillRoot]) -> Discovery;
pub fn render_index(skills: &[SkillSummary], budget: usize) -> (Option<Part>, Vec<SkillDiagnostic>);

pub struct LoadSkillTool { /* roots */ }
impl Tool for LoadSkillTool { /* ... */ }

pub const LOAD_SKILL_TOOL_NAME: &str = "load_skill";
```

### 8.2 参考 CLI

```toml
[skills]
enabled = true      # 缺省 true
paths = []          # 显式根；相对路径按进程 cwd 解析
```

- `run`：若启用，扫描 → 渲染索引 → 放进 `SessionSpec.system` → 注册 `LoadSkillTool`；无效技能警告到 stderr。
- `run --no-skills`：覆盖 `enabled`，本次新 session 不扫描、不注册工具。
- `resume`：不重新扫描索引；总是注册 `LoadSkillTool`（索引来自快照）。
- `config check`：扫描并打印诊断，包括被忽略的权限字段和被遮蔽/超预算的技能。

## 9. 失败与诊断

| 诊断 | 触发 | 处理 |
| --- | --- | --- |
| `FrontmatterMissing` | 文件不以 `---` 开始 | 跳过该技能 |
| `FrontmatterInvalid` | YAML 解析失败 | 跳过该技能 |
| `DescriptionMissing` | `description` 缺失或为空 | 跳过该技能 |
| `FieldTypeInvalid` | `name` / `description` 不是字符串 | 跳过该技能 |
| `NameInvalid` | 名称不匹配正则或超长 | 跳过该技能 |
| `NameMismatch` | frontmatter `name` 与目录名不同 | 跳过该技能 |
| `TooLarge` | `SKILL.md` >64 KiB 或 frontmatter >16 KiB | 跳过该技能 |
| `DuplicateInLayer` | 同一根内同名 | 两个都跳过 |
| `Shadowed` | 跨层同名 | 低优先级被遮蔽，报告 |
| `IndexBudgetExceeded` | 索引超过 8,000 字符 | 省略低优先级条目，报告 |
| `AuthorityFieldIgnored` | 出现 `allowed-tools` 等字段 | 正常加载，`config check` 提示不执行 |

- 诊断出口：`run` 的 stderr 与 `config check`。不新增事件、trace 类型或 `skill list` 子命令。
- 一个坏技能不影响其他技能，也不让 session 失败。
- 加载期错误是普通工具结果：`InvalidArgs` / `NotFound` / `Internal`。

## 10. 与相邻设计的边界

| 主题 | 归属 |
| --- | --- |
| 技能格式、发现、索引、`load_skill`、诊断 | **本文件（#63）** |
| 插件清单、外部能力、MCP 传输 | [plugins.md](./plugins.md) / [mcp.md](./mcp.md)；M6 不给插件增加 `Skills` 能力 |
| 工具契约、审批、并行调度 | [tools.md](./tools.md)；技能不新增工具语义，`load_skill` 是普通 `Tool` |
| system、`ConfigSnapshot`、工具快照 | [packaging.md](./packaging.md) / [protocol.md](./protocol.md)；只使用既有字段 |
| 事件日志、恢复、投影 | [event-log-recovery.md](./event-log-recovery.md)；不新增事件 |
| 压缩 | [compaction.md](./compaction.md)；索引不进历史，正文是普通工具结果 |
| 子智能体 | [subagents.md](./subagents.md)；继承 system 与工具快照 |
| 追踪、用量 | [tracing.md](./tracing.md) / [usage.md](./usage.md)；普通 tool span，无用量 |
| crate 打包、稳定层 | [packaging.md](./packaging.md)；M6、Tier 2 |

## 11. 验收场景

| # | 场景 | 断言 |
| --- | --- | --- |
| S1 | 项目/用户/显式根同名的技能 | 高优先级胜出；同层重名两个都跳过并诊断；其他技能不受影响 |
| S2 | 无效技能：缺 `description`、`name` 不匹配、正文超 64 KiB、未知字段 | 前三个跳过并诊断；未知字段正常加载 |
| S3 | 创建 session | 索引进入 `SessionCreated.config.system`；工具集含 `load_skill`；之后改文件不影响该 session 的索引 |
| S4 | 模型调用 `load_skill` | 返回正文；日志出现 `ReadOnly` + `Safe` 的调用与结果；不触发审批 |
| S5 | 崩溃恢复 | 已加载正文来自日志；未闭合调用按 `Safe` 重跑 |
| S6 | 文件在 resume 前被删除 | `load_skill` 返回 `NotFound`；session 不失败 |
| S7 | `run --no-skills` / `enabled = false` | 新 session 无索引、无工具；resume 的已有 session 仍可加载 |
| S8 | fresh 子智能体 | 继承 system 与工具快照，可 `load_skill`；父收窄工具集后子不可用 |
| S9 | 索引超过 8,000 字符 | 按优先级省略低优先级条目并诊断；高优先级条目完整 |
| S10 | 省略 `name` / `name` 与目录不符 | 省略时用目录名正常加载；不符时跳过并诊断 |

## 12. M6 出界

- 用户调用（`/skill`、`run --skill`）与命令 seam。
- 读取 `references/` / `assets/`，执行 `scripts/`。
- 热加载、运行期增删技能。
- 插件打包技能、远程 registry、安装/更新。
- per-skill 工具限制、预批准、审批策略、模型覆盖、`context: fork`、hooks。
- 把正文冻结进日志；M6 依赖工具结果作为持久记录。
- 技能级版本字段；版本随宿主或包管理。
- 递归发现、自动扫描 `.claude/skills`。
- 可配置的索引预算或单条描述上限之外的调参。
