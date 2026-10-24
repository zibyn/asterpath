# Codex CLI 与 Claude Code 命令面对比

> 完成日期：2026-10-24。用途：重设计参考 CLI（`asterpath-cli`），再由 CLI 倒推内核需要的机制。
>
> 一手来源：本机 `codex-cli 0.163.0` 与 `Claude Code 2.1.310` 的 `--help` 及各子命令 `--help`。交互模式内的斜杠命令不在 `--help` 里，相关条目按使用经验整理，标“未核实”。

## 0. 结论摘要

- 两者骨架相同：**无参数 = 交互会话；一个开关 = 无头运行；会话可续、可分叉；MCP 有管理子命令；配置文件 + 少量专用 flag**。差别在安全模型、配置覆盖方式和对外集成协议。
- **两者的命令面都在膨胀**：Claude Code 顶层约 70 个选项，Codex 也有 25 个子命令，大量是云端、后台、IDE、企业网关等产品功能。可借鉴的是它们共有的**核心子集**，不是全部表面。
- 对 Asterpath 更合适的组合：
  - **命令骨架与安全模型取 Codex**：`exec` 子命令、`resume`/`fork` 子命令、`-s` 沙箱 × `-a` 审批两条轴、`-c key=value` 通用覆盖。它和 ADR-0007 / 0019 的“审批层 + 沙箱层”一一对应，`[sandbox] mode` 已与它同名。
  - **无头输出协议取 Claude Code**：`--output-format text|json|stream-json`，加 `--input-format stream-json` 双向流。比 Codex 的 `--json`（只出不进）更完整，并且就是“宿主协议”的雏形。
  - **会话选择取两者交集**：`-c/--continue` 续最近一次；`-r/--resume [id]` 指定会话或进选择器。
- 现有 `inspect` / `trace` / `usage` 在两者中都没有顶层对应物；最接近的是 Codex 的 `codex debug prompt-input`（打印模型可见输入）和 Claude Code 交互内的 `/cost` `/context`（未核实）。

## 1. 入口与模式

| | Codex | Claude Code |
| --- | --- | --- |
| 交互 | `codex [PROMPT]`，全屏 TUI；`--no-alt-screen` 为行内模式 | `claude [prompt]`，行内 TUI |
| 无头 | **子命令** `codex exec [PROMPT]`（别名 `e`），有自己的子命令 `exec resume` / `exec fork` / `exec review` | **开关** `claude -p/--print` |
| stdin | `exec` 无 prompt 或 `-` 时读 stdin；同时给了 prompt 时 stdin 作为 `<stdin>` 块追加 | `-p` 读 stdin 管道 |
| 后台 | `queue` 给已有会话排一条消息；`app-server` 守护进程 | `--bg` / `attach` / `logs` / `stop` / `rm` / `agents` |

判断：

- **无头用子命令（Codex）优于开关（Claude Code）**。Claude Code 大量选项写着“only works with --print”，说明两种模式的参数集合不同，却挤在同一个参数表里；用子命令可以让 clap 直接拒绝无效组合。
- 后台和守护进程属于产品功能，Asterpath 的“没有后台 driver、宿主显式 drive”（packaging.md §1）本身就排除了这一块。

## 2. 无头接口

| | Codex `exec` | Claude Code `-p` |
| --- | --- | --- |
| 输出 | 默认人类可读；`--json` 输出 JSONL 事件；`-o FILE` 写最后一条消息 | `--output-format text`（默认，只有最终文本）/ `json`（单个结果对象）/ `stream-json`（逐事件）；`--include-partial-messages` 加增量 |
| 输入 | 只有 prompt / stdin | `--input-format stream-json`：stdin 实时喂 JSON 消息，可多轮 |
| 结构化输出 | `--output-schema FILE` | `--json-schema <schema>` |
| 不落盘 | `--ephemeral` | `--no-session-persistence` |
| 审批 | 由 `-a` 决定；`never` 时失败直接回给模型 | `--permission-prompts host|none`：交给宿主或一律拒绝 |
| 预算 | 无 | `--max-budget-usd` |

判断：

- **Claude Code 的三档输出格式更好**：`text` 适合管道，`json` 适合脚本拿结果和用量，`stream-json` 适合宿主集成。Codex 的 `--json` 只对应其中第三档。
- **`--input-format stream-json` 是关键差异**。有了它，`-p` 就是一个 stdio 上的双向宿主协议（Claude Agent SDK 就跑在它上面），审批请求可以从流里发出、从流里收到决定。Codex 走另一条路，单独做了 `app-server`（JSON-RPC）。对 Asterpath 来说，facade 本身已经是宿主 API，CLI 只需要把 `AgentEvent` 序列化出去，再接受 `approve` / `submit_input` 命令；不需要另起一个守护进程。
- 退出码：两者都没有在 `--help` 中声明；Asterpath 现有 `0/1/2` 约定可以保留。

## 3. 会话管理

| | Codex | Claude Code |
| --- | --- | --- |
| 续最近一次 | `resume --last`、`exec resume --last` | `-c/--continue`（限当前目录） |
| 指定会话 | `resume <id|name>`；不给 id 时进选择器；`--all` 关闭按 cwd 过滤 | `-r/--resume [id|name|搜索词]`，不给值时进选择器 |
| 分叉 | `fork [--last|id]` 子命令 | `--fork-session`，与 `-r`/`-c` 组合 |
| 命名 | 支持会话名 | `-n/--name` |
| 指定 id | 无 | `--session-id <uuid>` |
| 生命周期 | `archive` / `unarchive` / `delete` | `rm`（仅后台会话）、`purge` 整个项目 |

判断：

- 两者都**按工作目录过滤会话列表**，都支持**名字**，都有**分叉**。这三项是“会话目录”层面的共性需求。
- Claude Code 的 `-c` / `-r` 作为顶层开关，可以和交互、`-p` 两种模式组合，比 Codex 的 `resume` 子命令再分交互版 / `exec` 版两套更省；但 Codex 的子命令形式参数校验更干净。二选一时取 Claude Code 的形式成本更低：只有两个开关，不用复制子命令树。
- `archive` / `delete` 对应 Asterpath 的退役与保留策略（ADR-0016：策略归宿主），参考 CLI 可以不做。

## 4. 安全模型

| | Codex | Claude Code |
| --- | --- | --- |
| 沙箱 | `-s read-only|workspace-write|danger-full-access`，独立一条轴；`--add-dir` 增加可写目录 | 沙箱是设置项，不是主轴 |
| 审批 | `-a on-request|never`（未在帮助中列出的 `untrusted` 仍见于文档，未核实）；`.rules` 前缀规则（execpolicy）；`--approve-for-me` 自动审查 | `--permission-mode manual|acceptEdits|plan|auto|dontAsk|bypassPermissions`；`--allowedTools` / `--disallowedTools` 按模式匹配，如 `Bash(git *)` |
| 全放开 | `--dangerously-bypass-approvals-and-sandbox` | `--dangerously-skip-permissions` |

判断：

- **Codex 的两条正交轴更清楚**：沙箱回答“工具能碰到什么”，审批回答“什么时候问人”。这和 Asterpath ADR-0007（审批层与沙箱层分开）、ADR-0019 同构，现有 `[sandbox] mode` 已用同一组名字。
- **Claude Code 的按工具模式写规则更实用**：`Bash(git *)` 这种细粒度放行，在 Codex 里要写 `.rules` 文件。Asterpath 的 `ApprovalPolicy` 是宿主代码，规则匹配可以完全放在 CLI 里实现，不涉及内核。
- `plan` / `acceptEdits` / `auto` 这类模式是产品策略的打包，参考 CLI 不需要。

## 5. 配置

| | Codex | Claude Code |
| --- | --- | --- |
| 文件 | `~/.codex/config.toml`（TOML） | `settings.json`，分 user / project / local / managed 四层（JSON） |
| 覆盖 | `-c dotted.key=<toml值>`，任意键都能覆盖；`--enable` / `--disable` 开关特性 | `--settings <文件或JSON>`；`--setting-sources` 选择加载哪几层 |
| profile | `-p <name>` 叠加 `$CODEX_HOME/<name>.config.toml` | 无 |
| 严格模式 | `--strict-config` 拒绝未知字段 | 非交互下校验失败的设置文件被静默忽略 |
| 工作目录 | `-C/--cd` | 无（取进程 cwd） |
| 项目指令 | `AGENTS.md` | `CLAUDE.md` |

判断：

- **`-c key=value` 是抑制 flag 膨胀的关键机制**：配置文件里的每个键自动获得命令行覆盖能力，不需要逐个开 flag。Claude Code 没有它，所以才有 `--tools`、`--effort`、`--autocompact`、`--fallback-model`、`--append-system-prompt` 等一长串一一对应配置项的 flag。
- Asterpath 已经是 TOML，且 `FileConfig` 是 `deny_unknown_fields`，相当于默认开着 `--strict-config`，与 `-c` 天然契合。
- profile 可以用 `--config <path>` 指定另一个文件来替代，暂不需要。

## 6. 扩展管理

| | Codex | Claude Code |
| --- | --- | --- |
| MCP | `mcp list|get|add|remove|login|logout` | `mcp add|add-json|get|list|remove|login|logout|serve|…`；`--mcp-config`、`--strict-mcp-config` |
| 自身作为 MCP server | `mcp-server` | `mcp serve` |
| 插件 / 技能 | `plugin` | `plugin`、`--plugin-dir`、`--agents`、skills |

判断：

- `mcp list/get` 是只读诊断，对应 Asterpath 现有的 `config check` 中的 MCP 部分。`add/remove` 是改写用户配置文件，属于产品便利功能。
- “自身作为 MCP server”超出 v1 范围（ADR-0018 不做 ACP）。

## 7. 交互内斜杠命令（未核实，按使用经验）

两者共有：`/model`、`/compact`、`/new` 或 `/clear`、`/resume`、`/status`、`/init`（生成项目指令文件）、`/mcp`、`/diff`（Codex）、`/cost` `/context`（Claude Code）、`/approvals` 或 `/permissions`、`/exit`。

对照 Asterpath facade：`/model` → `update_config`；`/compact` → `compact_now`；`/new` → `create_session`；`/status` `/cost` → `usage` + `state`；Ctrl-C → `cancel`；审批提示 → `approve`。**交互模式需要的内核能力 facade 已基本具备。**

## 8. 诊断

- Codex：`doctor`、`debug models`（模型目录 JSON）、`debug prompt-input`（模型可见输入 JSON）、`features`。
- Claude Code：`doctor`、`-d/--debug [filter]`、`--debug-file`、`--verbose`。
- 对应 Asterpath：`config check` ≈ `doctor`；`inspect` ≈ `debug prompt-input`（投影就是模型可见输入）；`trace` 是 Asterpath 特有的原生追踪，两者都没有对应物。

## 9. 倒推：内核与 facade 缺口（初稿，待设计确认）

| CLI 需求 | 现状 | 缺口 |
| --- | --- | --- |
| `-c` 续最近 / `-r` 选择器按目录过滤、显示名字 | `CatalogEntry` 只有 `session` / `parent` / `created_at` / `last_activity`；facade 没有列会话的 API | 会话目录缺宿主元数据（工作目录、名字、首条输入摘要）；“工作区不进入内核”（packaging.md §2）与此冲突，需要决定元数据归内核还是归 CLI 自己的索引 |
| 分叉会话 | 无 | 是否在内核提供“从某水位复制日志新建 session” |
| `stream-json` 输出、交互显示工具调用 | `AgentEvent::ToolStarted` 只有工具名，没有参数；没有用量事件 | 事件是否携带参数与结果摘要、每步用量 |
| `--input-format stream-json` 多轮 | `submit_input` + `drive` + `approve` 已够 | 无 |
| `--ephemeral` | 内存 `SessionStore` 只在 `asterpath-testing` | 是否把内存存储作为正式适配器 |
| `-i` 附图、`--output-schema` | ADR-0031 已有文件输入与结构化输出 | 无 |
| `/model` `/compact` `/status` Ctrl-C | `update_config` / `compact_now` / `usage` / `cancel` | 无 |
| `Bash(git *)` 类规则 | `ApprovalPolicy` 是宿主代码 | 无（CLI 内实现） |
| `AGENTS.md` 项目指令 | `SessionSpec.system` | 无（CLI 读取后放进 system） |
