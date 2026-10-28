# 发布说明

## 未发布

### 行为变化

- **bash 工具与 MCP stdio 服务不再继承进程的环境变量**（ADR-0037）。子进程只拿到最小白名单（`PATH`、`HOME`、`USER`、`LOGNAME`、`LANG`、`LC_ALL`、`LC_CTYPE`、`TZ`、`TMPDIR`、`TERM`）和会话环境或 MCP 配置 `env` 给出的变量。参考 CLI 里，bash 看不到用户 shell 的其他变量（如 `SSH_AUTH_SOCK`、各类 token）；需要的变量在配置里放行：

  ```toml
  [sandbox]
  env = ["SSH_AUTH_SOCK"]
  ```

  没有注册会话环境的嵌入方同样按隔离处理。
- 参考 CLI 不再改变进程当前目录；工作根经会话环境交给工具、沙箱与技能根。没配 `cwd` 的 MCP stdio 服务仍在工作根启动。
- **审批端点按会话树注册**（ADR-0039）：`ApprovalGate::register(root)` / `is_open(root)` 取根会话 id。某会话树没有端点时，`Ask` 按 `NO_APPROVAL_ENDPOINT` 拒绝，同进程其他会话树的端点不再让它挂起。嵌入方把原来的 `register()` 改成为自己照看的每棵会话树注册（根 id 取 `OwnedSession::owner().root` 或 `protocol::root_of`）。`DefaultApprovalPolicy` 改为单元结构体，不再持有审批门。
- **多个进程可以共用一个数据库，驱动前要认领会话树**（ADR-0036）：`open_session` 等返回只读句柄，`claim()` 得到能 `drive` 的 `OwnedSession`，别的进程持有时得到 `OwnedElsewhere`。存活靠进程租约，`Asterpath` 在 tokio runtime 上定期续约（`lease_renew_interval`，默认 10 s；SQLite 的 TTL 是 `OpenOptions::lease_ttl`，默认 30 s）。正常退出立即释放；被 kill 的进程的会话树要等一个 TTL 才能在别处续跑。失去属主时驱动停止并调用新的 `Hooks::on_ownership_lost`。`SessionStore` 实现要加 `claim` / `owner` / `renew`；`StorageError::Conflict` 是可重试的冲突。
- **用户配置在首次运行时创建，API key 可以写在配置里**（ADR-0050）。默认路径的 `config.toml` 不存在时，CLI 与桌面端写一份只含注释示例的模板（目录 `0700`、文件 `0600`），创建失败只在 stderr 提示一行；`--config` / `ASTERPATH_CONFIG` 指定的文件仍须存在。`[providers.<id>] api_key` 不再是配置错误，环境变量优先；别处的 `api_key` / `secret` / `token` 与 `-c …api_key=…` 仍报错。Unix 上含 key 的文件对组或其他人有任何权限时报配置错误并提示 `chmod 600`。`doctor` 列出各 provider 的 key 来源（`env:VAR` / `file` / `not set`），不显示值。
- **新会话默认带上内置的 `default` 智能体档案**（ADR-0051）。没配 `default_agent` 时，CLI 与桌面端新建的会话取名为 `default` 的档案，其正文排在 `system` 最前、`AGENTS.md` 与技能索引之前。内置档案是随安装包分发的资源文件（CLI 找二进制旁的 `agents/` 或 `<prefix>/share/asterpath/agents`，桌面端找资源目录的 `agents/`），用户级或项目级的同名 `default.md` 覆盖它；找不到内置目录时 stderr 提示一行，照常运行。
- 参考 CLI 交互模式会提示子会话的审批请求；以前 `untrusted` 模式下子会话的变更类工具会让 CLI 一直挂起。
- **审批模式统一用 `read-only` / `ask` / `allow`**（#213）：CLI 的 `-a`、`config.toml` 的 `[approval] default` 与桌面端共用一个 `approval::ApprovalMode`（变体 `ReadOnly` / `Ask` / `Allow`，`Deny` 改名为 `ReadOnly`）。旧名 `untrusted` / `never` 作为 `ask` / `allow` 的别名继续可用；桌面端命令的 JSON 取值由 `deny` 改为 `read-only`。`config::ApprovalMode` 删除。

### 新增

- Postgres 存储后端（#162）：新 crate `asterpath-storage-postgres`，`from_pool(pool, OpenOptions)` 接收应用建好的 `deadpool-postgres` 连接池，实现 `SessionStore` 全部契约（含会话树属主、租约与纪元、退役）。`synchronous_commit=off` 时默认拒绝打开（`OpenOptions::allow_async_commit` 放宽）。facade 新增可选 feature `postgres`（默认关），再导出为 `asterpath::postgres`。设计见 `docs/design/storage-postgres.md`。
- 只读句柄新增 `SessionHandle::config()`（日志折叠出的生效配置，含别的进程写入的变更）与 `context_usage()`（`ContextUsage { used, window }`：最近一次模型请求的输入 token 与 `ModelSpec.context_window`，压缩后不刷新），不需要认领。
- 会话元数据可按键修改（#165，ADR-0044）：`SessionHandle::update_metadata(patch)`，`patch: MetadataPatch { set, remove }`（`asterpath_protocol`）：先设置 `set` 的键、再删除 `remove` 的键，未提到的键不变；不需要认领，子会话也可以改；已退役的会话 = `NotFound`。新事件 `MetadataChanged { set, remove }`，存储在追加事务里合并进目录行，合并结果超出上限 = `AsterError::Config`，不落日志。自建 `SessionStore` 要在 append 时处理它（`asterpath_protocol::patched_metadata`），契约套件已覆盖。会话环境与 `SessionContext.metadata` 仍读创建时的元数据。
- 从指定已提交回合分叉（#167，ADR-0045）：`Asterpath::fork_session_at(source, metadata, at)` 在 `source` 的已提交回合 `at` 的末尾分叉，历史与配置取当时的（之后压缩或改配置都不影响）。`at` 不存在、未提交或属于别的会话 = 新错误 `AsterError::InvalidForkPoint { session, turn }`。`fork_session` 不变，仍取最后一个已提交回合。
- 门面删除入口（#166）：`OwnedSession::retire()` 在认领下退役句柄所在的整棵会话树。树里有会话不空闲时返回 `NotIdle`，什么都不退役；否则自底向上逐个退役、根会话最后，中途中断后再调一次删完；认领已被别的进程接管时返回 `Storage(OwnedElsewhere)`。`store().retire(..)` 仍公开，但不检查认领。
- 智能体档案（ADR-0051）：`$XDG_CONFIG_HOME/asterpath/agents/` 与 `<root>/.asterpath/agents/` 下的 `<name>.md`，同名时项目级优先、都优先于内置档案。frontmatter 可写 `description`、`model`（`provider/model`）与 `tools`（工具白名单，不在可用工具里的名字是配置错误），正文是基础提示词。`config.toml` 新增顶层 `default_agent` 选新会话的档案，CLI 的 `--agent <name>` 覆盖它（续会话时给出是用法错误），`-m` 仍优先于档案的 `model`。档案在创建会话时读取一次，档案名记在会话元数据 `agent`，分叉会话沿用。新命令 `asterpath agents` 列出当前工作根的档案及其生效文件。
- 子智能体按名字选用智能体档案（ADR-0053）：`spawn_agent` 新增可选参数 `agent`，从会话环境的 `SessionEnvironment.agents`（`AgentProfile`：名字、描述、整份 system、可选固定模型、可选工具列表）里选。选中后子会话的 system 换成档案的，模型取档案固定的（须与父会话同 provider，此时不能再给 `model_alias`），工具取父会话 ∩ 档案 ∩ `tool_allowlist`，总是全新上下文；不给 `agent` 时行为不变。本地应用把根会话可见的全部档案放进 `agents`，每份 system 按根会话的规则组装（档案正文、`AGENTS.md`、技能索引、档案索引）；新会话的 system 末尾多一段档案索引。frontmatter 写坏的档案不提供给子智能体，stderr 提示一行。
- 桌面端新建会话可选智能体档案：侧栏项目旁的选择框列出该项目的档案，预选 `default_agent`（没配时是 `default`）；`default_agent` 指向不存在的档案时直接在侧栏报错。侧栏与状态栏显示会话的档案名。档案固定的 provider 必须是桌面端启动时已建好的（`[model] provider` 或模型别名用到的），否则建会话时报错。
- 输出风格（ADR-0052）：`$XDG_CONFIG_HOME/asterpath/styles/` 与 `<root>/.asterpath/styles/` 下的 `<name>.md`，同名时项目级优先、都优先于内置风格；frontmatter 只接受 `description`。选了风格时，正文加一句框定语后排在档案正文之后、`AGENTS.md` 之前。`config.toml` 新增顶层 `output_style`，CLI 的 `--style <name>` 覆盖它；`asterpath exec` 不读 `output_style`，只认 `--style`。风格在创建会话时读取一次，风格名记在会话元数据 `style`。新命令 `asterpath styles` 列出风格。桌面端侧栏在档案选择框旁加风格选择框（预选 `output_style`，没有风格文件时不显示），侧栏显示会话的风格名。

- 桌面参考应用 `asterpath-desktop`（ADR-0035）骨架：侧栏按项目（工作根）列出会话，读 CLI 的 `config.toml` 与数据库，CLI 建的会话出现在对应项目下。窗口在非默认 feature `tauri` 之后。
- 桌面端对话：在项目下新建会话（工具与技能按 CLI 新会话的规则），发送后由后台任务认领并驱动到空闲、空闲后释放认领；对话流显示流式文本与可折叠的推理；运行中继续输入即引导，可停止当前回合；被其他进程（如 CLI）驱动的会话只读显示。
- 桌面端工具与审批：工具调用默认折叠，普通档展开显示文件改动的内联 diff 或输出开头，详细档显示完整参数与输出；本进程驱动的会话树注册审批端点，审批卡片可允许一次、本会话允许此工具（按会话树记，子代理同样适用；内存状态，重启失效）或附理由拒绝（理由发给模型），子代理的请求也会出现；输入框下可切换整棵会话树的审批模式：拒绝修改（修改类工具一律拒绝，含 bash，会话树只读）/ 修改前询问 / 全部允许，默认取 `[approval] default`。审批模式只改审批，不改沙箱。`config::assemble` 改为由调用方传入审批策略。
- 桌面端设置页（ADR-0050）：缺 provider、模型或 key 时启动即显示，选择或新建 provider（`anthropic` / `openai`、配置里的实例，或新名字如 `deepseek`：选协议，变量默认 `<ID>_API_KEY`）、填写 Base URL、模型 id 与 API key 后写回 `config.toml`（保留注释与顺序，权限 `0600`）并重启后端，不用重开窗口；侧栏齿轮按钮随时打开。key 来自环境变量时提示界面里填的不生效。
- 桌面端多会话：多个会话同时在后台驱动，切换视图不中断；侧栏标出运行中 / 等待审批 / 空闲 / 被其他进程占用；状态栏可切换模型（配置的模型与模型别名；不需认领，被其他进程驱动时也可切换），显示会话及其子代理的累计 token 与最近一次驱动错误。
- 应用 crate `asterpath-app`（`apps/asterpath`，ADR-0049）：CLI 二进制 `asterpath` 与桌面窗口二进制 `asterpath-desktop`（非默认 feature `tauri`）是它的两个入口，共用按工作根组装、会话元数据键与续会话规则和 `config.toml` 解析。桌面端开发仍在 `apps/desktop` 下运行 `pnpm tauri dev`。行为不变。
- 会话环境适配器 `EnvironmentAdapter`（`AsterpathBuilder::environment`）：按会话的会话元数据解析该会话的工具、沙箱策略与子进程变量，子会话用根会话的；`ToolContext::spawn` 把策略与变量交给工具。
- 会话目录按元数据等值过滤：`SessionQuery.metadata`（`SessionQuery` 不再是 `Copy`）。
- `ModelRequest.session: Option<SessionContext>` 带上会话、根会话与根会话的会话元数据，应用包装 provider 可按用户选凭据；压缩摘要调用经 `CompactionJob.session` 同样带上。
- `ApprovalGate::pending_in(root)` 列出一棵会话树（含子会话）的待审批请求。
- `ApprovalRequest.root`：请求所在会话树的根会话，审批策略可按会话树记状态（子代理沿用父会话树的设置）。
