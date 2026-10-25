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
- 参考 CLI 交互模式会提示子会话的审批请求；以前 `untrusted` 模式下子会话的变更类工具会让 CLI 一直挂起。

### 新增

- 桌面参考宿主 `asterpath-desktop`（`apps/desktop`，ADR-0035）骨架：侧栏按项目（工作根）列出会话，读 CLI 的 `config.toml` 与数据库，CLI 建的会话出现在对应项目下。窗口在非默认 feature `tauri` 之后。
- 桌面端对话：在项目下新建会话（工具与技能按 CLI 新会话的规则），发送后由后台任务认领并驱动到空闲、空闲后释放认领；对话流显示流式文本与可折叠的推理；运行中继续输入即引导，可停止当前回合；被其他进程（如 CLI）驱动的会话只读显示。
- 桌面端多会话：多个会话同时在后台驱动，切换视图不中断；侧栏标出运行中 / 等待审批 / 空闲 / 被其他进程占用；状态栏可切换模型（配置的模型与模型别名），显示会话及其子代理的累计 token 与最近一次驱动错误。
- `asterpath-host`（ADR-0040）：参考宿主共用的按工作根组装、宿主元数据键与续会话规则；可选 `config` feature 解析 `config.toml`。参考 CLI 改用它，行为不变。
- 会话环境适配器 `EnvironmentAdapter`（`AsterpathBuilder::environment`）：按会话的宿主元数据解析该会话的工具、沙箱策略与子进程变量，子会话用根会话的；`ToolContext::spawn` 把策略与变量交给工具。
- 会话目录按元数据等值过滤：`SessionQuery.metadata`（`SessionQuery` 不再是 `Copy`）。
- `ModelRequest.session: Option<SessionContext>` 带上会话、根会话与根会话的宿主元数据，宿主包装 provider 可按用户选凭据；压缩摘要调用经 `CompactionJob.session` 同样带上。
- `ApprovalGate::pending_in(root)` 列出一棵会话树（含子会话）的待审批请求。
