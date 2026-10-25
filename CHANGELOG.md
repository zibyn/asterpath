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
- 参考 CLI 交互模式会提示子会话的审批请求；以前 `untrusted` 模式下子会话的变更类工具会让 CLI 一直挂起。

### 新增

- 会话环境适配器 `EnvironmentAdapter`（`AsterpathBuilder::environment`）：按会话的宿主元数据解析该会话的工具、沙箱策略与子进程变量，子会话用根会话的；`ToolContext::spawn` 把策略与变量交给工具。
- 会话目录按元数据等值过滤：`SessionQuery.metadata`（`SessionQuery` 不再是 `Copy`）。
- `ModelRequest.session: Option<SessionContext>` 带上会话、根会话与根会话的宿主元数据，宿主包装 provider 可按用户选凭据；压缩摘要调用经 `CompactionJob.session` 同样带上。
- `ApprovalGate::pending_in(root)` 列出一棵会话树（含子会话）的待审批请求。
