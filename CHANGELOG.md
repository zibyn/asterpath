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

### 新增

- 会话环境适配器 `EnvironmentAdapter`（`AsterpathBuilder::environment`）：按会话的宿主元数据解析该会话的工具、沙箱策略与子进程变量，子会话用根会话的；`ToolContext::spawn` 把策略与变量交给工具。
