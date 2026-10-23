# 内核里的会话工作区

Asterpath 内核不知道「工作区」「cwd」或项目目录。`SessionSpec`、`ConfigSnapshot`、日志和 `ToolContext` 都不承载工作区，也不为宿主按会话树定位工作区提供根会话 ID 之类的辅助字段。

## 为什么不做

工作区是工具的构造参数，不是 agent loop 的状态。主流框架都这样分层：

- LangChain `FileManagementToolkit(root_dir=...)`、Agno `FileTools(base_dir=...)`、pi `createCodingTools(cwd)`：根目录是工具工厂的参数。
- AutoGen `LocalCommandLineCodeExecutor(work_dir)` / `DockerCommandLineCodeExecutor`、smolagents `executor_type`、pi `BashOperations`：远端执行是换执行后端，工具与 agent 不变。

放进内核会带来三个问题：

- 纯对话的智能体（如 web 侧）根本没有工作区，字段对它们无意义。
- 远端沙箱的「cwd」是沙箱 ID + 沙箱内路径，`PathBuf` 写进日志在远端场景下要么填不了，要么被迫改义。
- 为了让子会话找到宿主侧的绑定，还得再给 `ToolContext` 补会话谱系，内核为一个工具层概念长出额外机制。

## 多项目怎么做

每个项目构建一个 `Asterpath` 实例，使用该项目自己的存储，并注册按项目根目录构造的工具：

```rust
// 宿主打开项目时
let tools = asterpath_tools::coding(project_root, sandbox, policy);
let asterpath = Asterpath::builder() /* 该项目的存储 */ .tools(tools) /* ... */;
```

子会话与父会话在同一实例内，自然使用同一组工具。重启后宿主按项目重建实例，再 `open_session` / `wakeable`。工具工厂见 #83。

## Prior requests

- #89: 阻塞：ToolContext 缺会话工作区，多项目下工具不知道 cwd（后改为「ToolContext 暴露根会话 ID」，同样拒绝）
