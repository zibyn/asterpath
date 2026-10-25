# v1 验收场景

> 来源：[#15](https://github.com/zibyn/asterpath/issues/15)，2026-10-22 已定。
> 本文件是规范：断言与通过条件。五个故障注入点的实现与 CI 场景归 [#19](https://github.com/zibyn/asterpath/issues/19)；crate 布局与 CLI 见 [packaging.md](../design/packaging.md)。

## 1. 原则

- **真进程、真 SQLite、真模块**：`testdrive` 用与参考 CLI 相同的 facade 组装，只有 provider 与 clock 是脚本化的。
- **真崩溃**：用 `SIGKILL` 杀进程，不用 panic / `abort` / 注入异常模拟。
- **断言读日志与外部副作用**：不读私有内存、不依赖 `recorded_at`。
- **确定性**：固定脚本、固定输入 id、`ManualClock`；测试之间用独立 temp dir 与独立端口。
- **平台**：验收测试 `cfg(unix)`，在 Linux CI 上运行。

## 2. 工具链

| 组件 | 位置 | 职责 |
| --- | --- | --- |
| `ScriptedProvider` | `asterpath-testing` | 按 fixture 回放模型响应，并断言收到的 `ModelRequest` |
| 四个故障注入装饰器 + `ManualClock` | `asterpath-testing` | `append:before_commit`、`append:after_commit`、`tool:before_effect`、`tool:after_effect`；`clock:advance` = `ManualClock::advance`（[testing.md](../design/testing.md)） |
| `testdrive` 二进制 | `asterpath-acceptance` | `run` / `resume` / `inspect`；用真实模块组装，向 stdout 输出 JSON 行进度标记 |
| mock HTTP server | `asterpath-acceptance` | 场景 (b) 回放 Anthropic / OpenAI 线上协议（wiremock 或等价） |

- `testdrive` 不依赖 `asterpath-app`；CLI 的行为另有 CLI 测试。
- 进度标记示例：`{"event":"effect_applied","call":"..."}`、`{"event":"parent_waiting","children":2}`。
- 测试通过 `assert_cmd` 启动 `testdrive`，从 stdout 等待标记，再 `libc::kill(pid, SIGKILL)`。
- 日志断言直接打开同一个 SQLite 文件读取 session 流；不复用被测进程的内存状态。

## 3. 场景 (a)：单回合中途崩溃恢复

三个子场景都必须通过；A1、A2 对应 charter 的"副作用不重复"。

### A1：`Safe` 工具执行中崩溃

脚本：第一步模型返回一个工具调用 `set_marker { key: "k", value: "v" }`；第二步在收到工具结果后返回最终文本。

工具 `set_marker`：`InProcess`、`ReplaySafety::Safe`，**幂等**地写入文件（覆盖同一 key），写完后打印 `effect_applied` 并 sleep，制造崩溃窗口。

步骤：

1. 启动 `testdrive run`；等待 `effect_applied`；发 `SIGKILL`。
2. 启动 `testdrive resume`；等待进程退出。

断言：

- 副作用文件内容恰为 `{"k":"v"}`，没有重复条目。
- 日志中该调用恰一条 `ToolResult::Completed`；恰一条 `StepCommitted`；恰一条 `TurnCommitted(Completed)`。
- 恢复后有 `StreamResumed`；没有第二条 `AssistantMessage`。
- 第二次模型请求的投影里恰有一个该调用对应的 `ToolResult`。

### A2：`Unsafe` 工具执行中崩溃

脚本同 A1，工具换成 `append_line`：`InProcess`、`ReplaySafety::Unsafe`，向文件追加一行后打印标记并 sleep。

断言：

- 副作用文件恰一行。
- 恢复不重跑工具；日志恰一条 `ToolResult { status: Interrupted }`，模型可见。
- 恰一条 `StepCommitted`、恰一条 `TurnCommitted(Completed)`（脚本让模型接受中断结果并收尾）。

### A3：模型流中途崩溃

`ScriptedProvider` 流到一半时打印 `stream_open` 并挂起；测试杀进程。

断言：

- 恢复补写 `ModelCallAborted { kind: Crash }`；该 step 重试。
- 最终恰一条 `AssistantMessage`、恰一条 `StepCommitted`、恰一条 `TurnCommitted(Completed)`。
- 工具副作用恰好发生一次（若脚本含工具调用）。

## 4. 场景 (b)：双 provider 等价

目的：验证两个 adapter 可互换，且规范化历史不依赖供应商。**不是** turn 内切换 provider：`SessionSpec.model` 在创建时快照，v1 不支持运行期更换。

- 同一 fixture 脚本、同一 `testdrive` 二进制、同一输入；只改 `--provider anthropic|openai` 与 mock HTTP base URL。
- mock server 各自回放该供应商的线上协议（Anthropic Messages / OpenAI Responses），内容对应同一规范化响应。
- 两边都包含一次工具调用与最终文本。

断言：

- 两次运行的规范化 `Item` / `Part` 投影逐项相等（忽略 `RawProviderData`）。
- 工具调用的名字与 effective args 相等；最终 assistant 文本相等。
- 两次都以 `TurnCommitted(Completed)` 结束。
- 各自的 `raw` 字段存在且不被跨 provider 比较。
- `testdrive` 与 agent loop 中没有 provider 分支：同一次构建、同一段业务代码。

## 5. 场景 (c)：子智能体 fan-out、等待中崩溃与用量归因

脚本：

- 父会话第一步返回两个并行的 `spawn` 工具调用（任务 A/B），两个子会话各自跑一个回合并返回任务总结。
- 父会话第二步在收到两个结果后返回最终文本。

步骤：

1. 启动 `testdrive run`；等待 `parent_waiting`（两个 `ChildSessionSpawned` 已写、父处于持久等待）；发 `SIGKILL`。
2. 启动 `testdrive resume`；等待进程退出。

断言：

- 子会话恰好两个；父日志每个 `parent_tool_call_id` 恰一条 `ChildSessionSpawned`。
- 恢复不创建第三个子会话；`ChildSessionSpawned` / `ChildSessionCompleted` 不重复。
- 每个子会话日志：`SessionCreated` → `InputRecorded` → `TurnStarted` → `AssistantMessage` → `StepCommitted` → `TurnCommitted(Completed)`。
- 父日志：恰两条 `ChildSessionCompleted`；两个 spawn 工具结果都是 `Completed`；恰一条 `TurnCommitted(Completed)`。
- 父的第二次模型请求包含两个子任务总结，不含子会话完整 transcript。
- `Asterpath::usage(scope)`：父 `Session` scope 的 `own` 等于父自身模型调用之和；`SessionSubtree` 的 `descendants` 等于两个子会话之和；`total` 不重复计数。
- `Asterpath::trace(session)`：父 trace 有两个 `subagent` span，分别挂到对应的 spawn tool span；子 trace 根 span 的 `parent_trace` / `parent_span` 指回父；恢复不产生重复 span，重放标记存在。

## 6. 与 #19 的边界

- #15 固定场景、断言与 `testdrive` 约定。
- #19 已定（[testing.md](../design/testing.md)）：四个装饰器 + `ManualClock`、`Halt` / `Panic` 动作、G2–G7 契约套件、CI 分层与 fixture 归属。只要断言不变，装饰器内部实现可变。
- 注入点不足以覆盖的崩溃窗口，用真 `SIGKILL` + stdout 标记；不用注入点替代进程级验收。有自然 marker 的场景不用 failpoint。
- `testdrive --failpoint <point>[=<n>]` 只构造 `Halt`；父测试等待 marker 上限 15 秒，随后 `SIGKILL`。

## 7. 通过条件

- `cargo test -p asterpath-acceptance --test acceptance` 在 Linux CI 全绿，无 `#[ignore]`。
- 三个场景各自的日志断言与外部副作用断言全部通过。
- 失败信息包含 session id、日志尾部和触发的断言，便于定位。
