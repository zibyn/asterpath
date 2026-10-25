# 设计审计：内核与宿主的边界（汇总）

> 完成日期：2026-10-25。用途：在「CLI、桌面、web 都是内核的不同展现形态；web 以库方式内嵌内核、单设备服务上千用户；各形态数据互通」的前提下，审计现行设计与 proposed ADR-0035/0036。
>
> 依据：[kernel-host-boundary-audit.md](kernel-host-boundary-audit.md)、[multi-process-storage-audit.md](multi-process-storage-audit.md)、[desktop-codex.md](desktop-codex.md)、[desktop-claude-code.md](desktop-claude-code.md)、[pi-and-pi-durable.md](pi-and-pi-durable.md)、[opencode.md](opencode.md)。

## 1. 参考产品的分层

| | 内核形态 | 外壳如何接入 | 会话归属 / 多进程 | 工作目录 | 多用户 |
| --- | --- | --- | --- | --- | --- |
| Codex | Rust 内核 + app-server | 所有外壳是 app-server（JSON-RPC）客户端 | 一个守护进程承载多 thread | thread 级 cwd | 无 |
| Claude Code | 一个程序 | SDK/桌面以子进程 + stream-json 调用 | 一进程一会话；无锁 | 进程 cwd | 无 |
| pi | 库（agent-core + coding-agent） | 进程内；RPC 只是 stdin/stdout | jsonl 无锁共享目录 | 进程 cwd | 无 |
| pi-durable | 库 + 持久执行 | 进程内；传输由宿主做；`watch()` 多观察者 | 整个存储单属主，其他进程只读 | — | 部署单元隔离 |
| opencode v2 | Core 库 + 可选 Server | HTTP+SSE；SDK 进程内调同一路由 | 每用户一个服务进程绕开；跨进程围栏未做 | 请求级 directory | 无身份；部署单元隔离 |

共同点：**内核是库，外壳是调用方式**；线协议（若有）是内核之上的一层，不是内核本身。没有一家同时做到「多进程共享存储 + 每会话执行围栏 + 单进程多用户」，这正是 Asterpath 的目标。

## 2. 现状判断

内核本身没有明显越界；问题在**缺位**，以及几处**进程级假设**。

### 2.1 缺位：所有宿主共用的组装约定只在 CLI 二进制里

AGENTS.md、技能、工作区工具、MCP 加载、元数据键 `cwd`/`name`、续会话时的工具注册规则（`crates/asterpath-cli/src/session.rs:104-547`）决定了不同形态能否互通，却不能被桌面或 web 复用。ADR-0035 让桌面依赖 `asterpath-cli` 的 lib target，会把 clap、`eprintln!`、退出码一起带进来。

### 2.2 进程级假设（阻断内嵌与多用户）

- **工作根是进程状态**：`set_current_dir`（`session.rs:271`），工具、技能根、沙箱可写根都读进程 cwd；工具在 builder 上构造时绑定根目录，一个 `Asterpath` 只服务一个工作根。
- **环境变量泄漏**：bash 工具与 MCP stdio 继承整个进程环境（`asterpath-tools/src/lib.rs:390`、`asterpath-mcp/src/servers.rs:271`），web 进程的密钥会进入模型可执行的 shell。
- **审批端点按进程计数**：`ApprovalGate::is_open` 只要进程里有一个端点就为真，无人值守用户的会话会挂起而不是拒绝（推断）。
- **事件只给驱动者**：子会话事件被丢弃（`scheduler/spawn.rs:1065-1075`），多视图、多标签页无法观察同一会话。
- **没有用户维度的机制**：会话目录只能按父会话过滤；provider 请求不带会话，无法按用户选凭据。
- **存储整库单写者**：见 ADR-0023；多进程共用存储被挡住。

### 2.3 现有缺陷（与方案无关，须先修）

`unique_bytes` 跨进程不保证唯一；退役后 append 不检查墓碑；首次建表并发竞态（见 multi-process-storage-audit.md）。

## 3. proposed ADR 评审

**ADR-0036（会话树属主）**：属主、认领、纪元属于内核，方向成立，参考产品里没有现成答案。缺口：

- 非属主可写的记录（输入、审批决定、配置变更）与「失去属主身份的写入」如何区分没有规定；
- 没有「谁是属主」的查询，宿主无法路由取消；
- 待审批请求本身只在属主内存，跨进程只能传决定；
- 动机写「多 worker」，后果承认 SQLite 下规模靠单进程，前后不一致；
- 2.3 的前置修复没有列入。

**ADR-0035（桌面参考宿主）**：进程内托管、后台驱动、「本会话允许」放内存、手写 TS 类型都属于宿主，归属正确。问题：

- 依赖 CLI 的 lib target（见 2.1）；
- 路由工具靠会话元数据找工作根，但子会话不继承元数据，存储也没有按 id 取元数据的接口；沙箱策略没有一起路由；
- 「MCP 全局共享」与工作根定义冲突。

## 4. 结论

1. 工作根、环境、凭据要从**进程级**改为**会话级**，由内核提供机制（会话创建/打开时按宿主元数据解析出该会话的工具集与环境，子会话继承），宿主提供策略。
2. 组装约定下沉为一个**宿主共用层**（不是内核），CLI、桌面、web 共用。
3. 观察与驱动分离：内核提供已提交日志的订阅（重放 + 实时），流式增量只在属主进程内扇出。
4. 线协议不是现在的事；先稳定门面（opencode v1/v2 并存的教训）。
5. ADR-0036 保留并补全；ADR-0035 在 1–3 落地后重写。
