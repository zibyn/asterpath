# 内核 / 宿主边界审计

> 日期：2026-10-25。来源：提交 `8783f2a`（`8783f2ac169a17a0b43113490d12e12bb6d5e95f`，main），外加工作区里尚未提交的 `GLOSSARY.md`、ADR-0023 状态行、`docs/adr/README.md`、ADR-0035、ADR-0036。只读审阅，没有运行代码；标「推断」的结论来自读代码，没有实测。
>
> 判断用的原则（来自维护者）：内核只做内核的事，宿主只做宿主的事；web、桌面、CLI 只是内核的不同展现形态。目标有两个：内核能嵌入 web 平台，单台设备服务上千用户；CLI 与桌面端数据互通。
>
> 读过的材料：`GLOSSARY.md`；`docs/adr/` 中的现行 ADR 与短桩；`docs/design/*.md`（重点读了 packaging、storage、tools、subagents、skills）；`deps-allowlist.toml`；`crates/asterpath/src/lib.rs`、`config.rs`；`crates/asterpath-cli/src/{session,config,drive,interactive,exec,stream,main}.rs`；`crates/asterpath-protocol` 中的 `contracts.rs`、`tool.rs`、`agent.rs`、`provider.rs`、`sandbox.rs`、`time.rs`；`asterpath-tools`、`asterpath-mcp`、`asterpath-storage-sqlite`、`scheduler/spawn.rs` 的相关片段；`docs/research/` 下的四份已有调研。本文只陈述事实和判断依据，不提出新设计。

层级缩写：**P** = `asterpath-protocol`（合约）；**E** = `asterpath` 引擎与门面；**A** = 适配器 crate（`-tools` / `-skills` / `-mcp` / `-storage-sqlite` / `-sandbox-linux` / `-provider-*`）；**CLI** = `asterpath-cli` 二进制；**无** = 没有任何一层负责。

## 1. 现状清单

| 能力 | 当前所在层 | 依据 | 备注 |
|---|---|---|---|
| Agent loop（回合、step、steering、重试） | E | ADR-0006、ADR-0026；`asterpath/src/agent/` | 不可替换，经 `Hooks` 扩展。归属正确 |
| 存储：日志、目录、退役 | P（`SessionStore` trait）+ A（sqlite）| ADR-0023；`protocol/src/contracts.rs:135-161`；`storage-sqlite/src/lib.rs` | 归属正确 |
| 属主与并发 | E（进程内 `busy` 集合）+ A（sqlite 整库 `flock`） | `agent/mod.rs:64,1167-1209`；`storage-sqlite/src/lib.rs:12-13,719-750`；ADR-0023「单进程独占写者」 | 粒度是整个库。ADR-0036（proposed）要缩小到会话树 |
| 唤醒发现 | E（`Asterpath::wakeable()` 折叠全部会话） | `lib.rs:379-399`；ADR-0023 | 何时驱动由宿主决定。全量折叠已标 shortcut |
| 调度（谁驱动、何时驱动） | 宿主 | ADR-0026「没有后台 driver」；`lib.rs:599-689` 文档注释给出 spawn + broadcast 模式 | CLI 在前台驱动（`drive.rs:71-104`）。没有可复用的驱动器实现 |
| 审批：策略 | P（`ApprovalPolicy`、`DefaultApprovalPolicy`）+ 宿主实现 | `protocol/src/tool.rs:236-261`；CLI `config.rs:240-264`（`untrusted` / `never`） | 归属正确 |
| 审批：端点与待审批列表 | P（`ApprovalGate`，每个 `Asterpath` 一个，只在进程内） | `protocol/src/contracts.rs:185-237`；`agent/pipeline.rs:804-806`；ADR-0007 / 0028 | `is_open()` 是全局计数，`pending()` 跨所有会话。见 §2.3 |
| 审批：决定 | E（`approve` 写 `ApprovalDecided`） | `lib.rs:699-708` | 持久 |
| 取消 | P（`CancellationRegistry`，内存）+ E（`SessionHandle::cancel`） | `protocol/src/agent.rs:205`；`lib.rs:692-695` | 只在本进程内生效；跨进程调用时静默无效（multi-process-storage-audit P3） |
| 配置解析（`config.toml`、`-c`、环境变量里的 key、provider 段） | CLI | `cli/src/config.rs:56-127,165-505` | 桌面端按 ADR-0035 计划复用 |
| 会话组装（工作区工具、`AGENTS.md`、技能、MCP、宿主元数据） | CLI（调用 A 的积木） | `cli/src/session.rs:104-236,406-547`；packaging.md §4.6 | 积木在 A（`asterpath_tools::coding`、`asterpath_skills::discover/render_index/LoadSkillTool`、`McpServers::load`），拼装逻辑只在 CLI |
| 工作根绑定 | CLI，用**进程全局状态**实现 | `session.rs:269-274`（`set_current_dir`）；`session.rs:451,481`、`config.rs:817` 都读 `current_dir()` | 内核刻意不知道工作根（ADR-0032、packaging.md:42）。`ToolContext` 只有 `session`（`protocol/src/tool.rs:37-43`） |
| 模型 / provider 预设 | P / A（profile 值类型和内置官方表）+ CLI（`[providers.<id>]` 解析，key 从环境变量读） | ADR-0029；`cli/src/config.rs:328-480` | 第三方预设属于宿主策略，归属正确；但解析代码只在 CLI |
| 事件流 | E（`drive()` 流，只发给调用方） | `lib.rs:683-689`；`protocol/src/agent.rs:68-111` | 除 `Idle` 外的事件变体不带 `session`。子会话的事件被丢弃（`scheduler/spawn.rs:1065-1075`） |
| 重放 / 重挂载 | E（`projection()`、`facts()`、`scan_all`） | `lib.rs:710-736` | 「投影 + 实时事件」的拼接归宿主 |
| 会话目录与元数据 | P / E（`sessions(query)`，元数据不可变、内核不解释）+ CLI（`cwd` / `name` 键约定、按 cwd 过滤、选择器） | ADR-0032；`lib.rs:369-374`；`session.rs:25-27,315-358` | 目录查询只能按父会话过滤；按元数据过滤要读出整个目录（`session.rs:333` shortcut） |
| 生效配置的读取 | P（`effective_config`）+ CLI 自己扫日志 | `protocol/src/config.rs:269`；`session.rs:391-404` | 门面没有直接读取生效配置的方法 |
| 用量与追踪 | E（日志的纯折叠）+ P（`Hooks::on_record`） | ADR-0011；`lib.rs:401-422` | 成本归宿主。归属正确 |
| 压缩 | E（`DefaultCompactor`，`compact_now`）+ P（`Compactor` trait） | ADR-0026；`lib.rs:738-741` | CLI 为 compactor 另建一份 `ProviderCatalog`（`session.rs:508-515`） |
| 子智能体 | E（scheduler、`spawn_agent` 工具） | ADR-0008；`lib.rs:158-169` | `max_live_children` 是每个 `Asterpath` 的全局上限（`lib.rs:163`） |
| 沙箱 | P（`SandboxProvider`）+ A（bwrap）+ CLI（解析策略、选择后端） | ADR-0019；`cli/src/session.rs:538-547`；`config.rs:813-821` | `WorkspaceWrite` 的可写根取进程 cwd |
| 子进程环境变量 | A（`asterpath-tools` bash、`asterpath-mcp` stdio 都**继承整个进程的环境变量**） | `asterpath-tools/src/lib.rs:390`；`asterpath-mcp/src/servers.rs:271` | 没有注入或过滤的参数 |
| 唯一 id 生成 | P（`unique_bytes`：时钟 xor pid，加进程内计数器） | `protocol/src/time.rs:7-17` | 只保证进程内唯一（multi-process-storage-audit P1） |
| 多租户 / 用户身份 | **无** | `SessionQuery`、`CatalogEntry`、`ToolContext`、`ModelRequest`、`ApprovalGate` 都没有租户或用户维度 | 只能把用户 id 写进不可变的宿主元数据，由宿主自己过滤 |

## 2. 越界与缺位

### 2.1 内核替宿主做了的事

审到的越界很少，而且都不严重：

- **`ApprovalGate` 和 `CancellationRegistry` 是带运行时状态的实现类型，却放在合约 crate 里**（`contracts.rs:185`、`agent.rs:205`）。ADR-0026 规定 P 里放「值类型、id、信封、适配器 trait、钩子、clock、错误」。这两个类型不是宿主策略，不算越界；但它们决定了「审批端点只有一个、按进程计数」的语义，宿主无法替换（§2.3 第 4 条）。
- **门面在 `sqlite` feature 下自己打开数据库**（`lib.rs:127-144`）。这是可选的便利，宿主可以用 `store()` 注入替代，不构成越界。
- 没有发现内核替宿主决定 UX、成本、保留策略、技能发现或工作区语义。ADR-0021、0029、0032 都把这些明确推给了宿主，代码与文档一致。

### 2.2 宿主（CLI 二进制）承担了本应可复用的职责

内核没有越界；问题在另一侧：**内核以外、所有宿主都会用到的那一层只写在 CLI 二进制里**，而且实现依赖进程全局状态。

1. **会话组装只有 CLI 版本。** 把 `AGENTS.md` 放进 system、技能发现并渲染索引放进 system 加 `LoadSkillTool`、按沙箱策略构造 `asterpath_tools::coding`、启动 MCP 并汇总状态、写元数据 `cwd` / `name`、续会话时「注册全部工作区工具，再由日志里的生效配置决定可见子集」、用 `--continue` 按 cwd 找最后活动的会话：这些规则都在 `cli/src/session.rs:104-547`。ADR-0035 的决定「`asterpath-cli` 增加 lib target，公开配置解析与会话组装」本身就说明：这是一层所有宿主共用的规则，只是现在放在了 CLI 里。同一库要「数据互通」，至少下面几项必须在 CLI、桌面、web 之间一致：
   - 元数据键 `cwd` / `name` 的含义：现在是 `session.rs:26-27` 里的私有常量，CLI 靠 `in_root` 按 cwd 过滤；
   - 续会话时的工具注册规则：续会话依赖「注册表包含快照里的所有工具」，否则会出现 `NotFound`（multi-process-storage-audit E17）；
   - provider 实例 id：快照记的是实例 id，恢复时要用同一 id 注册（ADR-0029 Consequences）。

   这些都是跨宿主的数据契约，不是某一种展现形态的私事。

2. **直接拿 CLI 当库会把 CLI 特有的东西一起带进来。** `asterpath-cli` 依赖 `clap`、`toml`（`Cargo.toml`）；组装函数里有面向终端的副作用，例如 `eprintln!("session: ...")`、`skill: ...`、`mcp: ...`（`session.rs:166,211,229,574`），以及交互式选择器（`session.rs:282-302`）；错误类型 `CliError` 带退出码；provider key 只从环境变量读（`config.rs:482-488`，packaging.md:412）；工作根靠 `set_current_dir` 实现（下一条）。web 宿主不会读 `config.toml`，也不会从进程环境变量取每个用户的 key，所以「配置文件 / 环境解析」和「已解析值到组装」在 CLI 里是同一段代码，没有分开。

3. **工作根是进程全局状态。** 新会话和续会话都调用 `enter(root)`，也就是 `std::env::set_current_dir`（`session.rs:125,140,271-274`）；随后 `workspace_tools`（`:481`）、`skill_roots`（`:451`）、`sandbox_policy` 的可写根（`config.rs:817`）都读 `current_dir()`；MCP stdio 服务在没配 `cwd` 时继承进程目录（`asterpath-mcp/src/stdio.rs:62` 只在 `Some` 时设置）。结果：一个进程同一时刻只能有一个工作根。ADR-0035 已经在 Decision 里写明「不靠改进程当前目录」，所以要复用的组装代码必须先去掉这条依赖。

4. **从日志推导结果的读取逻辑写在 CLI 里。** 包括生效配置快照（`session.rs:391-404`）、最新回合的最终文本与结构化输出（`drive.rs:169-237`），以及回合失败原因（`drive.rs:126-156`）。它们只用到 P 的公开函数，桌面或 web 要么复制，要么依赖 CLI。这属于门面或可复用层的查询缺口。

5. **「每个项目一个 `Asterpath` 实例」与存储锁冲突。** packaging.md:42 写的是「多项目宿主每个项目一个 `Asterpath` 实例」。但 sqlite 在打开时对 `<db>.lock` 加 `flock(LOCK_EX|LOCK_NB)`（`storage-sqlite/src/lib.rs:719-750`）：同一进程里对同一个库打开第二个实例，会落在另一个 open file description 上，得到 `WriterBusy`（推断）；而且各实例有各自的 `busy` 集合，同一个会话的单飞不再成立。所以「共用一个库」的多项目宿主实际上只能用一个 `Asterpath`，这正是 ADR-0035 改用路由工具的原因。packaging.md:42 与 ADR-0035 目前互相矛盾。

### 2.3 对 web 多用户场景的阻断

按严重程度排列。

1. **没有租户或用户维度。** 目录查询只能按父会话过滤（`contracts.rs:84-89`）。要列出「某个用户的会话」，只能读出整个目录后在宿主侧过滤，CLI 按 cwd 过滤已经这样做（`session.rs:315-343`）。`wakeable()` 返回所有用户的会话；`on_record` 收到所有会话的记录；`usage` 只有会话或子树两种范围。用户 id 只能写进创建时的宿主元数据，而元数据**不可变**，子会话也**不继承**（ADR-0032）。

2. **工具注册表、provider 目录和 compactor 在构造 `Asterpath` 时固定，不支持热替换**（GLOSSARY「热替换」；`lib.rs:114-172`）。工具名在注册表里全局唯一（`create_session` 的 `ToolSelection::Names` 按名字查注册表，`lib.rs:241-258`）。因此：
   - 每个用户自己的 MCP 服务、工作区工具或 API key 不能在运行时加入；
   - `ModelRequest` 不带会话或用户（`provider.rs:22-36`），provider 无法按用户选 key、限流或计费，只能为每个用户预先注册一个 provider 实例 id，而且要在构造时就注册好；
   - `ToolContext` 只有 `session`，工具要绑定到用户或工作根，就只能用 ADR-0035 那种路由工具，在每次调用时从存储反查元数据（存储没有按 id 取目录项的方法，`contracts.rs:135-161`，只能扫 `SessionCreated`）。

3. **进程全局状态。** 工作根依赖 `set_current_dir`（§2.2 第 3 条，只出现在 CLI）；bash 工具和 MCP stdio 服务继承**整个进程的环境变量**（`asterpath-tools/src/lib.rs:390`、`asterpath-mcp/src/servers.rs:271`），web 进程环境里的 provider key 等机密会进入模型能执行的 shell，而适配器没有提供注入或过滤环境变量的参数；`unique_bytes` 只保证进程内唯一（`time.rs:7-17`）。

4. **审批端点只有一个，按进程计数。** `ApprovalGate::is_open()` 只要本进程注册过任意一个端点就返回 true（`contracts.rs:209-211`），而 `pipeline.rs:804-806` 只在它为 false 时 fail closed。于是在 web 进程里，只要有一个用户在线并注册了端点，所有没人值守的用户会话遇到 `Ask` 都会挂起，不会按 `Deny` 收尾（推断）；`pending()` 也返回所有用户的请求（`contracts.rs:213-215`），按用户分发要由宿主自己过滤。ADR-0007 的「没有审批端就 fail closed」在多用户下的粒度是进程，不是会话或用户。

5. **事件只发给驱动者，子会话的事件被丢弃。** `drive()` 的流只交给调用方（`lib.rs:683-689`），多个订阅者要靠宿主自己做 broadcast（文档注释给出了模式，但没有实现）。除 `Idle` 外的 `AgentEvent` 都不带 `session`（`agent.rs:68-111`），宿主多路复用时要自己打标签。子会话由 scheduler 驱动，`drive_child` 读完事件就丢掉（`spawn.rs:1065-1075`）。子会话里的 `ApprovalRequested` 只能通过轮询 `ApprovalGate::pending()` 发现（`asterpath/tests/subagents.rs:567-570`），没有推送。CLI 的 `drive.rs:71-104` 只回应父流里的审批，不轮询 `pending()`；因此推断：在 `untrusted` 模式下，子会话里需要审批的变更类工具会让 CLI 一直挂起，只能按 Ctrl-C 结束。

6. **资源上限没有按用户划分。** `max_live_children` 是每个 `Asterpath` 的全局上限（`lib.rs:163`，packaging.md:135），用户之间没有公平性，一个用户可以占满。sqlite 只有一条读连接，由 `Mutex` 串行（multi-process-storage-audit S7），命令队列无界（S10）。

7. **跨进程语义缺失**（详见 multi-process-storage-audit）：非属主进程的 `cancel` 静默无效，非属主的 `approve` 不会唤醒属主进程，`wakeable` 无法排除其他进程正在驱动的会话。

## 3. 对两份 proposed ADR 的逐条评审

### ADR-0035 桌面参考宿主

| 决定 | 该归哪层 | 评审 |
|---|---|---|
| 头部「依赖：存储写者粒度改为每会话（待写 ADR）」 | — | ADR-0036 已经写出，引用应改为 0036。另外这个依赖只是因为要「CLI 与桌面端同时运行」才出现；参考对象 Codex 的做法是所有前端连同一个守护进程，存储只由守护进程打开（`desktop-codex.md` §1.7）。0035 拒绝了 app-server 方案，理由只是「要先设计线协议」；0036 也没有比较「CLI 作为桌面进程的客户端」这一选项。这个前提没有被论证 |
| 进程内托管：一个 `Asterpath` 承载所有项目；命令按 thread / turn / item 命名 | 宿主 | 归属正确。与 packaging.md:42「每个项目一个 `Asterpath` 实例」矛盾，需要统一（§2.2 第 5 条）。命令命名是宿主的事 |
| 项目 = 工作根；工作区工具和技能工具按名字注册路由工具，按 `ToolContext.session` 的会话元数据找到工作根 | 宿主机制，但 web 也需要同一机制，应放在可复用的宿主层 | 前提有三处问题：① **子会话不继承元数据**（ADR-0032），子会话里的工具调用按 `ToolContext.session` 查不到 `cwd`，需要沿 `parent()` 找到根会话，ADR 没有写；② 每次工具调用都要读存储查元数据，存储没有按 id 取目录项的接口（`contracts.rs:135-161`）；③ 沙箱策略的可写根也取决于工作根（`config.rs:813-821`），路由工具必须一起路由策略，ADR 只说了「转给该工作根的工具实例」。另外，「同名工具在各工作根下描述一致」只有在描述符不含路径时才成立，这一点 Consequences 已经指出 |
| MCP 服务全局共享 | 宿主 | 与 GLOSSARY 新增的「工作根」定义矛盾，后者写的是「MCP 服务都相对它解析」。stdio 服务没配 `cwd` 时继承进程目录（`stdio.rs:62`），沙箱可写根也取进程 cwd，所以桌面进程里的 MCP 服务实际绑定的是「桌面进程启动时的目录」。按项目隔离的 MCP 服务（例如以项目为根的文件系统服务）做不到 |
| 复用 CLI 组装：`asterpath-cli` 增加 lib target，组装函数改为显式接收工作根 | 应当是可复用的宿主层，不属于 CLI | 这一条说明会话组装、元数据键约定和配置到组装的映射是跨宿主的契约（§2.2 第 1 条）。挂在 CLI 的 lib target 下，会让桌面端（以后还有 web）依赖 `clap` / `toml`、终端输出副作用和 `CliError` 退出码；而且 web 不读 `config.toml`、也不从进程环境取 key，用不上「文件与环境解析」这一半。`deps-allowlist.toml` 里 app 角色可以依赖 `*`，所以依赖规则拦不住桌面 app 依赖另一个 app。是否把这一层下沉为独立 crate，由维护者决定；本文只确认这种需要存在 |
| 会话由后台任务驱动到空闲；视图只订阅；重挂载时先渲染 `projection()` 再回放缓存 | 宿主 | 与门面文档的模式一致（`lib.rs:606-681`）。但有三个缺口：子会话的事件不在流里，子会话的审批只能轮询 `pending()`（§2.3 第 5 条）；事件不带 `session`；投影与缓存事件之间没有共同的位置标记（`AgentEvent` 不带 log id），去重和拼接要由宿主自己定义。web 端会遇到同样的问题，所以这也是可复用层或门面的缺口，不只是桌面的 |
| 运行中的输入即引导 | 内核已提供 | ADR-0006 已经是这个语义，桌面只是沿用 |
| 「本会话允许」是宿主审批策略的内存状态 | 宿主 | 符合 ADR-0007 与 0019（策略不落日志）。在 0036 的多进程下，CLI 看不到桌面端的这个状态，这是可以接受的宿主差异 |
| 前端不生成类型，protocol 不加派生 | 宿主 | 符合「参考宿主不为自己改合约」。`AgentEvent` 已经有 `Serialize` 和 `type` 标签（packaging.md §3.4），手写 TS 类型实际上依赖了这份序列化格式，它是事实上的线格式，但没有声明为稳定面 |
| Consequences 里列出的门面缺口（改名、归档 / 删除、按回合分叉、按元数据查询、上下文窗口） | 门面 | 方向正确。审计中还发现以下缺口没有列出：读取生效配置、读取最新回合结果、子会话事件与审批的推送、按会话或用户划分的审批端点 |

### ADR-0036 会话树属主

| 决定 | 该归哪层 | 评审 |
|---|---|---|
| 属主以会话树为单位，根会话 id 是归属键 | 内核（存储契约 + 引擎） | 归属正确，依据充分：父会话在同一进程里驱动子会话，孤儿判定和退役都读父会话（multi-process-storage-audit §1、E4、E14） |
| 显式认领：打开只得到只读句柄；驱动、手动压缩、取消、调度子会话要先认领 | 内核 | 归属正确。规定不完整的地方：`submit_input`、`update_config`、`approve`、`fork_session` 这类**非属主写入**是否需要认领，ADR 只对审批作了说明。`update_config` 改工具集时先扫描再追加，存在 TOCTOU（multi-process-storage-audit E8） |
| 属主纪元进合约，`append` 携带属主凭证，存储拒绝过期纪元 | 内核 | 与上一条有冲突：非属主写入（审批、输入）不带凭证，存储怎样区分「合法的非属主写入」和「失去属主身份的进程写入」，ADR 没有交代。不用心跳租约的理由成立（Postgres advisory lock 掉线由纪元兜底，multi-process-storage-audit §4） |
| 跨进程审批走日志，属主有上限地轮询；ADR-0028 不变 | 内核 | **前提有问题**：只有「决定」能跨进程，「请求」不能。待审批列表只在属主进程内存里，`ApprovalRequested` 不落日志，非属主进程发现不了要审批什么。属主进程没有注册端点时，`Ask` 立即按 `Deny` 处理（`pipeline.rs:804-806`），别的进程里有审批者也没用。所以「CLI 审批桌面端的会话」只有在桌面端已注册端点、并且 CLI 通过其他渠道得知请求时才可行。ADR 写了「要做到即时需要修订 ADR-0028」，但没有指出这个发现缺口 |
| 非属主取消返回 `OwnedElsewhere`，路由到属主是宿主的事 | 内核返回错误；路由归宿主 | ADR 没有提供「谁是属主」的查询，宿主不知道该路由给谁。web 单进程场景不受影响，CLI 与桌面并存时受影响 |
| 唤醒发现只返回无主会话；谁认领由宿主决定 | 内核 | 归属正确。全量折叠的规模问题 Consequences 已经承认 |
| 降级语义：`max_live_children` 按进程计、`on_record` 跨进程可能重复、属主迁移时清缓存 | 内核文档 | 归属正确。但这些都没有按用户划分，不解决 §2.3 的多租户公平问题。这超出了 0036 的范围 |
| Context：「单设备服务上千用户、多 worker、滚动重启」作为动机 | — | 动机与 Consequences 不一致：后者说 sqlite 下的规模靠单进程，多进程只用于共存与滚动重启，横向扩展要靠 Postgres。也就是说，「多 worker」在 SQLite 下兑现不了，上千用户实际依赖单进程内的多租户能力（§2.3 第 1–6 条），而 0036 不涉及这部分 |
| 没有写进 0036 的前置条件 | 内核 | multi-process-storage-audit 列为必须先修的三项都没有出现在 Decision 或 Consequences 里：`unique_bytes` 跨进程撞号，会把别人的会话交给新用户（P1）；退役与非属主 append 的竞态，写入会落到墓碑下（S9）；首次建表竞态（S8）。不同进程的组装必须一致（E17）也没有提，而这一条正好与 ADR-0035 的「复用组装层」相关 |

## 4. 小结（判断，不是方案）

- 内核本身基本守住了边界。主要问题在于缺位：一层所有宿主共用的组装与数据约定只写在 CLI 二进制里，并且依赖 `set_current_dir`；ADR-0035 通过「依赖 CLI 的 lib target」暴露了这一点。
- web 多用户的硬阻断大多不在存储里，而在单个 `Asterpath` 的运行时模型里：没有用户维度，审批端点按进程计数，注册表在构造时固定且工具名全局唯一，provider 看不到会话，子进程继承整个进程的环境变量，事件只发给驱动者。ADR-0036 解决的是进程之间的问题，不覆盖这些。
- 0035 的路由工具方案没有考虑子会话不继承元数据，也没有覆盖 MCP 与沙箱策略按工作根解析；0036 的跨进程审批只覆盖了「决定」，没有覆盖「请求的发现」。
