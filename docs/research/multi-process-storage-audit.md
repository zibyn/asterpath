# 多进程存储审计：从「整库独占写者」到「每会话树一个属主进程」

> 日期：2026-10-25。用途：评估把写者独占粒度从整个数据库缩小到「每个会话（含子会话树）一个属主进程」是否可行。逐条列出内核里依赖「整个库只有本进程在写」或「所有会话都在本进程内存里」的地方，给出改动建议与工作量。新需求有三条：内核嵌入 web 平台、单设备服务上千用户、CLI 与桌面端同时打开同一个数据库。
>
> 来源：代码提交 `8783f2ac169a17a0b43113490d12e12bb6d5e95f`（main），只读审阅 `crates/asterpath`、`crates/asterpath-protocol`、`crates/asterpath-storage-sqlite`、`crates/asterpath-testing/src/{contract,memory}.rs`，对照 ADR-0005、0023、0028、0035 和 `docs/design/{storage,retention}.md`。文中的 SQLite 行为来自我对 SQLite 文档的了解，**没有跑过多进程实测**；标“需实测”的数字都要先跑基准再用。

## 0. 结论摘要

- **没有找到能推翻方案的硬阻断。** 内核读写都以 `SessionId` 为单位，`seq` 和去重按会话分配，等待、恢复和调度也是按会话（或按父子树）折叠日志。没有一处读写需要跨会话树的原子性（G6 本来就不承诺这一点）。
- **方案成立有一个前提：属主锁必须保护所有可能做恢复的入口。** 引擎把「drive 时发现 `TurnOpen`」直接当作上一个驱动者已经崩溃（`agent/mod.rs:227-229`、`245-253`），随后写入 `StreamResumed{Crash}`，并把未完成的不安全工具调用中断掉。今天这条推断成立，靠的是「只有一个进程」加进程内 `busy` 集合。改成多进程后，`drive`、`compact_now`、子会话驱动、`drain_child`，以及 CLI 的 `is_wakeable() → drive`，都必须先拿到会话树的属主锁。漏掉一个入口，就会把另一个进程正在跑的回合当成崩溃来恢复。
- **最大的工作量在非属主写入和跨进程信号上，不在存储。** 今天的 `approve`、`submit_input`、`update_config`、`cancel` 默认属主就在本进程：`cancel` 只改内存（`CancellationRegistry`），审批和子会话唤醒用的是进程内 `Notify`，`ApprovalGate` 的待审批列表也只在内存里。另一个进程调用这些方法时：`cancel` 静默无效；`approve` 能写进日志，但属主收不到唤醒，会一直挂起。
- **必须先修的正确性问题：`unique_bytes` 在进程之间不保证唯一**（`asterpath-protocol/src/time.rs:9-17`）。`SessionCreated` 又带去重键，一旦撞号，`create_session` 不会报错，而是返回别人的会话。多进程加上多用户会放大这个风险。修法是改成随机数，改动小。
- **SQLite 多进程写**：WAL 允许多个进程轮流写，但同一时刻只有一个写者。每秒几百次 append 在本地 SSD 上可行（需实测），主要风险在 `busy_timeout=0`、跨进程拿不到组提交、忙等退避带来的尾延迟、首次建表竞态。要承载「上千用户」，最好还是由**一个 web 进程**持有大部分会话；多进程主要用来让 CLI 和桌面端共存，不适合当作横向扩展手段。
- **`SessionStore` trait 的形状大体够 Postgres 用**（全是 async，按会话扫描，按会话游标）。还缺：属主声明 / 释放 / 防护（fencing）接口、可重试的冲突错误、跨进程变更通知、批量 append，以及一个能按「有未完成工作」筛选会话的查询（`wakeable` 现在要折叠全部会话，见 §3）。

## 1. 方案定义（本文的假设）

- **属主键 = 根会话 id。** 子会话的属主就是它所在树根的属主。理由：父会话在自己的进程内直接驱动子会话（`scheduler/spawn.rs:390`、`1065-1075`），R4 孤儿判定会读父会话日志（`agent/mod.rs:342-357`），退役时要检查父会话是否空闲（`storage-sqlite/src/lib.rs:441-446`）。这些都假设父和子在同一个进程里。
- 根会话可以沿 `SessionStore::parent` 链找到（调度器已经在这么做：`scheduler/spawn.rs:135-150`）。`fork_session` 生成的新会话没有父会话，自己成为新的根。
- 属主进程死亡后，锁由后端原生机制释放：SQLite 用 OS 文件锁，Postgres 用 advisory lock 绑定的连接。不使用心跳租约。

## 2. 逐条审计

工作量：小 ≈ 一个函数或几十行；中 ≈ 一个模块，要补测试；大 ≈ 跨模块，或要改契约 / ADR。

### 2.1 存储后端（`crates/asterpath-storage-sqlite/src/lib.rs`）

| # | 位置 | 当前假设 | 每会话属主下是否成立 / 要改什么 | 量 |
|---|---|---|---|---|
| S1 | `:12-13`、`:105`、`:719-750` `acquire_writer_lock` | 打开数据库时对 `<db>.lock` 加 `flock(LOCK_EX\|LOCK_NB)`，第二个进程得到 `WriterBusy` | **必须改。** 打开数据库时不再加整库锁，改为每个根会话一把锁。每会话一个 lock 文件意味着每个会话占一个 fd，上千会话会碰到 `ulimit -n`，不推荐。推荐**在一个 `<db>.owners` 文件上加 OFD 字节区间锁**（`F_OFD_SETLK`，Linux ≥3.15，锁跟着打开的文件描述走，不跟着进程走），偏移取 `hash(root_id)`；哈希碰撞只会带来假冲突，不影响正确性。macOS 没有 OFD 锁，POSIX `fcntl` 锁的归属是进程，同一进程关闭任一 fd 就会释放全部锁，所以要在进程内维护一张 `root → 引用计数` 表，只打开一次文件。Windows 用 `LockFileEx`。fork/exec 窗口的重试（`:723`）可以保留 | 中 |
| S2 | `:114-118`、`:213-263` 专用 writer 线程 + 组提交 | 本进程是唯一写者，组提交可以摊薄 fsync；`BEGIN IMMEDIATE` 不会遇到外部写者 | 线程模型可以保留。但组提交只能摊薄**本进程内**的写；另一个进程拿着 WAL 写锁时，`BEGIN IMMEDIATE` 会立刻 `SQLITE_BUSY`（见 S3），整批 64 条一起失败（`:255-258`）。要加忙等重试，并且只重试 `BEGIN` 这一步，不重放已经执行的 savepoint | 小 |
| S3 | `:714` `busy_timeout = 0`；`storage.md:187` | 不会有外部写者，所以遇到 BUSY 立即报错 | **必须改。** 多进程下 BUSY 是常态。至少要设 `busy_timeout`（例如 5 s），或在 writer 线程里自己做有上限的退避重试。`StorageError` 里应区分出可重试的 `Busy`，不要混进 `Backend` | 小 |
| S4 | `:327-333` `seq = MAX(seq)+1`，`:350` `log_id = last_insert_rowid()`（`AUTOINCREMENT`） | 单写者，`seq` 不会冲突 | **仍然成立。** SQLite 的写在所有进程之间串行（WAL 写锁），`BEGIN IMMEDIATE` 内的 `MAX+1` 不会被其他进程交错；`log_id` 全局单调，提交顺序就是 rowid 顺序，G3、G4 都保持。即使非属主写入同一会话（审批、输入）也安全 | 无 |
| S5 | `:312-325` 去重：先 SELECT，再 INSERT | 同上 | **SQLite 下仍然成立**（同一个写事务内，有 `UNIQUE(session_id,event_kind,dedup_key)` 兜底）。Postgres 不成立，见 §4 | 无 |
| S6 | `:351-365` 目录行 `INSERT OR REPLACE` 与日志在同一事务 | 同上 | 成立。append 返回之后，同一个进程用只读连接马上读就能读到（WAL 快照在语句开始时取）；其他进程的读也一样。注意：撞号的 `SessionCreated` 会被去重吞掉，见 P1 | 无 |
| S7 | `:107-111`、`:124` 单条只读连接，用 `Mutex` 保护 | 读负载小 | 多进程下仍然正确。但上千用户都在一个进程里时，所有 `scan`/`frontier`/`sessions` 都排在同一把锁上，而且是在 tokio 线程里同步执行。建议改成读连接池（与属主方案无关，但跟上千用户相关） | 小 |
| S8 | `:752-771` `migrate`：`user_version==0` 时用 DEFERRED 事务建表 | 打开数据库时没有其他进程 | **必须改。** 两个进程同时第一次打开数据库时，都会看到版本 0，后一个进程会失败（表已存在或 BUSY）。改成 `BEGIN IMMEDIATE` 之后再读一次 `user_version`。`PRAGMA journal_mode=WAL`（`:699-707`）也需要 busy 等待 | 小 |
| S9 | `:370-471` `retire_in`；`retention.md:99`「退役由单写者串行化；CAS 针对同进程内并发驱动，不是多进程」 | 退役与 append 由单写者串行 | 事务层面仍然原子（SQLite 跨进程串行）。但出现了新的竞态：会话折叠结果是 idle、但它的属主正准备写（例如另一个进程刚 `submit_input`，属主即将写 `TurnStarted`）时，退役先提交了。**`append_in` 不检查墓碑**，之后的写入会落到已退役的 id 下，所有读接口都看不见，等于静默丢数据（in-memory 后端 `memory.rs:157-210` 也一样）。要改：①退役必须持有该会话树的属主锁；②`append` 遇到墓碑返回 `NotFound` 一类的错误。②是一条 SQL，同时要更新契约套件 | 小～中 |
| S10 | `:112-113` 无界命令队列（已有 shortcut） | 单进程 | 与本方案无关；上千用户时 fsync 跟不上会让内存一直涨，应改为有界队列 | 小 |

### 2.2 协议层（`crates/asterpath-protocol`）

| # | 位置 | 当前假设 | 结论 / 要改什么 | 量 |
|---|---|---|---|---|
| P1 | `src/time.rs:9-17` `unique_bytes`：`(now ^ rotl(pid,17))`，后面接一个从 0 开始的进程内计数器 | 注释写的是“process-unique”；多进程同时生成 id 没有被认真考虑 | **必须改。** 当 `now1 ^ now2 == rotl(pid1^pid2, 17)` 且计数器值相同时，两个进程会撞号（启动时间差约 2^17 ms 的整数倍、计数器都还很小时，概率并不低）。`SessionId`、`TurnId`、`ToolCallId`、子会话 id 都用它生成。`SessionCreated` 带去重键（`lib.rs:266-274`），撞号时 append 返回已有的 `log_id`，`create_session` 会**把别人的会话交给新用户**。改成 128 位随机数或 UUIDv7（保留时间前缀）；测试里的确定性可以通过可注入的随机源保留 | 小 |
| P2 | `src/contracts.rs:181-237` `ApprovalGate`（ADR-0028：待审批只在进程内） | 能回应审批的一方，以及待审批列表，都在驱动会话的那个进程里 | 在属主进程内仍然成立。但 CLI 想审批桌面端的会话，或者 web 的另一个进程想审批，都看不到 `pending()`（列表只在属主内存），而 `is_open()` 也只统计属主进程注册的端点：属主进程没有注册端点时，会直接按 `NO_APPROVAL_ENDPOINT` 拒绝（`agent/pipeline.rs:804-806`），哪怕别的进程有人能审批。可选做法：(a) 宿主把审批路由到属主进程（不改内核，但宿主要能查到属主）；(b) 让 `ApprovalRequested` 变成持久记录，待审批就能从日志折叠出来（要改 ADR-0028/0007）。web 平台多半需要 (b)，或者至少需要一个“属主在哪个进程”的查询 | 中～大 |
| P3 | `src/agent.rs:200-241` `CancellationRegistry` | 驱动会话的进程就是调用 `cancel` 的进程 | **跨进程时不成立。** `SessionHandle::cancel`（`asterpath/src/lib.rs:692-695`）在非属主进程里是**静默的空操作**。需要：持久化的取消请求（新事件，例如 `CancelRequested`，属主轮询或接收通知后执行），或者由宿主路由到属主。至少要让非属主调用返回 `OwnedElsewhere`，不能假装成功 | 中 |
| P4 | `src/facts.rs:202-213` `is_idle` / `is_wakeable` | 纯日志折叠 | 成立（不依赖进程）。但它的含义变了：处在 `TurnOpen` 的会话，可能是“崩溃后待恢复”，也可能是“另一个进程正在跑”。光看日志分不出来，必须结合属主锁 | 无（语义交给 L3） |
| P5 | `src/contracts.rs:135-161` `SessionStore` | 没有任何属主 / 通知概念 | 见 §4 | 中 |
| P6 | `src/error.rs:8` `StorageError::WriterBusy` | 整库忙 | 改成或新增 `OwnedElsewhere { root }`，再加一个可重试的 `Busy` | 小 |

### 2.3 引擎（`crates/asterpath`）

| # | 位置 | 当前假设 | 结论 / 要改什么 | 量 |
|---|---|---|---|---|
| E1 | `agent/mod.rs:64`、`:149-154`、`:1167-1191` `busy: HashSet<SessionId>` 单飞 | 进程内互斥就是全局互斥 | **不够用。** `run` 拿到 `busy` 之后，还要拿根会话的属主锁（进程内引用计数，一棵树可能同时驱动父会话和若干子会话）。拿不到就返回 `AgentError::Busy`，或新增 `OwnedElsewhere` | 中 |
| E2 | `agent/mod.rs:177-232`、`:236-337` 恢复：drive 时发现 `TurnOpen` 就当作崩溃，写 `StreamResumed{Crash}`、`ModelCallAborted{Crash}`，不安全的工具调用改为 `Interrupt`（`pipeline.rs:794-798`） | 只有本进程会写，所以未完成的回合一定是崩溃留下的 | **前提就是 E1。** 只要属主锁覆盖所有驱动入口，这套逻辑一行都不用改；属主死亡、锁被释放后，接手的进程恢复也是正确的。如果锁有漏洞，就会两个进程同时写同一回合，日志直接坏掉。这一条是整个方案的核心不变量，应该进 ADR，并写一个多进程测试 | 无（依赖 E1） |
| E3 | `agent/mod.rs:1193-1223` `compact_now` | 用 `busy` 单飞 | 同 E1，也要拿属主锁 | 小 |
| E4 | `agent/mod.rs:342-357` R4 孤儿判定读父会话日志；`:362-434` `close_orphan` | 父子会话在同一进程 | 按“根会话属主”定义，成立 | 无 |
| E5 | `agent/mod.rs:1153-1163` `seq_of`：append 之后扫描找回自己写的那条 | append 之后立即读一定可见 | SQLite 成立；Postgres 要读主库（或同一连接） | 无 |
| E6 | `commit.rs:29-94` `CommitLog`：每个会话一个 `on_record` 游标，进程内存，永不回收 | 只有本进程写，`on_record` 恰好看到每条记录一次 | **语义变弱。** 每个进程只能看到“自己 append 时扫描到的那一段”，所以：①属主看不到非属主进程写入、之后又没被扫描到的记录（不过属主的下一次 append 会从游标处补扫，所以别的进程写的记录最终也会在属主这里出现）；②非属主写入时会先读 frontier，再显示自己写的那条，于是同一条记录可能在两个进程的 hook 里各出现一次。实时导出的宿主要按 `(session, log_id)` 去重；文档要把“恰好一次”改成“每个进程内恰好一次”。游标永不回收（已有 shortcut）在上千会话时会持续增长，释放属主时应顺手清理 | 小（文档）+ 小（回收） |
| E7 | `waits/mod.rs:175-207`、`:270-288` `Waiters`：进程内 `Notify`，`wait_until` 不轮询 | 写 `ApprovalDecided` / `ChildSessionCompleted` 的一方和等待方在同一进程 | 子会话完成：同一棵树、同一属主，成立。**审批：跨进程不成立**，非属主 `approve`（`lib.rs:699-708`）只能写日志，属主的 `await_approval` 永远收不到唤醒，直到下一次 drive。要给 `wait_until` 加一个有上限的轮询兜底（例如每秒查一次日志），或者接后端变更通知（§4） | 小（轮询）/ 中（通知） |
| E8 | `lib.rs:479-491` `submit_input`、`:504-564` `update_config` | 任何调用方都在本进程 | append 本身安全（S4、S5）。`update_config` 改工具集时先扫描再追加，有 TOCTOU；在同一进程里本来就有这个问题，多进程下更容易撞上，后果是工具重复加入（`ConfigChanged` 带 `UpdateId` 时会被去重）。唤醒属主是宿主的事（drive 文档里的 `Notify` 模式，`lib.rs:606-681`），这个模式在跨进程时失效，宿主需要通知机制（同 E7） | 小 |
| E9 | `lib.rs:379-399` `wakeable()`：每次调用都折叠全部会话（已有 shortcut） | 列出的会话都可以由本进程去驱动 | **语义要改：** 应该排除被别的活进程持有的树（或者调用方逐个尝试拿锁）。规模上：上千会话 × 完整日志扫描，在启动时代价很高。需要后端提供“有未完成意图的会话”索引，例如目录行上的一列 `open_intents`，在 append 时维护，可以从日志重建（与 ADR-0023「唤醒由折叠发现」冲突，要改 ADR） | 小（语义）/ 大（索引） |
| E10 | `lib.rs:347-354` `open_session`；CLI `interactive.rs:136`、`exec.rs:114`、`stream.rs:70` 发现 `is_wakeable()` 就去 drive | 一个会话只会在一个地方打开 | 打开只是读操作，没问题。CLI `--continue` 碰到桌面端正在跑的会话时，会进入“恢复”。有 E1 的锁之后会得到 `OwnedElsewhere`，CLI 要把它当作“只读旁观”处理（宿主的 UX，不归内核） | 小（宿主侧） |
| E11 | `lib.rs:285-333` `fork_session` | 只读源会话，两次 append 不是原子的（已有 shortcut） | 成立：只读源会话已提交的回合边界（`fork.rs`），新会话成为新的根，属主是发起 fork 的进程。非原子问题不变，批量 append 可以一并解决 | 无 |
| E12 | `scheduler/spawn.rs:40-41` `max_live_children`：**进程级**全局上限 | 全部会话都在本进程 | 含义变成“每进程上限”。web 单进程场景不受影响。如果要跨进程的全局上限，需要后端计数，不建议做；在文档里把它定义为进程级就行 | 小（文档） |
| E13 | `scheduler/spawn.rs:44-63`、`:445-493` `Admission` + `recovered` 集合；`:497-512` `prune_queue`；`:516-576` `admit`/`release_slot` | 准入状态只在内存，某个父会话一旦“已从日志重建”就不再重建 | 属主一直不变时成立。**属主迁移之后可能出错：** 进程 A 释放某棵树（idle 释放），进程 B 接手、派生、结束子会话，之后 A 再拿回这棵树，A 手里的 `recovered` 和 `running` 计数是过期的（正在运行的槽位泄漏，或少算）。释放属主时要清掉这棵树里每个父会话的 `Admission` 条目 | 小 |
| E14 | `scheduler/spawn.rs:390-438`、`:679-697`、`:1065-1075` 子会话由父会话进程用 `agent.run` 直接驱动；`Busy` 时等进程内唤醒（`:430-435`） | 子会话一定在本进程 | 成立的前提：子会话的 drive 也要走 E1 的根会话锁，并且父进程已经持有这把锁，进程内重入要能成功。注意 `:430-435`：如果 `Busy` 来自别的进程（违反了树属主约定），会一直等进程内唤醒；加上轮询兜底（E7）就能解决 | 小 |
| E15 | `agent/mod.rs:1335`、`protocol/src/dedup.rs:46-57` 压缩去重键 `compaction:{frontier log_id}` | 会话内的 `log_id` 单调 | 成立（S4）。Postgres 下要保证会话内 `log_id` 顺序和提交顺序一致（§4） | 无 |
| E16 | `views/usage.rs:24-111`、`views/trace.rs:29-41` | 当前没有缓存，全部是日志的纯折叠 | 成立，没有失效问题。上千用户时每次都要全量扫描，这是性能问题，不是正确性问题 | 无 |
| E17 | `tools/mod.rs:15` 工具表、provider 目录、compactor、审批策略、hooks 都是每进程各自组装 | 同一个会话一直由同一份组装来驱动 | 会话换属主（桌面端 ↔ CLI）时，接手进程可能缺少会话快照里的工具或 provider，结果是工具 `NotFound`，或者悬空的压缩意图因为没有 compactor 永远不重做（`agent/mod.rs:197-212`）。今天“按顺序换进程”时也会这样，多进程下更常见。宿主要保证共享同一个数据库的进程组装一致，或者在 `drive` 时校验并报错 | 小（校验）|

### 2.4 契约套件（`crates/asterpath-testing`）

| # | 位置 | 说明 | 量 |
|---|---|---|---|
| T1 | `contract.rs:351-383` G3、`:434-494` G4、`:496-526` G6、`:772-886` 退役 | 全部在单个 store 实例里串行执行，没有覆盖“两个实例（两个进程）写同一个库” | 加一个 `open_second()` 工厂方法，测：两个 store 交错写不同会话；两个 store 写同一会话（非属主写）时 seq 无洞；退役后 append 被拒绝（S9） | 中 |
| T2 | `storage-sqlite/tests/sqlite.rs:193` `second_process_gets_writer_busy` | 固化了整库独占这一旧语义 | 改写为：第二个进程可以写别的根会话，碰同一根会话时得到 `OwnedElsewhere`；持有锁的进程被 SIGKILL 后锁被释放 | 小 |
| T3 | `memory.rs:157-210` in-memory 后端 | 不检查墓碑（同 S9） | 与 S9 一起改 | 小 |

## 3. SQLite 多进程（WAL）在每秒数百次 append 下的可行性

**机制。** WAL 下读者之间、读者与写者之间互不阻塞，写者之间靠 WAL 写锁在**所有进程间串行**。`BEGIN IMMEDIATE` 一开始就去拿这把锁，拿不到就进入 busy handler。`synchronous=FULL` 下每次 `COMMIT` 都要 fsync 一次 WAL。

**吞吐估算（需实测）。** 上限大约是“每秒 fsync 次数 × 平均每次提交的条数”。本地 NVMe 的 fsync 大致在 0.1–1 ms，消费级 SATA SSD 或云盘在 1–5 ms 量级。只有一个进程时，组提交（`MAX_BATCH=64`）能把几百条 append 合进几十次 fsync，余量很大。多个进程时，每个进程只能合并自己队列里的命令，提交次数接近“各进程提交次数之和”。fsync 1 ms 时每秒约 1000 次提交，几百 append/s **还撑得住**；放到 fsync 5 ms 的盘上，余量就很紧了。仓库里已有 `tests/concurrency_bench.rs`（单进程），可以扩展成 N 个进程的版本来验证。

**主要风险：**
1. **`busy_timeout=0`（S3）**：只要有第二个写进程，就会马上出现 `Backend` 错误，回合直接失败。必须修。
2. **busy handler 不公平**：SQLite 默认的 busy handler 是睡眠退避（几毫秒起，逐步加到几十毫秒），不排队。高争用时会出现长尾延迟，甚至饥饿：持续写的进程可能一直压着偶尔写的进程。bundled 构建是否启用了 `HAVE_USLEEP` 需要核实；没有它时退避粒度是 1 秒。
3. **跨进程组提交失效**：见上面的吞吐估算。缓解办法：上千用户的负载集中在一个 web 进程里，CLI 和桌面端是低频的第二写者。
4. **长读者妨碍 checkpoint**：自动 checkpoint 由提交的写者触发；如果其他进程一直持有读快照，WAL 无法回卷，文件会持续变大。目前的读都是一次性 `collect`，持有时间很短，风险不大，但流式读要注意。
5. **首次建表竞态（S8）**：小改即可。
6. **只能在同一台主机上用**：WAL 的共享内存（`-shm`）不能放在网络文件系统上。“单设备”这个前提刚好满足。
7. **文件锁的可移植性（S1）**：Linux 有 OFD 锁可用；macOS 桌面端只能用 POSIX `fcntl` 锁，并且要在进程内做引用计数；不同平台要分别实现和测试。
8. **一个写错误会影响整批**：`COMMIT` 失败时整批 64 条都会收到错误（`:253`、`:260-262`）。多进程下 `COMMIT` 本身一般不会 BUSY（写锁已经拿到了），但 `BEGIN` 失败时整批都会被跳过，所以重试应该包在 `BEGIN` 外面。

**判断。** CLI 和桌面端共用一个库、偶尔并发写：**可行**，改动集中在 S1/S3/S8。“单设备上千用户”：一个 web 进程持有绝大多数会话树，单进程组提交已经够用，多进程不会带来吞吐收益。如果打算靠多个 web worker 进程分摊会话来扩展，SQLite 的单写锁就是天花板，应该改用 Postgres。

## 4. `SessionStore` 的形状够不够 Postgres 实现「每会话属主」

**已经够用的部分：** 所有方法都是 `BoxFuture`/`BoxStream`，适合 tokio-postgres 或 sqlx；读写都以会话为单位（`LogQuery.session`），没有跨会话的 `log_id` 扫描，所以 Postgres 序列“分配顺序 ≠ 提交顺序”的问题只需要在**会话内部**处理；`retire` 是可选方法，有默认实现。

**Postgres 实现要点（不需要改 trait）：**
- `seq` 与会话内的 `log_id` 顺序：在 append 事务里先锁住会话头行（`UPDATE session_heads SET seq = seq + 1 ... RETURNING seq`，或 `SELECT ... FOR UPDATE`），再分配 `log_id`，这样会话内 `log_id` 的顺序和提交顺序一致，G4（`scan(after)` 构成稳定前缀）得以保持。如果不锁，非属主写入（审批、输入）与属主写入并发时，可能出现 `seq` 唯一冲突，或者 `log_id` 和 `seq` 顺序相反导致游标漏读。
- 去重：用 `INSERT ... ON CONFLICT (session, kind, dedup_key) DO NOTHING RETURNING log_id`，没有返回行时再查一次，替代 SQLite 的“先 SELECT 再 INSERT”（S5 在 READ COMMITTED 下会出竞态）。
- 退役：锁住本会话、父会话的头行，以及“新增子会话”的入口（子会话的 `SessionCreated` 写入时要锁父会话头行），否则 `HasChildren` 检查有竞态。只要退役要求持有树属主锁，这个问题就自然没有了。
- append 之后立即读：必须读主库或同一个连接。

**缺少的部分（要改 trait 或 ADR）：**
1. **属主接口**：例如 `fn claim(&self, root: SessionId) -> BoxFuture<Result<OwnerGuard, StorageError>>`（drop 时释放），外加 `OwnedElsewhere` 错误。SQLite 用 OFD 锁实现；Postgres 用 `pg_try_advisory_lock(hash(root))`。
2. **防护（fencing）**：Postgres advisory lock 绑在**连接**上，网络抖动或连接重置都会让锁被静默释放，而原属主进程还活着、还在写，这样就有了两个属主。SQLite 的文件锁只会随进程死亡释放，没有这个问题。Postgres 有两个解法：①所有写都走持锁的那条连接，锁丢了写也就失败了，但会把各会话的写串行到一条连接上；②在头行上记录 `owner_epoch`，claim 时加一，append 时带上 `WHERE owner_epoch = $mine`，不匹配就返回 `OwnedElsewhere`。推荐②，它属于“后端原生机制加一个单调整数”，不是心跳租约。trait 需要能把 epoch 带到 append 上（例如 `OwnerGuard` 提供一个可以传给写操作的 token）。
3. **跨进程变更通知**：E7、E8、P3 都需要它。可以设计成 `fn watch(&self, session) -> BoxStream<()>`，Postgres 用 `LISTEN/NOTIFY` 实现，SQLite 轮询 `PRAGMA data_version` 或 frontier。不提供的话，引擎只能在等待处做有上限的轮询。
4. **可重试错误**：`StorageError` 只有 `Backend(String)`，宿主和引擎都分不出“序列化失败 / BUSY，可以重试”和“真坏了”。
5. **批量 append**：`fork_session` 的非原子 shortcut（`lib.rs:309`）已经提过这个需求；Postgres 下每次 append 一个事务，往返次数也更多。
6. **有未完成工作的会话查询**：见 E9。上千会话时 `wakeable()` 的全量折叠，放在 Postgres 上就是全表读。

## 5. 推荐的落地顺序

1. 无论选不选本方案都该先做：P1（随机 id）、S9（拒绝写入已退役会话）、S8（建表竞态）。
2. 属主机制：P5/P6（trait 加 `claim` 和错误）、S1（OFD 锁）、E1/E3/E14（所有驱动入口拿根会话锁）、E13（释放时清理准入状态）、T1/T2，再写一份 ADR 替代 ADR-0023 里“单进程独占写者”那条。
3. 跨进程信号：E7 轮询兜底（小），P3 非属主 `cancel` 返回错误（小），然后按需要做通知接口和持久化取消 / 审批（P2，要改 ADR-0028）。
4. 规模：S3 与忙等重试、S7 读连接池、E9 唤醒索引；Postgres 后端放到最后。
