# Agno 的多用户 / 多实例机制调研

调研对象：agno-agi/agno，源码看的是 tag **v3.1.10**（2026-10-23，commit `04330571`）；文档是 docs.agno.com 在 2026-10-25 的版本（v3 文档）。
标注约定：**[文档]** 表示文档明说；**[源码]** 表示从源码推断；**[未核实]** 表示没找到一手依据。
源码链接前缀：`https://github.com/agno-agi/agno/blob/v3.1.10/libs/agno/agno/`，下文简写为 `src:`。

## 结论摘要

1. **SDK 核心（`agno.agent` / `agno.db`）基本是"无状态执行体 + 共享 db"。** 会话行用 upsert 写入，后写覆盖先写，没有版本号；只有一个 `user_id` 属主守卫（`WHERE user_id = ? OR user_id IS NULL`）。同一 session 的两个进程内并发 run 不做任何串行化，`session_state`/summary 会丢更新。run 单独存行，彼此不互相覆盖。[源码]
2. **多机协调全部放在 AgentOS 层（同一个 pip 包里的 `agno.os` + `agno.job_queue`，需要 `agno[os]`），不在 Agent 核心里，而且默认关闭。** 打开 `QueueConfig(durable=True)` 后得到一个以 db 表为载体的作业队列：Postgres 用 `FOR UPDATE SKIP LOCKED` 认领作业，租约靠心跳维持，`attempt` 计数同时充当 fencing 纪元，job 行、run 行、事件流上的僵尸写一律拒绝。这与 asterpath 设想的"进程租约 / 属主纪元防双写"几乎同构。[文档+源码]
3. **跨实例取消和跨实例流式订阅靠 Redis，不靠存储。** 默认实现是进程内存（`InMemoryRunCancellationManager`、`InMemoryEventStream`）。多副本时需要 `QueueConfig(redis=...)`，它同时装上 `RedisRunCancellationManager` 和 `RedisEventStream`。不配 Redis 时文档要求做会话亲和（sticky）路由。轮询状态不受影响，因为 run 行在 db 里。[文档+源码]
4. **HITL 的暂停状态（run 行上的 requirements）落库，`continue_run(run_id=…)` 可以在任意进程从 db 读出来续跑。** 但只有 AgentOS 的 durable 队列路径才保证"一次批准只执行一次"：作业被 park 成 `paused`，continue 时把它翻回 `queued`，非队列的 continue 一律返回 409。纯 SDK 的 continue 没有认领、没有防双执行。崩溃恢复同样只在 durable 队列里有：默认 `max_attempts=1`，崩溃后标记 ERROR，不重跑；设 ≥2 才会由别的副本重跑整个 run。[文档+源码]
5. **用户 / 租户：** SDK 里的 `user_id` 只是列加过滤条件（记忆、会话、知识按它分区），外加 upsert 时的属主守卫。鉴权（JWT 验签、RBAC scope、`user_isolation`：用 JWT `sub` 强制覆盖 user_id、校验 run 归属）全部在 AgentOS 中间件里，而且 `user_isolation` 默认关闭。角色和 token 签发可以交给托管的 Control Plane（os.agno.com），也可以交给自有 IdP。[文档]
6. **同 session 的串行化文档与源码不一致。** durable-queue 文档仍写着"不提供 per-session 串行，应用自己协调"。但 v3.1.x 源码已加入 `QueueConfig.queue_per_session=True`（默认开，PR #9587，2026-10-13）：每个 session 至多一个 live 作业，暂停的 run 也占着 session，只在 durable 认领时生效，进程内路径不受控。[源码，文档过时]
7. **没有和 Temporal、DBOS 等外部 durable 引擎集成，也没有推荐过。** 官方选择是自己在 db 上实现队列、租约和 fencing。Agno 不保存副作用账本，要求工具自行幂等。[文档+源码，搜索为否定结果]
8. **对 asterpath 的参照意义：** Agno 把"租约 / 纪元 fencing / 跨进程审批续跑 / 跨实例取消 / 按用户过滤"做成了**运行时层的可选组件**。它放在嵌入式 Agent 核心之外，但和核心在同一个仓库、同一个包里。存储接口为此新增了条件写原语（`cancel_pending_run(expected_status)`、`update_approval(expected_status)`、`update_run_in_session(expected_attempt)`、`claim_job`、`claim_due_schedule`）。也就是说，机制由框架提供，但它没有进最小核心。

---

## 1. 存储抽象

- **后端 [文档]：** PostgresDb / AsyncPostgresDb、SqliteDb / AsyncSqliteDb、MySQLDb / AsyncMySQLDb、MongoDb / AsyncMongoDb、RedisDb、ValkeyDb、DynamoDb、FirestoreDb、SingleStoreDb、SurrealDB、GcsJsonDb、JsonDb、InMemoryDb；ClickHouse 只存 traces。文档明说"Backend capabilities differ"，启用调度、durable 作业前要先查能力。来源：https://docs.agno.com/features/storage 、https://docs.agno.com/database/providers/overview
- **接口范围 [文档+源码]：** 一个 `BaseDb` / `AsyncBaseDb` 覆盖全部域。表有 `agno_sessions`、`agno_runs`、`agno_memories`、`agno_learnings`、`agno_knowledge`、`agno_traces`/`agno_spans`、`agno_approvals`、`agno_schedules`/`agno_schedule_runs`、`agno_metrics`/`agno_eval_runs`、`agno_components`*、`agno_service_accounts`、`agno_jobs`；还有 auth token（`get_auth_token(provider, user_id, service)`）、authz 角色/策略、MCP OAuth、transcripts 等。来源：`src:db/base.py`（`class BaseDb` L299 起，方法 L558–L2640）。
- **会话写入是 upsert，没有版本号 [源码]：** Postgres 的 `upsert_session` 用 `INSERT … ON CONFLICT (session_id) DO UPDATE SET … WHERE user_id = :uid OR user_id IS NULL`，覆盖 `session_data`/`summary`/`metadata`。没有 version 列，没有 compare-and-swap。WHERE 子句只防"写进别人的 session"，并发写不防。来源：`src:db/postgres/postgres.py` L2045–L2150。
- **run 单独存表，v3 起 [源码+文档]：** `upsert_run` 是 O(1) 的单行写，`save_session` 只写会话行。来源：`src:db/base.py` L679、`src:agent/_session.py` L231–L260。
- **有条件写 / 锁的地方 [源码]：**
  - `cancel_pending_run(..., expected_status="PENDING")`：docstring 要求原子执行，并且"Unsupported adapters must fail rather than fall back to a read followed by an unconditional write"（`src:db/base.py` L640）。
  - `update_approval(approval_id, expected_status=…)`：原子状态守卫（`src:db/base.py` L2264）。文档要求管理员审批时带 `expected_status="pending"` 防竞争（https://docs.agno.com/hitl/approval ）。
  - `update_run_in_session(..., expected_attempt)`：`SELECT … FOR UPDATE`，attempt 落后时返回 `STALE_ATTEMPT`（僵尸写被 fence 掉），终态（completed/cancelled）不被覆盖（`TERMINAL_REFUSED`）（`src:db/postgres/postgres.py` L7584–L7640）。
  - `claim_job` / `complete_job` / 失败重试：`FOR UPDATE SKIP LOCKED` 认领，`attempt+1` 当 fencing generation，终态写按 `(locked_by, attempt)` fence（`src:db/postgres/postgres.py` L7576–L8050）。
  - `claim_due_schedule`：`UPDATE … RETURNING` 加子查询 `SKIP LOCKED` 的租约（`src:db/postgres/postgres.py` ≈L7400–L7430；接口在 `src:db/base.py` L2125）。
  - 组件配置有版本号乐观并发（`ComponentVersionConflictError`、`current_version_guard_clause`，`src:db/base.py` L96–L135），但这只针对组件配置，不覆盖会话。
- **队列存储只支持 Postgres 和 Redis [文档]：** 其他 db 配 `durable=True` 会在启动时报错。session 存在非 Postgres 上时会失去 attempt fencing（文档 Warning）。来源：https://docs.agno.com/agent-os/background-execution/durable-queue

## 2. 用户 / 租户概念

- **ID [文档+源码]：** `user_id`、`session_id`、`run_id`、`agent_id`/`team_id`/`workflow_id`。`session_id` 是会话表唯一主键，`user_id` 不在复合主键里，只是一个可选的属主列（https://docs.agno.com/features/storage ）。
- **SDK 怎么用 `user_id` [源码+文档]：**
  - 查询过滤：`get_session(s)`、`get_runs`、`get_user_memories`、知识、learnings、schedule 等都接受 `user_id` 参数（`src:db/base.py`）。
  - 记忆按 user 分区：文档示例里用 `asyncio.gather` 让多个 user/session 并发跑同一个 agent（https://docs.agno.com/memory/agent/multi-user-multi-session-chat-concurrent ）。
  - upsert 时有属主守卫，见第 1 节。
  - SDK 层没有鉴权，`user_id` 由调用方传入。
- **AgentOS 的 auth [文档]：**
  - `authorization=True` 会验签 JWT（`JWT_VERIFICATION_KEY` / JWKS / 多 issuer），并按 scope 控制端点（`agents:<id>:run`、`agent_os:admin` 等）。
  - 角色可以在托管 Control Plane 里定义（owner/admin/member，Enterprise 可自定义），也可以由自有 IdP 定义。网络层的限流、WAF、mTLS 交给反向代理。
  - `user_isolation=True` 默认关。打开后，非 admin 调用者的读写一律按 JWT `sub` 强制成 `user_id`；cancel/resume/continue 需要 `session_id`，并校验 run 归属。自定义路由要自己调用 scope helper。
  - 每个请求 `deep_copy()` 一份 agent。model、db、knowledge、MCP 句柄按引用共享，必须并发安全。
  - 来源：https://docs.agno.com/features/security-and-auth 、https://docs.agno.com/agent-os/security/authorization/user-isolation
- **per-session / per-user provider 凭据 [源码+未核实]：**
  - 存储层有 `get_auth_token(provider, user_id, service)` / `upsert_auth_token`，按 (provider, user, service) 存 OAuth token，实际用于部分工具，例如 `src:tools/xai_auth.py`。
  - 模型 API key 是否有 per-session 注入机制，没有找到专门文档。可行路径是 `dependencies` / `run_context` 回调（https://docs.agno.com 中 "Static vs per-request dependencies" 一节）或请求级 deep copy。**[未核实]**

## 3. 多实例 / 水平扩展

- **默认是单副本假设 [文档]：**
  - 不开 durable 时，后台 run 活在接受它的副本内存里，进程死了"every waiting and in-flight run on it is lost, and nothing marks them as failed"。
  - 不配 Redis 时，`/resume` 和 `/cancel` 只能在持有 run 的副本上用。
  - Railway 模板固定 `numReplicas: 1`，并提示扩副本前先配置共享运行时状态。
  - 来源：https://docs.agno.com/agent-os/background-execution/overview 、https://docs.agno.com/agent-platform/run-railway
- **官方多副本方案有三件套 [文档]：**
  - `db`（Postgres）是真相源，存 sessions、run 行和 `agno_jobs`。
  - `QueueConfig(durable=True)` 提供作业表：每个副本一个 worker 轮询认领（默认 1s，本地入队会立即唤醒），租约靠心跳维持（`lock_grace_seconds=60`，每 1/3 心跳一次），sweep 处理失联作业，`deployment_id` 做认领亲和，有 `Idempotency-Key`，`max_concurrency` 按副本计、`max_queue_depth` 按整个 fleet 计。
  - `QueueConfig(redis=...)` 负责事件流加取消信号，属于临时协调状态。
  - 文档明确要求各副本的时间参数完全一致。
  - 来源：https://docs.agno.com/agent-os/background-execution/durable-queue 、https://docs.agno.com/agent-os/background-execution/multi-replica 、https://docs.agno.com/agent-os/introduction
- **源码印证 [源码]：** `QueueWorker` 的 docstring 写着"One worker per AgentOS replica. SKIP LOCKED claiming arbitrates between replicas with zero coordination…heartbeats…sweeps…drains on stop…wakes on local enqueue"（`src:os/job_queue.py` L617–L631）。队列配置见 `src:job_queue/config.py`。
- **Scheduler [文档]：** 每个副本都可以跑调度循环，靠 db 租约（`claim_due_schedule`）保证同一到期任务只执行一次，支持 Postgres、SQLite、MongoDB（https://docs.agno.com/features/scheduling ）。
- **限流 [文档]：** Public Surface 的配额计数存在共享 Postgres 里；"Shared rate counters do not make active-run capacity global"（https://docs.agno.com/agent-os/public-surface ）。
- **官方对平台的要求 [文档]：** "Use an application-owned operation ID for each business mutation and serialize turns within a session across your application replicas … Test recovery with shared Postgres and Redis before relying on multi-replica behavior"（https://docs.agno.com/use-cases/harnesses-as-a-service ）。

## 4. 同一 session 上两个并发 run

- **SDK 进程内（`agent.run` / `arun`）没有防护 [源码]：**
  - 每次 run 读会话（`cache_session` 默认 False，见 `src:agent/agent.py` L110），结束时 `upsert_session` 覆盖会话行。
  - 两个并发 run 会各自基于旧快照写回 `session_data`（含 `session_state`）和 summary，后写者胜，丢更新。
  - run 行按 `run_id` 分开存，所以两个 run 的记录本身都在。`run_index` 取 `max(run_index)+1`，并发时是否可能撞号**[未核实]**。
  - 文档里没有找到关于同 session 并发的说明。
- **AgentOS durable 队列 [源码 vs 文档冲突]：**
  - 文档（durable-queue#session-serialization）写的是"does not provide a `serialize_sessions` option … Several accepted runs for one session can execute concurrently … coordinate those submissions in your application"；hitl-continuations 页也写"A paused run does not serialize other submissions to its session"。
  - v3.1.10 源码的行为是另一套：`QueueConfig.queue_per_session: bool = True`，注释为"At most one live job per session, in submission order. A head that cannot run holds its session: a PAUSED run until it is continued or cancelled … at durable-queue claim time only; the in-process path is not gated"。Postgres 认领时用 `pg_try_advisory_xact_lock` 复查 session 锁。外部 agent（Claude/Codex）组件设置了 `one_turn_per_session`，会被强制串行。
  - 来源：`src:job_queue/config.py` L164–L172、`src:job_queue/session.py`、`src:os/job_queue.py` L327–L340、`src:db/postgres/postgres.py` L7862–L7935；引入 PR 是 "feat: queue per session - at most one live run per session (head-of-line durable claims) (#9587)"，2026-10-13。
  - 结论：串行化只在 durable 认领这一点生效，库层面仍不保证。

## 5. 挂起 / 恢复（HITL）

- **类型 [文档]：** user confirmation、user input、dynamic user input、external tool execution、requires approval / audit（审批写入 `agno_approvals`）（https://docs.agno.com/hitl/overview ）。
- **状态存哪 [文档+源码]：** 暂停时 run 状态 `PAUSED`，requirements（工具调用、参数）跟着 run 落库到 run 行。审批类另有 `agno_approvals` 记录。文档："Continuation by run ID requires the same database and session identity"。源码 `continue_run_dispatch` 通过 `run_id` 从会话或存储里取出 run，找不到就抛 `RunNotFoundError`，已取消的 run 抛 `RunNotContinuableError`（`src:agent/_run.py` L3285 起，L3466–L3473）。所以只要共享 db，**另一进程可以 continue**。
- **防双执行只在 AgentOS durable 路径 [文档]：**
  - 暂停的 durable 作业 park 为 `paused`，不受 retention 清理。
  - continue（必须 `background=true`）把同一作业翻回 `queued`，合并确认结果，由任意副本认领执行。
  - 重复点击会附着到第一次 continue 上。过渡窗口内返回 409 + `Retry-After: 1`。
  - 队列持有的 run 走 inline continue 一律 409，理由是"an inline continue could validate against the run row while a durable continue validated against the job … An approved tool would then execute twice"。查不到作业时返回 503，不会不经校验就继续。
  - 来源：https://docs.agno.com/agent-os/background-execution/hitl-continuations
  - 纯 SDK 的 `continue_run` 没有认领 / CAS 保护**[源码推断]**。
- **崩溃恢复 [文档]：**
  - 不开 durable：不恢复，也不标记失败。
  - 开 durable：`max_attempts=1`（默认）时，超过 `lock_grace_seconds` 没有心跳，就由其他副本 sweep 标记 run 为 `ERROR`，不重跑（at-most-once），需要手动 `POST /queue/jobs/{id}/requeue`。`max_attempts≥2` 时由其他副本**从头**重跑整个 run，同一个 run_id，旧 attempt 的迟到写被丢弃。
  - 没有"从中间某一步恢复"的 checkpoint 语义。工具调用会重复执行，文档要求工具幂等。
  - continuation 崩溃后只能 requeue，requeue 会重放同一批确认。
  - 来源：https://docs.agno.com/agent-os/background-execution/durable-queue

## 6. 跨实例观察 / 订阅 / 取消

- **取消是协作式的 [文档]：** `cancel_run()` 只"records cancellation intent"，run 在下一个检查点停止，不回滚副作用（https://docs.agno.com/run-cancellation/overview ）。
- **取消的实现 [源码]：** 全局 `_cancellation_manager` 默认是 `InMemoryRunCancellationManager()`，也就是进程内存（`src:run/cancel.py`）。可以换成 `RedisRunCancellationManager`：Redis key 加 TTL（默认 1 天），`set … nx` 注册，`set "1"` 表示取消（`src:run/cancellation_management/redis_cancellation_manager.py`）。
- **存储侧取消 [源码]：** 另外有 `cancel_pending_run(expected_status)`，用条件写把 PENDING/RUNNING 的 run 行改成 CANCELLED（`src:db/base.py` L640）。durable 作业的 cancel 会把 job 和 run 行都改成 cancelled（hitl-continuations 页）。
- **流式订阅 [文档]：**
  - `background=True, stream=True` 时，事件写入进程内缓冲（每个 run 保留最近 10,000 条，带 `event_index`），客户端可以 `/resume` 重连。
  - 不在同一副本时，只能回放已持久化的事件然后关闭，**不能 tail 原副本上的实时任务**；`/cancel` 打到错误副本也停不了原任务。
  - 配 `QueueConfig(redis=...)` 后改用 `RedisEventStream`：每个 run 一个 stream key，最后活动 30 分钟后过期，约 1 万条。
  - 文档称实时视图是"best-effort"，以 run 行为准，事件序号只保证严格递增、不保证无缝。
  - v2 时代需要手动 `set_cancellation_manager`，而且事件流根本没有共享后端。
  - 来源：https://docs.agno.com/background-execution/overview 、https://docs.agno.com/agent-os/background-execution/multi-replica
- **变更通知 [未核实]：** 除 Redis 事件流外，没有发现基于 db 的变更通知或轮询器（类似 asterpath 的 poller）。跨实例查看状态只能轮询 `GET /runs/{run_id}`。

## 7. 分层：多用户 / 多机能力分别在哪

| 层 | 内容 | 多用户 / 多机相关能力 |
|---|---|---|
| SDK（`agno.agent`/`team`/`workflow`/`db`/`run`） | Agent 执行、存储适配器 | `user_id` 列与过滤、upsert 属主守卫、可插拔取消管理器（默认内存）、`continue_run` 从 db 读状态、存储条件写原语 |
| AgentOS（`agno.os` + `agno.job_queue`，同一 PyPI 包，`agno[os]` extra，FastAPI） | HTTP/SSE/MCP 运行时 | JWT/RBAC、`user_isolation`、请求级 deep copy、durable 队列（租约 / fencing / 重试 / 幂等 / per-session 串行）、Redis 事件流与取消、db 租约调度器、配额 |
| Control Plane（os.agno.com，托管） | 浏览器 UI，直接连到你的 AgentOS 端点 | 签发 JWT、管理角色（owner/admin/member）、查看会话、trace、审批；不承载执行 |

来源：https://docs.agno.com/agent-os/introduction （SDK / AgentOS / Control Plane 三层表）、https://docs.agno.com/agent-os/connect-your-os （"The Control Plane connects directly from your browser to the runtime endpoint"）、https://docs.agno.com/features/control-plane

另外：v3 的 `sandbox/postgres.py` 里有 session 与沙箱绑定的租约（`with_for_update` 加 lease），Sandbox API 要求"a durable queue with shared session admission"（`src:os/routers/agents/sandbox.py`）。这部分没有细看。**[未深究]**

## 8. 外部 durable 引擎

- 文档里搜 Temporal、DBOS、Restate、Inngest，结果为零。官方说的"durable execution"指的就是自家 `QueueConfig(durable=True)`（https://docs.agno.com/ 首页："Run those agents as a service with durable execution and multi-replica scaling"）。**[文档，否定结果]**
- 源码里没有 `temporalio` 等依赖或导入。GitHub issue 搜 "temporal" 只命中无关的 temporal memory 话题。**[源码+issue，否定结果]**
- 官方立场：Agno 不保存副作用账本，"If a tool must not run twice, make the tool itself idempotent"。崩溃时默认 at-most-once（durable-queue 页）。

## 来源

文档（访问于 2026-10-25）：
- https://docs.agno.com/features/storage
- https://docs.agno.com/database/providers/overview
- https://docs.agno.com/features/security-and-auth
- https://docs.agno.com/agent-os/security/authorization/user-isolation
- https://docs.agno.com/features/runtime
- https://docs.agno.com/agent-os/introduction
- https://docs.agno.com/agent-os/connect-your-os
- https://docs.agno.com/features/control-plane
- https://docs.agno.com/agent-os/background-execution/overview
- https://docs.agno.com/agent-os/background-execution/durable-queue
- https://docs.agno.com/agent-os/background-execution/multi-replica
- https://docs.agno.com/agent-os/background-execution/hitl-continuations
- https://docs.agno.com/background-execution/overview
- https://docs.agno.com/run-cancellation/overview
- https://docs.agno.com/run-cancellation/agent-cancel-run
- https://docs.agno.com/hitl/overview
- https://docs.agno.com/hitl/approval
- https://docs.agno.com/features/scheduling
- https://docs.agno.com/agent-os/scheduler/overview
- https://docs.agno.com/agent-os/public-surface
- https://docs.agno.com/agent-platform/run-railway
- https://docs.agno.com/use-cases/harnesses-as-a-service
- https://docs.agno.com/memory/agent/multi-user-multi-session-chat-concurrent
- https://docs.agno.com/llms-full.txt （全文检索用）

源码（tag v3.1.10，`https://github.com/agno-agi/agno/blob/v3.1.10/libs/agno/agno/` + 路径）：
- `db/base.py`（BaseDb 接口、`cancel_pending_run`、`update_approval`、`claim_due_schedule`、`get_auth_token`、组件版本守卫）
- `db/postgres/postgres.py`（`upsert_session` L2045、`upsert_run` L1125、`update_run_in_session` L7584、`claim_job` L7862、`complete_job` L7967）
- `job_queue/config.py`（`queue_per_session` L164–L172）、`job_queue/session.py`
- `os/job_queue.py`（`aenqueue_ticket` L327、`QueueWorker` L617）
- `os/routers/agents/sandbox.py`、`sandbox/postgres.py`
- `run/cancel.py`、`run/cancellation_management/{in_memory,redis}_cancellation_manager.py`
- `agent/_session.py`（`save_session` L231）、`agent/agent.py`（`cache_session` L110）、`agent/_run.py`（`continue_run_dispatch` L3285）

GitHub：
- Releases：https://github.com/agno-agi/agno/releases （v3.1.10，2026-10-23）
- 提交 "feat: queue per session - at most one live run per session (#9587)"（2026-10-13），通过 `gh api repos/agno-agi/agno/commits?path=libs/agno/agno/job_queue/config.py` 查到
