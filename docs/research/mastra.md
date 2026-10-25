# Mastra 的多用户与多实例机制调研

调研日期：2026-10-25。源码基准：`mastra-ai/mastra` main 分支 commit `d49143a299757859bdbca93aa4b688c1383ace1e`（2026-10-25），下文记为 `@d49143a`。源码链接格式为 `https://github.com/mastra-ai/mastra/blob/d49143a299757859bdbca93aa4b688c1383ace1e/<路径>`。文档取自 `https://mastra.ai/docs/**.md` 当日版本。

标记说明：
- **[文档]**：官方文档原话或直接转述
- **[源码]**：读源码确认
- **[推断]**：根据源码或文档推出来的，没有跑过
- **[未核实]**：没查到，或者查了但没有定论

> 重要背景：Mastra 在 2026 年加了一大批多实例机制。Durable Agents 从 `@mastra/core@1.45.0` 开始提供，至今仍是 Beta；Workers 也是 Beta。所以凭旧印象说「Mastra 只是无状态请求加共享存储」已经不对了。

---

## 结论摘要

1. **多机协调的机制做在 core 里，基础设施交给可插拔后端。** 进程租约、属主 generation（相当于 fencing token，也就是我们说的属主纪元）、存储层原子条件写拒绝过期属主、跨进程信号与取消、可续传事件流，都在 `@mastra/core` 里实现。跨机传输靠用户选的 PubSub 后端（Redis Streams、Valkey、GCP Pub/Sub、Unix socket）和存储适配器。core 不自带分布式队列服务。[源码][文档]
2. **防双写有两层。** 存储能做 fencing 时（pg、libsql 等十几个适配器都支持），`mastra_workflow_run_owners` 表保存 `generation/ownerId/leaseExpiresAt`。claim 时 generation 加一。每次 workflow 写和 memory 写都带上 fence，在同一原子操作里校验，校验失败抛 `RunFenceConflictError`。存储不支持时退到 PubSub lease：只有互斥和存活判断，没有 generation，旧属主的写会落盘。[源码]
3. **用户/租户概念只有 `resourceId`（属主）和 `threadId`。** core 拿它们做存储列、列表过滤，并在 `stream()` 前做「thread 属于该 resource」的 fail-closed 断言。鉴权（认证、`mapUserToResourceId`、403 越权检查、FGA）在 `@mastra/server` 和 auth 提供方这一层。没配 `mapUserToResourceId` 时，任何已认证调用者都能读写任意 resourceId 的 thread。[文档][源码]
4. **同一 thread 的并发是有条件串行。** 走 signals 的路径（`sendMessage`、`subscribeToThread`、空闲唤醒）有跨进程 thread lease，保证同一 thread 至多一个活跃 run，新消息会并入当前 run 或排队。普通 `agent.stream()` 也会尝试拿这个 lease，但拿不到也照样运行（fail-open）。默认 PubSub 是进程内的，跨实例时这个互斥不存在。[源码][文档]
5. **挂起/恢复的状态存在 workflows 存储域（snapshot）。** 可以在另一台实例上通过 `listSuspendedRuns()` 和 `approveToolCall()`/`resume()` 恢复。崩溃后：普通 workflow 默认在服务启动时自动重启（`recovery.workflows: 'auto'`）。Durable agent 的自动恢复默认关闭（`'off'`），要显式打开 `'auto'`。恢复时会重放 LLM 调用和工具调用，文档要求工具必须幂等。[文档][源码]
6. **跨实例观察和取消是有的，但多数是 best-effort。** 可以用 `observe(runId)` 或 `subscribeToThread` 跨实例订阅，前提是 PubSub、replay cache（如 Redis）和存储都共享。跨实例 abort 通过 PubSub 转发给属主进程，HTTP 返回 `aborted: true` 只代表请求已发出，不代表对方已确认。普通 workflow 的 `cancel()` 只中止本地 AbortController 并改存储状态，另一台机器上正在跑的 step 不会被中止。[文档][源码]
7. **分包情况：** core 放全部协调机制；server（Hono 路由加 auth helper）放鉴权和越权检查；deployer/CLI 负责打包、优雅停机 drain、启动时恢复、`MASTRA_WORKERS` 拆分进程；Mastra Platform（原 Cloud）只是托管（托管 DB、Redis、dedicated worker），多机语义不靠它。[文档]
8. **外部 durable 引擎是可替换的执行引擎。** `@mastra/inngest` 把 workflow 映射成 Inngest function，每个 step 映射成 Inngest step，交由 Inngest 做 memoize、重试和 flow control（含按 userId 的 concurrency key）。`createInngestAgent()` 把 agent loop 也放到 Inngest 上跑，只持久化 `suspended` snapshot，durability 交给 Inngest 的重放。Temporal 适配还在实验阶段。[文档]

**对 asterpath 的参照意义（我的判断）：** Mastra 走过的路，是把「租约 + generation fencing + 跨进程信号/取消 + 可续传流」逐步收进内核。原因写在 `leases.md` 里：没有 claim 时，`recover()` 或自动恢复会让两个 execution 同时写同一个 snapshot、thread 和 topic（#23734）。它没有收进内核的是认证、`resourceId` 的来源映射、FGA、分布式队列服务和租户级 flow control。

---

## 1. 存储抽象

**后端**：文档列出 22 个 provider，包括 libSQL、PostgreSQL、MongoDB、MySQL、MSSQL、DynamoDB、Cloudflare D1/KV/DO、Convex、Spanner、Aurora DSQL、Redis、Upstash、Valkey、ClickHouse、DuckDB、LanceDB 等。还有 `MastraCompositeStore`，可以按域把数据路由到不同后端，例如 observability 走 ClickHouse。[文档] https://mastra.ai/docs/storage

**按域拆分**：
- 主要域有 `memory`（threads/messages/resources/working memory/observational memory）、`workflows`（run snapshot）、`observability`（traces/spans/metrics/logs）、`scores`、`datasets`、`experiments`、`backgroundTasks`、`schedules`、`threadState`、`workflowDefinitions`。[文档] https://mastra.ai/docs/storage ，https://mastra.ai/reference/storage/overview
- 表结构：`threads(id, resourceId NOT NULL, title, metadata, …)`，`messages(id, thread_id, resourceId, content JSON, role, createdAt)`，`resources(id, workingMemory, metadata)`，`workflow snapshot(workflow_name, run_id, snapshot jsonb, …)`。[文档] https://mastra.ai/reference/storage/overview
- 存储没有 append-only 事件日志：消息按行存，snapshot 整体覆盖写。[推断]

**接口**：`WorkflowsStorage` 包含 `persistWorkflowSnapshot`、`loadWorkflowSnapshot`、`updateWorkflowResults`、`updateWorkflowState`、`listWorkflowRuns`、`getWorkflowRunById`、`deleteWorkflowRunById`。另有能力探测方法 `supportsConcurrentUpdates()`、`supportsWorkflowStepGuards()`、`supportsRunFencing()`，以及租约方法 `claimRunOwnership`、`renewRunOwnership`、`releaseRunOwnership`、`getRunOwnership`。[源码] `packages/core/src/storage/domains/workflows/base.ts@d49143a`

**锁、条件写与版本号**：
- **有 fencing（属主 generation）**：见 `packages/core/src/storage/run-fencing.ts@d49143a`。
  - `RunFence = {runId, generation, ownerId}`。
  - 带 fence 的写只有在 fence 仍是当前 claim 时才生效，检查和写入在存储里原子完成，否则抛 `RunFenceConflictError` 且什么都不写。
  - 写操作通过 AsyncLocalStorage（`resolveRunFence()`）隐式携带 fence，调用方不用手动传。
  - 首次 claim 时 generation 取存储时钟（epoch ms），之后每次加一。这样记录被 retention 清掉后，新的 claim 仍然比旧的 fence 大。
  - [源码]
- **PG 实现**：`claimRunOwnership` 在事务里先 `SELECT … FOR UPDATE`，再 `UPDATE … SET generation = generation + 1 … AND ($4 IS NULL OR generation = $4)`。`expectedGeneration` 起 CAS 的作用。lease 到期用数据库时钟判断。[源码] `stores/pg/src/storage/domains/workflows/index.ts@d49143a` 约 L409–480
- **memory 域也做 fencing**：
  - 有 `raiseRunFence` 和 `retireRunFence`，表为 `mastra_memory_run_fences`。
  - 加 fence 的写：`saveThread`、`updateThread`、`patchThread`、消息写入等。
  - 不加 fence 的写：`deleteThread`、`copyThread`、`cloneThread` 等。
  - [源码] `packages/core/src/storage/domains/memory/base.ts@d49143a` L89–143
- **通用乐观并发或版本号：没有。** `updateThreadMetadata` 的 read-modify-write 只用进程内 Promise 队列串行，注释原文是「within this storage instance. Transaction-capable adapters may override this to provide cross-process atomicity」。[源码] 同文件约 L206–238
- **background task lease**：靠任务行上的 `ownerId`/`leaseExpiresAt` 做 compare-and-set（`updateTask(…, {expectedStatus, expectedOwnerId, expectedLeaseExpiresAt})`）。这里用 worker 自己的时钟，不是存储时钟。[源码] `packages/core/src/agent/durable/leases.md@d49143a`
- **支持 run fencing 的适配器**：InMemory、pg、libsql/turso、mysql、mssql、oracledb、spanner、dsql、cloudflare-d1、cloudflare DO、convex、mongodb（需 replica set）、dynamodb、redis、upstash、valkey。[文档] https://mastra.ai/docs/harness/durable-agents#run-ownership

---

## 2. 用户/租户概念

**`resourceId` 和 `threadId`**：
- 文档定义：resource 是「A stable identifier for the user or entity」，thread 是「isolates a specific conversation」。
- 「Each thread has an owner (`resourceId`) that can't be changed after creation」。
- [文档] https://mastra.ai/docs/memory/overview

**core 拿它们做什么**：
- **存列和过滤**：threads、messages、workflow snapshot 都带 resourceId 列。`listSuspendedRuns({threadId, resourceId})` 等接口按它过滤。[文档]
- **记忆隔离**：resource 级 working memory、observational memory、语义召回都按 resourceId 划分范围。subagent 委派时自动用新的 threadId，并从父 resourceId 派生子 resourceId。[文档] https://mastra.ai/docs/memory/overview
- **执行前 fail-closed 断言**：`assertThreadOwnedByResource`。已存在的 thread 和传入的 resourceId 不匹配时，在调模型前就抛 `AGENT_MEMORY_THREAD_RESOURCE_MISMATCH`。没有 resourceId 的旧行视为无主，放行。[源码] `packages/core/src/agent/memory-thread-ownership.ts@d49143a`
- **多人共享 thread**：官方做法是让所有参与者共用一个以会话为键的 resourceId（如 `doc_42`），说话人身份写进消息体（`<turn author_id=…>`），或者用 `MASTRA_MESSAGE_AUTHOR_KEY`/signal `attributes`。也就是说，core 没有「thread 成员」这样的 ACL 模型。[文档] https://mastra.ai/docs/memory/multi-user-threads ，https://mastra.ai/docs/server/request-context

**RequestContext（旧名 RuntimeContext）**：
- 它是单次请求的 KV 容器。instructions、model、tools、memory 都能写成 `({requestContext}) => …` 的函数，动态解析。[文档] https://mastra.ai/docs/server/request-context
- 这就是 per-request / per-tenant 的 provider 凭据机制：model 配置支持 `{id, apiKey, headers}`，并且「Each field also accepts a function of `requestContext`」。[文档] https://mastra.ai/models
- 没有内建的「按会话持久化凭据」存储。恢复 durable run 时凭据从哪里来，没有查到明确说法。[未核实]
- 保留键：
  - `MASTRA_RESOURCE_ID_KEY` 强制所有 memory 操作使用该 resourceId，server 校验访问的 thread 属于它，不属于时返回 403。
  - `MASTRA_THREAD_ID_KEY` 覆盖客户端传入的 threadId。
  - 还有 `MASTRA_SCOPES_KEY` 和 `MASTRA_MESSAGE_AUTHOR_KEY`。
  - `mastra__` 前缀的保留键不会写进 tracing 和 score。
  - [文档] https://mastra.ai/docs/server/request-context#reserved-keys ，https://mastra.ai/docs/guides/authentication-identity

**auth 放在哪一层**：
- 在 server 和部署层。`server.auth` 配置 provider：内建 Simple Auth/JWT，集成 Auth0、Clerk、WorkOS、Better Auth、Supabase 等。认证成功后，`@mastra/server` 调 `mapUserToResourceId(user)`，再写入 `MASTRA_RESOURCE_ID_KEY`。[源码] `packages/server/src/server/auth/helpers.ts@d49143a` L505–517
- 文档警告原文：「If `server.auth` is configured without `mapUserToResourceId` … any authenticated caller can read and write threads under any resource ID」。[文档] https://mastra.ai/docs/guides/authentication-identity
- FGA（细粒度授权，如 WorkOS FGA）对路由和 thread 做 `memory:read/write` 检查。但「FGA authorizes access … It doesn't automatically filter stored records」，要过滤存储记录得另开 `scope: true`。[文档] https://mastra.ai/docs/auth/fga
- 不配 auth 时所有路由都是公开的。[文档] https://mastra.ai/docs/auth/overview

---

## 3. 多实例与水平扩展

Mastra 不是「无状态请求 + 共享存储，并发全交给平台」那种模式。core 自己提供了下面这些机制：

| 机制 | 作用 | 存放位置 | 来源 |
|---|---|---|---|
| Run ownership claim（租约 + generation） | 一个 durable run 只由一个 execution 驱动，并拒绝过期写 | workflows 存储，存储不支持时退到 PubSub lease | [源码] `agent/durable/leases.md`、`execution-fence.ts` |
| Thread lease | 同一 memory thread 跨进程至多一个活跃 run | PubSub `LeaseProvider` | [源码] `agent/thread-stream-runtime.ts` |
| Thread claim lease | 一个进程独占地「持有」空闲 thread，负责为它启动 run | PubSub | [源码] |
| Evented step fence | broker 重投递时不重复执行 step | PubSub | [源码] |
| Background task lease | 一个后台工具调用只在一个 worker 上跑，worker 死后可被别人接手 | 任务行 CAS | [源码] |
| Workflow run ownership | 普通 workflow 的存活标记（不 fence 写），供其他副本启动恢复时跳过活跃 run | workflows 存储 | [源码] `packages/core/src/workflows/run-ownership.ts@d49143a` |

- **租约参数**：TTL 15s，每 TTL/3 续约一次，即 5s。可用 `MASTRA_AGENT_THREAD_LEASE_TTL_MS` 和 `MASTRA_AGENT_THREAD_LEASE_RENEW_INTERVAL_MS` 配置。[源码] `packages/core/src/agent/run-ownership.ts@d49143a`
  - 文档之间有矛盾：retention 参考页写的是「renews its claim every 10 seconds … 30-second lease」，和源码、durable-agents 页的 15s/5s 不一致。以源码为准。[文档] https://mastra.ai/reference/storage/retention
- **`LeaseProvider` 接口**：`acquireLease`、`renewLease`、`releaseLease`、`transferLease`、`getLeaseOwner`。实现方有 EventEmitter（进程内 map）、UnixSocket、redis-streams、valkey-streams。GCP Pub/Sub 只是 `NoopLeaseProvider`，每次 acquire 都成功，等于没有跨进程互斥。[源码] `packages/core/src/events/pubsub.ts@d49143a`、`leases.md`
- **丢失租约后**：
  - step 执行前后都要检查归属。
  - 正在进行的工具调用和模型调用通过 AbortSignal 协作取消。
  - `fencePubSub` 丢弃已失去归属的 execution 的发布。
  - 流事件带 generation，订阅方丢弃旧代事件。
  - 文档明确说「doesn't guarantee exactly-once」，工具必须幂等。
  - [源码][文档]
- **分布式执行和队列**：
  - 有 orchestration、scheduler、background task 三类 worker，用 `MASTRA_WORKERS` 拆分到不同进程。拆分部署需要 pull 模式的分布式 PubSub（Redis Streams、Valkey、GCP）加共享存储。
  - orchestration worker 通过 `MASTRA_STEP_EXECUTION_URL` 回调 API 进程执行 step。
  - 文档原文：「Don't run more than one scheduler instance」。scheduler 不做主节点选举，单例由用户自己保证。
  - [文档] https://mastra.ai/docs/deployment/workers
- **横向扩展的官方说法**：
  - 默认的 `EventEmitterPubSub`「suits a single instance」。
  - 「Use a distributed backend when you run more than one instance … sending a signal to an agent run requires the signal event to cross the process boundary to the instance that owns the run」。
  - 多副本都可以开 `recovery.durableAgents: 'auto'`，因为「only one claim succeeds per run」，前提是共享的 fencing 存储或共享 lease。两者都没有时，「gate recovery behind your own leader election」。
  - [文档] https://mastra.ai/docs/server/pubsub ，https://mastra.ai/docs/harness/durable-agents#multi-instance-deployments
- **普通 workflow 的启动恢复**：默认 `recovery.workflows: 'auto'`，启动时重启 RUNNING/WAITING 的 run。注释写道：「Set it to 'off' … when several replicas share one database and you recover runs another way」。在支持 fencing 的存储上，活跃 run 会因 `WORKFLOW_RESTART_RUN_OWNED` 被跳过。[源码] `packages/core/src/mastra/index.ts@d49143a` L661–720，`packages/core/src/workflows/workflow.ts@d49143a` L3456
  - 生产服务器入口具体在哪里调用这个启动钩子，我只找到 dev 路由 `/__restart-active-workflow-runs`（`packages/deployer/src/server/handlers/restart-active-runs.ts`）。生产环境的调用点 [未核实]。

---

## 4. 同一 thread 上两个并发 run

- **signals 路径会串行**：`sendMessage`/`sendSignal` 发给 thread 时，如果有活跃 run，就投递给它（模型在下一次请求时看到）。也可以 `queueMessage` 排到当前 run 之后，或者设成 persist/discard。thread 有已暂停的 run（等审批）时，新的 run 会收到 `blocked`。[文档] https://mastra.ai/docs/signals/sending-signals ，https://mastra.ai/docs/signals/thread-streams
- **跨进程依赖 thread lease**：`resourceId\0threadId` 作为键，run 作为 owner。严格注册（strict）拿不到 lease 时抛 `AgentThreadLeaseConflictError`。一个 run 结束、排队的下一个 run 接手时用 `transferLease`，中间不留空窗。[源码] `agent/durable/leases.md`、`agent/thread-stream-runtime.ts@d49143a` L4193–4216
- **普通 `agent.stream()` 是 fail-open**：
  - 非 strict 的 `registerRun` 会去 acquire thread lease，但「Fail-open on loss or error … proceed and never roll back the local registration」。连 acquire 抛错也当作已拿到。[源码] `thread-stream-runtime.ts@d49143a` 约 L4150–4170
  - 所以两个直接 `stream()` 同一 thread 的调用都会执行。lease 只用来让 signals 和存活判断串到它后面。[推断]
  - lease 续约失败时，「the current run is allowed to finish: a duplicate is preferred over dropped messages」。[源码] `leases.md`
- **单进程默认配置**：lease 只在进程内生效，跨实例没有任何互斥。[文档][源码]
- **thread lease 不 fence 写**。durable run 同时持有 run claim 和 thread lease，前者才拒绝过期写。[源码] `leases.md`

---

## 5. 挂起与恢复

- **状态存在 workflows 域的 snapshot 里**：
  - agent 的工具审批和 `suspend()` 都依赖 snapshot，不配存储会报「snapshot not found」。
  - agent run 的 snapshot 是「minimal resume artifacts … deleted once the run finishes」。
  - [文档] https://mastra.ai/docs/agents/human-in-the-loop
  - workflow 的 `suspend()` 把状态写入 snapshot，「persist across deployments and application restarts」。[文档] https://mastra.ai/docs/workflows/suspend-and-resume
- **可以在另一台实例上 resume**：文档专门有「Resuming after a restart」一节：「approval often arrives … on a different server instance behind a load balancer」。用 `agent.listSuspendedRuns({threadId, resourceId})` 从存储重新找到 run（`@mastra/core@1.43.0` 起），再 `approveToolCall`/`declineToolCall`/`resumeStream`。HTTP 端是 `GET /agents/:agentId/suspended-runs`。`sendToolApproval()` 在内存中找不到 run 时会自动查存储。[文档] https://mastra.ai/docs/agents/human-in-the-loop#resuming-after-a-restart ，https://mastra.ai/reference/agents/listSuspendedRuns
- **跨实例并发 resume 的防护**：durable agent 的 `resume()` 也要先 claim run。claim 被别人持有时等待最多 5s，然后抛 `DURABLE_AGENT_EXECUTION_CONFLICT`。[文档] https://mastra.ai/docs/harness/durable-agents#run-ownership
  - 普通（非 durable）agent 两台实例同时 approve 同一个 runId 会怎样，没有查到防护。[未核实]
- **崩溃时进行中的 run**：
  - **普通 `agent.stream()`**：「can't resume after its server process exits … treat the turn as interrupted」。[文档] https://mastra.ai/docs/deployment/mastra-server
  - **普通 workflow**：默认启动时自动 `restart()` 活跃 run（`recovery.workflows: 'auto'`）。`Run.restart()` 的用途是恢复「an active workflow run that lost connection to the server」，从最后一个活跃 step 继续。[源码][文档] https://mastra.ai/reference/workflows/run-methods/restart
  - **Durable agent**：
    - 默认 `recovery.durableAgents: 'off'`，并且这时不写每一步的 `running` checkpoint，只持久化 pending/paused/suspended。开 `'auto'` 后每一步都 checkpoint，启动时 `recoverAllDurableAgents()` 认领孤儿 run 并重新驱动。
    - 「can reissue LLM calls (with real cost) and re-execute tool calls」。
    - 优雅停机时会 drain，之后释放 claim，下一次启动立即可以恢复。
    - `recover(runId, {force: true})` 可以强制接管仍活着的 run。
    - [文档] https://mastra.ai/docs/harness/durable-agents#crash-recovery
  - **Inngest agent**：只持久化 suspended snapshot，durability 交给 Inngest 重放。[文档] https://mastra.ai/reference/agents/inngest-agent

---

## 6. 跨实例观察、流式订阅与取消

- **durable run 的观察**：`durableAgent.observe(runId, {offset?})` 先重放缓存的事件，再接上实时流。observe 不占有 run（「Observing doesn't own the run」）。跨进程需要「share the replay cache, PubSub transport, and run storage」，cache 用 Redis 之类。默认 cache 在进程内，run 结束后默认 30s 清理，过后无法重放。[文档] https://mastra.ai/docs/harness/durable-agents#resumable-streams
- **thread 流**：
  - `subscribeToThread({resourceId, threadId})` 是长期订阅，跨多个 run，会重放待处理的审批和挂起，可选附带初始历史。
  - 「A reconnect … can't recover events held only in another process's in-memory cache」。
  - [文档] https://mastra.ai/docs/signals/thread-streams
- **PubSub 是公开的扩展点**：
  - 默认 `EventEmitterPubSub` 只在单进程内。`UnixSocketPubSub` 覆盖单机多进程，并有 broker 选举。`RedisStreamsPubSub` 和 `GoogleCloudPubSub` 跨主机，并持久化事件以便重投递。
  - `CachingPubSub` 给进程内后端加上重放能力。
  - [文档] https://mastra.ai/docs/server/pubsub
- **跨实例取消**：
  - `POST /api/agents/:agentId/threads/abort` 原文：「forwarded to the active owner when necessary. For a remote owner, `aborted: true` means the request was sent, not acknowledged」。
  - 取消排队信号也是「best-effort and asynchronous」。
  - [文档] https://mastra.ai/reference/server/routes
  - 「With shared PubSub, the clear request reaches the owning process but clears only its local queue」。[文档] https://mastra.ai/docs/signals/sending-signals
- **普通 workflow 的 `Run.cancel()`**：
  - 实现是 `this.abortController.abort()` 加 `updateWorkflowState({status:'canceled'})`。[源码] `packages/core/src/workflows/workflow.ts@d49143a` L3995–4021
  - 在另一台实例调用时，只有存储状态变了，正在执行的 step 不会收到 abort。[推断]
  - Inngest 引擎的 cancel 走 `inngest.send({name: 'cancel.workflow.<id>'})`，由 Inngest 跨机取消。[源码] `workflows/inngest/src/run.ts@d49143a` L250–272
- **协作式取消**：只能通过 `abortSignal` 生效，不理会信号的工具会继续跑。[文档]

---

## 7. 分包

| 层 | 多用户相关 | 多机相关 |
|---|---|---|
| `@mastra/core` | resourceId/threadId 模型；thread 属主断言；RequestContext 与保留键；动态 model/凭据 | 存储域接口和 fencing 契约；LeaseProvider；run claim、thread lease、step fence、task lease；signals、thread stream；durable/evented agent；workflow 引擎；recovery；`shutdown()` drain；worker 抽象；`EventEmitter`/`UnixSocket`/`CachingPubSub` |
| `stores/*`（如 `@mastra/pg`、`@mastra/libsql`） | 按 resourceId 存列和过滤 | 实现 `supportsRunFencing`，即原子的 claim/renew 和带 fence 的写 |
| `@mastra/redis-streams` 等 | — | 跨主机 PubSub 加 lease；Redis cache 负责流重放 |
| `@mastra/server`（Hono 路由与 handler） | 认证接入、`mapUserToResourceId`、越权返回 403、FGA 路由检查、`scope` 存储过滤 | HTTP 的 abort、observe、approval 路由（只是转发到 core） |
| `@mastra/deployer` / CLI | 构建时把 auth 配置打进产物 | `mastra build` 生成 Hono 服务器；SIGTERM 时 drain（`server.drainTimeout`，默认 5s）；启动时恢复；`mastra worker build/start` 与 `MASTRA_WORKERS` |
| Mastra Platform（原 Cloud） | 托管 Studio auth | 托管 DB（Turso/Neon/Postgres）；有 `REDIS_URL` 时可开 dedicated worker 服务，否则后台任务在进程内跑；按区域部署。没有看到额外的多机语义，只是把上面这些组件托管起来。[文档] https://mastra.ai/docs/mastra-platform/deploy ，https://mastra.ai/docs/mastra-platform/server |

`AgentController`/`Session`（core）是给交互式产品用的会话宿主，「Each user or active task works through an isolated Session」。但 Session 的实时状态（审批、授权、活跃 run）「don't automatically survive process recreation」。[文档] https://mastra.ai/docs/harness/agent-controller

---

## 8. 与 Inngest 等外部 durable 引擎的集成

- **workflow 层**：`@mastra/inngest` 的 `serve()` 把每个 Mastra workflow 注册成一个 Inngest function，`createStep` 映射成 Inngest step。Inngest 负责 memoize 每一步的结果，重试和 resume 时跳过已完成的步骤。step 状态仍然记在 Mastra storage 里。[文档] https://mastra.ai/integrations/deploy/inngest
- **flow control 交给 Inngest**：`createWorkflow({concurrency: {limit, key: 'event.data.userId'}})`，还有 rate limit、throttle、debounce、priority。这是唯一能看到「按用户限流」由框架配置、但由外部引擎执行的地方。另有 `connect()` 模式，worker 可以独立扩缩，配合 `maxWorkerConcurrency`。[文档] 同上
- **agent 层**：`createInngestAgent({agent, inngest})` 把 agent loop 放到 Inngest 上执行，每个工具调用是一个可独立重试的 memoized step。流默认走 `InngestPubSub`（Inngest Realtime，跨进程可用），可以接 cache 做重放。`shouldPersistSnapshot` 会被忽略，只存 suspended snapshot，供人工审批后恢复使用。[文档] https://mastra.ai/reference/agents/inngest-agent
- **取消**：用 Inngest 事件 `cancel.workflow.<id>`。[源码] `workflows/inngest/src/run.ts@d49143a`
- **Temporal**：`@mastra/temporal`，每个 step 映射成一个 activity，标注「experimental and not ready for production」。[文档] https://mastra.ai/docs/deployment/workflow-runners
- **集成方式**：可替换的执行引擎（`engineType`）。启动时自动重启只对 `engineType === 'default'` 生效，其他引擎的恢复交给外部引擎自己。[源码] `packages/core/src/workflows/workflow.ts@d49143a` L3456–3460

---

## 来源

**官方文档**（均为 2026-10-25 抓取的 `.md` 版本）：
- https://mastra.ai/llms.txt
- https://mastra.ai/docs/storage
- https://mastra.ai/reference/storage/overview
- https://mastra.ai/reference/storage/retention
- https://mastra.ai/docs/memory/overview
- https://mastra.ai/docs/memory/multi-user-threads
- https://mastra.ai/docs/server/request-context
- https://mastra.ai/docs/server/pubsub
- https://mastra.ai/docs/auth/overview
- https://mastra.ai/docs/auth/fga
- https://mastra.ai/docs/guides/authentication-identity
- https://mastra.ai/models
- https://mastra.ai/docs/harness/overview
- https://mastra.ai/docs/harness/durable-agents
- https://mastra.ai/docs/harness/agent-controller
- https://mastra.ai/docs/signals/sending-signals
- https://mastra.ai/docs/signals/thread-streams
- https://mastra.ai/docs/agents/human-in-the-loop
- https://mastra.ai/reference/agents/listSuspendedRuns
- https://mastra.ai/docs/workflows/suspend-and-resume
- https://mastra.ai/reference/workflows/run-methods/cancel
- https://mastra.ai/reference/workflows/run-methods/restart
- https://mastra.ai/reference/server/routes
- https://mastra.ai/docs/deployment/workers
- https://mastra.ai/docs/deployment/mastra-server
- https://mastra.ai/docs/deployment/workflow-runners
- https://mastra.ai/integrations/deploy/inngest
- https://mastra.ai/reference/agents/inngest-agent
- https://mastra.ai/docs/mastra-platform/deploy
- https://mastra.ai/docs/mastra-platform/server

**源码**（`mastra-ai/mastra` main 分支 `@d49143a299757859bdbca93aa4b688c1383ace1e`）：
- `packages/core/src/agent/durable/leases.md`：所有租约的设计说明，最关键的一份
- `packages/core/src/storage/run-fencing.ts`：fence 契约与 `RunFenceConflictError`
- `packages/core/src/storage/domains/workflows/base.ts`：claim/renew/release 的语义
- `packages/core/src/storage/domains/memory/base.ts`：memory fence；进程内的 metadata 串行
- `stores/pg/src/storage/domains/workflows/index.ts`：`FOR UPDATE` 加 generation CAS
- `packages/core/src/agent/run-ownership.ts`：TTL 与续约间隔
- `packages/core/src/agent/memory-thread-ownership.ts`：thread 属主断言
- `packages/core/src/agent/run-lease-fence.ts`
- `packages/core/src/agent/thread-stream-runtime.ts`：thread lease，非 strict 时 fail-open
- `packages/core/src/workflows/run-ownership.ts`
- `packages/core/src/workflows/workflow.ts`：`cancel()`、`restartAllActiveWorkflowRuns()`
- `packages/core/src/mastra/index.ts`：`MastraRecoveryConfig` 及默认值
- `packages/core/src/events/pubsub.ts`：`LeaseProvider`
- `packages/server/src/server/auth/helpers.ts`：`mapUserToResourceId` 写入 `MASTRA_RESOURCE_ID_KEY`
- `packages/deployer/src/server/index.ts`、`packages/deployer/src/server/handlers/restart-active-runs.ts`
- `workflows/inngest/src/run.ts`

**未查阅**：官方博客和 changelog（这次没读）；`packages/core/src/agent/durable/execution-fence.ts` 已取回，但没有逐行读。
