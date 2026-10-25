# LlamaIndex agent/workflow 体系的多用户与多机能力调研

调研日期：2026-10-25。标注说明：
- **[文档]**：官方文档明确写出
- **[源码]**：从源码读出或推断（给出文件路径和 commit）
- **[未核实]**：没有找到一手证据

源码版本：
- `run-llama/workflows-py` main @ `b409b50`（2026-10-23）。这个 monorepo 现在同时放着 `llama-index-workflows`（核心）、`llama-agents-server`（WorkflowServer）、`llama-agents-dbos`、`llama-agents-appserver`、`llama-agents-control-plane`、`llamactl` 和 Go operator。下文写作 `WF/<path>`，URL 前缀为 `https://github.com/run-llama/workflows-py/blob/b409b50/`
- `run-llama/llama_index` main @ `619d746`（2026-10-14），写作 `LI/<path>`，URL 前缀为 `https://github.com/run-llama/llama_index/blob/619d746/`
- `run-llama/llama_deploy`：main @ `c0ce080`（v0.9.2，README 写明 **deprecated**，指向 workflows-py），以及旧架构 tag `v0.6.1`，写作 `LD061/<path>`，URL 前缀为 `https://github.com/run-llama/llama_deploy/blob/v0.6.1/`

`llama_index.core.workflow.Context` 现在只是 `from workflows.context import Context` 的再导出，见 `LI/llama-index-core/llama_index/core/workflow/context.py`。所以 core agents（`AgentWorkflow`/`FunctionAgent`）的运行时就是 workflows 库 **[源码]**。

---

## 结论摘要

1. **核心库（`llama-index-workflows`）只管单进程**。它负责 Context 序列化（`to_dict/from_dict`），以及一个可插拔的 `Runtime`/适配器接口：`InternalRunAdapter` 跑在本进程，`ExternalRunAdapter` 可以跨进程。默认的 `BasicRuntime` 只在内存里运行。核心里没有用户、租户、run 归属、锁或租约。文档明说“没有内置 checkpointer”，持久化要用户自己在 `StepStateChanged` 时调 `to_dict()` 写库，语义是 at-least-once。**[文档][源码]**
2. **Context 和 chat store 都没有乐观并发控制**。Context 里的 `version` 字段是序列化格式版本，不是行版本号。`edit_state()` 的锁是进程内的，源码注释写明“其他进程或副本的写者不被串行化”。`PostgresChatStore` 是按 key upsert 的 JSON 数组，后写覆盖前写。`SQLAlchemyChatStore.set_messages` 先删后插，两步不在同一个事务里。**[源码]**
3. **多机能力不在核心，在 `llama-agents-dbos`，而且大部分交给 DBOS 实现**。每个副本由一个 `executor_id` 标识，副本**拥有自己启动的 run**，成为一个有状态分片：一个 run 的所有 step 都在同一进程执行，不在副本间分发。崩溃后只恢复本 `executor_id` 名下的 run。跨副本发事件和取消走 `DBOS.send/recv`，也就是 Postgres。准入队列跨副本共享。缩容后被移除副本的 run 会被“遗弃”，直到同一个 id 回来，或者 DBOS Conductor 通过心跳重新分配。**[文档][源码]**
4. **默认的 WorkflowServer（不带 DBOS）不支持多副本**：
   - `AbstractWorkflowStore` 的 docstring 写着“不支持多个并发运行的 server 共享同一个 store 实例”。**[源码]**
   - 启动时会恢复 store 里所有 `status=running` 的 handler，handler 上没有属主字段，只用一个 30 秒的“新建 handler 宽限期”启发式跳过刚建的。**[源码]**
   - `send_event` 发给本进程不活跃的 run 时，会从 ticks 重放并在本地重新拉起这个 run。这一步用的是进程内的 `KeyedLock`。**[源码]**
   - 推断：如果两个默认 server 共享 Postgres，同一个 run 可能被拉起两次。**[源码推断]**
5. **最接近 asterpath 设想的部件是 `llama-agents-dbos`，而且标着 experimental**：
   - `ExecutorLeaseManager`：Postgres 表 + `FOR UPDATE SKIP LOCKED` + holder uuid + 心跳 + 超时回收。丢了租约，runtime 就自己 `destroy()`。日志写入不带纪元或 fencing token。
   - 空闲释放和恢复用 `RunLifecycleLock`：`SELECT … FOR UPDATE` 加状态机 `active→releasing→released→resuming→active`，把 `updated_at` 当作 fencing token，防止过期的恢复者覆盖新属主。

   这些都在平台/runtime 插件层，不在核心。**[源码]**
6. **用户/租户在框架里只是字符串 key，没有权限语义**：
   - `chat_store_key` 和 `Memory(session_id=…)` 只用来分区存取。文档示例直接写 `chat_store_key="user1"`。**[文档]**
   - WorkflowServer 没有鉴权，只开放 Starlette `middleware=` 参数。**[源码]**
   - LlamaCloud 部署使用“和 LlamaCloud 一样的鉴权”，粒度是**项目级 API token**，没有终端用户的概念。**[文档]**
   - Agent Data 按 `deployment_name`/`collection` 归属，按项目授权。**[文档]**
7. **观察和挂起/恢复做在 server 层（WorkflowServer + store）**：
   - 事件写进 store，带单调的 `sequence`。SSE 支持 `Last-Event-ID` 和 `after_sequence` 游标续读，也支持 `"now"`。**[文档]**
   - `subscribe_events` 默认靠**轮询** `query_events`（`poll_interval`）。Postgres store 加了 `LISTEN/NOTIFY`，并在重连后补查。**[源码]**
   - HITL 状态是 tick 日志。空闲超时后 run 会被释放出内存，新事件到达时重放 ticks 恢复。核心库自己的 `handler.stream_events()` 只能消费一次。**[文档]**
8. **外部 durable 引擎的集成方式是“替换 Runtime”**。`DBOSRuntime` 实现了 core 的 `Runtime` ABC，WorkflowServer 用 `runtime=` 和 `workflow_store=runtime.create_workflow_store()` 接入。replay 依赖 reducer 的确定性，所以代码变更前要么先排空 in-flight run，要么换一个 `workflow_name`。仓库里没有 Temporal 集成，只有 AWS AgentCore 的打包，见 `WF/packages/llama-agents-agentcore`，未细看。**[文档][源码]**
9. **旧的 llama_deploy（≤0.6，控制面 + 消息队列）已经废弃**：
   - 当时有 control plane、Kafka/RabbitMQ/Redis/SQS/Solace 消息队列和 session。session 状态存在 `BaseKVStore`（Redis/Mongo）里，做的是无版本的读改写，配一个进程内的 `asyncio.Lock`。**[源码]**
   - 新架构是 control plane（K8s CRD）+ Go operator + 每个部署一个 appserver pod。operator 写死 `replicas := int32(1)`。**[源码]**
   - 也就是说，LlamaCloud 托管部署本身是单副本，横向扩展只在 DBOS 路线里有文档。

**对 asterpath 的参照意义（个人判断）**：LlamaIndex 把“进程租约/属主、跨进程事件和取消、变更通知、多副本恢复”都放在核心之外（server 包 + DBOS runtime 插件），核心只留一个跨进程友好的适配器边界（External/Internal adapter）和可重放的 tick 日志。用户过滤和凭据完全留给宿主平台。另外它的默认 server 在多副本下不安全，这正好是“机制不在内核时，默认实现容易漏防护”的反例。

---

## 1. 持久化抽象

**Context 序列化 [文档]**
- `Context.to_dict()` 序列化 in-flight 事件和 state store，`Context.from_dict(workflow, d)` 加 `run(ctx=…)` 可以在**另一个进程**继续执行。文档原话是“This survives a restart, but not a crash during the run”。
- 没有内置 checkpointer。要扛崩溃，得在 `stream_events(expose_internal=True)` 里遇到 `StepStateChanged(NOT_RUNNING)` 时快照。
- 恢复是 at-least-once：执行到一半的 step 会从头重跑，`num_workers` 个并发 step 最多会重跑这么多个。
- Resource（客户端、模型句柄）不进快照。
- 来源：https://developers.llamaindex.ai/python/llamaagents/workflows/durable_workflows/

**State store 与锁 [文档][源码]**
- `ctx.store.edit_state()` 让同一 run 内并发的 step 之间做原子的读改写（见 https://developers.llamaindex.ai/python/llamaagents/workflows/managing_state/ ）。
- 源码注释 `WF/packages/llama-index-workflows/src/workflows/context/state_store.py`（`StateStoreFacade` docstring）写明：“Writers serialize on the per-run lock… Writers in other processes or replicas are not serialized; cross-replica consistency requires backend-level atomicity.”
- **序列化版本 [源码]**：`WF/packages/llama-index-workflows/src/workflows/context/context_types.py` 里的 `version: int = CURRENT_SERIALIZED_VERSION` 是格式版本，用于迁移 V0 格式，不是并发版本号。

**Chat store / Memory [文档][源码]**
- 接口 `BaseChatStore` 按 `key` 做 `set/get/add/delete_messages`（`LI/llama-index-core/llama_index/core/storage/chat_store/base.py`）。新接口是 `AsyncDBChatStore`（`base_db.py`），多了 `archive_oldest_messages` 和消息状态。两套接口都没有版本参数或条件写参数。
- `Memory` 默认用 `SQLAlchemyChatStore(table_name="llama_index_memory")`，URI 默认是 `sqlite+aiosqlite:///:memory:`（`LI/.../storage/chat_store/sql.py`，`LI/.../memory/memory.py`）。文档写明默认是内存 SQLite，可以换成 `postgresql+asyncpg://…`（https://developers.llamaindex.ai/python/framework/module_guides/deploying/agents/memory/ ）。
- `SQLAlchemyChatStore.set_messages`：先调 `delete_messages(key)`，再在另一个 session 里插入。两步不在同一事务，并发调用时可能交错。**[源码推断]**
- `PostgresChatStore`（`LI/llama-index-integrations/storage/chat_store/llama-index-storage-chat-store-postgres/llama_index/storage/chat_store/postgres/base.py`）：一个 key 一行，值是 JSON 数组。`add_message` 用 `ON CONFLICT (key) DO UPDATE SET value = array_cat(...)`，单条语句是原子追加。`set_messages` 是 upsert 覆盖，后写者胜。没有版本号或 CAS。
- Agent 的 memory 对象放在 `ctx.store["memory"]` 里，随 Context 一起序列化（`LI/llama-index-core/llama_index/core/agent/workflow/base_agent.py` 约 296–301 行）。

**WorkflowServer 的 store [源码]**
- `AbstractWorkflowStore`（`WF/packages/llama-agents-server/src/llama_agents/server/_store/abstract_workflow_store.py`）的接口：`query/update/delete` handler 记录（`PersistentHandler`：status、run_id、result、idle_since），`append_event/query_events/subscribe_events`（带 sequence），`append_tick/get_ticks/stream_ticks`（可重放的 tick 日志），以及按 `(run_id, namespace)` 划分的 state store。
- `update(handler)` 是整行 upsert：`ON CONFLICT(handler_id) DO UPDATE`。`update_handler_status` 先 query 再改字段再 `update`，是非原子的读改写。整个接口没有条件写和版本号。
- docstring 原文：“One server binds one store instance… Sharing a store instance between concurrently running servers is not supported.”
- 实现有 Memory、SQLite、Postgres（`postgres_workflow_store.py`），以及 `AgentDataStore`（LlamaCloud Agent Data API，state store 在内存，靠 ticks 重建）。
- Postgres 的 `append_event`/`append_tick` 用 `COALESCE(MAX(sequence),-1)+1` 分配序号，遇到唯一约束冲突就**重试**。所以并发写者会被交错进同一条日志，而不是被拒绝，没有属主 fencing。**[源码推断]**
- 服务端文档：https://developers.llamaindex.ai/python/llamaagents/workflows/deployment/ （Persistence 一节：默认 `MemoryWorkflowStore`，单进程用 `SqliteWorkflowStore`，Postgres 加多副本用 DBOS）。

**DBOS durable [文档]**
- `llama-agents-dbos`：每次 step 迁移都写日志，崩溃后用同一个 `run_id` 再调 `run()` 就从断点继续。默认 SQLite，多副本用 Postgres。
- 来源：https://developers.llamaindex.ai/python/llamaagents/workflows/dbos/

## 2. 用户/租户概念

- **chat_store_key / session_id [文档]**：“Chat stores can organize sequences of chat messages by keys (like `user_ids`…)”，示例写的是 `chat_store_key="user1"`（https://developers.llamaindex.ai/python/framework/module_guides/storing/chat_stores/ ）。`Memory.from_defaults(session_id=…)` 默认取 uuid4（`LI/.../memory/memory.py: generate_chat_store_key`）。框架拿它只做存储分区。`BaseChatStoreMemory` 的 docstring 自称 “multi-tenant chat history”（`LI/.../memory/types.py`），但实际上只是 `chat_store_key` 字段，默认值为 `"chat_history"`。**[源码]**
- **workflows 核心**：没有 user/tenant 字段。`HandlerQuery` 只能按 `handler_id/run_id/workflow_name/status/is_idle` 过滤，没有“按用户列会话”。**[源码]**
- **鉴权层级**：
  - `WorkflowServer` 不带鉴权，只有 `middleware=`（默认是宽松 CORS），见 `WF/.../server/server.py` 和 `_api.py`。**[源码]**
  - llamactl/LlamaCloud：“During local development, the API is unprotected. After deployment, your API uses the same authorization as LlamaCloud. Create an API token in the same project as the agent”（https://developers.llamaindex.ai/python/llamaagents/llamactl/workflow-api/ ）。粒度是项目级 token。**[文档]**
  - appserver 代码里没找到 token 校验逻辑，推测在 LlamaCloud 网关做。**[未核实，网关闭源]**
  - control plane 的管理 API 按 `project_id` 校验部署归属（`WF/packages/llama-agents-control-plane/src/llama_agents/control_plane/manage_api/deployments_service.py`）。**[源码]**
- **Agent Data [文档]**：记录挂在 `deployment_name` + `collection` 下，“You can only read/write data for agents in projects you can access”（https://developers.llamaindex.ai/python/llamaagents/cloud/agent-data-overview/ ）。
- **per-session provider 凭据**：没有找到相关机制。LLM 客户端按惯例放进 Resource 或 workflow 实例里，由宿主自行注入。**[未核实，没有专门文档]**

## 3. 多实例/水平扩展

- **核心 [文档]**：`num_concurrent_runs` 的限制“restricts concurrency within a process”。跨 run 限流靠用户自己放 `asyncio.Semaphore`，“If you deploy multiple processes, each has its own.”（https://developers.llamaindex.ai/python/llamaagents/workflows/concurrent_execution/ ）
- **默认 WorkflowServer [源码]**：
  - `PersistenceDecorator._on_server_start`（`WF/packages/llama-agents-server/src/llama_agents/server/_runtime/persistence_runtime.py`）查询所有 `status_in=["running"], is_idle=False` 的 handler，逐个从 ticks 重放并 `workflow.run(...)`。只跳过 `started_at` 在启动前 30 秒内的（`RESUME_FRESH_HANDLER_GRACE`）以及本进程 `_active_run_ids` 里已有的。没有属主过滤。
  - `IdleReleaseDecorator`（`_runtime/idle_release_runtime.py`）：外部 `send_event` 时，如果 `run_id` 不在本进程 `_active_run_ids` 里，就在进程内 `KeyedLock` 下重放 ticks，把 run 在本地拉起。
  - 推断：多个默认 server 共享一个 Postgres store 时，会出现重复恢复或重复执行，官方也没把这种用法当作受支持的配置。**[源码推断]**
- **DBOS 路线 [文档][源码]**：
  - 每个副本需要唯一的 `executor_id`，并**拥有自己启动的所有 workflow**。“A workflow and all of its steps run in the same process — there is no distribution of individual steps across replicas.”
  - 副本 id 和副本数量要保持稳定。缩容时先 drain，否则 run 会被遗弃，直到同一个 id 回来，或者 DBOS Conductor 通过心跳把 in-flight 的 run 重新分配出去。
  - `num_concurrent_runs` 在 DBOS 下按副本计，等待中的 run 进入**跨副本共享的 DBOS 队列**，任何空闲副本都能接。
  - 来源：https://developers.llamaindex.ai/python/llamaagents/workflows/dbos/ 和 `WF/packages/llama-agents-dbos/ARCHITECTURE.md`（“stateful shard”“horizontal scaling works by adding replicas that each own a slice of the workload”）。
- **执行者租约（experimental）[源码]**：`WF/packages/llama-agents-dbos/src/llama_agents/dbos/executor_lease.py`
  - 机制：Postgres 表 `executor_leases`，有 `pool_size` 个槽。获取时 `SELECT … WHERE holder IS NULL OR heartbeat_at < NOW()-timeout … FOR UPDATE SKIP LOCKED`。心跳是 `UPDATE … WHERE slot_id=$1 AND holder=$2 RETURNING`，更新不到行就判定租约丢失。
  - 丢失后的处理：`runtime.py: _watch_lease` 记一条 `logger.critical("Executor lease lost — shutting down runtime")`，然后 `destroy()`，也就是靠自杀来自我隔离。日志写入没有带 fencing token。
  - 运维要求（runtime.py docstring）：副本数不能超过 `pool_size`，K8s 要设 `maxSurge: 0`。缩容到低于持有活跃 run 的副本数时，那些 run 会成为孤儿，直到租约过期被别的副本回收。
  - 用途是在普通 Deployment 上替代 StatefulSet 提供的稳定主机名。
- **托管平台 [源码]**：
  - 新 llama_deploy 架构（`WF/architecture-docs/overall-architecture.md`）：llamactl → Control Plane（FastAPI）→ `LlamaDeployment` CRD → Go Operator → appserver pod。
  - `WF/operator/internal/controller/resources.go` 写死 `replicas := int32(1)`（suspended 时为 0）。所以托管部署是单副本，平台层不做横向扩展。
  - operator 自己用 K8s `coordination.k8s.io/leases` 做 leader election（overall-architecture.md 的 Namespace Layout 一节）。
- **旧 llama_deploy v0.6 [源码]**：
  - 组件：control plane，`message_queues/` 下的 apache_kafka、rabbitmq、redis、aws、solace、simple，以及 services 和 orchestrators。
  - session 和 service 注册表存在 `BaseKVStore` 里（`LD061/llama_deploy/control_plane/server.py`、`config.py`，`state_store_uri` 支持 redis/mongodb）。
  - `update_session_state` 先 `get_session` 再 `state.update` 再 `aput`，是无版本的读改写（`server.py` 约 596 行）。
  - `WorkflowService` 用进程内的 `asyncio.Lock`（`LD061/llama_deploy/services/workflow.py` 约 122 行）。
  - 现在 README 写着 “This project is deprecated”。

## 4. 同一会话/同一 run 并发

- **同一个 Context 对象 [源码]**：`Context._workflow_run` 遇到 `ExternalContext.is_running` 时抛 `ContextStateError("Cannot start a new run while context is already running…")`（`WF/packages/llama-index-workflows/src/workflows/context/context.py` 约 289 行）。这只防住同一个内存对象。
- **同一份快照被两个进程 `from_dict`**：没有任何防护，两边会各自独立运行。**[源码推断]**
- **同一 run 内的并发 step**：靠 `edit_state()` 的 per-run 锁，限进程内。**[文档][源码]**
- **WorkflowServer 同一 `handler_id`**：`start_workflow` 先 `load_handler`，非终态就抛 `HandlerAlreadyRunningError`（`WF/.../server/_service.py` 约 258 行）。这是先查后写，不是原子操作，两个并发请求可能都通过。**[源码推断]**
- **同一 chat_store_key 并发写**：Postgres 的追加是原子的，`set` 是后写者胜；SQLAlchemy 的 `set` 是先删后插。没有会话级锁。**[源码]**
- **DBOS**：run 由一个副本独占（executor_id 属主）。空闲释放后的恢复用 `RunLifecycleLock` 做 CAS 和 `updated_at` token 防并发恢复（`WF/packages/llama-agents-dbos/src/llama_agents/dbos/journal/lifecycle.py`：`SELECT … FOR UPDATE`）。SQLite 版本退化成进程内的 `KeyedLock`。**[源码]**

## 5. 挂起/恢复（HITL）

- **模式 [文档]**：
  - 一个 step 返回 `InputRequiredEvent`，另一个 step 消费 `HumanResponseEvent`。调用方在 `stream_events()` 里看到请求，再用 `handler.send_event()` 回送响应。
  - 也可以用 `ctx.wait_for_event(...)`，它会重放 waiter 之前的代码，所以那部分代码必须可重复执行。官方推荐事件分步写法。
  - 来源：https://developers.llamaindex.ai/python/llamaagents/workflows/human_in_the_loop/
- **跨进程（纯核心）[文档]**：
  - 做法：看到 `InputRequiredEvent` 时先 `to_dict()` 存库，再 `cancel_run()`。响应到达后 `from_dict` 加 `run(ctx=…)`，再 `send_event`。
  - 文档原文：“In a web app, the process that sees the prompt is often not the same request that receives the answer.” 状态存在哪里由用户决定。
- **WorkflowServer [源码][文档]**：
  - 所有 run 都持久化为 tick 日志。控制循环发现所有 step 都在等外部输入时，会发布 `WorkflowIdleEvent`。`idle_timeout`（默认 60 秒）后 run 被释放出内存，新事件 `POST /events/{handler_id}` 到达时从 ticks 重放恢复（`WF/architecture-docs/server-architecture.md` 的 Server Lifecycle 一节，`control-loop.md`）。
  - 默认 runtime 下“另一个进程”只能是同一个 server 重启后的实例，见第 3 节。
- **崩溃时 in-flight run 是否自动恢复**：
  - 纯核心：不会，要用户自己做 checkpoint loop。**[文档]**
  - WorkflowServer + SQLite/Postgres store：`start()` 时自动恢复非 idle 的 running handler。重放出来是终态的就直接 finalize，没有任何 tick 的就标 failed。**[源码：`persistence_runtime.py`]**
  - DBOS：副本重启后自动恢复本 `executor_id` 名下的未完成 run，`max_recovery_attempts` 限制重放次数。**[文档][源码：`runtime.py` docstring]**

## 6. 跨进程观察/流式与取消

- **核心 [文档][源码]**：`handler.stream_events()` 只能消费一次。“If you need to broadcast workflow events to multiple clients, consume the stream once in your application and fan those events out yourself.”（https://developers.llamaindex.ai/python/llamaagents/workflows/streaming/ ，`WF/.../workflows/handler.py`）
- **WorkflowServer [文档][源码]**：
  - 事件先写进 store，再由 store 分配单调递增的 `sequence`。`GET /events/{handler_id}` 支持 SSE 和 NDJSON，用 `Last-Event-ID` 或 `after_sequence` 续读，`"now"` 表示只看新事件。客户端断线后会自动重连，最多 3 次（https://developers.llamaindex.ai/python/llamaagents/workflows/client/ ，`server-architecture.md`）。
  - 部署文档的 “Important considerations” 还写着“Only one reader is allowed… Once the events have been streamed, they cannot be recovered”，和架构文档、客户端文档的游标模型矛盾。推测这一段是旧文案。**[文档内部不一致，以源码为准]**
  - 通知机制：`AbstractWorkflowStore.subscribe_events` 默认轮询（`poll_interval`，基类 0.1 秒，Postgres 默认 1 秒），`MemoryWorkflowStore` 用 `asyncio.Condition`。`PostgresWorkflowStore` 用 `pg_notify` 加 `LISTEN`，并带重连退避，重连后会补发 notify，防止漏通知（`postgres_workflow_store.py` 约 141–250 行）。**[源码]**
- **取消 [文档][源码]**：
  - 核心：`handler.cancel_run()`。
  - 服务端：`POST /handlers/{id}/cancel?purge=` → `_WorkflowService.cancel_handler` → 外部适配器 `cancel()`。默认 runtime 下，这条路径只对本进程的 run 有效，或者会先把 run 在本地拉起。**[源码推断，未逐行核实 BasicRuntime 对未知 run 的行为]**
  - DBOS：取消用 `TickCancelRun`，通过 `DBOS.send_async` 写进 Postgres，属主副本用 `recv_async` 收到后执行，所以可以跨副本。还在队列里没开始的 run 会把取消请求存下来，等它被接纳后自己停止（`ARCHITECTURE.md`，`runtime.py` 中的 `InternalDBOSAdapter`）。
  - DBOS 在进程内调 `stream_events()` 时，每次都从头重放，server 端则用游标。**[文档]**

## 7. 分层

| 层 | 包 | 多用户/多机相关能力 |
|---|---|---|
| 应用框架 | `llama-index-core`（agents、memory、chat_store） | chat_store_key/session_id 作为分区 key；可接 Postgres/Redis 等 store；没有锁、版本、鉴权 |
| 执行核心 | `llama-index-workflows` | Context 序列化；Runtime/Adapter ABC（External 可跨进程）；纯 reducer + tick 可重放；只有进程内并发控制 |
| 服务 | `llama-agents-server`（WorkflowServer） | HTTP API；Memory/SQLite/Postgres/AgentData store；持久化事件日志 + 游标流；空闲释放和重载；重启恢复；无鉴权；单 server 绑定单 store |
| durable runtime 插件 | `llama-agents-dbos` | executor_id 属主；崩溃恢复；跨副本 send/recv/cancel；共享准入队列；experimental 执行者租约；空闲释放的分布式生命周期锁 |
| 部署平台 | `llama-agents-appserver`、`control-plane`、`operator`、`llamactl`、LlamaCloud | 从 git 构建；K8s CRD；单副本 pod；项目级 token 鉴权（网关）；Agent Data 按项目授权 |

来源：`WF/architecture-docs/core-overview.md`、`server-architecture.md`、`overall-architecture.md`；https://developers.llamaindex.ai/python/llamaagents/overview/

## 8. 与外部 durable 引擎集成

- **方式 [文档][源码]**：
  - 实现 core 的 `Runtime` ABC（`WF/packages/llama-index-workflows/src/workflows/runtime/types/plugin.py`）。`DBOSRuntime` 作为 base runtime，server 在它外面再套一层 decorator 链。
  - 用法：`Workflow(runtime=runtime)`，`WorkflowServer(workflow_store=runtime.create_workflow_store(), runtime=runtime.build_server_runtime(idle_timeout=…))`。
  - 内部适配器的映射：send 对应 `DBOS.send_async`，receive 对应 `DBOS.recv_async`，发布事件对应 `DBOS.write_stream_async`。时间用 durable 时间，任务完成顺序写进 journal，保证确定性重放。
- **约束 [文档]**：
  - workflow 名就是 durable 身份，改名后旧 run 不可见。
  - 改代码可能导致重放不确定，要先 drain 或者换名。
  - DBOS 的应用版本指纹在增删 workflow 或升级包时会变，旧 run 只能由旧代码的 worker 完成（`ARCHITECTURE.md`）。
- **Temporal 等**：在 workflows-py 仓库和官方文档里都没有找到 Temporal 集成。**[未核实]** 仓库里有 `packages/llama-agents-agentcore`（AWS Bedrock AgentCore 打包），这次没看。DBOS 方面另有 https://docs.dbos.dev/integrations/llamaindex ，属于第三方 DBOS 文档，未引用其内容。

---

## 来源

官方文档：
- https://developers.llamaindex.ai/python/llamaagents/workflows/durable_workflows/
- https://developers.llamaindex.ai/python/llamaagents/workflows/dbos/
- https://developers.llamaindex.ai/python/llamaagents/workflows/deployment/
- https://developers.llamaindex.ai/python/llamaagents/workflows/human_in_the_loop/
- https://developers.llamaindex.ai/python/llamaagents/workflows/managing_state/
- https://developers.llamaindex.ai/python/llamaagents/workflows/streaming/
- https://developers.llamaindex.ai/python/llamaagents/workflows/concurrent_execution/
- https://developers.llamaindex.ai/python/llamaagents/workflows/client/
- https://developers.llamaindex.ai/python/llamaagents/llamactl/workflow-api/
- https://developers.llamaindex.ai/python/llamaagents/cloud/agent-data-overview/
- https://developers.llamaindex.ai/python/framework/module_guides/storing/chat_stores/
- https://developers.llamaindex.ai/python/framework/module_guides/deploying/agents/memory/

源码（workflows-py @ b409b50）：
- `architecture-docs/{core-overview,server-architecture,control-loop,overall-architecture}.md`
- `packages/llama-index-workflows/src/workflows/context/{context.py,context_types.py,state_store.py}`，`handler.py`，`runtime/types/plugin.py`
- `packages/llama-agents-server/src/llama_agents/server/{server.py,_service.py,_keyed_lock.py}`
- `packages/llama-agents-server/src/llama_agents/server/_store/{abstract_workflow_store.py,postgres_workflow_store.py,agent_data_store.py}`
- `packages/llama-agents-server/src/llama_agents/server/_runtime/{persistence_runtime.py,idle_release_runtime.py}`
- `packages/llama-agents-dbos/ARCHITECTURE.md`，`src/llama_agents/dbos/{runtime.py,executor_lease.py,idle_release.py,journal/lifecycle.py}`
- `examples/dbos/server_replicas.py`
- `operator/internal/controller/resources.go`
- `packages/llama-agents-control-plane/src/llama_agents/control_plane/manage_api/deployments_service.py`

源码（llama_index @ 619d746）：
- `llama-index-core/llama_index/core/workflow/context.py`
- `llama-index-core/llama_index/core/storage/chat_store/{base.py,base_db.py,sql.py}`
- `llama-index-core/llama_index/core/memory/{memory.py,types.py}`
- `llama-index-core/llama_index/core/agent/workflow/base_agent.py`
- `llama-index-integrations/storage/chat_store/llama-index-storage-chat-store-postgres/llama_index/storage/chat_store/postgres/base.py`

源码（llama_deploy）：
- main @ c0ce080 `README.md`（deprecated 声明）
- v0.6.1：`llama_deploy/control_plane/{server.py,config.py}`，`llama_deploy/services/workflow.py`，`llama_deploy/message_queues/`
