# 多用户与多机部署归集成平台；内核只保证会话树单写者与两种存储后端

**Status**: accepted (2026-10-25)
**部分取代**: [ADR-0036](0036-per-session-tree-ownership.md) 的「取消请求落日志」「由变更通知唤醒」与 Context 中的多机理由；[ADR-0039](0039-multi-user-embedding-mechanisms.md) 的 Context 前提
**否决**: [ADR-0041](0041-wake-discovery-caches-folds.md)、[ADR-0042](0042-approval-asks-are-logged.md)、[ADR-0043](0043-change-notification-is-a-framework-hint.md)
**相关**: [ADR-0046](0046-local-hosts-and-web-sdk-do-not-share.md)
**调研**: [pi-and-pi-durable.md](../research/pi-and-pi-durable.md)、[mastra.md](../research/mastra.md)、[agno.md](../research/agno.md)、[llamaindex.md](../research/llamaindex.md)

## Context

ADR-0036、0039、0041–0043 把「web 单设备服务上千用户、多台机器共用一个 Postgres、处理请求的节点无状态」当作内核要满足的目标：内核里有了询问落日志、持久审批端点、持久化取消请求、每进程一个变更轮询器与宿主变更总线、为上千会话准备的唤醒缓存。

ADR-0046 之后，web 是把 `asterpath` 门面作为 SDK 集成进平台。多用户与多机部署是平台用这个 SDK 搭出来的，不是内核的形态。

参照框架的分工：

- **用户、鉴权、按用户选凭据、请求路由、跨节点传输**：pi-durable、Mastra、Agno、LlamaIndex 都不放进核心。用户最多是一个可过滤的字符串列（Mastra 的 `resourceId`、Agno 的 `user_id`、LlamaIndex 的 `chat_store_key`），鉴权在 server 层，路由与传输由部署层或外部组件（Redis、DBOS、Inngest）承担。
- **同一份状态只有一个写者**：pi-durable 一个存储只有一个属主，跨进程互斥由宿主提供；Mastra 在 core 里用租约加 generation fencing、存储条件写拒绝过期属主，起因是两个执行写同一个 run 的事故；Agno 在运行时层用 attempt 计数作 fencing；LlamaIndex 默认 server 没有这层防护，共用存储时同一个 run 可能被拉起两次。

内核自身仍然是多进程的：CLI 与桌面端共用一个 SQLite 库，可以同时运行。一棵会话树同一时刻只能有一个写者，否则日志顺序与恢复判定都不成立；这是日志的正确性要求，与部署多少台机器无关。

## Decision

- **内核不承担多用户与多机部署**：不提供跨节点的询问可见、持久化的取消、变更通知或推送、节点间路由，也不为单进程上千会话做专门的优化。这些由集成平台用 SDK 搭建。
- **内核保证会话树单写者**：ADR-0036 的显式认领、进程租约与属主纪元保留，定位是「一棵会话树至多一个写者」的正确性保证，适用于任何共用存储的进程组合。存储仍只做读、写与条件写。
- **跨进程只保留已有的外部写入**：用户输入、审批决定、配置变更仍是任何进程可写的外部写入，属主在驱动与等待时轮询日志读到（ADR-0036，已实现）。取消与驱动只在属主进程内执行，不落日志；非属主进程要取消，由宿主路由到属主（本地两端只读观察，web 平台自选粘滞路由、转发或队列）。`owner(session)` 查询保留，供宿主找到属主。
- **存储后端是 SQLite 与 Postgres**：Postgres 后端实现完整的 `SessionStore` 契约（含进程租约与属主行），不承担多机协调。
- **通用机制保留**：只读句柄的 `watch`（ADR-0038，跨进程靠轮询存储）、会话目录按元数据过滤、按会话树注册审批端点、provider 请求携带会话上下文（ADR-0039）、可修改的宿主元数据（ADR-0044）。它们是 CLI 与桌面端也在用的机制，平台可以拿来实现多用户，内核不赋予它们用户语义。
- **唤醒发现**保持 ADR-0023：全量折叠，实测到瓶颈再加索引。

## Consequences

- ADR-0041、0042、0043 否决；#148、#155、#158、#159、#160、#163、#164 关闭。
- 审批仍按 ADR-0007：询问只活在属主进程内，没有在线审批端点即 fail closed；别的进程要应答，得先经宿主知道有询问（例如路由到属主进程，或从 `watch` 看到工具调用意图），再写入审批决定。
- 没有持久化的取消请求：取消由属主进程执行；属主在别处时，宿主把取消路由过去。
- 多机部署的平台要自己负责：把同一会话树的请求送到属主、跨节点感知新记录（可以用 `watch` 或自己的通知）、属主宕机后的接管时机（进程租约过期后任何进程可认领）。
- 内核与参照框架里 Mastra、Agno 的做法一致之处只在单写者 fencing；其余与 pi-durable、LlamaIndex 一样留在核心之外。

## Considered alternatives

- **保留 0041–0043，让处理请求的节点无状态**：把平台的部署形态写进内核，多出询问记录、持久审批端点、取消记录、轮询器与总线接口；本地两端用不到。
- **单写者也交给宿主（pi-durable 式，宿主提供排他锁）**：CLI 与桌面端就要在宿主里自己实现互斥与崩溃检测，web 平台再实现一份；漏掉时日志会双写，LlamaIndex 默认 server 就是这种情况。
- **只保留一个可选的 fencing 原语，认领与存活全交给宿主**：同上，且已实现的进程租约要拆掉重写。
