# Postgres 存储后端

> 来源：[#162](https://github.com/zibyn/asterpath/issues/162)（父票 [#161](https://github.com/zibyn/asterpath/issues/161)）。
>
> 前置：[storage.md](storage.md)。契约与 SQLite 相同，本文只写 Postgres 的实现选择。
>
> 相关 ADR：[0036 会话树属主](../adr/0036-per-session-tree-ownership.md)、[0010 不透明数据原样回放](../adr/0010-canonical-protocol-model.md)、[0026 适配器只依赖 protocol](../adr/0026-engine-is-one-crate-only-adapter-traits-public.md)。

## 1. 决策摘要

- `asterpath-storage-postgres` 实现 `SessionStore` 全部契约，跑同一个 `session_store_contract`。facade 的可选 feature `postgres`（默认关）把它再导出为 `asterpath::postgres`；应用建好存储后经 `AsterpathConfig.store` 注入。
- **只当持久化层**（ADR-0036）：只用读、写和条件写（`ON CONFLICT`、行锁 `FOR UPDATE` / `FOR SHARE`），不用 advisory lock、`LISTEN/NOTIFY`，也不依赖连接生命周期。行锁只在单个事务内持有。
- **驱动** `tokio-postgres` + `deadpool-postgres`。构造函数 `from_pool(pool, OpenOptions)` 接收应用建好的连接池；TLS、认证、池大小、`search_path` 都由应用负责。crate 再导出 `deadpool_postgres`，保证版本一致。
- **每次 append 一个事务**，读已提交（READ COMMITTED），客户端不做 group commit。
- 需要 Postgres ≥ 13（`gen_random_uuid()`）。

## 2. 运行时

存储自带一个两线程的 tokio runtime，所有语句都在它上面执行，连接也在它上面建立。原因：认领的 `Drop` 和句柄的 `Drop` 要等释放提交后才返回（别的进程才能立即认领），而同步 `Drop` 不能依赖调用方的 runtime（当前线程 runtime 上会死锁）。

认领、释放、续约走一个队列，按序执行（同 SQLite 的写线程）；append、退役与读并发执行。句柄 drop 时队列关闭，控制任务清掉本进程租约、释放所有树，然后关闭 runtime。

应用最好给存储一个独立的池：池里已有、由应用 runtime 建立的连接，其驱动任务在应用 runtime 上。

## 3. DDL

表建在连接的 `search_path` 里，不加前缀：

```sql
CREATE TABLE schema_version (
    singleton  boolean  PRIMARY KEY DEFAULT true CHECK (singleton),
    version    bigint   NOT NULL
);
CREATE TABLE session_heads (session_id bytea PRIMARY KEY);
CREATE TABLE sessions (
    session_id bytea PRIMARY KEY, parent_id bytea,
    created_at bigint NOT NULL, metadata text NOT NULL
);
CREATE TABLE log (
    log_id          bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      bytea   NOT NULL,
    seq             bigint  NOT NULL,
    recorded_at     bigint  NOT NULL,
    schema_version  integer NOT NULL,
    event_kind      text    NOT NULL,
    dedup_key       text,
    payload         text    NOT NULL,
    UNIQUE (session_id, seq),
    UNIQUE (session_id, event_kind, dedup_key)
);
CREATE TABLE retired_sessions (
    session_id bytea PRIMARY KEY, parent_id bytea,
    last_log_id bigint NOT NULL, retired_at bigint NOT NULL
);
CREATE TABLE tree_owners (root bytea PRIMARY KEY, epoch bigint NOT NULL, process bytea);
CREATE TABLE process_leases (process bytea PRIMARY KEY, expires_at bigint NOT NULL);
```

- `payload` 是 `text`，存 `serde_json` 写出的原文，不用 `jsonb`（`jsonb` 会重排 key、去空白、改写数字），满足不透明数据逐字节回放（ADR-0010）。
- 时间戳是 `bigint` 毫秒，取存储句柄的时钟，不取服务器的 `now()`；租约是否过期按读者的时钟判断（同 SQLite）。
- `session_heads` 是会话头行，只用来加锁；会话的第一条记录建它，退役后保留，排在它上面的 append 才能看到墓碑。
- 会话目录的元数据过滤在查询时把 `metadata` 转成 `jsonb` 做 `@>`；目录元数据是框架写的规范 JSON，不是不透明数据。

## 4. 写路径

### append

1. 在事务外解码 payload；坏 payload 是 `InvalidEntry`。
2. `INSERT INTO session_heads … ON CONFLICT DO NOTHING`，再 `SELECT … FOR UPDATE` 锁住本会话头行。`SessionCreated` 带父会话时，同样锁父会话头行。
3. 有墓碑 → `Retired`。
4. 属主写入：`SELECT … FROM tree_owners WHERE root, epoch, process = 本进程 FOR SHARE`，没有 → `OwnedElsewhere`。共享锁一直持有到提交，接管者的 `UPDATE` 要等它提交，因此属主写入不会落在接管之后；同一棵树的多个属主写入可以并发。
5. `seq = MAX(seq) + 1`，`INSERT INTO log … ON CONFLICT (session_id, event_kind, dedup_key) DO NOTHING RETURNING log_id`。冲突时查出已有记录的 `log_id` 返回，事务回滚，语义同 SQLite。
6. `MetadataChanged` 合并进 `sessions.metadata`（超限 = `InvalidEntry`，整条回滚）；`SessionCreated` 写 `sessions`。
7. 提交。

**会话内顺序。** 头行锁持有到提交，同一会话的 append 串行；`log_id` 在锁内分配，所以同一会话内 `seq`、`log_id` 与提交顺序一致。读已提交下，看得到某条记录的快照一定也看得到同会话更早提交的记录，`scan(after)` 返回的是稳定前缀：属主写入与外部写入并发时，按游标跟读不漏不重。跨会话的 `log_id` 顺序与提交顺序不一致，变更通知由 #164 处理。

`log_id` 可能有空洞（去重冲突与回滚的事务消耗了标识值），`seq` 没有。

### 退役

锁本会话头行 → 墓碑检查 → `NotFound` → 空闲 → frontier CAS → 活子会话 → 锁父会话头行并检查父会话空闲 → 写墓碑、删日志与目录行。顺序同 [retention.md](retention.md) §5。

- 与本会话 append 互斥：同一头行。
- 与子会话创建互斥：子会话的 `SessionCreated` 锁父会话头行，即本会话头行，因此「无活子会话」的判断在提交前不会失效。
- 与父会话 append 互斥：锁父会话头行后再判断父会话空闲。

加锁顺序总是从子到父，没有环；万一死锁（如损坏的父链），Postgres 报 `40P01`，归为 `Conflict`。

### 认领、释放、续约

- **认领**：一个事务里续约本进程租约，`SELECT … FROM tree_owners o LEFT JOIN process_leases … FOR UPDATE OF o` 锁属主行。属于本进程 → 共享原纪元；属于租约未过期的别的进程 → `OwnedElsewhere`；否则 `UPDATE … epoch = epoch + 1`。没有属主行时 `INSERT … ON CONFLICT DO NOTHING`，被别人抢先插入就整笔重试。
- **释放**：`UPDATE tree_owners SET process = NULL WHERE root, epoch, process = 本进程`，纪元已被接管时什么也不改。
- **续约**：续本进程租约，删掉已过期的租约行，报告不再持有的认领。
- **正常退出**：删本进程租约，释放本进程持有的全部树。

## 5. 打开

1. `synchronous_commit` 是 `off` 时拒绝打开（`Backend`），除非 `OpenOptions::allow_async_commit`（只用于测试与开发）。只在打开时检查一次池里的一个连接；按角色或数据库设置它是应用的事。
2. 建表在一个事务里：先建 `schema_version`，库里的版本比代码新 → `SchemaTooNew`；再 `CREATE TABLE IF NOT EXISTS` 其余表，插入版本行（`ON CONFLICT DO NOTHING`）。多个节点同时首次打开时，后到的事务在系统目录上撞唯一约束（`23505` / `42P07` / `42710`），整笔重试，直到表已存在。
3. `uuid_send(gen_random_uuid())` 作本进程 id。

## 6. 错误映射

| SQLSTATE | 归类 |
| --- | --- |
| `40001` serialization_failure、`40P01` deadlock_detected、`55P03` lock_not_available | `Conflict`（可重试） |
| 建表时的 `23505`、`42P07`、`42710` | `Conflict`，打开时内部重试 |
| 其他，及取连接失败 | `Backend` |

认领、释放、续约、退出和建表在内部重试 `Conflict`（1 ms 间隔，最多 1000 次）；append 与退役不重试，`Conflict` 交给调用方。连接中途断开时提交结果未知，不算可重试。

## 7. 用 rustls 建池

crate 不带 TLS，应用自己选。示例（应用依赖 `tokio-postgres-rustls`、`rustls`、`webpki-roots`）：

```rust
use asterpath::postgres::deadpool_postgres::{Manager, ManagerConfig, Pool};

let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth(),
);
let mut pg: tokio_postgres::Config = std::env::var("DATABASE_URL")?.parse()?;
pg.options("-c search_path=asterpath");
let pool = Pool::builder(Manager::from_config(pg, tls, ManagerConfig::default()))
    .max_size(16)
    .build()?;
let store = asterpath::postgres::from_pool(pool, Default::default()).await?;
```

## 8. 测试

`crates/asterpath-storage-postgres/tests/postgres.rs` 跑共享契约套件（含两个句柄当两个进程的属主、租约、退出与被杀）、同一会话上属主写入与外部写入并发时游标跟读、多个节点同时首次打开、`synchronous_commit` 检查、payload 原文往返与 schema 版本。需要环境变量 `ASTERPATH_TEST_POSTGRES_URL`（用户能建 schema）；没设时跳过并打印说明。每个存储用一个随机 schema，测完删除。不依赖 Docker 或 testcontainers。
