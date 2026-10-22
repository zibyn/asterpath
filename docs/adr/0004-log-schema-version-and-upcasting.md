# 日志 schema 用单调整数版本，只在读取时 upcast

**Status**: accepted (2026-10-22)
**来源**: [#4 日志版本、重放确定性与迁移](https://github.com/zibyn/asterpath/issues/4)（范围随 ADR-0003 收缩为 session 日志）

## Context

日志会跨版本存活：升级后的二进制必须能恢复旧日志。Duroxide 只拒绝未知事件类型、没有 replay-schema 版本字段，检测不到语义层变化；这个洞不能继承。

原来的 #4 还包含工作流代码版本化与重放确定性。ADR-0003 取消工作流引擎后，这些问题随之消失：session 恢复是检查点语义，不是确定性重放，代码变化不会让历史「重放错」。剩下的只有一件事——**持久化事件 payload 的形状与语义版本**。

## Decision

- **一个全局单调整数** `LOG_SCHEMA_VERSION: u16`，每条 `LogEntry` 记录写入时的值。旧 entry 永远带旧版本号。
- **任何持久化 payload 图的形状或语义变化都 +1**，包括新增可选字段；嵌套类型（`Item`、`Usage`、`ConfigSnapshot`）同样计入。仅内存结构变化、仅消费行为变化（事件形状不变）不 bump。
- **只在读取时 upcast**：`upcast_vN_to_vN+1(Value) -> Result<Value>` 纯函数链，逐级升到当前版本后一次性反序列化。日志从不原地重写。物理存储迁移（DDL）归 #5，与本版本号无关。
- 只有真正改变形状的版本才写 upcaster；纯新增字段不写，读者忽略未知字段即可。
- **未知事件变体 → 硬错误**，整个 session 拒绝恢复。**未知字段 → 忽略**。**缺失必填字段 → 硬错误**，绝不默认填充；默认值只允许出现在显式 upcaster 里。
- **无降级**：发现 `max(schema_version)` 高于自身支持版本时拒绝打开，不做只读 `inspect` 例外。
- **session 不记录代码版本，也没有版本栅栏**：恢复与框架/tool 代码版本无关。
- upcaster 放在拥有 `EventPayload` 的合约 crate（当前判断是 `protocol`；最终 crate 归属由 #15 确认）。
- v1 不做 upcast 缓存：每次扫描逐条 upcast。代码里留 `shortcut:` 注明升级条件。

## 兼容性测试

每个历史版本一组黄金 fixture（`tests/fixtures/log/v<N>/*.jsonl`），CI 对每组断言：

1. 能解码；
2. 能逐级 upcast 到当前版本；
3. 折叠投影结果等于 fixture 附带的 canonical 期望（模型历史 + frontier 状态）。

另加一条当前版本写入 → 读回 → 投影的 round-trip。新增版本时，上一版 fixture 必须继续通过；这条就是兼容性门禁。

## Consequences

- 版本号是粗粒度解码器契约：一次 bump 会让旧二进制拒绝所有含新版本 entry 的 session。这与「无降级」一致，代价是回滚后新写入的 session 不可恢复，必须保留匹配的旧二进制。
- 日志不可变，恢复路径没有「迁移到一半崩溃」的中间态。
- 跨版本解码成本按扫描量线性增加；v1 可接受。
- 每个 schema 版本都要维护 fixture，这是有意的发布成本。

## Considered alternatives

- **每个事件类型独立版本**：16 种事件各背一个版本矩阵，收益为零。
- **写入时原地迁移**：会让 append-only 日志可写，引入迁移中断状态，与 ADR-0002 冲突。
- **逐版本强类型结构体 upcast**：类型安全，但为每个历史版本复制整套枚举，v1 版本少、不划算。
- **session 代码版本栅栏**：检查点恢复不依赖代码重放，栅栏只会无谓地阻止升级。
