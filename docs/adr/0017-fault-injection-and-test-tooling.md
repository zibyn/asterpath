# 故障注入用装饰器，崩溃验证用真 SIGKILL

**Status**: accepted (2026-10-22)
**来源**: [#19 崩溃注入与确定性测试工具链](https://github.com/zibyn/asterpath/issues/19)
**前置**: [ADR-0005](0005-storage-contract-and-sqlite.md)、[ADR-0014](0014-subagent-scheduling-api.md)、[ADR-0015](0015-packaging-and-stability.md)、[验收场景](../spec/acceptance.md)

## Context

[#5](../design/storage.md) 固定了故障注入点的名字（`append:before_commit`、`append:after_commit`、`tool:before_effect`、`tool:after_effect`、`clock:advance`、`queue:before_ack`），但把它们的物理形态、动作与 CI 编排留给了 #19。约束有四条：

- **core / protocol 无 feature**（ADR-0015）；注入逻辑不能进 Tier 1 稳定面，也不能让生产构建带钩子。
- **验收必须是真崩溃**（[acceptance.md](../spec/acceptance.md) §1）：用 `SIGKILL` 验证 OS 级持久性，panic / `abort` / 注入异常都不算。
- **`Storage` 已显式收 `now: Timestamp`**（ADR-0005），但 `runtime` / `scheduler` 仍需要一个可替换的时间来源。
- **两个后端**（in-memory 与 SQLite）都要满足 G2–G7，不能各写一套测试；而 G1 的持久性只对 SQLite 有意义。

## Decision

- **四个注入点是 `asterpath-testing` 里的 trait 装饰器**：`FailpointEventLog`（`append` 前/后）、`FailpointTool`（`invoke` 前/后）。装饰器实现 core/protocol 的既有 trait，core / protocol 零改动。一个装饰器承载一个点，多点嵌套。
- **`clock:advance` 不是装饰器**：core 新增最小 `Clock { fn now(&self) -> Timestamp }` 与 `SystemClock`；`asterpath-testing` 提供 `ManualClock`（`set` / `advance`）。`Storage` 签名不变，仍收显式 `now`。
- **动作只有两档**：`Halt`（写一行 JSON marker 到 stdout 后永久挂起，由父测试 `SIGKILL`）与 `Panic`（进程内测试断言中间状态）。`testdrive --failpoint <point>[=<n>]` 只构造 `Halt`；未知点 = 退出码 2。
- **进程级验收仍用真 `SIGKILL`**：有自然 marker 的场景（A1–A3、(c)）沿用工具/provider 自己的 marker；failpoint 只补没有自然窗口的边界（append）。不用注入点替代任何进程级验收。
- **后端中立契约套件**：G2–G7 在 in-memory 与 SQLite 上共享同一套测试；G1、G8、`Corrupt`、WAL/`flock`、迁移只测 SQLite。in-memory 显式声明不支持 G1，且不实现 `SessionRetirement`。
- **CI 三层**：单测+契约 5 分钟；进程级验收仅 Linux、整套 5 分钟、单 marker 等待 15 秒；feature 矩阵 5 分钟。进程级测试不得 `#[ignore]`。
- **验收 fixture 单一来源**：`asterpath-acceptance/fixtures/*.json`；#6 的 L 场景与 #17 的 R 场景复用，不在各自 crate 内另建。

## Consequences

- **2026-10-22 #32 修订**：存储不再有 `ack` / 租约，`queue:before_ack` 与 `FailpointStorage` 删除；注入点从六个降为五个（四个装饰器 + `clock:advance`）。
- 生产构建里没有任何注入钩子；`asterpath-testing` 是 Tier 3，可自由变形。
- core 多了一个一方法的 `Clock` trait；`Storage` 的显式 `now` 保持不变，不引入两套时间来源。
- in-memory 后端不能满足 G1，因此它不能用于崩溃恢复测试；这是刻意的能力差异，必须写进它的文档。
- 装饰器只能包住 trait 边界。进程内的工具不能靠它获得 OS 沙箱，`tool:*` 只包 `Tool::invoke`，不涉及外部进程。
- 计数式 `occurrence` 依赖固定脚本；脚本变了，命中点也随之改变。需要新窗口时扩展 `Point`，不引入环境变量或全局开关。
- 验收仍以真实崩溃为准，CI 成本集中在 Linux 的进程级作业；没有 macOS/Windows 的强杀路径。

## Considered alternatives

- **core 加 `#[cfg(feature = "failpoints")]` 钩子**：调用点近，但污染 Tier 1、违背 core/protocol 无 feature 的约束，还有把测试开关带进生产构建的风险。拒绝。
- **panic / `abort` / `process::exit` 冒充崩溃**：便宜，但不验证提交原子性、WAL 与 OS 释放 `flock` 的行为，且与验收原则冲突。拒绝。
- **子进程故障开关 / `LD_PRELOAD`**：能测真实边界，但 Linux-only、不透明、无法用于 in-memory，也无法在单测里组合。拒绝。
- **把时钟塞进 `Storage`**：少一个 trait，但会改 ADR-0005 的签名，并让 `recorded_at` 也变成可控——`recorded_at` 本就不参与逻辑。拒绝。
- **每个后端各写一套契约测试**：复制快、漂移也快；共享套件能让两个后端同时被 G2–G7 约束。拒绝。
- **macOS / Windows CI 也跑进程级验收**：收益小、成本高，v1 只为 Linux 承诺强杀验收。拒绝。
- **用 failpoint 替代 stdout marker 做所有进程级场景**：需要把注入点散布到工具与 provider 内部，扩大测试专用 API；自然 marker 已足够。拒绝。
