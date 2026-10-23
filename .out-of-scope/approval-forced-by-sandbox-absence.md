# 框架按「有无沙箱」强制审批

Asterpath 不会因为缺少 `SandboxProvider` 就强制 `Unsafe` 工具走 `Ask`，也不会禁止宿主把这类工具放宽为 `Allow`。审批判定只来自宿主装配的 `ApprovalPolicy`，框架不读取沙箱状态。

## 为什么不做

[ADR-0007](../docs/adr/0007-approval-and-sandbox-layers.md) 和 [ADR-0019](../docs/adr/0019-sandbox-seam-and-policy.md) 把审批与沙箱定为正交的两层：`Allow` 不放宽沙箱，没有沙箱也不收紧审批。两者的策略都归宿主。框架如果规定「没有沙箱就必须审批，并且不准放宽」，就是在内核里写死一条安全策略，而且把两层重新绑在一起。

缺少后端时的安全默认已经存在，不需要再加这条规则：

- 受限沙箱策略（`ReadOnly` / `WorkspaceWrite`）下没有 provider，进程启动 fail closed，不会裸跑。
- 只有宿主显式选择 `FullAccess` 时才会不受约束地执行，这是宿主自己的决定。
- External 工具在没有审批端时默认 `Deny`。

宿主（例如桌面端）如果想在没有沙箱的平台上强制审批，可以自己实现 `ApprovalPolicy`：按 `SideEffect::Mutating` 或 `ReplaySafety::Unsafe` 返回 `Ask`，并忽略用户的放宽配置。

```rust
impl ApprovalPolicy for NoSandboxPolicy {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        // 宿主自己知道当前平台没有沙箱后端
        if request.side_effect == SideEffect::Mutating { ApprovalVerdict::Ask } else { self.inner.classify(request) }
    }
}
```

## Prior requests

- #88: 风险：非 Linux 平台无沙箱，bash 工具须强制审批（第一阶段方案拒绝；macOS 后端部分保留在 #88）
