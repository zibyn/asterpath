# 规范化或自动声明服务端工具

Asterpath 不会按模型能力自动声明服务端工具（供应商托管的 web search、web fetch、code execution 等），也不提供规范化的声明方式（例如在 profile 里写一个 `web_search` 能力，再由 adapter 渲染成各家的配置）。服务端工具仍按 [ADR-0031](../docs/adr/0031-server-tools-structured-output-and-file-input.md) 的方式处理：宿主按 provider 声明供应商原始 JSON，adapter 原样追加。声明之后，什么时候调用由模型决定。

## 为什么不做

服务端工具的配置形状各家不同，而且演进很快：type 名带日期，参数也各有一套，比如域名白名单、最大调用次数、用户位置。规范化意味着规范类型会跟着供应商 API 膨胀，供应商出了新版本工具也要等框架发版才能用。ADR-0031 正是因此拒绝了这条路。

自动声明还会带来宿主看不见的副作用：
- 工具集指纹会变（ADR-0030），可能导致 Anthropic 回放 thinking 时被拒；
- 产生按次计费；
- 模型开始访问外部网络。

这些都属于宿主策略，框架不应该替宿主打开。

同类产品也没有这么做。OpenCode 没有启用任何供应商托管的工具，web search 用的是自家的客户端工具（接 Exa 或 Parallel 的 MCP 接口），并且需要显式开启。pi 不处理服务端工具。

宿主想要 web search，有两条现成的路：

- **用供应商的服务端工具**：在 `RequestConfig.server_tools` 里按 provider 写原始 JSON。
- **用客户端工具**：接一个搜索 MCP 服务器（MCP 是工具来源，见 ADR-0027），或者自己注册一个工具。这样跨 provider 也能用。

```toml
# CLI 示例：通过 MCP 接入一个客户端搜索工具
[mcp_servers.search]
url = "https://example.com/mcp"
```

## Prior requests

- #128: Provider：是否按模型能力自动声明或规范化服务端工具（web search 等）
