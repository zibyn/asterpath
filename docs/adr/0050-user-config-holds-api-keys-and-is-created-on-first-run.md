# 用户配置在首次运行时创建，API key 可以存在里面

**Status**: accepted (2026-10-26)
**取代**: [packaging.md](../design/packaging.md) §4.7 中「API key 只从环境变量读，配置文件出现 `api_key` 字段直接报错」（原为设计规则，没有 ADR）

## Context

本地应用（CLI 与桌面端，ADR-0049）共用 `$XDG_CONFIG_HOME/asterpath/config.toml`。此前的规则：

- 默认路径不存在时用默认值，不创建文件；默认值里没有 provider，新会话直接报「a provider is required」。
- API key 只从环境变量读（`ANTHROPIC_API_KEY`、`OPENAI_API_KEY` 或 `api_key_env` 指定的变量），配置文件里出现 `api_key` / `secret` / `token` 即配置错误。

这套规则对在 shell 里启动的 CLI 可行，对桌面端不行：从桌面菜单、Dock 或文件管理器启动的进程不读 shell 的启动文件，`.bashrc` 里 export 的 key 它看不到，用户也没有地方给它设环境变量。结果是桌面端开箱即失败，用户要先知道配置文件的路径、自己建目录写文件，key 还得另找办法放进图形会话的环境。

配置因此分散在两处：模型与 provider 在文件里，key 在环境里。环境变量也不是更安全的存放处：它会被子进程继承（ADR-0037 为此才默认隔离子进程环境），并且要写在 shell 启动文件里，同样是明文。

## Decision

- **首次运行时创建用户配置**：CLI 与桌面端的每个入口，在默认路径的配置文件不存在时写一份模板，目录权限 `0700`、文件 `0600`（Unix）。模板只含注释掉的示例，不替用户选 provider 或模型。`--config` / `ASTERPATH_CONFIG` 指定的文件不存在仍是配置错误，不在任意路径上建文件。创建失败（如目录不可写）不是错误：打印一行提示，按默认值继续。
- **API key 可以写在用户配置里**：位置是 provider 表的 `api_key` 字段（`[providers.<id>] api_key = "…"`），别处的 `api_key` / `secret` / `token` 仍是配置错误。环境变量优先于文件，CI 与临时覆盖照旧。
- **含 key 的文件必须只有属主可读**：Unix 上文件含 `api_key`，且组或其他人有任何权限时，是配置错误，提示 `chmod 600`。不含 key 的文件不检查。其他平台不检查。
- **key 不经过 `-c`**：`-c providers.<id>.api_key=…` 是配置错误，免得 key 进入 shell 历史与进程参数。
- **key 不出现在输出里**：`doctor`、错误信息与日志中只显示是否已设置和来源（文件或哪个环境变量），不显示值。
- **写回配置文件归应用**：桌面端的首次设置把 provider、模型与 key 写回同一份文件，保留用户的注释与顺序。框架与门面不涉及配置文件（ADR-0049）。

## Consequences

- 桌面端从图形环境启动也能拿到 key，首次设置可以在界面里完成；CLI 用户可以只维护一个文件。
- key 以明文存在磁盘上，安全性取决于文件权限与用户的家目录；权限检查让「误把配置设成全员可读」立即失败，而不是悄悄泄露。
- 配置文件不再适合原样放进公开的 dotfiles 仓库；需要分享配置的用户继续用环境变量，文件里不写 key。
- 测试与 CI 把 `XDG_CONFIG_HOME` 指向不可写位置时，入口会多一行「无法创建配置」的提示；测试按需断言或设到临时目录。
- 以后接系统钥匙串时，是在文件与环境变量之外再加一个来源，不改本 ADR 的优先级与输出规则。

## Considered alternatives

- **维持只读环境变量**：桌面端从图形环境启动时拿不到 key，只能要求用户改桌面会话的环境，开箱体验不可用。
- **系统钥匙串（Secret Service、macOS Keychain、Windows Credential Manager）**：比明文文件安全，但要加平台依赖；Linux 上没有图形会话（SSH、服务器、容器）时常常没有 Secret Service，CLI 仍需另一条路。可以以后作为额外来源加入。
- **单独的凭据文件（如 `credentials.toml`）**：权限可以单独收紧，但仍是两个文件，用户要记住 key 在哪；与「避免配置分散」的目标相反。权限检查已经覆盖了单独文件想要的那部分保护。
- **权限过宽时只警告**：警告在桌面端看不到，在 CLI 里容易被忽略；与仓库 fail closed 的默认一致，选择报错。
