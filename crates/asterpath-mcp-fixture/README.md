# asterpath-mcp-fixture

Scriptable MCP server for the `2026-07-28` stateless revision. It implements the
subset `asterpath-mcp` needs and is used by the `asterpath-mcp` tests and
the M5 end-to-end test. `publish = false`; dependencies are only `serde_json`
and `tokio`.

## Transports

```text
asterpath-mcp-fixture --stdio [OPTIONS]
asterpath-mcp-fixture --http <ADDR> [OPTIONS]
```

- `--stdio`: one JSON-RPC message per stdin line. stdout carries only messages
  (one line each); nothing else is written there.
- `--http <ADDR>`: POST-only. Bind `127.0.0.1:0` to get an ephemeral port; the
  bound address is printed to stdout as `listening on 127.0.0.1:<port>`.
  - Requests must use `Content-Type: application/json` (otherwise 415) and a
    `Content-Length` (otherwise 400).
  - `MCP-Protocol-Version` is validated when present: an unsupported version
    returns JSON-RPC `-32022` with `error.data.supported`.
  - `Mcp-Method`, when present, must equal the body `method`; `Mcp-Name`, when
    present on `tools/call`, must equal `params.name`. A mismatch is HTTP 400.
  - `Accept` chooses the response form. `text/event-stream` **without**
    `application/json` yields request-scoped SSE; anything else yields JSON.
  - SSE responses emit one `notifications/progress` event, then the final
    response event, then close (EOF-delimited, no `Content-Length`).
  - Notifications (no `id`) return `202 Accepted` with an empty body.

Every response uses `Connection: close`. HTTP has no sessions.

## Methods

| Method | Behaviour |
| --- | --- |
| `server/discover` | Returns `supportedVersions`, `capabilities.tools`, `serverInfo`. Reads `params._meta["io.modelcontextprotocol/protocolVersion"]`; an unsupported requested version returns `-32022` with `error.data.supported`. Client name/capabilities are accepted but never required. |
| `tools/list` | Paginated with `nextCursor`; `--page-size` controls page size (default 2). A cursor is the next index as a string; anything else is `-32602`. `--fault-repeat-cursor` always echoes the current cursor; `--fault-endless-pages` always returns a fresh one (exercises the 100-page cap). |
| `tools/call` | `params.arguments` must be an object. Unknown tool → `-32602`. |
| `notifications/cancelled` | Accepted and ignored (no response). |

Unknown methods return `-32601`; malformed JSON returns `-32700` with `id: null`.

## Tools

| Name | Annotations | Result |
| --- | --- | --- |
| `echo` | `readOnlyHint: true` | text block echoing `arguments.text` (default `"echo"`) |
| `mutate` | none (mutating) | text `mutation <n>` plus `structuredContent.count` |
| `content` | `readOnlyHint: true` | one block of every type: `text`, `image`, `audio`, `resource_link`, `resource`, plus `structuredContent` |

Calling any other name exercises the unknown-tool `-32602` path.

`--tool-set` changes the advertised set: `default` (the table above),
`collision` (`a.b` plus `a_b`, both sanitize to the same name) and `oversized`
(one 70-character name that cannot compose within 64 characters).

## Fault switches

All switches are CLI flags; there are no environment variables. Faults apply to
every `tools/call` with this precedence: crash → delay → error-code → is-error →
input-required → oversized → normal dispatch.

| Flag | Reproduces |
| --- | --- |
| `--fault-error-code <CODE>` | Fixed JSON-RPC error, e.g. `-32603` (M7/M8/M9 mapping). |
| `--fault-crash-after <N>` | Answers the first `N` calls; the next call exits the process without a response (stdio EOF / dropped HTTP connection). |
| `--fault-delay-ms <MS>` | Sleeps before answering each call (timeout and cancellation scenarios). |
| `--fault-is-error` | `result.isError = true` with two text blocks (`scripted`, `failure`) for join testing. |
| `--fault-input-required` | `result.resultType = "input_required"` (MRTR rejection). |
| `--fault-oversized <BYTES>` | One text block of exactly `BYTES` bytes. |
| `--fault-sse-interrupt` | SSE responses send the progress notification then close before the final response. Applies to HTTP only. |
| `--force-version-mismatch` | `server/discover` always returns `-32022` with `data.supported` (no-common-version path). |
| `--fault-repeat-cursor` | `tools/list` repeats the cursor it received (repeat-cursor failure). |
| `--fault-endless-pages` | `tools/list` always returns a fresh `nextCursor` (100-page cap failure). |
| `--supported-versions <V[,V...]>` | Overrides the advertised/validated versions (default `2026-07-28`). |

Examples:

```bash
# stdio: discover, one list page, a call
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hi"}}}' \
  | asterpath-mcp-fixture --stdio --page-size 1

# HTTP JSON
asterpath-mcp-fixture --http 127.0.0.1:0          # prints "listening on ..."
curl -s http://127.0.0.1:PORT/ -H 'Content-Type: application/json' \
  -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: server/discover' \
  --data '{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}'

# HTTP SSE
curl -sN http://127.0.0.1:PORT/ -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' --data \
  '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hi"}}}'

# Simulate a crash on the second call
asterpath-mcp-fixture --stdio --fault-crash-after 1
```

Tests live in `tests/protocol.rs` and spawn the binary for both transports plus
one case per fault switch. Child processes are killed and reaped by RAII guards,
and HTTP tests bind ephemeral ports, so no processes or ports are left behind.
