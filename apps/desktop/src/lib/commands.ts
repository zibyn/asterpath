// Hand-written mirrors of the Tauri commands in
// `apps/asterpath/src/bin/asterpath-desktop/main.rs` and the types of
// `apps/asterpath/src/desktop.rs` (ADR-0035: no generated types).
import { Channel, invoke } from "@tauri-apps/api/core";

/** A work root in the sidebar. */
export interface Project {
  root: string;
  name: string;
}

/** A root session of a project. Timestamps are milliseconds since the epoch. */
export interface SessionSummary {
  id: string;
  name: string | null;
  created_at: number;
  last_activity: number;
  status: Status;
}

/** A session's sidebar badge; `elsewhere` = another process drives it. */
export type Status = "running" | "awaiting_approval" | "idle" | "elsewhere";

export const projects = () => invoke<Project[]>("projects");

/** Opens the folder picker; `null` when cancelled. */
export const addProject = () => invoke<Project | null>("add_project");

export const sessions = (root: string) => invoke<SessionSummary[]>("sessions", { root });

export const createSession = (root: string, name?: string) =>
  invoke<SessionSummary>("create_session", { root, name: name ?? null });

/** Who may drive a session: this app, nobody, or another process (read-only here). */
export type Access = "here" | "free" | "elsewhere";

export const access = (session: string) => invoke<Access>("access", { session });

/** Log a message and drive the session to idle; during a turn it steers. */
export const send = (session: string, message: string) => invoke<void>("send", { session, message });

/** Stop the turn this app drives. */
export const cancel = (session: string) => invoke<void>("cancel", { session });

/** A tool call waiting for the user. `session` is the asking one (a subagent's, possibly). */
export interface ApprovalRequest {
  session: string;
  tool_call_id: string;
  tool: string;
  /** The arguments as a JSON string. */
  args: string;
  side_effect: "ReadOnly" | "Mutating";
}

/** The waiting requests of a session's tree. */
export const approvals = (session: string) => invoke<ApprovalRequest[]>("approvals", { session });

/** A deny's reason is sent to the model. */
export type Answer = { kind: "allow_once" } | { kind: "allow_for_session" } | { kind: "deny"; reason: string | null };

export const approve = (request: ApprovalRequest, answer: Answer) =>
  invoke<void>("approve", { session: request.session, call: request.tool_call_id, answer });

/** What a session tree's mutating tool calls get; in memory, applies from the next call. */
export type ApprovalMode = "deny" | "ask" | "allow";

export const approvalMode = (session: string) => invoke<ApprovalMode>("approval_mode", { session });

export const setApprovalMode = (session: string, mode: ApprovalMode) =>
  invoke<void>("set_approval_mode", { session, mode });

export interface ModelSpec {
  provider: string;
  model: string;
  context_window: number | null;
}

/** The status bar: the next step's model, committed tokens incl. subagents, the last drive error. */
export interface SessionInfo {
  model: ModelSpec;
  tokens: number;
  error: string | null;
}

/** The configured model and the model aliases' models. */
export const models = () => invoke<ModelSpec[]>("models");

export const sessionInfo = (session: string) => invoke<SessionInfo>("session_info", { session });

/** Use `model` from the session's next step on. */
export const setModel = (session: string, model: ModelSpec) => invoke<void>("set_model", { session, model });

/** Where a provider's key comes from; the key itself never leaves the backend. */
export type KeySource = { kind: "env"; name: string } | { kind: "file" } | { kind: "missing" };

export interface ProviderKey {
  id: string;
  key: KeySource;
}

/** The settings page. `needed`: no provider, model or key, so nothing can run yet. */
export interface Setup {
  path: string | null;
  provider: string | null;
  model: string | null;
  providers: ProviderKey[];
  needed: boolean;
  /** The configuration fails to load for another reason. */
  error: string | null;
}

export const setup = () => invoke<Setup>("setup");

/** Write `config.toml` and restart the backend; an empty key keeps the file's. */
export const saveSetup = (provider: string, model: string, apiKey: string) =>
  invoke<Setup>("save_setup", { provider, model, apiKey });

/** Replay the session's records, then follow it. Resolves to the function that stops watching. */
export async function watch(session: string, onEvent: (event: ViewEvent) => void): Promise<() => void> {
  const channel = new Channel<ViewEvent>();
  channel.onmessage = onEvent;
  await invoke("watch", { session, after: null, onEvent: channel });
  return () => void invoke("unwatch", { channel: channel.id });
}

// The view stream. Records are the log's serde encoding (externally tagged
// enums); live events are `AgentEvent` (tag `type`). Only the variants the
// view renders are typed; the rest is ignored.

export type ViewEvent =
  | { kind: "record"; log_id: number; event: SessionEvent }
  | { kind: "synced" }
  | { kind: "live"; event: AgentEvent };

// An externally tagged enum is an object with one key; unknown variants
// have none of the typed keys.
export type Part = { Text?: { text: string } };

export type Item = {
  Message?: { role: "User" | "Assistant"; parts: Part[] };
  Reasoning?: { summary: Part[] };
};

/** `"Completed"`, `"Cancelled"`, … or `{ Stopped: { reason } }`. */
export type TurnEndReason = string | Record<string, unknown>;

export type SessionEvent = {
  InputRecorded?: { input: Part[] };
  TurnStarted?: { turn_id: string };
  AssistantMessage?: { turn_id: string; step_id: string; items: Item[] };
  ModelCallAborted?: { turn_id: string; step_id: string };
  TurnCommitted?: { turn_id: string; reason: TurnEndReason };
  ToolCallRequested?: { tool_call_id: string; name: string; raw_args: string };
  ToolResult?: {
    tool_call_id: string;
    status: "Completed" | "Failed" | "Interrupted";
    output: Part[] | null;
    error: { kind: unknown; message: string } | null;
  };
};

export type PartDelta =
  | { type: "text"; text: string }
  | { type: "reasoning_summary"; text: string }
  | { type: "json_args"; fragment: string };

export type AgentEvent =
  | { type: "turn_started"; turn: string }
  | { type: "model_delta"; turn: string; step: string; output_index: number; part_index: number; delta: PartDelta }
  | { type: "other" };
