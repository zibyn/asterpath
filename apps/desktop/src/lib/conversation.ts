// Folds the view stream into what the conversation renders.
import type { Part, TurnEndReason, ViewEvent } from "@/lib/commands";

export type Entry =
  | { kind: "user" | "assistant" | "reasoning"; text: string }
  /** A turn that ended other than `Completed`. */
  | { kind: "ended"; reason: string };

/** A block still streaming; replaced by the step's record. */
export interface Draft {
  key: string;
  step: string;
  kind: "assistant" | "reasoning";
  text: string;
}

export interface Conversation {
  entries: Entry[];
  drafts: Draft[];
  /** A turn started and has not ended. */
  running: boolean;
  /** The replay caught up. */
  synced: boolean;
}

export const empty: Conversation = { entries: [], drafts: [], running: false, synced: false };

const text = (parts: Part[]) => parts.map((part) => part.Text?.text ?? "").join("");

const reasonName = (reason: TurnEndReason) => (typeof reason === "string" ? reason : Object.keys(reason)[0]);

export function reduce(state: Conversation, view: ViewEvent): Conversation {
  if (view.kind === "synced") return { ...state, synced: true };
  if (view.kind === "live") {
    const event = view.event;
    if (event.type === "turn_started") return { ...state, running: true };
    if (event.type !== "model_delta" || event.delta.type === "json_args") return state;
    const key = `${event.step}/${event.output_index}/${event.part_index}`;
    const delta = event.delta.text;
    const drafts = state.drafts.some((draft) => draft.key === key)
      ? state.drafts.map((draft) => (draft.key === key ? { ...draft, text: draft.text + delta } : draft))
      : [
          ...state.drafts,
          { key, step: event.step, kind: event.delta.type === "text" ? "assistant" : "reasoning", text: delta } as Draft,
        ];
    return { ...state, drafts };
  }

  const event = view.event;
  if (event.InputRecorded) {
    return { ...state, entries: [...state.entries, { kind: "user", text: text(event.InputRecorded.input) }] };
  }
  if (event.TurnStarted) return { ...state, running: true };
  if (event.AssistantMessage) {
    const { step_id, items } = event.AssistantMessage;
    const entries = items.flatMap((item): Entry[] => {
      const entry: Extract<Entry, { text: string }> | undefined = item.Reasoning
        ? { kind: "reasoning", text: text(item.Reasoning.summary) }
        : item.Message && { kind: "assistant", text: text(item.Message.parts) };
      return entry?.text ? [entry] : [];
    });
    return {
      ...state,
      entries: [...state.entries, ...entries],
      drafts: state.drafts.filter((draft) => draft.step !== step_id),
    };
  }
  if (event.ModelCallAborted) {
    const step = event.ModelCallAborted.step_id;
    return { ...state, drafts: state.drafts.filter((draft) => draft.step !== step) };
  }
  if (event.TurnCommitted) {
    const reason = reasonName(event.TurnCommitted.reason);
    const entries: Entry[] = reason === "Completed" ? state.entries : [...state.entries, { kind: "ended", reason }];
    return { ...state, entries, drafts: [], running: false };
  }
  return state;
}
