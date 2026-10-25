import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Send, Square } from "lucide-react";
import { useEffect, useReducer, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import * as commands from "@/lib/commands";
import { empty, reduce } from "@/lib/conversation";
import { cn } from "@/lib/utils";

/** One session: its history and live stream, the composer, and stop. Key it by session. */
export function Conversation({ session }: { session: string }) {
  const queryClient = useQueryClient();
  const [state, dispatch] = useReducer(reduce, empty);
  const [watchError, setWatchError] = useState<string>();
  const [draft, setDraft] = useState("");
  const bottom = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let stopped = false;
    let unwatch: (() => void) | undefined;
    commands
      .watch(session, (event) => !stopped && dispatch(event))
      .then((stop) => (stopped ? stop() : (unwatch = stop)))
      .catch((error) => setWatchError(String(error)));
    return () => {
      stopped = true;
      unwatch?.();
    };
  }, [session]);

  useEffect(() => bottom.current?.scrollIntoView({ block: "end" }), [state.entries, state.drafts]);

  // shortcut: ownership changes are polled; switch to a change notification (ADR-0043) once the facade has one
  const access = useQuery({
    queryKey: ["access", session],
    queryFn: () => commands.access(session),
    refetchInterval: 1000,
  });
  const readOnly = access.data === "elsewhere";

  const send = useMutation({
    mutationFn: (message: string) => commands.send(session, message),
    onSuccess: () => {
      setDraft("");
      void queryClient.invalidateQueries({ queryKey: ["access", session] });
      void queryClient.invalidateQueries({ queryKey: ["sessions"] });
    },
  });
  const cancel = useMutation({ mutationFn: () => commands.cancel(session) });

  const submit = () => {
    if (draft.trim() && !send.isPending) send.mutate(draft);
  };

  return (
    <div className="flex h-full flex-col">
      <div className="flex-1 overflow-y-auto px-6 py-4">
        <div className="mx-auto flex max-w-3xl flex-col gap-3">
          {watchError && <p className="text-sm text-destructive">{watchError}</p>}
          {state.entries.map((entry, index) =>
            entry.kind === "ended" ? (
              <p key={index} className="text-center text-xs text-muted-foreground">
                Turn ended: {entry.reason}
              </p>
            ) : (
              <Block key={index} kind={entry.kind} text={entry.text} />
            ),
          )}
          {state.drafts.map((draft) => (
            <Block key={draft.key} kind={draft.kind} text={draft.text} />
          ))}
          {state.running && <p className="text-xs text-muted-foreground">Working…</p>}
          <div ref={bottom} />
        </div>
      </div>

      <div className="border-t p-3">
        <div className="mx-auto max-w-3xl">
          {readOnly && (
            <p className="mb-2 text-sm text-muted-foreground">
              Another process is driving this session; it is read-only here.
            </p>
          )}
          {(send.error || cancel.error) && (
            <p className="mb-2 text-sm text-destructive">{String(send.error ?? cancel.error)}</p>
          )}
          <div className="flex items-end gap-2">
            <textarea
              aria-label="Message"
              className="min-h-10 flex-1 resize-none rounded-md border bg-background px-3 py-2 text-sm outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:opacity-50"
              rows={2}
              placeholder={state.running ? "Steer the running turn…" : "Message"}
              value={draft}
              disabled={readOnly || !state.synced}
              onChange={(event) => setDraft(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
                  event.preventDefault();
                  submit();
                }
              }}
            />
            {state.running && access.data === "here" && (
              <Button variant="outline" size="icon" aria-label="Stop" onClick={() => cancel.mutate()}>
                <Square />
              </Button>
            )}
            <Button size="icon" aria-label="Send" disabled={readOnly || !draft.trim() || send.isPending} onClick={submit}>
              <Send />
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

function Block({ kind, text }: { kind: "user" | "assistant" | "reasoning"; text: string }) {
  if (kind === "reasoning") {
    return (
      <details className="text-sm text-muted-foreground">
        <summary className="cursor-pointer select-none">Reasoning</summary>
        <p className="mt-1 whitespace-pre-wrap">{text}</p>
      </details>
    );
  }
  return (
    <div
      className={cn(
        "whitespace-pre-wrap text-sm",
        kind === "user" && "self-end max-w-[80%] rounded-lg bg-muted px-3 py-2",
      )}
    >
      {text}
    </div>
  );
}
