import { useState } from "react";

import { Button } from "@/components/ui/button";
import type { Answer, ApprovalRequest } from "@/lib/commands";
import type { ToolCall } from "@/lib/conversation";
import { cn } from "@/lib/utils";

/** How much a tool call shows once expanded. */
export type Detail = "normal" | "detailed";

type Args = Record<string, unknown>;

function parse(args: string): Args {
  try {
    const value: unknown = JSON.parse(args);
    return value && typeof value === "object" ? (value as Args) : {};
  } catch {
    return {};
  }
}

const str = (value: unknown) => (typeof value === "string" ? value : undefined);

/** The removed and added lines of a `write_file` / `edit_file` call. */
function change(name: string, args: Args): { path: string; removed: string[]; added: string[] } | undefined {
  const path = str(args.path);
  if (path === undefined) return undefined;
  const lines = (text: string | undefined) => (text ? text.split("\n") : []);
  if (name === "write_file") return { path, removed: [], added: lines(str(args.content)) };
  if (name === "edit_file") return { path, removed: lines(str(args.old_string)), added: lines(str(args.new_string)) };
  return undefined;
}

/** One line naming what the call does. */
function summary(name: string, args: Args): string {
  const target = str(args.path) ?? str(args.command) ?? str(args.pattern);
  return target ? `${name} ${target}` : name;
}

function Diff({ removed, added }: { removed: string[]; added: string[] }) {
  return (
    <pre className="overflow-x-auto rounded-md border font-mono text-xs">
      {removed.map((line, index) => (
        <div key={`-${index}`} className="bg-red-500/10 px-2 text-red-700 dark:text-red-400">
          - {line}
        </div>
      ))}
      {added.map((line, index) => (
        <div key={`+${index}`} className="bg-green-500/10 px-2 text-green-700 dark:text-green-400">
          + {line}
        </div>
      ))}
    </pre>
  );
}

function Pre({ text, className }: { text: string; className?: string }) {
  return (
    <pre className={cn("max-h-80 overflow-auto rounded-md bg-muted px-2 py-1 font-mono text-xs whitespace-pre-wrap", className)}>
      {text}
    </pre>
  );
}

const PREVIEW_LINES = 12;

/** A tool call, collapsed to one line; `detail` decides what expanding shows. */
export function ToolCallView({ call, detail }: { call: ToolCall; detail: Detail }) {
  const args = parse(call.args);
  const edit = change(call.name, args);
  const result = call.result;
  const failed = result && result.status !== "Completed";
  const output = result?.output ?? "";
  const preview = output.split("\n").slice(0, PREVIEW_LINES).join("\n");
  return (
    <details className="text-sm">
      <summary className="cursor-pointer select-none font-mono text-xs text-muted-foreground">
        <span className={cn(failed && "text-destructive")}>{summary(call.name, args)}</span>
        {!result && " …"}
        {failed && ` (${result.status.toLowerCase()})`}
      </summary>
      <div className="mt-1 flex flex-col gap-1">
        {detail === "detailed" ? (
          <Pre text={JSON.stringify(args, null, 2)} />
        ) : (
          edit && <Diff removed={edit.removed} added={edit.added} />
        )}
        {result?.error && <Pre text={result.error} className="text-destructive" />}
        {detail === "detailed" ? output && <Pre text={output} /> : !edit && preview && <Pre text={preview} />}
      </div>
    </details>
  );
}

/** Allow once, allow the tool (bash: the command prefixes) for the session when offered, or deny with a reason the model sees. */
export function ApprovalCard({
  request,
  onAnswer,
  pending,
}: {
  request: ApprovalRequest;
  onAnswer: (answer: Answer) => void;
  pending: boolean;
}) {
  const [denying, setDenying] = useState(false);
  const [reason, setReason] = useState("");
  const args = parse(request.args);
  const edit = change(request.tool, args);
  return (
    <div role="group" aria-label={`Approve ${request.tool}`} className="flex flex-col gap-2 rounded-lg border p-3 text-sm">
      <p className="font-medium">
        Allow <span className="font-mono">{summary(request.tool, args)}</span>?
      </p>
      {edit ? <Diff removed={edit.removed} added={edit.added} /> : <Pre text={JSON.stringify(args, null, 2)} />}
      {denying ? (
        <form
          className="flex gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            onAnswer({ kind: "deny", reason: reason.trim() || null });
          }}
        >
          <input
            aria-label="Reason for the model"
            autoFocus
            className="h-8 flex-1 rounded-md border bg-background px-2 text-sm outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50"
            placeholder="Why not, or what to do instead (sent to the model)"
            value={reason}
            onChange={(event) => setReason(event.target.value)}
          />
          <Button type="submit" size="sm" variant="outline" disabled={pending}>
            Deny
          </Button>
        </form>
      ) : (
        <div className="flex flex-wrap gap-2">
          <Button size="sm" disabled={pending} onClick={() => onAnswer({ kind: "allow_once" })}>
            Allow once
          </Button>
          {request.session_rules && (
            <Button size="sm" variant="outline" disabled={pending} onClick={() => onAnswer({ kind: "allow_for_session" })}>
              {request.tool === "bash" ? (
                <>
                  Don't ask again for{" "}
                  <span className="font-mono">{request.session_rules.map((prefix) => `${prefix} …`).join(", ")}</span> this
                  session
                </>
              ) : (
                <>Allow {request.tool} for this session</>
              )}
            </Button>
          )}
          <Button size="sm" variant="ghost" disabled={pending} onClick={() => setDenying(true)}>
            Deny…
          </Button>
        </div>
      )}
    </div>
  );
}
