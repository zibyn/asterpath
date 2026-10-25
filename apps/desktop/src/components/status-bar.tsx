import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import * as commands from "@/lib/commands";

const key = (model: commands.ModelSpec) => `${model.provider}/${model.model}`;

/** The session's model picker, committed tokens and last drive error. */
export function StatusBar({ session }: { session: string }) {
  const queryClient = useQueryClient();
  const models = useQuery({ queryKey: ["models"], queryFn: commands.models, staleTime: Infinity });
  // shortcut: polled like access; switch to a change notification (ADR-0043) once the facade has one
  const info = useQuery({
    queryKey: ["session-info", session],
    queryFn: () => commands.sessionInfo(session),
    refetchInterval: 1000,
  });
  const setModel = useMutation({
    mutationFn: (model: commands.ModelSpec) => commands.setModel(session, model),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["session-info", session] }),
  });

  const current = info.data?.model;
  // The session may use a model the config does not list (e.g. set with the CLI's `-m`).
  const options = [...(models.data ?? [])];
  if (current && !options.some((model) => key(model) === key(current))) options.unshift(current);
  const error = info.data?.error ?? setModel.error ?? info.error;

  return (
    <footer className="flex items-center gap-4 border-t px-4 py-1.5 text-xs text-muted-foreground">
      <select
        aria-label="Model"
        className="rounded border bg-background px-1 py-0.5"
        value={current ? key(current) : ""}
        disabled={!current || setModel.isPending}
        onChange={(event) => {
          const model = options.find((option) => key(option) === event.target.value);
          if (model) setModel.mutate(model);
        }}
      >
        {options.map((model) => (
          <option key={key(model)} value={key(model)}>
            {key(model)}
          </option>
        ))}
      </select>
      {info.data && <span>{info.data.tokens.toLocaleString()} tokens</span>}
      {error && (
        <span className="truncate text-destructive" title={String(error)}>
          {String(error)}
        </span>
      )}
    </footer>
  );
}
