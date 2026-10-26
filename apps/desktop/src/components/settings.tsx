import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import * as commands from "@/lib/commands";

const builtins: commands.ProviderKey[] = [
  { id: "anthropic", key: { kind: "missing" } },
  { id: "openai", key: { kind: "missing" } },
];

function describe(key: commands.KeySource) {
  if (key.kind === "env") return `Set (env:${key.name})`;
  if (key.kind === "file") return "Set (config file)";
  return "Not set";
}

/** Provider, model and API key, written back to `config.toml` (ADR-0050). */
export function Settings() {
  const setup = useQuery({ queryKey: ["setup"], queryFn: commands.setup });
  if (!setup.data) {
    return setup.error ? <p className="p-6 text-sm text-destructive">{String(setup.error)}</p> : null;
  }
  // Keyed so the form starts from the saved values after each save.
  return <SettingsForm key={JSON.stringify(setup.data)} setup={setup.data} />;
}

function SettingsForm({ setup }: { setup: commands.Setup }) {
  const queryClient = useQueryClient();
  const providers = setup.providers.length > 0 ? setup.providers : builtins;
  const [provider, setProvider] = useState(setup.provider ?? providers[0].id);
  const [model, setModel] = useState(setup.model ?? "");
  const [apiKey, setApiKey] = useState("");
  const save = useMutation({
    mutationFn: () => commands.saveSetup(provider, model.trim(), apiKey),
    onSuccess: (saved) => {
      queryClient.setQueryData(["setup"], saved);
      // The backend was replaced; everything it served is stale.
      void queryClient.invalidateQueries();
    },
  });
  const key = providers.find((option) => option.id === provider)?.key ?? { kind: "missing" };

  return (
    <form
      className="mx-auto flex max-w-md flex-col gap-4 p-6 text-sm"
      onSubmit={(event) => {
        event.preventDefault();
        save.mutate();
      }}
    >
      <h1 className="text-lg font-semibold">Settings</h1>
      {setup.needed && <p className="text-muted-foreground">Choose a provider and model to start.</p>}
      {setup.error && <p className="text-destructive">{setup.error}</p>}
      <label className="flex flex-col gap-1">
        Provider
        <select
          className="rounded border bg-background px-2 py-1"
          value={provider}
          onChange={(event) => setProvider(event.target.value)}
        >
          {providers.map((option) => (
            <option key={option.id} value={option.id}>
              {option.id}
            </option>
          ))}
        </select>
      </label>
      <label className="flex flex-col gap-1">
        Model id
        <input
          className="rounded border bg-background px-2 py-1"
          value={model}
          required
          onChange={(event) => setModel(event.target.value)}
        />
      </label>
      <label className="flex flex-col gap-1">
        API key <span className="text-xs text-muted-foreground">{describe(key)}</span>
        <input
          type="password"
          autoComplete="off"
          className="rounded border bg-background px-2 py-1"
          placeholder={key.kind === "missing" ? "" : "Leave empty to keep"}
          value={apiKey}
          onChange={(event) => setApiKey(event.target.value)}
        />
        {key.kind === "env" && (
          <span className="text-xs text-amber-600">
            {key.name} is set and wins; a key entered here has no effect.
          </span>
        )}
      </label>
      {setup.path && <p className="text-xs text-muted-foreground">Saved to {setup.path}</p>}
      {save.error && <p className="text-destructive">{String(save.error)}</p>}
      <Button type="submit" disabled={save.isPending || !model.trim()}>
        Save
      </Button>
    </form>
  );
}
