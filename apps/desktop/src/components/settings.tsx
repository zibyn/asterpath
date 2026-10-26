import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import * as commands from "@/lib/commands";

const builtins = ["anthropic", "openai"];

function describe(key: commands.KeySource) {
  if (key.kind === "env") return `Set (env:${key.name})`;
  if (key.kind === "file") return "Set (config file)";
  return "Not set";
}

/** Provider, endpoint, model and API key, written back to `config.toml` (ADR-0050). */
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
  const known = (id: string) => setup.providers.find((option) => option.id === id);
  const [provider, setProvider] = useState(setup.provider ?? "anthropic");
  const [protocol, setProtocol] = useState<commands.Protocol>(known(provider)?.protocol ?? "anthropic");
  const [baseUrl, setBaseUrl] = useState(known(provider)?.base_url ?? "");
  const [model, setModel] = useState(setup.model ?? "");
  const [apiKey, setApiKey] = useState("");
  const id = provider.trim();
  // Built-in names imply their protocol; any other id is an instance (ADR-0029).
  const custom = !builtins.includes(id);
  const save = useMutation({
    mutationFn: () =>
      commands.saveSetup({
        provider: id,
        model: model.trim(),
        protocol: custom ? protocol : null,
        base_url: baseUrl.trim(),
        api_key: apiKey || null,
      }),
    onSuccess: (saved) => {
      queryClient.setQueryData(["setup"], saved);
      // The backend was replaced; everything it served is stale.
      void queryClient.invalidateQueries();
    },
  });
  const key = known(id)?.key ?? { kind: "missing" };

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
        <input
          className="rounded border bg-background px-2 py-1"
          list="providers"
          value={provider}
          required
          onChange={(event) => {
            const next = known(event.target.value.trim());
            setProvider(event.target.value);
            if (next) {
              setProtocol(next.protocol);
              setBaseUrl(next.base_url ?? "");
            }
          }}
        />
        <datalist id="providers">
          {setup.providers.map((option) => (
            <option key={option.id} value={option.id} />
          ))}
        </datalist>
        <span className="text-xs text-muted-foreground">anthropic, openai, or a name of your own (e.g. deepseek)</span>
      </label>
      {custom && (
        <label className="flex flex-col gap-1">
          Protocol
          <select
            className="rounded border bg-background px-2 py-1"
            value={protocol}
            onChange={(event) => setProtocol(event.target.value as commands.Protocol)}
          >
            <option value="anthropic">anthropic</option>
            <option value="openai">openai</option>
          </select>
        </label>
      )}
      <label className="flex flex-col gap-1">
        Base URL
        <input
          className="rounded border bg-background px-2 py-1"
          placeholder={custom ? "https://api.example.com" : "Default endpoint"}
          value={baseUrl}
          required={custom}
          onChange={(event) => setBaseUrl(event.target.value)}
        />
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
      <Button type="submit" disabled={save.isPending || !id || !model.trim()}>
        Save
      </Button>
    </form>
  );
}
