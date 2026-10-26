import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { FolderPlus, Plus, Settings } from "lucide-react";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import * as commands from "@/lib/commands";
import { cn } from "@/lib/utils";

/** Projects (work roots), each with its root sessions, last active first. */
export function Sidebar() {
  const queryClient = useQueryClient();
  const projects = useQuery({ queryKey: ["projects"], queryFn: commands.projects });
  const add = useMutation({
    mutationFn: commands.addProject,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["projects"] }),
  });

  return (
    <nav className="flex w-72 shrink-0 flex-col border-r bg-muted/40">
      <div className="flex items-center justify-between p-3">
        <span className="text-sm font-semibold">Projects</span>
        <div className="flex">
          <Button variant="ghost" size="icon" aria-label="Settings" asChild>
            <Link to="/settings">
              <Settings />
            </Link>
          </Button>
          <Button variant="ghost" size="icon" aria-label="Add project" onClick={() => add.mutate()}>
            <FolderPlus />
          </Button>
        </div>
      </div>
      {add.error && <p className="px-3 text-sm text-destructive">{String(add.error)}</p>}
      <div className="flex-1 overflow-y-auto px-2 pb-3">
        {projects.error && <p className="px-1 text-sm text-destructive">{String(projects.error)}</p>}
        {projects.data?.map((project) => <ProjectItem key={project.root} project={project} />)}
      </div>
    </nav>
  );
}

function ProjectItem({ project }: { project: commands.Project }) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const sessions = useQuery({
    queryKey: ["sessions", project.root],
    queryFn: () => commands.sessions(project.root),
    // shortcut: statuses are polled; switch to a change notification (ADR-0043) once the facade has one
    refetchInterval: 1000,
  });
  const agents = useQuery({ queryKey: ["agents", project.root], queryFn: () => commands.agents(project.root) });
  // `null` until picked: the configured default.
  const [picked, setPicked] = useState<string | null>(null);
  const agent = picked ?? agents.data?.default ?? null;
  const create = useMutation({
    mutationFn: () => commands.createSession(project.root, undefined, agent),
    onSuccess: (session) => {
      void queryClient.invalidateQueries({ queryKey: ["sessions", project.root] });
      void navigate({ to: "/sessions/$sessionId", params: { sessionId: session.id } });
    },
  });

  return (
    <section className="mb-3">
      <div className="flex items-center justify-between">
        <h2 className="truncate px-1 py-1 text-xs font-medium text-muted-foreground" title={project.root}>
          {project.name}
        </h2>
        {!!agents.data?.agents.length && (
          <select
            aria-label={`Agent of a new session in ${project.name}`}
            className="ml-auto mr-1 max-w-28 rounded border bg-background px-1 py-0.5 text-xs"
            value={agent ?? ""}
            onChange={(event) => setPicked(event.target.value)}
          >
            {!agent && <option value="">No agent</option>}
            {agents.data.agents.map((choice) => (
              <option key={choice.name} value={choice.name} title={choice.description ?? undefined}>
                {choice.name}
              </option>
            ))}
          </select>
        )}
        <Button
          variant="ghost"
          size="icon"
          className="size-6"
          aria-label={`New session in ${project.name}`}
          disabled={create.isPending}
          onClick={() => create.mutate()}
        >
          <Plus />
        </Button>
      </div>
      {create.error && <p className="px-1 text-sm text-destructive">{String(create.error)}</p>}
      {sessions.error && <p className="px-1 text-sm text-destructive">{String(sessions.error)}</p>}
      <ul>
        {sessions.data?.map((session) => (
          <li key={session.id}>
            <Link
              to="/sessions/$sessionId"
              params={{ sessionId: session.id }}
              className="flex items-center gap-2 rounded-md px-2 py-1 text-sm hover:bg-accent [&.active]:bg-accent"
            >
              <span className="flex-1 truncate">{session.name ?? session.id.slice(0, 8)}</span>
              {session.agent && <span className="shrink-0 text-xs text-muted-foreground">{session.agent}</span>}
              <StatusBadge status={session.status} />
            </Link>
          </li>
        ))}
      </ul>
    </section>
  );
}

const badges: Record<commands.Status, { label: string; className: string }> = {
  running: { label: "Running", className: "bg-blue-500 animate-pulse" },
  awaiting_approval: { label: "Awaiting approval", className: "bg-amber-500" },
  idle: { label: "Idle", className: "bg-muted-foreground/30" },
  elsewhere: { label: "Driven by another process", className: "bg-violet-500" },
};

function StatusBadge({ status }: { status: commands.Status }) {
  const { label, className } = badges[status];
  return <span role="img" aria-label={label} title={label} className={cn("size-2 shrink-0 rounded-full", className)} />;
}
