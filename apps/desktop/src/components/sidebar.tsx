import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { FolderPlus, Plus } from "lucide-react";

import { Button } from "@/components/ui/button";
import * as commands from "@/lib/commands";

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
        <Button variant="ghost" size="icon" aria-label="Add project" onClick={() => add.mutate()}>
          <FolderPlus />
        </Button>
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
  });
  const create = useMutation({
    mutationFn: () => commands.createSession(project.root),
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
              className="block truncate rounded-md px-2 py-1 text-sm hover:bg-accent [&.active]:bg-accent"
            >
              {session.name ?? session.id.slice(0, 8)}
            </Link>
          </li>
        ))}
      </ul>
    </section>
  );
}
