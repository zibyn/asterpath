import { useQuery } from "@tanstack/react-query";
import { createRootRoute, createRoute, createRouter, Outlet } from "@tanstack/react-router";

import { Conversation } from "@/components/conversation";
import { Settings } from "@/components/settings";
import { Sidebar } from "@/components/sidebar";
import { StatusBar } from "@/components/status-bar";
import * as commands from "@/lib/commands";

const rootRoute = createRootRoute({
  component: function Root() {
    const setup = useQuery({ queryKey: ["setup"], queryFn: commands.setup });
    // Nothing runs without a provider, model and key: settings first.
    if (setup.data?.needed) return <Settings />;
    return (
      <div className="flex h-screen">
        <Sidebar />
        <main className="flex-1 overflow-hidden">
          <Outlet />
        </main>
      </div>
    );
  },
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => <p className="p-6 text-sm text-muted-foreground">Pick a session.</p>,
});

const sessionRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/sessions/$sessionId",
  component: function Session() {
    const { sessionId } = sessionRoute.useParams();
    return (
      <div className="flex h-full flex-col">
        <div className="min-h-0 flex-1">
          <Conversation key={sessionId} session={sessionId} />
        </div>
        <StatusBar session={sessionId} />
      </div>
    );
  },
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: Settings,
});

export const router = createRouter({
  routeTree: rootRoute.addChildren([indexRoute, sessionRoute, settingsRoute]),
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
