import { createRootRoute, createRoute, createRouter, Outlet } from "@tanstack/react-router";

import { Conversation } from "@/components/conversation";
import { Sidebar } from "@/components/sidebar";

const rootRoute = createRootRoute({
  component: () => (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex-1 overflow-hidden">
        <Outlet />
      </main>
    </div>
  ),
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
    return <Conversation key={sessionId} session={sessionId} />;
  },
});

export const router = createRouter({ routeTree: rootRoute.addChildren([indexRoute, sessionRoute]) });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
