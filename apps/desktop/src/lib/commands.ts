// Hand-written mirrors of the Tauri commands in `src-tauri/src/main.rs` and
// the types of `src-tauri/src/lib.rs` (ADR-0035: no generated types).
import { invoke } from "@tauri-apps/api/core";

/** A work root in the sidebar. */
export interface Project {
  root: string;
  name: string;
}

/** A root session of a project. Timestamps are milliseconds since the epoch. */
export interface SessionSummary {
  id: string;
  name: string | null;
  created_at: number;
  last_activity: number;
}

export const projects = () => invoke<Project[]>("projects");

/** Opens the folder picker; `null` when cancelled. */
export const addProject = () => invoke<Project | null>("add_project");

export const sessions = (root: string) => invoke<SessionSummary[]>("sessions", { root });
