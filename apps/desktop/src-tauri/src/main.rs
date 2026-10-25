//! The Tauri window: one command per `Desktop` method (ADR-0035). The
//! frontend's `src/lib/commands.ts` mirrors these names and types.

use std::path::PathBuf;

use asterpath_desktop::{Desktop, Project, SessionSummary};
use tauri::{Manager, State};
use tauri_plugin_dialog::DialogExt;

/// The backend, or why it did not start (shown by every command).
struct Backend(Result<Desktop, String>);

impl Backend {
    fn get(&self) -> Result<&Desktop, String> {
        self.0.as_ref().map_err(Clone::clone)
    }
}

#[tauri::command]
async fn projects(backend: State<'_, Backend>) -> Result<Vec<Project>, String> {
    backend.get()?.projects().await
}

/// Pick a folder and add it; `None` when the picker is cancelled.
#[tauri::command]
async fn add_project(
    app: tauri::AppHandle,
    backend: State<'_, Backend>,
) -> Result<Option<Project>, String> {
    let Some(dir) = app.dialog().file().blocking_pick_folder() else {
        return Ok(None);
    };
    let dir = dir.into_path().map_err(|error| error.to_string())?;
    backend.get()?.add_project(&dir).map(Some)
}

#[tauri::command]
async fn sessions(
    backend: State<'_, Backend>,
    root: PathBuf,
) -> Result<Vec<SessionSummary>, String> {
    backend.get()?.sessions(&root).await
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let projects_file = app.path().app_data_dir()?.join("projects.json");
            let desktop = tauri::async_runtime::block_on(Desktop::open(projects_file));
            app.manage(Backend(desktop));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![projects, add_project, sessions])
        .run(tauri::generate_context!())
        .expect("error while running the desktop app");
}
