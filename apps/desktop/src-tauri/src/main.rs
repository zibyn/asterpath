//! The Tauri window: one command per `Desktop` method (ADR-0035). The
//! frontend's `src/lib/commands.ts` mirrors these names and types.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use asterpath::protocol::ModelSpec;
use asterpath::{LogId, SessionId};
use asterpath_desktop::{
    Access, Answer, ApprovalMode, Desktop, Project, SessionInfo, SessionSummary, ViewEvent,
};
use asterpath_protocol::{ApprovalRequest, ToolCallId};
use futures_util::StreamExt;
use tauri::async_runtime::JoinHandle;
use tauri::ipc::Channel;
use tauri::{Manager, State};
use tauri_plugin_dialog::DialogExt;

/// The backend, or why it did not start (shown by every command).
struct Backend(Result<Desktop, String>);

impl Backend {
    fn get(&self) -> Result<&Desktop, String> {
        self.0.as_ref().map_err(Clone::clone)
    }
}

/// The open conversation views, by channel id.
// shortcut: a webview reload leaves its views running until the app exits; stop them on page load if that matters
#[derive(Default)]
struct Views(Mutex<HashMap<u32, JoinHandle<()>>>);

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

#[tauri::command]
async fn create_session(
    backend: State<'_, Backend>,
    root: PathBuf,
    name: Option<String>,
) -> Result<SessionSummary, String> {
    backend.get()?.create_session(&root, name.as_deref()).await
}

#[tauri::command]
async fn access(backend: State<'_, Backend>, session: SessionId) -> Result<Access, String> {
    backend.get()?.access(session).await
}

/// Stream the session to `on_event` until `unwatch` with the channel's id.
#[tauri::command]
async fn watch(
    backend: State<'_, Backend>,
    views: State<'_, Views>,
    session: SessionId,
    after: Option<LogId>,
    on_event: Channel<ViewEvent>,
) -> Result<(), String> {
    let mut stream = backend.get()?.watch(session, after).await?;
    let id = on_event.id();
    let view = tauri::async_runtime::spawn(async move {
        while let Some(Ok(event)) = stream.next().await {
            if on_event.send(event).is_err() {
                return;
            }
        }
    });
    let replaced = views.0.lock().unwrap().insert(id, view);
    if let Some(replaced) = replaced {
        replaced.abort();
    }
    Ok(())
}

#[tauri::command]
fn unwatch(views: State<'_, Views>, channel: u32) {
    if let Some(view) = views.0.lock().unwrap().remove(&channel) {
        view.abort();
    }
}

#[tauri::command]
async fn send(
    backend: State<'_, Backend>,
    session: SessionId,
    message: String,
) -> Result<(), String> {
    backend.get()?.send(session, message).await
}

#[tauri::command]
async fn cancel(backend: State<'_, Backend>, session: SessionId) -> Result<(), String> {
    backend.get()?.cancel(session).await
}

#[tauri::command]
fn approvals(
    backend: State<'_, Backend>,
    session: SessionId,
) -> Result<Vec<ApprovalRequest>, String> {
    Ok(backend.get()?.approvals(session))
}

#[tauri::command]
async fn approve(
    backend: State<'_, Backend>,
    session: SessionId,
    call: ToolCallId,
    answer: Answer,
) -> Result<(), String> {
    backend.get()?.approve(session, call, answer).await
}

#[tauri::command]
async fn approval_mode(
    backend: State<'_, Backend>,
    session: SessionId,
) -> Result<ApprovalMode, String> {
    backend.get()?.approval_mode(session).await
}

#[tauri::command]
async fn set_approval_mode(
    backend: State<'_, Backend>,
    session: SessionId,
    mode: ApprovalMode,
) -> Result<(), String> {
    backend.get()?.set_approval_mode(session, mode).await
}

#[tauri::command]
fn models(backend: State<'_, Backend>) -> Result<Vec<ModelSpec>, String> {
    Ok(backend.get()?.models())
}

#[tauri::command]
async fn session_info(
    backend: State<'_, Backend>,
    session: SessionId,
) -> Result<SessionInfo, String> {
    backend.get()?.session_info(session).await
}

#[tauri::command]
async fn set_model(
    backend: State<'_, Backend>,
    session: SessionId,
    model: ModelSpec,
) -> Result<(), String> {
    backend.get()?.set_model(session, model).await
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(Views::default())
        .setup(|app| {
            let projects_file = app.path().app_data_dir()?.join("projects.json");
            let desktop = tauri::async_runtime::block_on(Desktop::open(projects_file));
            app.manage(Backend(desktop));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            projects,
            add_project,
            sessions,
            create_session,
            access,
            watch,
            unwatch,
            send,
            cancel,
            approvals,
            approve,
            approval_mode,
            set_approval_mode,
            models,
            session_info,
            set_model
        ])
        .run(tauri::generate_context!())
        .expect("error while running the desktop app");
}
