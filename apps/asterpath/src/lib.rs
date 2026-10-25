//! The local application (ADR-0049): the CLI (`asterpath`) and the desktop
//! window (`asterpath-desktop`, behind the `tauri` feature) share these
//! modules. Not a stable surface.

pub mod config;
pub mod desktop;
pub mod workspace;
