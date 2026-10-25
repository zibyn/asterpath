fn main() {
    // Only the window needs Tauri's code generation.
    #[cfg(feature = "tauri")]
    tauri_build::build();
}
