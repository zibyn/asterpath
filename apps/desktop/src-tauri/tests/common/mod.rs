use asterpath_protocol::{ModelId, ModelSpec, ProviderId, SessionSpec, ToolSelection};

/// A session spec on the scripted provider `p`.
pub fn template() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
        tools: ToolSelection::All,
        request: Default::default(),
        turn: Default::default(),
        compaction: Default::default(),
        subagents: Default::default(),
        metadata: Default::default(),
    }
}
