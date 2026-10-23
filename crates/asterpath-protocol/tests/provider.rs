//! Provider contract behaviour: catalog identity, error classification and the
//! typed log encoder the agent builds `NewEntry` payloads with.

use std::sync::Arc;

use asterpath_protocol::{DedupKey, SessionId};
use asterpath_protocol::{
    Provider, ProviderCapabilities, ProviderCatalog, ProviderError, ProviderErrorKind, ProviderId,
    SessionEvent, StopReason, StreamEvent, decode_entry, encode_entry,
};
use futures_util::stream::{self, StreamExt};

struct NullProvider {
    id: ProviderId,
}

impl Provider for NullProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn capabilities(&self, _spec: &asterpath_protocol::ModelSpec) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    fn stream(
        &self,
        _request: asterpath_protocol::ModelRequest,
    ) -> futures_core::stream::BoxStream<'_, Result<StreamEvent, ProviderError>> {
        stream::empty().boxed()
    }
}

fn provider(id: &str) -> Arc<dyn Provider> {
    Arc::new(NullProvider {
        id: ProviderId::new(id),
    })
}

#[test]
fn catalog_rejects_duplicate_provider_ids() {
    let error = ProviderCatalog::new(vec![provider("p"), provider("p")]).unwrap_err();
    assert_eq!(error.id, ProviderId::new("p"));
}

#[test]
fn catalog_resolves_by_id() {
    let catalog = ProviderCatalog::new(vec![provider("a"), provider("b")]).unwrap();
    assert!(catalog.get(&ProviderId::new("a")).is_some());
    assert!(catalog.get(&ProviderId::new("missing")).is_none());
}

#[test]
fn error_classification_defaults_and_context_overflow() {
    let transport = ProviderError::new(ProviderErrorKind::Transport, "reset");
    assert!(transport.retryable);
    assert!(!transport.context_exceeded());

    let overloaded = ProviderError::new(ProviderErrorKind::Overloaded, "busy");
    assert!(overloaded.retryable);

    let context = ProviderError::new(ProviderErrorKind::ContextExceeded, "too long");
    assert!(!context.retryable);
    assert!(context.context_exceeded());

    let rejected = ProviderError::new(ProviderErrorKind::InvalidRequest, "bad");
    assert!(!rejected.retryable);
}

#[test]
fn encode_entry_produces_a_decodable_envelope() {
    let session = SessionId::from_bytes([7; 16]);
    let event = SessionEvent::TurnStarted {
        turn_id: asterpath_protocol::TurnId::from_bytes([9; 16]),
    };
    let entry = encode_entry(session, Some(DedupKey::new("k")), &event).unwrap();

    assert_eq!(entry.session, session);
    assert_eq!(entry.schema_version, asterpath_protocol::LOG_SCHEMA_VERSION);
    assert_eq!(entry.dedup_key.as_ref().unwrap().as_str(), "k");
    let decoded = decode_entry(&asterpath_protocol::LogEntry {
        log_id: asterpath_protocol::LogId::from_i64(1),
        session,
        seq: 1,
        recorded_at: 0,
        schema_version: entry.schema_version,
        dedup_key: entry.dedup_key,
        payload: entry.payload,
    })
    .unwrap();
    assert_eq!(decoded, event);
}

#[test]
fn stop_reason_carries_provider_label_for_unknown_values() {
    let reason = StopReason::Other {
        provider_label: "mystery".to_owned(),
    };
    assert_eq!(
        reason,
        StopReason::Other {
            provider_label: "mystery".to_owned()
        }
    );
}

fn capability_request() -> asterpath_protocol::ModelRequest {
    asterpath_protocol::ModelRequest::new(
        asterpath_protocol::ModelSpec {
            provider: ProviderId::new("p"),
            model: asterpath_protocol::ModelId::new("m"),
            context_window: None,
        },
        0,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &asterpath_protocol::RequestConfig::default(),
    )
}

#[test]
fn capability_check_names_the_unsupported_feature() {
    use asterpath_protocol::{ImageSource, Item, Part, Role, SamplingParams, ToolChoice};

    let none = ProviderCapabilities::default();
    assert_eq!(none.unsupported_feature(&capability_request()), None);

    let mut forced = capability_request();
    forced.tool_choice = ToolChoice::Required;
    assert_eq!(
        none.unsupported_feature(&forced),
        Some("forced tool choice")
    );

    let mut sampling = capability_request();
    sampling.sampling = Some(SamplingParams::default());
    assert_eq!(none.unsupported_feature(&sampling), Some("sampling"));

    let mut serial = capability_request();
    serial.disable_parallel_tool_use = true;
    assert_eq!(
        none.unsupported_feature(&serial),
        Some("disabling parallel tool calls")
    );

    let mut image = capability_request();
    image.items.push(Item::Message {
        role: Role::User,
        parts: vec![Part::Image {
            media_type: "image/png".to_owned(),
            source: ImageSource::Url {
                url: "https://example.invalid/x.png".to_owned(),
            },
        }],
    });
    assert_eq!(none.unsupported_feature(&image), Some("image input"));

    let all = ProviderCapabilities {
        forced_tool_choice: true,
        sampling: true,
        parallel_tool_calls: true,
        image_input: true,
        ..ProviderCapabilities::default()
    };
    for request in [forced, sampling, serial, image] {
        assert_eq!(all.unsupported_feature(&request), None);
    }
}
