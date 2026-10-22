//! `ScriptedProvider` replays deterministic responses, records the requests it
//! saw, and fails without touching the network.

use asterpath_protocol::{
    Item, ModelRequest, ModelSpec, Part, Provider, ProviderCapabilities, ProviderError,
    ProviderErrorKind, RequestConfig, Role, SamplingParams, StopReason, StreamEvent, Usage,
};
use asterpath_testing::ScriptedProvider;

fn spec() -> ModelSpec {
    ModelSpec {
        provider: asterpath_protocol::ProviderId::new("scripted"),
        model: asterpath_protocol::ModelId::new("m1"),
        context_window: None,
    }
}

fn request(request_config: &RequestConfig) -> ModelRequest {
    ModelRequest::new(spec(), 0, vec![], vec![], vec![], request_config)
}

fn assistant_message(text: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
    }
}

#[test]
fn replays_a_scripted_response_in_canonical_order() {
    let provider = ScriptedProvider::new("scripted").respond(
        vec![assistant_message("hi")],
        StopReason::EndTurn,
        Usage::default(),
    );

    let events: Vec<_> = futures_executor::block_on(async {
        use futures_util::StreamExt;
        provider
            .stream(request(&RequestConfig::default()))
            .collect::<Vec<_>>()
            .await
    });

    assert!(matches!(
        events.first(),
        Some(Ok(StreamEvent::ResponseStarted { .. }))
    ));
    assert!(matches!(
        events.last(),
        Some(Ok(StreamEvent::ResponseEnded {
            stop_reason: StopReason::EndTurn,
            ..
        }))
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::PartDelta {
            delta: asterpath_protocol::PartDelta::Text { text },
            ..
        }) if text == "hi"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::ItemCompleted { item, .. }) if *item == assistant_message("hi")
    )));
}

#[test]
fn records_the_received_request() {
    let provider = ScriptedProvider::new("scripted").respond(
        vec![assistant_message("ok")],
        StopReason::EndTurn,
        Usage::default(),
    );
    let request_config = RequestConfig {
        max_output_tokens: std::num::NonZeroU32::new(1024).unwrap(),
        ..RequestConfig::default()
    };

    let _ = futures_executor::block_on(async {
        use futures_util::StreamExt;
        provider
            .stream(ModelRequest::new(
                spec(),
                2,
                vec![Part::Text {
                    text: "be brief".to_owned(),
                }],
                vec![],
                vec![],
                &request_config,
            ))
            .collect::<Vec<_>>()
            .await
    });

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].prefix_epoch, 2);
    assert_eq!(requests[0].max_output_tokens, 1024);
}

#[test]
fn mid_stream_failure_never_ends_the_response() {
    let error = ProviderError::new(ProviderErrorKind::Transport, "cut");
    let provider = ScriptedProvider::new("scripted")
        .partial_then_fail(vec![assistant_message("partial")], error.clone());

    let events: Vec<_> = futures_executor::block_on(async {
        use futures_util::StreamExt;
        provider
            .stream(request(&RequestConfig::default()))
            .collect::<Vec<_>>()
            .await
    });

    assert!(events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::ItemCompleted { item, .. }) if *item == assistant_message("partial")
    )));
    assert_eq!(events.last(), Some(&Err(error)));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. })))
    );
}

#[test]
fn capability_mismatch_fails_before_the_script_is_used() {
    let provider = ScriptedProvider::new("scripted")
        .with_capabilities(ProviderCapabilities::default())
        .respond(
            vec![assistant_message("never")],
            StopReason::EndTurn,
            Usage::default(),
        );
    let request_config = RequestConfig {
        sampling: Some(SamplingParams {
            temperature: Some(0.2),
            ..SamplingParams::default()
        }),
        ..RequestConfig::default()
    };

    let events: Vec<_> = futures_executor::block_on(async {
        use futures_util::StreamExt;
        provider
            .stream(request(&request_config))
            .collect::<Vec<_>>()
            .await
    });

    assert_eq!(
        events,
        vec![Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            "sampling is not supported by this model"
        ))]
    );
    assert_eq!(provider.remaining(), 1);
}
