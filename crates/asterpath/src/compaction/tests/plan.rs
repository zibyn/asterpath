//! Pure planning behaviour of the default compactor.

use std::sync::Arc;

use crate::compaction::{DefaultCompactor, usable_window};
use asterpath_protocol::{
    CompactPlan, CompactionConfig, CompactionInput, CompactionTrigger, Compactor, Item, ModelId,
    ModelSpec, Part, ProjectedEntry, ProjectedOrigin, ProviderCatalog, ProviderId, Role,
    TurnConfig, Usage, UsageAnchor,
};
use asterpath_protocol::{LogId, LogIdRange};

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn message(role: Role, value: &str) -> Vec<Item> {
    vec![Item::Message {
        role,
        parts: vec![text(value)],
        raw: None,
    }]
}

fn log(id: i64, items: Vec<Item>) -> ProjectedEntry {
    ProjectedEntry {
        origin: ProjectedOrigin::Log(LogId::from_i64(id)),
        items,
    }
}

fn model(window: Option<u32>) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("m"),
        context_window: window,
    }
}

fn config() -> CompactionConfig {
    let mut config = CompactionConfig::default();
    config.reserve_output_tokens = Some(0);
    config.target_ratio = 0.5;
    config.max_auto_compactions_per_turn = 1;
    config.max_summary_tokens = 2048;
    config
}

fn input(
    trigger: CompactionTrigger,
    history: Vec<ProjectedEntry>,
    step_starts: Vec<i64>,
) -> CompactionInput {
    CompactionInput {
        request: None,
        trigger,
        model: model(Some(100)),
        config: config(),
        turn: TurnConfig::default(),
        history,
        step_starts: step_starts.into_iter().map(LogId::from_i64).collect(),
        last_usage: Some(UsageAnchor {
            origin: LogId::from_i64(1),
            usage: Usage::default(),
        }),
    }
}

fn compactor() -> DefaultCompactor {
    DefaultCompactor::new(Arc::new(ProviderCatalog::default()))
}

#[test]
fn missing_context_window_is_not_configured() {
    let mut input = input(
        CompactionTrigger::Manual,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, "")),
        ],
        vec![1, 3],
    );
    input.model.context_window = None;
    assert!(matches!(
        compactor().plan(&input),
        CompactPlan::NotConfigured
    ));
    assert_eq!(usable_window(&input.model, &input.config), None);
}

#[test]
fn threshold_skips_without_usage_or_below_window() {
    let mut input = input(
        CompactionTrigger::Threshold,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, "ok")),
        ],
        vec![1, 3],
    );
    input.last_usage = None;
    assert!(matches!(compactor().plan(&input), CompactPlan::NotNeeded));

    input.last_usage = Some(UsageAnchor {
        origin: LogId::from_i64(1),
        usage: Usage::default(),
    });
    assert!(matches!(compactor().plan(&input), CompactPlan::NotNeeded));
}

#[test]
fn one_complete_step_has_nothing_to_do() {
    let input = input(
        CompactionTrigger::Manual,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, "ok")),
        ],
        vec![3],
    );
    assert!(matches!(compactor().plan(&input), CompactPlan::NothingToDo));
}

#[test]
fn keeps_the_latest_step_when_it_already_meets_the_target() {
    let large = "word ".repeat(100);
    let input = input(
        CompactionTrigger::Manual,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, &large)),
            log(5, message(Role::Assistant, &large)),
        ],
        vec![3, 5],
    );
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    assert_eq!(job.first_kept, LogId::from_i64(5));
    assert_eq!(job.replaces.first, LogId::from_i64(1));
    assert_eq!(job.replaces.last, LogId::from_i64(3));
    assert_eq!(job.usable_window, 100);
    assert_eq!(job.max_summary_tokens, 2048);
}

#[test]
fn walks_back_until_retention_reaches_the_target() {
    let large = "word ".repeat(100);
    let input = input(
        CompactionTrigger::Manual,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, &large)),
            log(5, message(Role::Assistant, &large)),
            log(7, message(Role::Assistant, "tiny")),
        ],
        vec![3, 5, 7],
    );
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    assert_eq!(job.first_kept, LogId::from_i64(5));
}

#[test]
fn never_reaches_target_keeps_the_earliest_deletable_step() {
    let input = input(
        CompactionTrigger::Manual,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, "a")),
            log(5, message(Role::Assistant, "b")),
            log(7, message(Role::Assistant, "c")),
        ],
        vec![3, 5, 7],
    );
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    assert_eq!(job.first_kept, LogId::from_i64(5));
}

#[test]
fn replaces_starts_at_a_previous_summary_range() {
    let large = "word ".repeat(100);
    let input = input(
        CompactionTrigger::Manual,
        vec![
            ProjectedEntry {
                origin: ProjectedOrigin::Summary(LogIdRange {
                    first: LogId::from_i64(1),
                    last: LogId::from_i64(4),
                }),
                items: vec![Item::Compaction {
                    parts: vec![text("old summary")],
                }],
            },
            log(5, message(Role::Assistant, &large)),
            log(7, message(Role::Assistant, &large)),
        ],
        vec![5, 7],
    );
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    assert_eq!(job.replaces.first, LogId::from_i64(1));
    assert_eq!(job.replaces.last, LogId::from_i64(5));
    let rendered = format!("{:?}", job.summary_input);
    assert!(rendered.contains("old summary"));
}

#[test]
fn summary_input_is_text_only() {
    let history = vec![
        log(1, message(Role::User, "hello")),
        log(
            2,
            vec![Item::Message {
                role: Role::User,
                parts: vec![Part::File {
                    media_type: "application/pdf".to_owned(),
                    filename: Some("secret-report.pdf".to_owned()),
                    source: asterpath_protocol::FileSource::Base64 {
                        data: "JVBERi0xLjQ".to_owned(),
                    },
                }],
                raw: None,
            }],
        ),
        log(
            3,
            vec![
                Item::Reasoning {
                    summary: vec![text("secret reasoning")],
                    binding: asterpath_protocol::ReasoningBinding {
                        provider: ProviderId::new("p"),
                        model: ModelId::new("m"),
                        prefix_epoch: 0,
                        tools_hash: String::new(),
                    },
                    raw: asterpath_protocol::RawProviderData {
                        provider: ProviderId::new("p"),
                        model: None,
                        kind: "raw".to_owned(),
                        json: asterpath_protocol::JsonString::new("{}"),
                    },
                },
                Item::Message {
                    role: Role::Assistant,
                    parts: vec![
                        text("answer"),
                        Part::Image {
                            media_type: "image/png".to_owned(),
                            source: asterpath_protocol::ImageSource::Url {
                                url: "https://example.invalid/x.png".to_owned(),
                            },
                        },
                        Part::ProviderOpaque {
                            provider: ProviderId::new("p"),
                            kind: "wire-opaque".to_owned(),
                            raw: asterpath_protocol::RawProviderData {
                                provider: ProviderId::new("p"),
                                model: None,
                                kind: "raw".to_owned(),
                                json: asterpath_protocol::JsonString::new("{}"),
                            },
                        },
                    ],
                    raw: None,
                },
                Item::ToolCall {
                    call_id: "call-1".to_owned(),
                    provider_item_id: None,
                    name: asterpath_protocol::ToolName::new("shell"),
                    raw_args: asterpath_protocol::JsonString::new("{\"cmd\":\"ls\"}"),
                    raw: Some(asterpath_protocol::RawProviderData {
                        provider: ProviderId::new("p"),
                        model: None,
                        kind: "raw".to_owned(),
                        json: asterpath_protocol::JsonString::new("{}"),
                    }),
                },
            ],
        ),
        log(5, message(Role::Assistant, &"word ".repeat(100))),
    ];
    let input = input(CompactionTrigger::Manual, history, vec![3, 5]);
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    let rendered = format!("{:?}", job.summary_input);
    assert!(!rendered.contains("secret reasoning"));
    assert!(!rendered.contains("image/png"));
    assert!(!rendered.contains("https://example.invalid"));
    assert!(!rendered.contains("wire-opaque"));
    assert!(!rendered.contains("secret-report.pdf"));
    assert!(!rendered.contains("JVBERi0xLjQ"));
    assert!(rendered.contains("answer"));
    assert!(rendered.contains("non-text content omitted"));
    assert!(rendered.contains("tool call shell"));
}

#[test]
fn summary_model_defaults_to_the_session_model_and_can_be_overridden() {
    let large = "word ".repeat(100);
    let mut input = input(
        CompactionTrigger::Manual,
        vec![
            log(1, message(Role::User, "hi")),
            log(3, message(Role::Assistant, &large)),
            log(5, message(Role::Assistant, &large)),
        ],
        vec![3, 5],
    );
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    assert_eq!(job.model, input.model);

    let override_model = ModelSpec {
        provider: ProviderId::new("other"),
        model: ModelId::new("summary"),
        context_window: Some(10),
    };
    input.config.summary_model = Some(override_model.clone());
    input.config.cache_ttl.messages = asterpath_protocol::CacheLifetime::OneHour;
    let CompactPlan::Summarize(job) = compactor().plan(&input) else {
        panic!("expected summarize");
    };
    assert_eq!(job.model, override_model);
    assert_eq!(job.cache_ttl, input.config.cache_ttl);
}
