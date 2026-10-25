//! ADR-0039: users are host metadata. Two users share one `Asterpath`; the
//! catalog filters by metadata and a host provider wrapper picks
//! credentials from the request's session context.
#![cfg(feature = "sqlite")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::Asterpath;
use asterpath::protocol::{
    CompactionConfig, InputId, Item, JsonString, ModelId, ModelRequest, ModelSpec, Part, Provider,
    ProviderCapabilities, ProviderError, ProviderId, RequestConfig, Role, SessionId, SessionQuery,
    SessionSpec, StopReason, StreamEvent, SubagentConfig, ToolName, ToolSelection, TurnConfig,
    Usage, UserInput,
};
use asterpath_testing::ScriptedProvider;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

fn spec(user: &str) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: BTreeMap::from([("user".to_owned(), user.to_owned())]),
    }
}

fn says(text: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
        raw: None,
    }]
}

/// A host wrapper: picks an API key by the session's `user` metadata.
struct PerUserKeys {
    inner: ScriptedProvider,
    /// `(session, root, key)` per request.
    used: Mutex<Vec<(SessionId, SessionId, String)>>,
}

impl Provider for PerUserKeys {
    fn id(&self) -> ProviderId {
        self.inner.id()
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        self.inner.capabilities(spec)
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let context = request.session.clone().expect("a session's request");
        let key = format!("key-of-{}", context.metadata["user"]);
        self.used
            .lock()
            .unwrap()
            .push((context.session, context.root, key));
        self.inner.stream(request)
    }
}

#[tokio::test]
async fn two_users_share_one_asterpath() {
    let db = std::env::temp_dir().join(format!("asterpath-multi-user-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db);
    let spawn = Item::ToolCall {
        call_id: "call-spawn".to_owned(),
        provider_item_id: None,
        name: ToolName::new("spawn_agent"),
        raw_args: JsonString::new(r#"{"task":"help"}"#),
        raw: None,
    };
    let provider = Arc::new(PerUserKeys {
        inner: ScriptedProvider::new("p")
            .respond(vec![spawn], StopReason::ToolUse, Usage::default())
            .respond(says("child done"), StopReason::EndTurn, Usage::default())
            .respond(says("alice done"), StopReason::EndTurn, Usage::default())
            .respond(says("bob done"), StopReason::EndTurn, Usage::default()),
        used: Mutex::new(Vec::new()),
    });
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider.clone() as Arc<dyn Provider>])
        .build()
        .await
        .expect("build");

    let mut roots = Vec::new();
    for (byte, user) in [(1, "alice"), (2, "bob")] {
        let handle = asterpath
            .create_session(spec(user))
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([byte; 16]),
                parts: vec![Part::Text { text: "hi".into() }],
            })
            .await
            .unwrap();
        let mut stream = handle.drive();
        while let Some(event) = stream.next().await {
            event.expect("drive");
        }
        roots.push(handle.id());
    }
    let (alice, bob) = (roots[0], roots[1]);

    // Each user lists only their own sessions.
    let mine = |user: &str| SessionQuery {
        metadata: BTreeMap::from([("user".to_owned(), user.to_owned())]),
        ..SessionQuery::default()
    };
    let listed = |entries: Vec<asterpath::protocol::CatalogEntry>| {
        entries
            .iter()
            .map(|entry| entry.session)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        listed(asterpath.sessions(mine("alice")).await.unwrap()),
        [alice]
    );
    assert_eq!(
        listed(asterpath.sessions(mine("bob")).await.unwrap()),
        [bob]
    );

    // Every request carries its session; a child's carries its tree's root
    // and the root's metadata, since children do not inherit metadata.
    let used = provider.used.lock().unwrap().clone();
    assert_eq!(used.len(), 4, "{used:?}");
    let child = used[1].0;
    assert_ne!(child, alice);
    assert_eq!(
        used,
        [
            (alice, alice, "key-of-alice".to_owned()),
            (child, alice, "key-of-alice".to_owned()),
            (alice, alice, "key-of-alice".to_owned()),
            (bob, bob, "key-of-bob".to_owned()),
        ]
    );

    asterpath.shutdown().await.unwrap();
    for suffix in ["", "-wal", "-shm", ".owners"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}
