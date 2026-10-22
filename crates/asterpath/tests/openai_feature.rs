#![cfg(feature = "openai")]

use asterpath_protocol::{Provider, ProviderId};

#[test]
fn facade_reexports_the_openai_adapter() {
    let provider = asterpath::openai::OpenAiProvider::new("test-key");
    assert_eq!(provider.id(), ProviderId::new("openai"));
}
