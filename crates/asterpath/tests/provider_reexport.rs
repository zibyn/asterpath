//! The facade re-exports the Anthropic adapter under its feature flag but never
//! assembles one itself: providers remain application-injected.

#![cfg(feature = "anthropic")]

use asterpath::anthropic::AnthropicProvider;
use asterpath::protocol::Provider;

#[test]
fn anthropic_adapter_is_reexported_under_the_feature_flag() {
    let provider = AnthropicProvider::new("test-key");
    assert_eq!(provider.id().as_str(), "anthropic");
}
