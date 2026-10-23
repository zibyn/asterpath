//! Shared HTTP failure classification and header extraction.

use std::time::Duration;

use asterpath_http::{classify_status, request_id, retry_after};
use asterpath_protocol::ProviderErrorKind;
use reqwest::header::{HeaderMap, HeaderValue};

#[test]
fn status_codes_map_to_provider_error_kinds() {
    let cases = [
        (400, ProviderErrorKind::InvalidRequest),
        (401, ProviderErrorKind::Auth),
        (403, ProviderErrorKind::Auth),
        (404, ProviderErrorKind::InvalidRequest),
        (408, ProviderErrorKind::Timeout),
        (422, ProviderErrorKind::InvalidRequest),
        (429, ProviderErrorKind::RateLimited),
        (500, ProviderErrorKind::Server),
        (502, ProviderErrorKind::Server),
        (503, ProviderErrorKind::Overloaded),
        (504, ProviderErrorKind::Server),
        (529, ProviderErrorKind::Overloaded),
        (599, ProviderErrorKind::Server),
    ];
    for (status, kind) in cases {
        assert_eq!(classify_status(status), kind, "HTTP {status}");
    }
}

fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(*name, HeaderValue::from_static(value));
    }
    map
}

#[test]
fn retry_after_reads_integer_seconds_only() {
    assert_eq!(
        retry_after(&headers(&[("retry-after", " 7 ")])),
        Some(Duration::from_secs(7))
    );
    assert_eq!(
        retry_after(&headers(&[(
            "retry-after",
            "Wed, 21 Oct 2026 07:28:00 GMT"
        )])),
        None
    );
    assert_eq!(retry_after(&HeaderMap::new()), None);
}

#[test]
fn request_id_reads_either_header_name() {
    assert_eq!(
        request_id(&headers(&[("request-id", "req_a")])).as_deref(),
        Some("req_a")
    );
    assert_eq!(
        request_id(&headers(&[("x-request-id", "req_b")])).as_deref(),
        Some("req_b")
    );
    assert_eq!(request_id(&HeaderMap::new()), None);
}
