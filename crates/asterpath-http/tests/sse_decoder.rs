use asterpath_http::{SseDecoder, SseEvent};

fn decode(chunks: &[&[u8]]) -> Vec<SseEvent> {
    let mut decoder = SseDecoder::new();
    let mut events = Vec::new();
    for chunk in chunks {
        events.extend(decoder.push(chunk).expect("valid stream"));
    }
    events.extend(decoder.finish().expect("valid stream"));
    events
}

fn data(data: &str) -> SseEvent {
    SseEvent {
        event: None,
        data: data.to_owned(),
    }
}

#[test]
fn multibyte_text_split_across_chunks_decodes_exactly() {
    let body = "data: 你好🙂世界\n\n".as_bytes();
    // Split inside a 3-byte CJK character and inside the 4-byte emoji.
    let cjk_mid = "data: ".len() + 1;
    let emoji_mid = "data: 你好".len() + 2;
    let events = decode(&[
        &body[..cjk_mid],
        &body[cjk_mid..emoji_mid],
        &body[emoji_mid..],
    ]);
    assert_eq!(events, vec![data("你好🙂世界")]);
}

#[test]
fn crlf_framed_stream_yields_each_event_like_lf() {
    let lf = "event: a\ndata: {\"n\":1}\n\nevent: b\ndata: {\"n\":2}\n\n";
    let crlf = lf.replace('\n', "\r\n");
    let mut decoder = SseDecoder::new();
    // Events arrive as soon as their frame completes, not at end of stream.
    let first_frame = &crlf.as_bytes()[..crlf.find("\r\n\r\n").unwrap() + 4];
    assert_eq!(decoder.push(first_frame).expect("valid").len(), 1);
    assert_eq!(decode(&[crlf.as_bytes()]), decode(&[lf.as_bytes()]));
    assert_eq!(
        decode(&[lf.as_bytes()]),
        vec![
            SseEvent {
                event: Some("a".to_owned()),
                data: "{\"n\":1}".to_owned()
            },
            SseEvent {
                event: Some("b".to_owned()),
                data: "{\"n\":2}".to_owned()
            },
        ]
    );
}

#[test]
fn crlf_split_between_chunks_still_ends_the_event() {
    let events = decode(&[b"data: x\r", b"\n\r", b"\ndata: y\r\n\r\n"]);
    assert_eq!(events, vec![data("x"), data("y")]);
}

#[test]
fn comments_are_skipped_and_multiline_data_joins_with_newline() {
    let events = decode(&[b": keep-alive\n\ndata: one\n: note\ndata:two\nid: 7\n\n"]);
    assert_eq!(events, vec![data("one\ntwo")]);
}

#[test]
fn trailing_event_without_blank_line_is_dispatched_at_finish() {
    assert_eq!(decode(&[b"data: tail"]), vec![data("tail")]);
}

#[test]
fn invalid_utf8_is_a_stream_protocol_error() {
    let mut decoder = SseDecoder::new();
    let error = decoder
        .push(b"data: \xff\xfe\n\n")
        .expect_err("invalid UTF-8 must not be replaced");
    assert_eq!(
        error.kind,
        asterpath_protocol::ProviderErrorKind::StreamProtocol
    );
}
