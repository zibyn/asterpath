//! Minimal blocking HTTP/1.1 mock server for routing-test wire payloads.
//!
//! Hand-rolled instead of a mocking crate: the adapter tests only need exact
//! request bytes and a fixed response per connection.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

pub struct MockResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
    /// Byte offsets at which the body is cut into separate TCP writes.
    pub split_at: Vec<usize>,
}

impl MockResponse {
    pub fn sse(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".to_owned(), "text/event-stream".to_owned())],
            body: body.into(),
            split_at: Vec::new(),
        }
    }

    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: body.into(),
            split_at: Vec::new(),
        }
    }

    #[must_use]
    pub fn split_at(mut self, offsets: &[usize]) -> Self {
        self.split_at = offsets.to_vec();
        self
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

pub struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockServer {
    pub fn start(responses: Vec<MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        thread::spawn(move || {
            for response in responses {
                let Ok((mut socket, _)) = listener.accept() else {
                    return;
                };
                let raw = read_request(&mut socket);
                captured
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(String::from_utf8_lossy(&raw).into_owned());
                let mut head = format!(
                    "HTTP/1.1 {} {}\r\ncontent-length: {}\r\nconnection: close\r\n",
                    response.status,
                    reason(response.status),
                    response.body.len()
                );
                for (name, value) in &response.headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                head.push_str("\r\n");
                let _ = socket.write_all(head.as_bytes());
                let _ = socket.set_nodelay(true);
                let body = response.body.as_bytes();
                let mut start = 0;
                for end in response.split_at.iter().copied().chain([body.len()]) {
                    let _ = socket.write_all(&body[start..end]);
                    let _ = socket.flush();
                    start = end;
                    if end < body.len() {
                        // Give the client time to read this piece as its own chunk.
                        thread::sleep(Duration::from_millis(30));
                    }
                }
            }
        });
        Self { addr, requests }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    /// Raw requests in arrival order, headers and body.
    pub fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Request bodies in arrival order.
    pub fn request_bodies(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|raw| {
                raw.split_once("\r\n\r\n")
                    .map(|(_, body)| body.to_owned())
                    .unwrap_or_default()
            })
            .collect()
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        529 => "Overloaded",
        _ => "Status",
    }
}

fn read_request(socket: &mut TcpStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 8192];
    let mut body_start = None;
    while let Ok(read) = socket.read(&mut buf) {
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buf[..read]);
        if body_start.is_none() {
            body_start = data
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4);
        }
        if let Some(start) = body_start {
            let head = String::from_utf8_lossy(&data[..start]).to_lowercase();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if data.len() >= start + length {
                data.truncate(start + length);
                break;
            }
        }
    }
    data
}
