//! Loopback HTTP/1.1 server replaying scripted provider wire responses.
//!
//! One reply per connection (`connection: close`), consumed in FIFO order, and
//! every request recorded. Rendering replies in a provider's wire format stays
//! with each adapter's tests. Once the replies run out the server stops
//! accepting, so an unexpected extra request fails to connect.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    Complete,
    /// Accept and read the request, then close without writing a response.
    Close,
    /// Declare `content-length` for the full body, send `n` bytes, then close.
    Truncate(usize),
}

#[derive(Clone, Debug)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub mode: Mode,
    /// Byte offsets at which the body is cut into separate TCP writes.
    pub split_at: Vec<usize>,
}

impl Reply {
    pub fn new(status: u16, content_type: &str, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), content_type.to_owned())],
            body: body.into(),
            mode: Mode::Complete,
            split_at: Vec::new(),
        }
    }

    pub fn sse(body: impl Into<String>) -> Self {
        Self::new(200, "text/event-stream", body)
    }

    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self::new(status, "application/json", body)
    }

    pub fn truncated_sse(body: impl Into<String>, cut: usize) -> Self {
        Self {
            mode: Mode::Truncate(cut),
            ..Self::sse(body)
        }
    }

    pub fn close() -> Self {
        Self {
            headers: Vec::new(),
            mode: Mode::Close,
            ..Self::new(0, "", "")
        }
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    #[must_use]
    pub fn split_at(mut self, offsets: &[usize]) -> Self {
        self.split_at = offsets.to_vec();
        self
    }
}

/// One received request: head (request line and headers) and body.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub head: String,
    pub body: String,
}

impl Recorded {
    pub fn json_body(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).expect("recorded body is JSON")
    }

    /// The request-target of the request line.
    pub fn path(&self) -> &str {
        self.head.split_whitespace().nth(1).unwrap_or_default()
    }
}

pub struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl MockServer {
    pub fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        thread::spawn(move || {
            for reply in replies {
                let Ok((mut socket, _)) = listener.accept() else {
                    return;
                };
                if let Some(request) = read_request(&mut socket) {
                    recorded
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(request);
                    serve(&mut socket, &reply);
                }
                let _ = socket.shutdown(std::net::Shutdown::Both);
            }
        });
        Self { addr, requests }
    }

    /// `http://127.0.0.1:<port>`, without a trailing slash or path.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn serve(socket: &mut TcpStream, reply: &Reply) {
    let body = reply.body.as_bytes();
    let sent = match reply.mode {
        Mode::Close => return,
        Mode::Complete => body.len(),
        Mode::Truncate(cut) => cut.min(body.len()),
    };
    let mut head = format!(
        "HTTP/1.1 {} X\r\ncontent-length: {}\r\nconnection: close\r\n",
        reply.status,
        body.len()
    );
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = socket.write_all(head.as_bytes());
    let _ = socket.set_nodelay(true);
    let mut start = 0;
    for end in reply.split_at.iter().copied().chain([sent]) {
        let end = end.min(sent);
        let _ = socket.write_all(&body[start..end]);
        let _ = socket.flush();
        start = end;
        if end < sent {
            // Give the client time to read this piece as its own chunk.
            thread::sleep(Duration::from_millis(30));
        }
    }
}

fn read_request(socket: &mut TcpStream) -> Option<Recorded> {
    let mut data = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = socket.read(&mut buf).ok()?;
        if read == 0 {
            return None;
        }
        data.extend_from_slice(&buf[..read]);
        let Some(head_end) = data.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&data[..head_end]).into_owned();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        let body_start = head_end + 4;
        if data.len() >= body_start + length {
            let body = String::from_utf8_lossy(&data[body_start..body_start + length]).into_owned();
            return Some(Recorded { head, body });
        }
    }
}
