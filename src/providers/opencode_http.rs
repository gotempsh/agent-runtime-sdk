//! Loopback HTTP/1.1 and Server-Sent Events carrier for `opencode serve`.
//!
//! `opencode serve` speaks HTTP and SSE on a loopback port and leaves its own
//! stdio empty, so the turn loop cannot read it directly. This module bridges
//! the two: it exposes [`crate::ProtocolStreams`] whose reader yields
//! newline-delimited JSON frames and whose writer accepts them, and it runs a
//! task that translates those frames into real HTTP requests and a real SSE
//! subscription.
//!
//! The bridge is deliberately *dumb*. It performs requests it is told to
//! perform and reports what came back; it knows nothing about sessions,
//! permissions or prompts. All protocol sequencing lives in
//! [`super::opencode_serve`]'s synchronous state machine, which keeps that
//! logic unit-testable against scripted lines exactly like the Codex
//! app-server transport, and keeps this module testable against a scripted
//! HTTP server.
//!
//! No HTTP crate is used. `reqwest` is an optional dependency wired only to
//! the `temps-sandbox` feature, and pulling a TLS-capable client stack into
//! the default feature set to talk to `127.0.0.1` would be a poor trade. The
//! surface needed here is small and entirely plaintext loopback: one request
//! per connection with `Connection: close`, plus one long-lived connection
//! for the event stream. `crate::tailnet::proxy` already speaks HTTP/1.1 over
//! `tokio::net::TcpStream` in this crate for the same reason.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream,
};
use tokio::net::TcpStream;

use crate::ProtocolStreams;

/// Buffer shared by the bridge and the turn loop, in bytes.
///
/// Large enough that a burst of SSE events does not stall the reader task,
/// small enough to apply real backpressure if the turn loop stops consuming.
const BRIDGE_BUFFER_BYTES: usize = 256 * 1024;

/// How long to keep polling before declaring that the server never came up.
///
/// `opencode serve` signals readiness on no pipe this process can observe —
/// the runtime owns the child's stdio and the server writes nothing useful to
/// it — so readiness is polled against a real endpoint, matching the
/// reference driver.
const READINESS_ATTEMPTS: u32 = 50;
const READINESS_INTERVAL: Duration = Duration::from_millis(200);

/// Cap on a single HTTP response body, so a wedged or hostile server cannot
/// exhaust memory through the bridge.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Cap on one SSE payload line.
const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

/// One request the state machine asked the bridge to perform.
#[derive(Debug, serde::Deserialize)]
pub(super) struct BridgeCommand {
    /// Correlation identifier echoed on the response frame.
    #[serde(default)]
    pub id: Option<u64>,
    /// `GET`, `POST`, or the bridge-private `SUBSCRIBE`.
    pub method: String,
    /// Request target, including any query string.
    pub path: String,
    /// Optional JSON request body.
    #[serde(default)]
    pub body: Option<Value>,
}

/// Frame kinds the bridge emits. Prefixed so they cannot collide with a
/// native OpenCode event type.
pub(super) const FRAME_READY: &str = "@ready";
pub(super) const FRAME_RESPONSE: &str = "@response";
pub(super) const FRAME_EVENT: &str = "@event";
pub(super) const FRAME_ERROR: &str = "@error";
/// Emitted once the event stream is accepted and before any payload.
///
/// The prompt must not be sent until this arrives: the reference driver
/// subscribes first precisely so nothing emitted in a turn's first moments is
/// missed, and only the bridge can know when the stream is actually open.
pub(super) const FRAME_SUBSCRIBED: &str = "@subscribed";

/// Start the bridge for a turn and hand the runtime its protocol streams.
///
/// The returned streams are live immediately; the task behind them polls the
/// server for readiness first and emits [`FRAME_READY`] once it answers.
pub(super) fn connect(port: u16) -> ProtocolStreams {
    // Two independent pipes rather than the two halves of one. Splitting a
    // single duplex stream would keep it alive until *both* halves drop, so
    // the runtime closing its writer at the end of a turn would never reach
    // the bridge as end-of-input — and the bridge would keep the reader open
    // while the runtime waited for it to close. Separate pipes make each
    // direction close on its own.
    let (runtime_reader, bridge_writer) = tokio::io::duplex(BRIDGE_BUFFER_BYTES);
    let (bridge_reader, runtime_writer) = tokio::io::duplex(BRIDGE_BUFFER_BYTES);
    tokio::spawn(run_bridge(port, bridge_reader, bridge_writer));
    ProtocolStreams {
        reader: Box::new(runtime_reader),
        writer: Box::new(runtime_writer),
    }
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

/// Drive one turn's HTTP traffic until the state machine stops writing or the
/// server stops answering.
async fn run_bridge(port: u16, incoming: DuplexStream, outgoing: DuplexStream) {
    let outgoing = std::sync::Arc::new(tokio::sync::Mutex::new(outgoing));

    if let Err(error) = wait_until_ready(port).await {
        emit_error(&outgoing, &error).await;
        return;
    }
    if emit(&outgoing, &json!({"type": FRAME_READY}))
        .await
        .is_err()
    {
        return;
    }

    let mut commands = BufReader::new(incoming).lines();
    let mut subscription: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        // The turn loop closing its writer is the normal end of a turn.
        let Ok(Some(line)) = commands.next_line().await else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(command) = serde_json::from_str::<BridgeCommand>(&line) else {
            emit_error(
                &outgoing,
                "the OpenCode bridge received an unreadable command",
            )
            .await;
            break;
        };
        if command.method == "SUBSCRIBE" {
            if subscription.is_none() {
                subscription = Some(tokio::spawn(stream_events(
                    port,
                    command.path,
                    std::sync::Arc::clone(&outgoing),
                )));
            }
            continue;
        }
        let outcome = perform(port, &command.method, &command.path, command.body.as_ref()).await;
        let frame = match outcome {
            Ok((status, body)) => json!({
                "type": FRAME_RESPONSE,
                "id": command.id,
                "status": status,
                "body": body,
            }),
            Err(error) => json!({
                "type": FRAME_RESPONSE,
                "id": command.id,
                "status": 0,
                "error": error.to_string(),
            }),
        };
        if emit(&outgoing, &frame).await.is_err() {
            break;
        }
    }
    if let Some(subscription) = subscription {
        subscription.abort();
    }
}

/// Poll a cheap, always-safe endpoint until the server answers.
async fn wait_until_ready(port: u16) -> std::result::Result<(), String> {
    for attempt in 0..READINESS_ATTEMPTS {
        match tokio::time::timeout(
            Duration::from_secs(1),
            perform(port, "GET", "/global/health", None),
        )
        .await
        {
            Ok(Ok((200, body))) if body.get("healthy").and_then(Value::as_bool) == Some(true) => {
                return Ok(())
            }
            // Connection refused while the server is still binding its port is
            // expected for the first attempts.
            _ => {}
        }
        if attempt + 1 < READINESS_ATTEMPTS {
            tokio::time::sleep(READINESS_INTERVAL).await;
        }
    }
    Err("OpenCode's server never became reachable on its loopback port.".to_string())
}

/// Perform one request on its own connection and read the whole response.
async fn perform(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> std::io::Result<(u16, Value)> {
    let mut stream = TcpStream::connect(address(port)).await?;
    stream.set_nodelay(true).ok();
    let encoded = body.map(ToString::to_string).unwrap_or_default();
    let content_type = if body.is_some() {
        "Content-Type: application/json\r\n"
    } else {
        ""
    };
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Accept: application/json\r\nConnection: close\r\n{content_type}\
         Content-Length: {}\r\n\r\n",
        encoded.len()
    );
    stream.write_all(head.as_bytes()).await?;
    if !encoded.is_empty() {
        stream.write_all(encoded.as_bytes()).await?;
    }
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let (status, framing) = read_head(&mut reader).await?;
    let body = read_body(&mut reader, framing).await?;
    let body = if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&body).unwrap_or(Value::Null)
    };
    Ok((status, body))
}

/// Subscribe to the event stream and forward every payload as one frame.
///
/// The connection dying is how a `opencode serve` process that exited
/// mid-turn becomes visible: the kernel closes its sockets, this read fails,
/// and the turn is failed with a real diagnostic instead of hanging until the
/// turn deadline.
async fn stream_events(
    port: u16,
    path: String,
    outgoing: std::sync::Arc<tokio::sync::Mutex<DuplexStream>>,
) {
    let stream = match TcpStream::connect(address(port)).await {
        Ok(stream) => stream,
        Err(error) => {
            emit_error(
                &outgoing,
                &format!("Couldn't open OpenCode's event stream: {error}"),
            )
            .await;
            return;
        }
    };
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream);
    let head = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\n\r\n"
    );
    {
        let inner = reader.get_mut();
        if let Err(error) = inner.write_all(head.as_bytes()).await {
            emit_error(
                &outgoing,
                &format!("Couldn't request OpenCode's event stream: {error}"),
            )
            .await;
            return;
        }
        let _ = inner.flush().await;
    }
    let framing = match read_head(&mut reader).await {
        Ok((200, framing)) => framing,
        Ok((status, _)) => {
            emit_error(
                &outgoing,
                &format!("OpenCode's event stream returned HTTP {status}."),
            )
            .await;
            return;
        }
        Err(error) => {
            emit_error(
                &outgoing,
                &format!("Couldn't read OpenCode's event stream: {error}"),
            )
            .await;
            return;
        }
    };

    if emit(&outgoing, &json!({"type": FRAME_SUBSCRIBED}))
        .await
        .is_err()
    {
        return;
    }

    let mut body = BodyReader::new(&mut reader, framing);
    let mut payload = String::new();
    loop {
        match body.read_line().await {
            Ok(Some(line)) => {
                let line = line.trim_end_matches(['\r', '\n']);
                if let Some(data) = line.strip_prefix("data:") {
                    if payload.len() + data.len() > MAX_EVENT_BYTES {
                        emit_error(&outgoing, "An OpenCode event exceeded the size limit.").await;
                        return;
                    }
                    payload.push_str(data.trim_start());
                } else if line.is_empty() && !payload.is_empty() {
                    // A blank line terminates one SSE event.
                    let event = serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null);
                    payload.clear();
                    if !event.is_null()
                        && emit(&outgoing, &json!({"type": FRAME_EVENT, "event": event}))
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
            }
            Ok(None) => {
                emit_error(
                    &outgoing,
                    "OpenCode's event stream ended before the turn finished.",
                )
                .await;
                return;
            }
            Err(error) => {
                emit_error(
                    &outgoing,
                    &format!("OpenCode's event stream failed: {error}"),
                )
                .await;
                return;
            }
        }
    }
}

/// How a response body is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// Exactly this many bytes follow.
    Length(usize),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// Read until the connection closes.
    ToClose,
}

/// Read a status line and headers, returning the status and body framing.
async fn read_head<R>(reader: &mut BufReader<R>) -> std::io::Result<(u16, Framing)>
where
    R: AsyncRead + Unpin,
{
    let mut status_line = String::new();
    if reader.read_line(&mut status_line).await? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "the server closed the connection before responding",
        ));
    }
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the server sent an unreadable status line",
            )
        })?;

    let mut framing = Framing::ToClose;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).await? == 0 {
            break;
        }
        let header = header.trim_end_matches(['\r', '\n']);
        if header.is_empty() {
            break;
        }
        let Some((name, value)) = header.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim().to_ascii_lowercase(), value.trim());
        if name == "content-length" {
            if let Ok(length) = value.parse::<usize>() {
                framing = Framing::Length(length.min(MAX_RESPONSE_BYTES));
            }
        } else if name == "transfer-encoding" && value.to_ascii_lowercase().contains("chunked") {
            framing = Framing::Chunked;
        }
    }
    Ok((status, framing))
}

/// Read a complete, bounded response body.
async fn read_body<R>(reader: &mut BufReader<R>, framing: Framing) -> std::io::Result<String>
where
    R: AsyncRead + Unpin,
{
    match framing {
        Framing::Length(length) => {
            let mut buffer = vec![0_u8; length];
            reader.read_exact(&mut buffer).await?;
            Ok(String::from_utf8_lossy(&buffer).into_owned())
        }
        Framing::ToClose => {
            let mut buffer = Vec::new();
            reader
                .take(MAX_RESPONSE_BYTES as u64)
                .read_to_end(&mut buffer)
                .await?;
            Ok(String::from_utf8_lossy(&buffer).into_owned())
        }
        Framing::Chunked => {
            let mut body = BodyReader::new(reader, framing);
            let mut collected = String::new();
            while let Some(line) = body.read_line().await? {
                if collected.len() + line.len() > MAX_RESPONSE_BYTES {
                    break;
                }
                collected.push_str(&line);
            }
            Ok(collected)
        }
    }
}

/// Line reader that transparently removes chunked-transfer framing.
///
/// Server-Sent Events are always delivered chunked, so the interleaved
/// hex-length prefixes and their trailing `CRLF`s have to be stripped before
/// anything can look for `data:` lines. Scanning the raw socket for that
/// prefix instead would work right up until a chunk boundary split an event.
struct BodyReader<'a, R> {
    reader: &'a mut BufReader<R>,
    framing: Framing,
    /// Bytes still owed by the chunk currently being read.
    remaining: usize,
    finished: bool,
}

impl<'a, R> BodyReader<'a, R>
where
    R: AsyncRead + Unpin,
{
    fn new(reader: &'a mut BufReader<R>, framing: Framing) -> Self {
        let remaining = match framing {
            Framing::Length(length) => length,
            _ => 0,
        };
        Self {
            reader,
            framing,
            remaining,
            finished: false,
        }
    }

    /// Next logical body line, including its terminator, or `None` at the end.
    async fn read_line(&mut self) -> std::io::Result<Option<String>> {
        if self.finished {
            return Ok(None);
        }
        if self.framing == Framing::Chunked {
            return self.read_chunked_line().await;
        }
        let mut line = String::new();
        if self.reader.read_line(&mut line).await? == 0 {
            self.finished = true;
            return Ok(None);
        }
        if let Framing::Length(_) = self.framing {
            self.remaining = self.remaining.saturating_sub(line.len());
            if self.remaining == 0 {
                self.finished = true;
            }
        }
        Ok(Some(line))
    }

    /// Assemble one line from however many chunks it spans.
    async fn read_chunked_line(&mut self) -> std::io::Result<Option<String>> {
        let mut line = Vec::new();
        loop {
            if self.remaining == 0 && !self.next_chunk_header().await? {
                self.finished = true;
                return Ok((!line.is_empty()).then(|| String::from_utf8_lossy(&line).into_owned()));
            }
            let mut byte = [0_u8; 1];
            self.reader.read_exact(&mut byte).await?;
            self.remaining -= 1;
            line.push(byte[0]);
            if self.remaining == 0 {
                // Consume the CRLF that terminates the chunk itself.
                let mut terminator = [0_u8; 2];
                self.reader.read_exact(&mut terminator).await?;
            }
            if byte[0] == b'\n' {
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            if line.len() > MAX_EVENT_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "a response line exceeded the size limit",
                ));
            }
        }
    }

    /// Read the next chunk size line. Returns `false` on the terminating chunk.
    async fn next_chunk_header(&mut self) -> std::io::Result<bool> {
        let mut header = String::new();
        if self.reader.read_line(&mut header).await? == 0 {
            return Ok(false);
        }
        let header = header.trim();
        if header.is_empty() {
            // Tolerate the stray CRLF some servers emit between chunks.
            return Box::pin(self.next_chunk_header()).await;
        }
        let size = header.split(';').next().unwrap_or("0");
        let size = usize::from_str_radix(size.trim(), 16).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the server sent an unreadable chunk size",
            )
        })?;
        if size == 0 {
            return Ok(false);
        }
        self.remaining = size;
        Ok(true)
    }
}

async fn emit<W>(
    outgoing: &std::sync::Arc<tokio::sync::Mutex<W>>,
    frame: &Value,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(frame).unwrap_or_else(|_| b"{}".to_vec());
    bytes.push(b'\n');
    let mut guard = outgoing.lock().await;
    guard.write_all(&bytes).await?;
    guard.flush().await
}

async fn emit_error<W>(outgoing: &std::sync::Arc<tokio::sync::Mutex<W>>, message: &str)
where
    W: AsyncWrite + Unpin,
{
    let _ = emit(outgoing, &json!({"type": FRAME_ERROR, "message": message})).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;

    /// Feed a canned HTTP response through the head/body readers.
    async fn parse(response: &[u8]) -> (u16, String) {
        let mut reader = BufReader::new(std::io::Cursor::new(response.to_vec()));
        let (status, framing) = read_head(&mut reader).await.unwrap();
        let body = read_body(&mut reader, framing).await.unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn reads_a_content_length_response() {
        let (status, body) =
            parse(b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\n{\"id\":\"abc\"}\r\n").await;
        assert_eq!(status, 200);
        assert!(body.starts_with("{\"id\":\"abc\"}"));
    }

    #[tokio::test]
    async fn reads_a_chunked_response() {
        let (status, body) = parse(
            b"HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{\"a\"\r\n4\r\n:1}\n\r\n0\r\n\r\n",
        )
        .await;
        assert_eq!(status, 201);
        assert_eq!(body.trim(), "{\"a\":1}");
    }

    /// The case a naive scan for `data:` would get wrong: one SSE event split
    /// across two chunks.
    #[tokio::test]
    async fn reassembles_an_event_split_across_chunk_boundaries() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    8\r\ndata: {\"\r\n\
                    13\r\ntype\":\"session.idle\r\n\
                    4\r\n\"}\n\n\r\n\
                    0\r\n\r\n";
        let mut reader = BufReader::new(std::io::Cursor::new(raw.to_vec()));
        let (_, framing) = read_head(&mut reader).await.unwrap();
        let mut body = BodyReader::new(&mut reader, framing);

        let first = body.read_line().await.unwrap().unwrap();
        assert_eq!(first.trim_end(), "data: {\"type\":\"session.idle\"}");
    }

    #[tokio::test]
    async fn an_unreadable_chunk_size_is_an_error_not_a_silent_truncation() {
        let mut reader = BufReader::new(std::io::Cursor::new(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n".to_vec(),
        ));
        let (_, framing) = read_head(&mut reader).await.unwrap();
        let mut body = BodyReader::new(&mut reader, framing);
        assert!(body.read_line().await.is_err());
    }

    /// End-to-end against a real socket: the bridge must report readiness,
    /// perform a correlated request, and forward an SSE payload.
    #[tokio::test]
    async fn drives_a_real_server_through_the_frame_contract() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut reader = BufReader::new(&mut socket);
                    let mut request = String::new();
                    reader.read_line(&mut request).await.unwrap();
                    // Drain headers.
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).await.unwrap() == 0
                            || header.trim().is_empty()
                        {
                            break;
                        }
                    }
                    let response: &[u8] = if request.contains("/event") {
                        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                          1E\r\ndata: {\"type\":\"session.idle\"}\n\r\n\
                          1\r\n\n\r\n"
                    } else if request.starts_with("POST") {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\n{\"ok\":\"yes\"}\r\n"
                    } else if request.starts_with("GET /global/health ") {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n{\"healthy\":true}"
                    } else {
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\n\r\n{}"
                    };
                    let _ = socket.write_all(response).await;
                    let _ = socket.flush().await;
                });
            }
        });

        let streams = connect(port);
        let mut writer = streams.writer;
        let mut lines = BufReader::new(streams.reader).lines();

        let ready: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(ready["type"], json!(FRAME_READY));

        writer
            .write_all(b"{\"id\":7,\"method\":\"POST\",\"path\":\"/session\",\"body\":{}}\n")
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let response: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(response["type"], json!(FRAME_RESPONSE));
        assert_eq!(response["id"], json!(7));
        assert_eq!(response["body"]["ok"], json!("yes"));

        writer
            .write_all(b"{\"method\":\"SUBSCRIBE\",\"path\":\"/event\"}\n")
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let subscribed: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(
            subscribed["type"],
            json!(FRAME_SUBSCRIBED),
            "the stream must be confirmed open before the prompt is sent"
        );
        let event: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(event["type"], json!(FRAME_EVENT));
        assert_eq!(event["event"]["type"], json!("session.idle"));
    }

    /// A server that dies mid-turn must surface as a frame, not a hang.
    #[tokio::test]
    async fn a_server_that_disappears_is_reported_rather_than_awaited() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut reader = BufReader::new(&mut socket);
                let mut request = String::new();
                let _ = reader.read_line(&mut request).await;
                loop {
                    let mut header = String::new();
                    if reader.read_line(&mut header).await.unwrap_or(0) == 0
                        || header.trim().is_empty()
                    {
                        break;
                    }
                }
                if request.contains("/event") {
                    // Accept the subscription, then drop the connection the
                    // way a crashing process would.
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        .await;
                    let _ = socket.flush().await;
                    drop(socket);
                    return;
                }
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n{\"healthy\":true}")
                    .await;
            }
        });

        let streams = connect(port);
        let mut writer = streams.writer;
        let mut lines = BufReader::new(streams.reader).lines();
        assert!(lines.next_line().await.unwrap().is_some(), "ready frame");

        writer
            .write_all(b"{\"method\":\"SUBSCRIBE\",\"path\":\"/event\"}\n")
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let subscribed: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(subscribed["type"], json!(FRAME_SUBSCRIBED));
        let frame: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(frame["type"], json!(FRAME_ERROR));
    }
}
