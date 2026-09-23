//! Served-OpenCode coverage against a scripted `opencode serve`.
//!
//! The fixture is a real HTTP server bound to the very port the adapter
//! reserved for the turn, speaking the real protocol: `/global/health` for readiness,
//! `/session` to open one, `/event` as a chunked Server-Sent Events stream,
//! `/session/{id}/message` to prompt, and
//! `/session/{id}/permissions/{id}` to answer a permission. Nothing here
//! shells out to an OpenCode binary, and nothing stubs the HTTP or SSE
//! carrier — the bytes really cross a socket, so the transport is covered
//! end to end rather than only the state machine above it.

#![cfg(feature = "opencode")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use temps_agent_runtime::providers::{OpenCode, OpenCodeTurnMode};
use temps_agent_runtime::{
    AgentRuntime, ApprovalDecision, ApprovalRequest, EventSink, ExecutionTransport,
    InteractionHandler, McpServerConfig, PermissionMode, Provider, ProviderReadiness,
    QuestionAnswer, QuestionRequest, Result, RuntimeError, SandboxCapabilities,
    TransportCapabilities, TransportError, TransportErrorKind, TransportExitStatus,
    TransportProcess, TransportProcessControl, TransportProcessHandle, TransportReader,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, TransportWriter, TurnEvent,
    TurnRequest,
};
use tokio::io::{duplex, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// Turn shape the fixture server plays out once the prompt arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Script {
    Complete,
    /// Ask one `bash` permission, then reply according to the decision.
    Permission,
    /// Close the event stream mid-turn, the way a crashing server does.
    Crash,
    /// Stream one delta and then go quiet, so the turn must be cancelled.
    Interrupt,
    /// Pass health, then never answer session setup.
    SessionHang,
    /// Hang the second session lookup; the replacement process answers it.
    SessionHangOnce,
}

/// One request the SDK made, recorded for assertions.
#[derive(Debug, Clone)]
struct Recorded {
    path: String,
    body: Value,
}

#[derive(Clone)]
struct Server {
    script: Script,
    spawns: Arc<AtomicUsize>,
    health: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
    stoppers: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    requests: Arc<Mutex<Vec<Recorded>>>,
    /// Argument vector the SDK asked the transport to spawn.
    arguments: Arc<Mutex<Vec<String>>>,
    /// The per-turn policy the SDK put in the child's environment.
    config: Arc<Mutex<Option<String>>>,
}

impl Server {
    fn new(script: Script) -> Self {
        Self {
            script,
            spawns: Arc::new(AtomicUsize::new(0)),
            health: Arc::default(),
            stoppers: Arc::default(),
            requests: Arc::new(Mutex::new(Vec::new())),
            arguments: Arc::new(Mutex::new(Vec::new())),
            config: Arc::new(Mutex::new(None)),
        }
    }

    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    /// The `OPENCODE_CONFIG_CONTENT` the served turn was started with.
    fn config(&self) -> Value {
        let raw = self.config.lock().unwrap().clone().expect("policy was set");
        serde_json::from_str(&raw).expect("the policy is JSON")
    }

    fn arguments(&self) -> Vec<String> {
        self.arguments.lock().unwrap().clone()
    }

    fn first_matching(&self, needle: &str) -> Option<Recorded> {
        self.requests()
            .into_iter()
            .find(|recorded| recorded.path.contains(needle))
    }

    /// Wait until the SDK issued a request whose path contains `needle`.
    async fn wait_for(&self, needle: &str) -> Option<Recorded> {
        for _ in 0..400 {
            if let Some(recorded) = self.first_matching(needle) {
                return Some(recorded);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }
}

#[async_trait]
impl ExecutionTransport for Server {
    fn name(&self) -> &'static str {
        "fixture-opencode-serve"
    }

    fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            remote: false,
            interactive_stdin: true,
            reconnect: false,
            managed_processes: false,
            process_tree_termination: true,
            sandbox: SandboxCapabilities::NONE,
        }
    }

    async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness> {
        Ok(ProviderReadiness {
            provider: request.provider,
            installed: true,
            executable: Some(request.program),
            version: Some("opencode 1.4.3".to_string()),
            detail: "fixture".to_string(),
        })
    }

    async fn validate_working_directory(&self, _working_directory: &Path) -> TransportResult<()> {
        Ok(())
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        self.spawns.fetch_add(1, Ordering::SeqCst);
        let arguments = request
            .command
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            arguments.first().map(String::as_str),
            Some("serve"),
            "the served turn mode must not launch `opencode run`"
        );
        // The policy is the whole enforcement boundary, so it must arrive in
        // the child's environment rather than being left to the machine.
        let config = request
            .command
            .environment
            .get(std::ffi::OsStr::new("OPENCODE_CONFIG_CONTENT"))
            .map(|value| value.to_string_lossy().into_owned())
            .expect("the served turn must supply a permission policy");
        *self.config.lock().unwrap() = Some(config);

        let port = arguments
            .iter()
            .position(|argument| argument == "--port")
            .and_then(|index| arguments.get(index + 1))
            .and_then(|port| port.parse::<u16>().ok())
            .expect("the served turn must pin a port");
        *self.arguments.lock().unwrap() = arguments;

        // Bind the port the adapter reserved, exactly as the real child does.
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("the reserved port is free for the child to bind");
        let health = Arc::new(AtomicBool::new(true));
        self.health.lock().unwrap().push(health.clone());
        let task = tokio::spawn(accept_loop(
            listener,
            self.script,
            Arc::clone(&self.requests),
            health,
        ));
        self.stoppers.lock().unwrap().push(task.abort_handle());

        let (sdk_stdin, child_input) = duplex(1024);
        let (sdk_stdout, child_output) = duplex(1024);
        let (sdk_stderr, child_stderr) = duplex(1024);
        // `opencode serve` says nothing on its own stdio; the protocol lives
        // entirely on the socket above.
        drop(child_input);
        drop(child_output);
        drop(child_stderr);
        Ok(TransportProcess::new(
            TransportProcessHandle {
                transport: "fixture-opencode-serve".to_string(),
                native_id: "1".to_string(),
            },
            None,
            Some(Box::new(sdk_stdin) as TransportWriter),
            Box::new(sdk_stdout) as TransportReader,
            Box::new(sdk_stderr) as TransportReader,
            Control {
                stopped: false,
                task: Some(task),
            },
        ))
    }

    async fn attach(
        &self,
        _handle: &TransportProcessHandle,
        _cursor: Option<u64>,
    ) -> TransportResult<TransportProcess> {
        Err(TransportError::new(
            TransportErrorKind::Unsupported,
            self.name(),
            "attach",
            "the fixture server cannot be reattached",
            false,
        ))
    }
}

/// Mirrors a real server's lifetime: it runs until something stops it.
#[derive(Default)]
struct Control {
    stopped: bool,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Control {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl TransportProcessControl for Control {
    async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        if !self.stopped {
            // `opencode serve` never exits because a turn ended, so a runtime
            // that waited for it here would hang until the turn deadline.
            std::future::pending::<()>().await;
        }
        Ok(TransportExitStatus {
            // Real servers stopped by the runtime exit by signal/nonzero.
            success: false,
            code: None,
        })
    }

    async fn terminate(&mut self) -> TransportResult<()> {
        self.stopped = true;
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
}

async fn accept_loop(
    listener: TcpListener,
    script: Script,
    requests: Arc<Mutex<Vec<Recorded>>>,
    health: Arc<AtomicBool>,
) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(handle(
            socket,
            script,
            Arc::clone(&requests),
            health.clone(),
        ));
    }
}

/// Read one request, record it, and answer it.
async fn handle(
    mut socket: TcpStream,
    script: Script,
    requests: Arc<Mutex<Vec<Recorded>>>,
    health: Arc<AtomicBool>,
) {
    let mut reader = BufReader::new(&mut socket);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
        return;
    }
    let mut length = 0_usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).await.unwrap_or(0) == 0 || header.trim().is_empty() {
            break;
        }
        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let body = if length > 0 {
        let mut buffer = vec![0_u8; length];
        reader.read_exact(&mut buffer).await.ok();
        serde_json::from_slice(&buffer).unwrap_or(Value::Null)
    } else {
        Value::Null
    };

    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    requests.lock().unwrap().push(Recorded {
        path: path.clone(),
        body,
    });

    if path.starts_with("/event") {
        stream_events(socket, script, requests).await;
        return;
    }
    if path == "/global/health" && !health.load(Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    if script == Script::SessionHang && (path.starts_with("/session?") || path == "/session") {
        std::future::pending::<()>().await;
    }
    if script == Script::SessionHangOnce
        && (path.starts_with("/session?")
            || path == "/session"
            || path.split('?').next() == Some("/session/session-fixture"))
        && requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request.path.starts_with("/session") && !request.path.contains("/message")
            })
            .count()
            == 2
    {
        std::future::pending::<()>().await;
    }
    let payload = if path == "/global/health" {
        json!({"healthy": true}).to_string()
    } else if path == "/app" {
        "<!doctype html><html>OpenCode UI</html>".to_string()
    } else if path.starts_with("/session?")
        || path == "/session"
        || (request_line.starts_with("GET ")
            && path.split('?').next() == Some("/session/session-fixture"))
    {
        json!({"id": "session-fixture", "title": "Fixture session"}).to_string()
    } else {
        json!({}).to_string()
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.flush().await;
}

/// Write one SSE payload as its own chunk, as a real server does.
async fn send(socket: &mut TcpStream, event: &Value) -> bool {
    let payload = format!("data: {event}\n\n");
    let chunk = format!("{:X}\r\n{payload}\r\n", payload.len());
    socket.write_all(chunk.as_bytes()).await.is_ok() && socket.flush().await.is_ok()
}

/// Play the scripted turn out over a chunked SSE stream.
async fn stream_events(mut socket: TcpStream, script: Script, requests: Arc<Mutex<Vec<Recorded>>>) {
    let prompts_before = requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.path.contains("/message"))
        .count();
    let _ = socket
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await;
    let _ = socket.flush().await;

    for _ in 0..400 {
        if requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.path.contains("/message"))
            .count()
            > prompts_before
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Establish the assistant message every part below belongs to.
    if !send(
        &mut socket,
        &json!({"type": "message.updated", "properties": {
            "sessionID": "session-fixture",
            "info": {"id": "message-1", "role": "assistant"}
        }}),
    )
    .await
    {
        return;
    }

    if script == Script::Crash {
        // Exactly what a dying server looks like from the SDK's side.
        drop(socket);
        return;
    }

    if matches!(script, Script::Complete | Script::SessionHangOnce)
        && !send(&mut socket, &json!({"type":"message.part.updated","properties":{"sessionID":"session-fixture","part":{"id":"part-1","messageID":"message-1","type":"text","text":"fixture reply"}}})).await { return; }

    if script == Script::Permission {
        if !send(
            &mut socket,
            &json!({"type": "permission.asked", "properties": {
                "sessionID": "session-fixture",
                "id": "permission-1",
                "permission": "bash",
                "patterns": ["rm -rf /"]
            }}),
        )
        .await
        {
            return;
        }
        // Wait for the decision to come back on the permission endpoint.
        let mut answer = None;
        for _ in 0..400 {
            let found = requests
                .lock()
                .unwrap()
                .iter()
                .find(|recorded| recorded.path.contains("/permissions/"))
                .cloned();
            if let Some(found) = found {
                answer = found
                    .body
                    .get("response")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let text = match answer.as_deref() {
            Some("reject") => "I was not allowed to run that.",
            Some(_) => "Removed everything, as instructed.",
            None => "No decision ever arrived.",
        };
        if !send(
            &mut socket,
            &json!({"type": "message.part.updated", "properties": {
                "sessionID": "session-fixture",
                "part": {"id": "part-1", "messageID": "message-1", "type": "text", "text": text}
            }}),
        )
        .await
        {
            return;
        }
    }

    if script == Script::Interrupt {
        if !send(
            &mut socket,
            &json!({"type": "message.part.updated", "properties": {
                "sessionID": "session-fixture",
                "part": {"id": "part-1", "messageID": "message-1",
                         "type": "text", "text": "Working on it"}
            }}),
        )
        .await
        {
            return;
        }
        // Go quiet: only cancellation can end this turn.
        std::future::pending::<()>().await;
    }

    let _ = send(
        &mut socket,
        &json!({"type": "session.idle", "properties": {"sessionID": "session-fixture"}}),
    )
    .await;
}

/// Answers every approval with one fixed decision.
struct Decide(ApprovalDecision);

#[async_trait]
impl InteractionHandler for Decide {
    async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
        self.0.clone()
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}

/// Refuses to answer, so a turn that consults it would stall.
struct NeverAsked;

#[async_trait]
impl InteractionHandler for NeverAsked {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        panic!(
            "the application must not be asked to approve `{}`",
            request.tool_name
        );
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}

#[derive(Clone, Default)]
struct Collected(Arc<Mutex<Vec<TurnEvent>>>);

#[async_trait]
impl EventSink for Collected {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        self.0.lock().unwrap().push(event);
        Ok(())
    }
}

impl Collected {
    fn events(&self) -> Vec<TurnEvent> {
        self.0.lock().unwrap().clone()
    }
}

fn runtime(server: &Server) -> AgentRuntime {
    let mut builder = AgentRuntime::builder().transport(server.clone());
    builder.register(OpenCode::serve());
    builder.build().expect("runtime builds")
}

fn turn(mode: PermissionMode) -> TurnRequest {
    let mut request = TurnRequest::new(
        Provider::OpenCode,
        std::env::temp_dir(),
        "delete everything",
    );
    request.permission_mode = mode;
    request.timeout = Duration::from_secs(30);
    request.interaction_timeout = Duration::from_secs(10);
    request
}

#[tokio::test]
async fn an_allowed_permission_is_answered_once_and_the_turn_continues() {
    let server = Server::new(Script::Permission);
    let events = Collected::default();

    let result = runtime(&server)
        .run(
            turn(PermissionMode::Default),
            &events,
            Some(&Decide(ApprovalDecision::Allow)),
        )
        .await
        .expect("the turn completes");

    let answer = server
        .first_matching("/permissions/")
        .expect("the decision reached OpenCode");
    assert_eq!(answer.body["response"], json!("once"));
    assert!(answer
        .path
        .contains("/session/session-fixture/permissions/permission-1"));
    assert_eq!(result.text, "Removed everything, as instructed.");
    assert!(events.events().iter().any(
        |event| matches!(event, TurnEvent::ApprovalRequested(request)
            if request.tool_name == "Bash"
                && request.description.as_deref().is_some_and(|text| text.contains("rm -rf /")))
    ));
}

#[tokio::test]
async fn a_denied_permission_is_rejected_on_the_wire() {
    let server = Server::new(Script::Permission);
    let events = Collected::default();

    let result = runtime(&server)
        .run(
            turn(PermissionMode::Default),
            &events,
            Some(&Decide(ApprovalDecision::Deny {
                reason: Some("not on my machine".into()),
            })),
        )
        .await
        .expect("a refused tool call is still a completed turn");

    assert_eq!(
        server
            .first_matching("/permissions/")
            .expect("the refusal reached OpenCode")
            .body["response"],
        json!("reject")
    );
    assert_eq!(result.text, "I was not allowed to run that.");
}

#[tokio::test]
async fn a_plan_turn_denies_both_permission_categories_and_never_asks() {
    let server = Server::new(Script::Permission);
    let events = Collected::default();

    let result = runtime(&server)
        .run(turn(PermissionMode::Plan), &events, Some(&NeverAsked))
        .await
        .expect("the turn completes");

    // The policy the server was started with is the real boundary.
    let config = server.config();
    assert_eq!(config["permission"]["edit"], json!("deny"));
    assert_eq!(config["permission"]["bash"], json!("deny"));
    // And a permission that arrives anyway is refused without the
    // application ever being consulted — `NeverAsked` would panic.
    assert_eq!(
        server
            .first_matching("/permissions/")
            .expect("the refusal reached OpenCode")
            .body["response"],
        json!("reject")
    );
    assert_eq!(result.text, "I was not allowed to run that.");
}

#[tokio::test]
async fn the_requested_policy_and_mcp_servers_reach_the_child_environment() {
    let server = Server::new(Script::Permission);
    let events = Collected::default();
    let mut request = turn(PermissionMode::AcceptEdits);
    request.environment.insert(
        "FLEET_TOKEN".into(),
        temps_agent_runtime::SecretString::new("super-secret-value"),
    );
    request.launch_context.mcp_servers.insert(
        "temps_fleet".into(),
        McpServerConfig::Stdio {
            command: "/opt/tools/temps fleet".into(),
            args: vec!["mcp".into(), "serve".into()],
            environment_from: BTreeMap::from([("TOKEN".into(), "FLEET_TOKEN".into())]),
        },
    );

    runtime(&server)
        .run(request, &events, Some(&Decide(ApprovalDecision::Allow)))
        .await
        .expect("the turn completes");

    let config = server.config();
    assert_eq!(config["permission"]["edit"], json!("allow"));
    assert_eq!(config["permission"]["bash"], json!("ask"));
    let fleet = &config["mcp"]["temps_fleet"];
    assert_eq!(fleet["type"], json!("local"));
    assert_eq!(
        fleet["command"],
        json!(["/opt/tools/temps fleet", "mcp", "serve"])
    );
    assert_eq!(fleet["environment"]["TOKEN"], json!("{env:FLEET_TOKEN}"));
    assert!(
        !server
            .config
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains("FLEET_TOKEN=")
            && !format!("{:?}", server.arguments()).contains("FLEET_TOKEN"),
        "a credential must be referenced by name, never serialized"
    );
}

#[tokio::test]
async fn a_server_that_dies_mid_turn_fails_instead_of_hanging() {
    let server = Server::new(Script::Crash);
    let events = Collected::default();

    let error = runtime(&server)
        .run(turn(PermissionMode::Default), &events, Some(&NeverAsked))
        .await
        .expect_err("a turn whose server disappeared cannot succeed");

    match error {
        RuntimeError::ProcessFailed { stderr, .. } => {
            assert!(
                stderr.contains("event stream"),
                "the diagnostic should say the stream died, got `{stderr}`"
            );
        }
        other => panic!("expected a process failure, got {other:?}"),
    }
}

#[tokio::test]
async fn cancelling_a_turn_aborts_the_session_before_the_process_is_killed() {
    let server = Server::new(Script::Interrupt);
    let events = Collected::default();
    let cancellation = CancellationToken::new();
    let mut request = turn(PermissionMode::Default);
    request.cancellation = cancellation.clone();

    let running = {
        let runtime = runtime(&server);
        let events = events.clone();
        tokio::spawn(async move { runtime.run(request, &events, Some(&NeverAsked)).await })
    };

    // Cancel only once the turn is genuinely under way.
    server
        .wait_for("/message")
        .await
        .expect("the prompt was delivered");
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancellation.cancel();

    let error = running.await.unwrap().expect_err("a cancelled turn fails");
    assert!(matches!(
        error,
        RuntimeError::Cancelled {
            provider: Provider::OpenCode
        }
    ));
    assert!(
        server.wait_for("/abort").await.is_some(),
        "the session must be asked to stop cooperatively before the process is killed"
    );
}

#[tokio::test]
async fn the_run_turn_mode_keeps_its_one_shot_behaviour() {
    use temps_agent_runtime::AgentAdapter;

    let adapter = OpenCode::default();
    assert_eq!(adapter.turn_mode(), OpenCodeTurnMode::Run);
    assert!(
        !adapter.permission_support().live_approvals,
        "`opencode run` cannot answer a permission mid-turn and must not claim to"
    );
    assert!(OpenCode::serve().permission_support().live_approvals);
}

async fn retained_fixture(
    server: &Server,
    active: Duration,
) -> (
    temps_agent_runtime::retained::InProcessRuntimeClient,
    temps_agent_runtime::retained::RuntimeHandle,
) {
    use temps_agent_runtime::retained::{InProcessRuntimeClient, RuntimeClient, RuntimeSpec};
    let mut builder = AgentRuntime::builder()
        .transport(server.clone())
        .provider_process_retention(temps_agent_runtime::ProviderProcessRetention {
            max_processes: 1,
            idle_timeout: Duration::from_secs(30),
            initialization_timeout: Duration::from_secs(3),
            active_inactivity_timeout: Some(active),
        });
    builder.register(OpenCode::serve());
    let client = InProcessRuntimeClient::new(builder.build().unwrap());
    let handle = client
        .acquire(RuntimeSpec::new(
            temps_agent_runtime::lifecycle::RuntimeId::new("retained-opencode").unwrap(),
            Provider::OpenCode,
            std::env::temp_dir(),
        ))
        .await
        .unwrap();
    (client, handle)
}

#[tokio::test]
async fn retained_server_reuses_port_and_session_for_two_turns() {
    use temps_agent_runtime::{
        lifecycle::{InvocationId, RuntimeId},
        retained::{RuntimeClient, TurnInput},
    };
    let server = Server::new(Script::Complete);
    let (client, handle) = retained_fixture(&server, Duration::from_secs(3)).await;
    for id in ["first", "second"] {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            handle
                .start_turn(TurnInput::new(InvocationId::new(id).unwrap(), id))
                .await
                .unwrap()
                .wait(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.session_id.as_deref(), Some("session-fixture"));
    }
    assert_eq!(server.spawns.load(Ordering::SeqCst), 1);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path.contains("/message"))
            .count(),
        2
    );
    client
        .dispose(&RuntimeId::new("retained-opencode").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_server_stall_is_bounded_and_does_not_replay() {
    use temps_agent_runtime::{
        lifecycle::{InvocationId, RuntimeId},
        retained::{RuntimeClient, TurnInput},
    };
    let server = Server::new(Script::Interrupt);
    let (client, handle) = retained_fixture(&server, Duration::from_millis(100)).await;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle
            .start_turn(TurnInput::new(
                InvocationId::new("stalled").unwrap(),
                "stalled",
            ))
            .await
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path.contains("/message"))
            .count(),
        1
    );
    client
        .dispose(&RuntimeId::new("retained-opencode").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_session_setup_hang_uses_initialization_deadline() {
    use temps_agent_runtime::{
        lifecycle::{InvocationId, RuntimeId},
        retained::{RuntimeClient, TurnInput},
    };
    let server = Server::new(Script::SessionHang);
    // The active allowance is deliberately much longer than the fixture's
    // three-second initialization deadline.
    let (client, handle) = retained_fixture(&server, Duration::from_secs(30)).await;
    let started = tokio::time::Instant::now();
    let result = handle
        .start_turn(TurnInput::new(
            InvocationId::new("session-hang").unwrap(),
            "never submitted",
        ))
        .await
        .unwrap()
        .wait()
        .await;
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.path.contains("/message"))
            .count(),
        0
    );
    client
        .dispose(&RuntimeId::new("retained-opencode").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_replaces_when_session_setup_hangs_before_prompt() {
    use temps_agent_runtime::{
        lifecycle::{InvocationId, RuntimeId},
        retained::{RuntimeClient, TurnInput},
    };
    let server = Server::new(Script::SessionHangOnce);
    let (client, handle) = retained_fixture(&server, Duration::from_secs(30)).await;
    for id in ["first", "second"] {
        handle
            .start_turn(TurnInput::new(InvocationId::new(id).unwrap(), id))
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    assert_eq!(server.spawns.load(Ordering::SeqCst), 2);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.path.contains("/message"))
            .count(),
        2
    );
    client
        .dispose(&RuntimeId::new("retained-opencode").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_unresponsive_server_recovers_before_prompt_delivery() {
    use temps_agent_runtime::{
        lifecycle::{InvocationId, RuntimeId},
        retained::{RuntimeClient, TurnInput},
    };
    let server = Server::new(Script::Complete);
    let (client, handle) = retained_fixture(&server, Duration::from_secs(3)).await;
    handle
        .start_turn(TurnInput::new(InvocationId::new("first").unwrap(), "first"))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    server.health.lock().unwrap()[0].store(false, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(8),
        handle
            .start_turn(TurnInput::new(
                InvocationId::new("recovered").unwrap(),
                "recovered",
            ))
            .await
            .unwrap()
            .wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(server.spawns.load(Ordering::SeqCst), 2);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path.contains("/message"))
            .count(),
        2
    );
    client
        .dispose(&RuntimeId::new("retained-opencode").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_dead_server_recovers_before_prompt_delivery() {
    use temps_agent_runtime::{
        lifecycle::{InvocationId, RuntimeId},
        retained::{RuntimeClient, TurnInput},
    };
    let server = Server::new(Script::Complete);
    let (client, handle) = retained_fixture(&server, Duration::from_secs(3)).await;
    handle
        .start_turn(TurnInput::new(InvocationId::new("first").unwrap(), "first"))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    server.stoppers.lock().unwrap()[0].abort();
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(
        Duration::from_secs(8),
        handle
            .start_turn(TurnInput::new(
                InvocationId::new("recovered").unwrap(),
                "recovered",
            ))
            .await
            .unwrap()
            .wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(server.spawns.load(Ordering::SeqCst), 2);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path.contains("/message"))
            .count(),
        2
    );
    client
        .dispose(&RuntimeId::new("retained-opencode").unwrap())
        .await
        .unwrap();
}
