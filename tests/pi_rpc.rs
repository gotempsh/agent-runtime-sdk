//! End-to-end pi coverage against a scripted `pi --mode rpc`.
//!
//! The fixture speaks pi's real JSONL RPC protocol: it answers `get_state`
//! and `prompt`, streams session events through `agent_settled`, issues
//! extension UI requests, honors `abort`, and serves the model-catalog and
//! `--list-models` probes. Nothing here shells out to a pi binary.

#![cfg(feature = "pi")]

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use temps_agent_runtime::lifecycle::{DeliveryState, InvocationId, RuntimeId};
use temps_agent_runtime::providers::Pi;
use temps_agent_runtime::retained::{
    InProcessRuntimeClient, RuntimeClient, RuntimeSpec, TurnInput,
};
use temps_agent_runtime::{
    AgentRuntime, ApprovalDecision, ApprovalRequest, EventSink, ExecutionTransport,
    HarnessAuthenticationStatus, HarnessCatalogStatus, HarnessStatus, InteractionHandler,
    McpServerConfig, PermissionMode, Provider, ProviderProcessErrorKind, ProviderReadiness,
    QuestionAnswer, QuestionRequest, Result, RuntimeError, SandboxCapabilities, ToolCallStatus,
    TransportCapabilities, TransportError, TransportErrorKind, TransportExitStatus,
    TransportProcess, TransportProcessControl, TransportProcessHandle, TransportReader,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, TransportWriter, TurnEvent,
    TurnRequest,
};
use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

/// Event-line limit used by the oversized-frame scenarios.
const SMALL_FRAME_LIMIT: usize = 8 * 1024;

/// Run shape the fixture plays out after `prompt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Script {
    /// Call one tool, then answer.
    ToolRun,
    /// Like `ToolRun`, but the run summary and one tool result exceed
    /// [`SMALL_FRAME_LIMIT`].
    OversizedSummary,
    /// Like `OversizedSummary`, but the tool with the oversized result fails.
    OversizedToolFailure,
    /// The final assistant message itself exceeds [`SMALL_FRAME_LIMIT`].
    OversizedAnswer,
    /// An extension asks for confirmation before the answer.
    Confirm,
    /// Stream one delta and then wait for `abort`.
    Hang,
    /// Report an empty session for any requested resume.
    MissingSession,
    /// Reject the prompt for lack of credentials.
    RejectedPrompt,
}

#[derive(Clone)]
struct PiFixture {
    script: Script,
    /// Every command the SDK wrote, in order, across processes.
    frames: Arc<Mutex<Vec<Value>>>,
    /// Arguments of every spawned process.
    spawns: Arc<Mutex<Vec<Vec<String>>>>,
}

impl PiFixture {
    fn new(script: Script) -> Self {
        Self {
            script,
            frames: Arc::new(Mutex::new(Vec::new())),
            spawns: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn frames(&self) -> Vec<Value> {
        self.frames.lock().unwrap().clone()
    }

    fn commands(&self) -> Vec<String> {
        self.frames()
            .iter()
            .filter_map(|frame| frame.get("type").and_then(Value::as_str).map(str::to_owned))
            .collect()
    }

    fn spawns(&self) -> Vec<Vec<String>> {
        self.spawns.lock().unwrap().clone()
    }

    async fn wait_for(&self, command: &str) -> bool {
        for _ in 0..200 {
            if self.commands().iter().any(|seen| seen == command) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }
}

#[async_trait]
impl ExecutionTransport for PiFixture {
    fn name(&self) -> &'static str {
        "fixture-pi"
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
            installed: request.program == Path::new("pi"),
            executable: Some(request.program),
            version: Some("1.0.0".to_string()),
            detail: "fixture".to_string(),
        })
    }

    async fn validate_working_directory(&self, _working_directory: &Path) -> TransportResult<()> {
        Ok(())
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        assert_eq!(request.command.program, Path::new("pi"));
        assert!(request.command.clear_environment);
        let arguments = request
            .command
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        self.spawns.lock().unwrap().push(arguments.clone());
        let (sdk_stdin, input) = duplex(64 * 1024);
        let (sdk_stdout, output) = duplex(64 * 1024);
        let (sdk_stderr, stderr) = duplex(1024);
        drop(stderr);
        let script = self.script;
        let frames = Arc::clone(&self.frames);
        tokio::spawn(async move {
            if arguments.iter().any(|argument| argument == "--list-models") {
                list_models(output).await;
            } else if arguments.iter().any(|argument| argument == "--no-session") {
                serve_catalog(input, output).await;
            } else {
                let resumed = arguments.iter().any(|argument| argument == "--session-id");
                serve_turn(script, resumed, frames, input, output).await;
            }
        });
        Ok(TransportProcess::new(
            TransportProcessHandle {
                transport: "fixture-pi".to_string(),
                native_id: "1".to_string(),
            },
            None,
            Some(Box::new(sdk_stdin) as TransportWriter),
            Box::new(sdk_stdout) as TransportReader,
            Box::new(sdk_stderr) as TransportReader,
            Control,
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
            "the pi fixture cannot be reattached",
            false,
        ))
    }
}

struct Control;

#[async_trait]
impl TransportProcessControl for Control {
    async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        Ok(TransportExitStatus {
            success: true,
            code: Some(0),
        })
    }

    async fn terminate(&mut self) -> TransportResult<()> {
        Ok(())
    }
}

async fn send(output: &mut DuplexStream, value: &Value) {
    let _ = output
        .write_all(format!("{}\n", pi_json(value)).as_bytes())
        .await;
    let _ = output.flush().await;
}

/// Serialize the way pi does. pi builds every record as a JavaScript object
/// literal that starts with `type` (a message, with `role`; a tool event,
/// with its call id and name before the result, and `isError` after it), and
/// `JSON.stringify` keeps that order. `serde_json` would sort the keys
/// instead, and the adapter reads oversized frames by their first and last
/// bytes.
fn pi_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_by_key(|key| match key.as_str() {
                "type" | "role" => 0,
                "toolCallId" => 1,
                "toolName" => 2,
                "isError" => 4,
                _ => 3,
            });
            let fields = keys
                .into_iter()
                .map(|key| format!("{}:{}", Value::from(key.as_str()), pi_json(&map[key])))
                .collect::<Vec<_>>();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(pi_json).collect::<Vec<_>>().join(",")
        ),
        other => other.to_string(),
    }
}

fn assistant(text: &str, stop_reason: &str) -> Value {
    json!({"role": "assistant", "provider": "anthropic", "model": "claude-sonnet-4-5",
        "content": [{"type": "text", "text": text}],
        "usage": {"input": 100, "output": 20, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 120,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0.01}},
        "stopReason": stop_reason})
}

fn answer(text: &str) -> Vec<Value> {
    vec![
        json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
        json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": text}}),
        json!({"type": "message_end", "message": assistant(text, "stop")}),
        json!({"type": "agent_end", "messages": [], "willRetry": false}),
        json!({"type": "agent_settled"}),
    ]
}

fn tool_run(script: Script) -> Vec<Value> {
    let padding = "x".repeat(2 * SMALL_FRAME_LIMIT);
    let tool_output = if matches!(
        script,
        Script::OversizedSummary | Script::OversizedToolFailure
    ) {
        padding.clone()
    } else {
        "src\nCargo.toml\n".to_string()
    };
    let tool_failed = script == Script::OversizedToolFailure;
    let mut frames = vec![
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": {"role": "user", "content": [{"type": "text", "text": "list"}]}}),
        json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
        json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "Listing files."}}),
        json!({"type": "message_end", "message": {"role": "assistant", "provider": "anthropic", "model": "claude-sonnet-4-5",
            "content": [{"type": "text", "text": "Listing files."},
                {"type": "toolCall", "id": "call-1", "name": "bash", "arguments": {"command": "ls"}}],
            "stopReason": "toolUse"}}),
        json!({"type": "tool_execution_start", "toolCallId": "call-1", "toolName": "bash", "args": {"command": "ls"}}),
        json!({"type": "tool_execution_end", "toolCallId": "call-1", "toolName": "bash",
            "result": {"content": [{"type": "text", "text": tool_output}]}, "isError": tool_failed}),
        json!({"type": "message_end", "message": {"role": "toolResult", "toolCallId": "call-1",
            "content": [{"type": "text", "text": tool_output}]}}),
        json!({"type": "turn_end", "message": {}, "toolResults": [{"content": tool_output}]}),
        json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
    ];
    if script == Script::OversizedAnswer {
        frames.push(json!({"type": "message_end", "message": assistant(&padding, "stop")}));
    } else {
        frames.push(json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": " Two entries."}}));
        frames.push(json!({"type": "message_end", "message": assistant(" Two entries.", "stop")}));
    }
    frames.push(json!({"type": "agent_end", "messages": [{"role": "toolResult", "content": tool_output}], "willRetry": false}));
    frames.push(json!({"type": "agent_settled"}));
    frames
}

/// Scripted `pi --mode rpc` for one turn process.
async fn serve_turn(
    script: Script,
    resumed: bool,
    frames: Arc<Mutex<Vec<Value>>>,
    input: DuplexStream,
    mut output: DuplexStream,
) {
    let mut lines = BufReader::new(input).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(command) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        frames.lock().unwrap().push(command.clone());
        let id = command.get("id").cloned().unwrap_or(Value::Null);
        match command.get("type").and_then(Value::as_str) {
            Some("get_state") => {
                let message_count = match (script, resumed) {
                    (Script::MissingSession, _) | (_, false) => 0,
                    (_, true) => 4,
                };
                send(&mut output, &json!({"id": id, "type": "response", "command": "get_state", "success": true,
                    "data": {"sessionId": "session-fixture", "messageCount": message_count, "thinkingLevel": "medium",
                        "model": {"provider": "anthropic", "id": "claude-sonnet-4-5", "contextWindow": 200_000}}})).await;
            }
            Some("prompt") if script == Script::RejectedPrompt => {
                send(&mut output, &json!({"id": id, "type": "response", "command": "prompt", "success": false,
                    "error": "No API key found for anthropic.\n\nUse /login to log into a provider"})).await;
            }
            Some("prompt") => {
                send(
                    &mut output,
                    &json!({"id": id, "type": "response", "command": "prompt", "success": true,
                    "data": {"disposition": "started"}}),
                )
                .await;
                let frames = match script {
                    Script::Confirm => vec![json!({"type": "extension_ui_request", "id": "ui-1",
                        "method": "confirm", "title": "Allow bash?", "message": "rm -rf build"})],
                    Script::Hang => vec![
                        json!({"type": "agent_start"}),
                        json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "Working"}}),
                    ],
                    _ => tool_run(script),
                };
                for frame in frames {
                    send(&mut output, &frame).await;
                }
            }
            Some("extension_ui_response") => {
                let confirmed = command.get("confirmed").and_then(Value::as_bool) == Some(true);
                for frame in answer(if confirmed { "Approved." } else { "Declined." }) {
                    send(&mut output, &frame).await;
                }
            }
            Some("abort") => {
                for frame in [
                    json!({"type": "message_end", "message": assistant("Working", "aborted")}),
                    json!({"type": "agent_end", "messages": [], "willRetry": false}),
                    json!({"type": "agent_settled"}),
                    json!({"type": "response", "command": "abort", "success": true}),
                ] {
                    send(&mut output, &frame).await;
                }
            }
            _ => {}
        }
    }
    // Closing stdin is pi's orderly shutdown: stdout ends with the process.
}

async fn serve_catalog(input: DuplexStream, mut output: DuplexStream) {
    let mut lines = BufReader::new(input).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(command) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = command.get("id").cloned().unwrap_or(Value::Null);
        match command.get("type").and_then(Value::as_str) {
            Some("get_available_models") => {
                send(&mut output, &json!({"id": id, "type": "response", "command": "get_available_models", "success": true,
                    "data": {"models": [
                        {"id": "claude-sonnet-4-5", "name": "Claude Sonnet 4.5", "provider": "anthropic",
                            "reasoning": true, "contextWindow": 200_000},
                        {"id": "gpt-4o-mini", "name": "GPT-4o mini", "provider": "openai",
                            "reasoning": false, "contextWindow": 128_000}
                    ]}})).await;
            }
            Some("get_state") => {
                send(&mut output, &json!({"id": id, "type": "response", "command": "get_state", "success": true,
                    "data": {"model": {"provider": "anthropic", "id": "claude-sonnet-4-5"}, "thinkingLevel": "medium",
                        "sessionId": "probe", "messageCount": 0}})).await;
            }
            _ => {}
        }
    }
}

async fn list_models(mut output: DuplexStream) {
    let _ = output
        .write_all(
            b"provider   model              context  max-out  thinking  images\n\
              anthropic  claude-sonnet-4-5  200K     64K      yes       yes\n",
        )
        .await;
}

#[derive(Default)]
struct Collector {
    events: Arc<Mutex<Vec<TurnEvent>>>,
}

#[async_trait]
impl EventSink for Collector {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

impl Collector {
    fn events(&self) -> Vec<TurnEvent> {
        self.events.lock().unwrap().clone()
    }
}

struct Responder {
    decision: ApprovalDecision,
    approvals: Arc<Mutex<Vec<ApprovalRequest>>>,
}

impl Responder {
    fn new(decision: ApprovalDecision) -> Self {
        Self {
            decision,
            approvals: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl InteractionHandler for Responder {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        self.approvals.lock().unwrap().push(request);
        self.decision.clone()
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}

fn runtime(transport: PiFixture) -> AgentRuntime {
    AgentRuntime::builder()
        .transport(transport)
        .build()
        .unwrap()
}

fn small_frame_runtime(transport: PiFixture) -> AgentRuntime {
    AgentRuntime::builder()
        .transport(transport)
        .max_event_line_bytes(SMALL_FRAME_LIMIT)
        .build()
        .unwrap()
}

fn request(prompt: &str) -> TurnRequest {
    let mut request = TurnRequest::new(Provider::Pi, ".", prompt);
    request.permission_mode = PermissionMode::FullAccess;
    request.timeout = Duration::from_secs(20);
    request.interaction_timeout = Duration::from_secs(10);
    request
}

#[tokio::test]
async fn runs_a_tool_using_turn_end_to_end() {
    let transport = PiFixture::new(Script::ToolRun);
    let events = Collector::default();
    let result = runtime(transport.clone())
        .run(request("list the files"), &events, None)
        .await
        .unwrap();

    assert_eq!(result.text, "Listing files. Two entries.");
    assert_eq!(result.session_id.as_deref(), Some("session-fixture"));
    assert_eq!(result.model.as_deref(), Some("anthropic/claude-sonnet-4-5"));
    assert_eq!(result.usage.input_tokens, Some(100));
    assert_eq!(transport.commands(), ["get_state", "prompt"]);
    assert_eq!(transport.frames()[1]["message"], "list the files");
    assert_eq!(transport.spawns(), [["--mode", "rpc", "--no-approve"]]);

    let events = events.events();
    assert_eq!(
        events.first(),
        Some(&TurnEvent::SessionStarted {
            session_id: "session-fixture".into(),
            title: None
        })
    );
    let tool_states = events
        .iter()
        .filter_map(|event| match event {
            TurnEvent::ToolCall {
                id, status, output, ..
            } => Some((id.clone(), *status, output.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tool_states,
        [
            (Some("call-1".into()), ToolCallStatus::Started, None),
            (
                Some("call-1".into()),
                ToolCallStatus::Succeeded,
                Some("src\nCargo.toml\n".into())
            ),
        ]
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, TurnEvent::Usage(usage) if usage.context_window.is_some())));
}

#[tokio::test]
async fn oversized_run_summaries_do_not_fail_a_long_turn() {
    let transport = PiFixture::new(Script::OversizedSummary);
    let events = Collector::default();
    let result = small_frame_runtime(transport)
        .run(request("list the files"), &events, None)
        .await
        .unwrap();

    assert_eq!(result.text, "Listing files. Two entries.");
    let completed = events
        .events()
        .into_iter()
        .find_map(|event| match event {
            TurnEvent::ToolCall {
                status: ToolCallStatus::Succeeded,
                id,
                output,
                ..
            } => Some((id, output)),
            _ => None,
        })
        .expect("the oversized tool result still completes its call");
    assert_eq!(completed.0.as_deref(), Some("call-1"));
    assert!(completed.1.unwrap().contains("omitted"));
}

#[tokio::test]
async fn an_oversized_failed_tool_result_is_reported_as_failed() {
    let transport = PiFixture::new(Script::OversizedToolFailure);
    let events = Collector::default();
    let result = small_frame_runtime(transport)
        .run(request("list the files"), &events, None)
        .await
        .unwrap();

    assert_eq!(result.text, "Listing files. Two entries.");
    let finished = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            TurnEvent::ToolCall {
                status: status @ (ToolCallStatus::Succeeded | ToolCallStatus::Failed),
                id,
                output,
                error,
                ..
            } => Some((id, status, output, error)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(id, status, output, error)] = finished.as_slice() else {
        panic!("expected one finished tool call, got {finished:?}");
    };
    assert_eq!(id.as_deref(), Some("call-1"));
    assert_eq!(*status, ToolCallStatus::Failed);
    assert_eq!(*output, None);
    assert!(error.as_deref().unwrap().contains("omitted"), "{error:?}");
}

#[tokio::test]
async fn an_oversized_answer_still_fails_the_turn() {
    let transport = PiFixture::new(Script::OversizedAnswer);
    let error = small_frame_runtime(transport)
        .run(request("list the files"), &Collector::default(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Protocol { provider: Provider::Pi, ref message } if message.contains("exceeded")),
        "{error:?}"
    );
}

#[tokio::test]
async fn extension_confirmations_reach_the_interaction_handler() {
    for (decision, expected) in [
        (ApprovalDecision::Allow, "Approved."),
        (
            ApprovalDecision::Deny {
                reason: Some("no".into()),
            },
            "Declined.",
        ),
    ] {
        let transport = PiFixture::new(Script::Confirm);
        let responder = Responder::new(decision);
        let result = runtime(transport.clone())
            .run(request("clean up"), &Collector::default(), Some(&responder))
            .await
            .unwrap();
        assert_eq!(result.text, expected);
        let approvals = responder.approvals.lock().unwrap().clone();
        assert_eq!(approvals.len(), 1);
        assert_eq!(approvals[0].id, "ui-1");
        assert_eq!(
            approvals[0].description.as_deref(),
            Some("Allow bash?\n\nrm -rf build")
        );
        let reply = transport
            .frames()
            .into_iter()
            .find(|frame| frame["type"] == "extension_ui_response")
            .expect("the decision was written back to pi");
        assert_eq!(reply["id"], "ui-1");
        assert_eq!(reply["confirmed"], expected == "Approved.");
    }
}

#[tokio::test]
async fn cancellation_asks_pi_to_abort() {
    let transport = PiFixture::new(Script::Hang);
    let runtime = runtime(transport.clone());
    let request = request("work forever");
    let cancellation = request.cancellation.clone();
    let turn = tokio::spawn(async move {
        runtime
            .run(request, &Collector::default(), None)
            .await
            .map(|result| result.text)
    });
    assert!(transport.wait_for("prompt").await);
    cancellation.cancel();
    let error = turn.await.unwrap().unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::Cancelled {
                provider: Provider::Pi
            }
        ),
        "{error:?}"
    );
    assert!(transport.commands().contains(&"abort".to_string()));
}

#[tokio::test]
async fn a_missing_resumed_session_never_receives_the_prompt() {
    let transport = PiFixture::new(Script::MissingSession);
    let mut request = request("continue");
    request.session_id = Some("session-gone".into());
    let error = runtime(transport.clone())
        .run(request, &Collector::default(), None)
        .await
        .unwrap_err();
    match error {
        RuntimeError::ProcessFailed {
            provider_code,
            delivery,
            ..
        } => {
            assert_eq!(provider_code.as_deref(), Some("pi::session_not_found"));
            assert_eq!(delivery, DeliveryState::NotSent);
        }
        other => panic!("expected a session failure, got {other:?}"),
    }
    assert_eq!(transport.commands(), ["get_state"]);
    assert!(transport.spawns()[0]
        .windows(2)
        .any(|pair| pair == ["--session-id", "session-gone"]));
}

#[tokio::test]
async fn a_rejected_prompt_reports_missing_credentials() {
    let transport = PiFixture::new(Script::RejectedPrompt);
    let error = runtime(transport)
        .run(request("hello"), &Collector::default(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::ProcessFailed {
                kind: ProviderProcessErrorKind::AuthenticationFailed,
                delivery: DeliveryState::NotSent,
                ..
            }
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn permission_modes_pi_cannot_enforce_are_rejected_before_spawning() {
    let transport = PiFixture::new(Script::ToolRun);
    let mut request = request("hello");
    request.permission_mode = PermissionMode::Default;
    let error = runtime(transport.clone())
        .run(request, &Collector::default(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::InvalidRequest {
                field: "permission_mode",
                ..
            }
        ),
        "{error:?}"
    );
    let spawns = transport.spawns();
    assert!(spawns.is_empty(), "{spawns:?}");
}

#[tokio::test]
async fn turn_scoped_mcp_servers_are_rejected_before_spawning() {
    let transport = PiFixture::new(Script::ToolRun);
    let mut request = request("hello");
    request.launch_context.mcp_servers.insert(
        "docs".into(),
        McpServerConfig::Http {
            url: "https://mcp.example.test/mcp".into(),
            headers_from: std::collections::BTreeMap::new(),
        },
    );
    let error = runtime(transport.clone())
        .run(request, &Collector::default(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::InvalidRequest { .. }),
        "{error:?}"
    );
    let spawns = transport.spawns();
    assert!(spawns.is_empty(), "{spawns:?}");
}

#[tokio::test]
async fn discovery_reports_models_and_configured_credentials() {
    let transport = PiFixture::new(Script::ToolRun);
    let inventory = runtime(transport).discover_harnesses().await;
    let pi = inventory
        .harnesses
        .iter()
        .find(|harness| harness.provider == Provider::Pi)
        .expect("pi is registered by default");
    assert_eq!(pi.status, HarnessStatus::Ready);
    assert_eq!(
        pi.authentication.status,
        HarnessAuthenticationStatus::Authenticated
    );
    assert_eq!(pi.models.status, HarnessCatalogStatus::Ready);
    let ids = pi
        .models
        .models
        .iter()
        .map(|model| (model.id.as_str(), model.is_default))
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [
            ("anthropic/claude-sonnet-4-5", true),
            ("openai/gpt-4o-mini", false)
        ]
    );
    assert!(pi.permissions.full_access && pi.permissions.plan && !pi.permissions.default);
}

#[tokio::test]
async fn a_retained_conversation_resumes_its_pi_session() {
    let transport = PiFixture::new(Script::ToolRun);
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(Pi::default());
    let client = InProcessRuntimeClient::new(builder.build().unwrap());
    let mut spec = RuntimeSpec::new(
        RuntimeId::new("pi-fixture-runtime").unwrap(),
        Provider::Pi,
        ".",
    );
    spec.permission_mode = PermissionMode::FullAccess;
    let handle = client.acquire(spec).await.unwrap();

    for (index, prompt) in ["first", "second"].into_iter().enumerate() {
        let result = handle
            .start_turn(TurnInput::new(
                InvocationId::new(format!("turn-{index}")).unwrap(),
                prompt,
            ))
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(result.session_id.as_deref(), Some("session-fixture"));
    }
    let spawns = transport.spawns();
    assert_eq!(spawns.len(), 2, "pi does not retain its process yet");
    assert!(!spawns[0].contains(&"--session-id".to_string()));
    assert!(spawns[1]
        .windows(2)
        .any(|pair| pair == ["--session-id", "session-fixture"]));
}
