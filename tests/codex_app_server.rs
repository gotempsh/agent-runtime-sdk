//! Bidirectional Codex coverage against a scripted `codex app-server`.
//!
//! The fixture speaks the real JSON-RPC-over-stdio protocol: it answers
//! `initialize`, `thread/start`, `thread/resume` and `turn/start`, then drives
//! notifications and server-initiated requests exactly as the app server does.
//! Nothing here shells out to a Codex binary.

#![cfg(feature = "codex")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use temps_agent_runtime::providers::{Codex, CodexTurnMode};
use temps_agent_runtime::retained::TurnAttachment;
use temps_agent_runtime::{
    AgentRuntime, ApprovalDecision, ApprovalRequest, EventSink, ExecutionTransport,
    InteractionHandler, McpServerConfig, PermissionMode, Provider, ProviderReadiness,
    QuestionAnswer, QuestionRequest, Result, RuntimeError, SandboxCapabilities, SecretString,
    TransportCapabilities, TransportError, TransportErrorKind, TransportExitStatus,
    TransportProcess, TransportProcessControl, TransportProcessHandle, TransportReader,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, TransportWriter, TurnEvent,
    TurnRequest,
};
use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

/// Turn shape the fixture app server plays out after `turn/start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Script {
    /// Ask for command approval and finish once the decision arrives.
    Approval,
    /// Ask a blocking question and finish once the answer arrives.
    BlockingQuestion,
    /// Ask a non-blocking question; the turn must not wait for a reply.
    AsyncQuestion,
    /// Stream one delta and then wait for `turn/interrupt`.
    Interrupt,
}

#[derive(Clone)]
struct AppServer {
    script: Script,
    /// Every frame the SDK wrote to the app server, in order.
    frames: Arc<Mutex<Vec<Value>>>,
    /// Arguments the SDK asked the transport to spawn `codex` with.
    arguments: Arc<Mutex<Vec<String>>>,
}

impl AppServer {
    fn new(script: Script) -> Self {
        Self {
            script,
            frames: Arc::new(Mutex::new(Vec::new())),
            arguments: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn arguments(&self) -> Vec<String> {
        self.arguments.lock().unwrap().clone()
    }

    fn frames(&self) -> Vec<Value> {
        self.frames.lock().unwrap().clone()
    }

    fn method_frame(&self, method: &str) -> Option<Value> {
        self.frames()
            .into_iter()
            .find(|frame| frame.get("method").and_then(Value::as_str) == Some(method))
    }

    /// Wait until the SDK wrote a frame for `method`, or give up.
    async fn wait_for(&self, method: &str) -> Option<Value> {
        for _ in 0..200 {
            if let Some(frame) = self.method_frame(method) {
                return Some(frame);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }
}

#[async_trait]
impl ExecutionTransport for AppServer {
    fn name(&self) -> &'static str {
        "fixture-app-server"
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
            version: Some("codex-cli 0.155.1".to_string()),
            detail: "fixture".to_string(),
        })
    }

    async fn validate_working_directory(&self, _working_directory: &Path) -> TransportResult<()> {
        Ok(())
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        assert_eq!(
            request
                .command
                .args
                .first()
                .map(|argument| argument.to_string_lossy().into_owned())
                .as_deref(),
            Some("app-server"),
            "the app-server turn mode must not launch `codex exec`"
        );
        assert!(request.command.interactive_stdin);
        *self.arguments.lock().unwrap() = request
            .command
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        let (sdk_stdin, server_input) = duplex(64 * 1024);
        let (sdk_stdout, server_output) = duplex(64 * 1024);
        let (sdk_stderr, server_stderr) = duplex(1024);
        drop(server_stderr);
        let script = self.script;
        let frames = Arc::clone(&self.frames);
        tokio::spawn(async move { serve(script, frames, server_input, server_output).await });
        Ok(TransportProcess::new(
            TransportProcessHandle {
                transport: "fixture-app-server".to_string(),
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
            "the fixture app server cannot be reattached",
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

/// Scripted app server: reads client frames and writes protocol replies.
async fn serve(
    script: Script,
    frames: Arc<Mutex<Vec<Value>>>,
    input: tokio::io::DuplexStream,
    mut output: tokio::io::DuplexStream,
) {
    async fn send(output: &mut tokio::io::DuplexStream, value: Value) {
        let _ = output.write_all(format!("{value}\n").as_bytes()).await;
        let _ = output.flush().await;
    }
    let mut lines = BufReader::new(input).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        frames.lock().unwrap().push(message.clone());
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        match message.get("method").and_then(Value::as_str) {
            Some("initialized") => {}
            Some("thread/start") => {
                send(
                    &mut output,
                    json!({"jsonrpc":"2.0","id":id,"result":{
                        "thread":{"id":"thread-fixture","name":"Fixture thread"}
                    }}),
                )
                .await;
            }
            Some("thread/resume") => {
                let resumed = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-fixture")
                    .to_string();
                send(
                    &mut output,
                    json!({"jsonrpc":"2.0","id":id,"result":{"thread":{"id":resumed}}}),
                )
                .await;
            }
            Some("turn/start") => {
                send(
                    &mut output,
                    json!({"jsonrpc":"2.0","id":id,"result":{"turn":{"id":"turn-1"}}}),
                )
                .await;
                send(
                    &mut output,
                    json!({"jsonrpc":"2.0","method":"item/agentMessage/delta",
                        "params":{"itemId":"item-1","delta":"Working"}}),
                )
                .await;
                for frame in opening_frames(script) {
                    send(&mut output, frame).await;
                }
            }
            // A reply to one of our server-initiated requests.
            None => {
                for frame in completion_frames(script) {
                    send(&mut output, frame).await;
                }
            }
            // `initialize` and `turn/interrupt` need only a bare acknowledgement.
            _ => {
                send(&mut output, json!({"jsonrpc":"2.0","id":id,"result":{}})).await;
            }
        }
    }
}

fn opening_frames(script: Script) -> Vec<Value> {
    match script {
        Script::Approval => vec![
            json!({"jsonrpc":"2.0","method":"item/started","params":{"item":{
                "id":"item-2","type":"commandExecution","command":"cargo test","status":"inProgress"
            }}}),
            json!({"jsonrpc":"2.0","id":"server-1","method":"item/commandExecution/requestApproval",
                "params":{"threadId":"thread-fixture","turnId":"turn-1","itemId":"item-2",
                    "command":"cargo test","startedAtMs":1}}),
        ],
        Script::BlockingQuestion => vec![json!({
            "jsonrpc":"2.0","id":"server-2","method":"item/tool/requestUserInput",
            "params":{"threadId":"thread-fixture","turnId":"turn-1","itemId":"item-3",
                "isBlocking":true,
                "questions":[{"id":"q1","header":"Fruit","question":"Which fruit?",
                    "options":[{"label":"Banana","description":"Yellow"},
                               {"label":"Plantain","description":"Also yellow"}]}]}
        })],
        Script::AsyncQuestion => vec![json!({
            "jsonrpc":"2.0","id":"server-3","method":"item/tool/requestUserInput",
            "params":{"threadId":"thread-fixture","turnId":"turn-1","itemId":"item-4",
                "isBlocking":false,
                "questions":[{"id":"q2","header":"Theme","question":"Dark or light?",
                    "options":[{"label":"Dark","description":"Dim"}]}]}
        })],
        Script::Interrupt => Vec::new(),
    }
}

fn completion_frames(script: Script) -> Vec<Value> {
    let mut frames = Vec::new();
    if script == Script::Approval {
        frames.push(
            json!({"jsonrpc":"2.0","method":"item/completed","params":{"item":{
                "id":"item-2","type":"commandExecution","command":"cargo test",
                "status":"completed","aggregatedOutput":"ok","exitCode":0
            }}}),
        );
    }
    frames.extend([
        json!({"jsonrpc":"2.0","method":"thread/tokenUsage/updated","params":{
            "threadId":"thread-fixture",
            "tokenUsage":{"last":{"inputTokens":120,"outputTokens":34,"totalTokens":154},
                "modelContextWindow":272_000}
        }}),
        json!({"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{
            "id":"turn-1","status":"completed","model":"gpt-5-codex",
            "items":[{"type":"agentMessage","text":"Working"}]
        }}}),
    ]);
    frames
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
    answer: Option<QuestionAnswer>,
    approvals: Arc<Mutex<Vec<ApprovalRequest>>>,
    questions: Arc<Mutex<Vec<QuestionRequest>>>,
}

impl Responder {
    fn new(decision: ApprovalDecision, answer: Option<QuestionAnswer>) -> Self {
        Self {
            decision,
            answer,
            approvals: Arc::new(Mutex::new(Vec::new())),
            questions: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl InteractionHandler for Responder {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        self.approvals.lock().unwrap().push(request);
        self.decision.clone()
    }

    async fn answer(&self, request: QuestionRequest) -> Option<QuestionAnswer> {
        self.questions.lock().unwrap().push(request);
        self.answer.clone()
    }
}

fn runtime(transport: AppServer) -> AgentRuntime {
    let mut builder = AgentRuntime::builder().transport(transport);
    builder.register(Codex::app_server());
    builder.build().unwrap()
}

fn request() -> TurnRequest {
    let mut request = TurnRequest::new(Provider::Codex, ".", "review the workspace");
    request.permission_mode = PermissionMode::Default;
    request.timeout = Duration::from_secs(20);
    request.interaction_timeout = Duration::from_secs(10);
    request
}

/// Every frame the SDK sent for `method`, decoded as its params.
fn params_of(frames: &[Value], method: &str) -> Option<Value> {
    frames
        .iter()
        .find(|frame| frame.get("method").and_then(Value::as_str) == Some(method))
        .map(|frame| frame.get("params").cloned().unwrap_or(Value::Null))
}

#[tokio::test]
async fn the_app_server_mode_advertises_live_interactions() {
    assert_eq!(Codex::default().turn_mode(), CodexTurnMode::Exec);
    let transport = AppServer::new(Script::Approval);
    let runtime = runtime(transport);
    let support = runtime.permission_support(Provider::Codex).unwrap();

    assert!(support.live_approvals);
    assert!(support.live_questions);

    let turn = runtime.turn_capabilities(Provider::Codex).unwrap();
    assert!(turn.context_window_usage);
    assert!(turn.native_image_attachments);
}

#[tokio::test]
async fn an_approval_is_accepted_through_the_interaction_handler() {
    let transport = AppServer::new(Script::Approval);
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Allow, None);
    let runtime = runtime(transport.clone());

    let result = runtime
        .run(request(), &events, Some(&responder))
        .await
        .unwrap();

    assert_eq!(result.text, "Working");
    assert_eq!(result.session_id.as_deref(), Some("thread-fixture"));
    assert_eq!(result.model.as_deref(), Some("gpt-5-codex"));
    assert_eq!(result.usage.input_tokens, Some(120));
    let approvals = responder.approvals.lock().unwrap().clone();
    assert_eq!(approvals.len(), 1);
    assert_eq!(approvals[0].tool_name, "command_execution");
    assert_eq!(
        approvals[0].description.as_deref(),
        Some("Run `cargo test`")
    );
    let decision = transport
        .frames()
        .into_iter()
        .find(|frame| frame.get("id").and_then(Value::as_str) == Some("server-1"))
        .expect("the approval was answered");
    assert_eq!(decision.pointer("/result/decision"), Some(&json!("accept")));
    assert!(events.events().iter().any(|event| matches!(
        event,
        TurnEvent::ToolCall { name, .. } if name == "command_execution"
    )));
    assert!(events
        .events()
        .iter()
        .any(|event| matches!(event, TurnEvent::ApprovalRequested(_))));
}

#[tokio::test]
async fn a_turn_streams_the_active_context_window_for_the_selected_model() {
    let transport = AppServer::new(Script::Approval);
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Allow, None);
    let runtime = runtime(transport);
    let mut request = request();
    request.model = Some("gpt-5-codex".to_string());

    let result = runtime
        .run(request, &events, Some(&responder))
        .await
        .unwrap();

    let usage = events
        .events()
        .into_iter()
        .find_map(|event| match event {
            TurnEvent::Usage(usage) => usage.context_window,
            _ => None,
        })
        .expect("the turn reported context-window occupancy");
    assert_eq!(usage.used_tokens, Some(154));
    assert_eq!(usage.limit_tokens, Some(272_000));
    assert_eq!(usage.model.as_deref(), Some("gpt-5-codex"));
    assert!(!usage.estimated);
    assert_eq!(result.usage.context_window, Some(usage));
}

#[tokio::test]
async fn a_turn_carries_stdio_mcp_overrides_and_native_image_inputs() {
    let transport = AppServer::new(Script::Approval);
    let responder = Responder::new(ApprovalDecision::Allow, None);
    let runtime = runtime(transport.clone());
    let mut request = request();
    request.launch_context.mcp_servers.insert(
        "temps_fleet".to_string(),
        McpServerConfig::Stdio {
            command: "/usr/local/bin/temps-fleet".into(),
            args: vec!["mcp-server".to_string()],
            environment_from: BTreeMap::from([(
                "TEMPS_FLEET_MCP_PARENT_TOKEN".to_string(),
                "TEMPS_FLEET_MCP_PARENT_TOKEN".to_string(),
            )]),
        },
    );
    request.environment.insert(
        "TEMPS_FLEET_MCP_PARENT_TOKEN".to_string(),
        SecretString::new("fleet-token"),
    );
    request.attachments = vec![TurnAttachment {
        path: "/tmp/screenshot.png".into(),
        display_name: Some("Screenshot".to_string()),
        media_type: Some("image/png".to_string()),
    }];

    runtime
        .run(request, &Collector::default(), Some(&responder))
        .await
        .unwrap();

    let arguments = transport.arguments();
    assert!(arguments
        .iter()
        .any(|argument| argument
            == r#"mcp_servers.temps_fleet.command="/usr/local/bin/temps-fleet""#));
    assert!(arguments
        .iter()
        .any(|argument| argument == r#"mcp_servers.temps_fleet.args=["mcp-server"]"#));
    assert!(arguments.iter().any(|argument| argument
        == r#"mcp_servers.temps_fleet.env_vars=["TEMPS_FLEET_MCP_PARENT_TOKEN"]"#));
    assert!(!arguments.join(" ").contains("fleet-token"));

    let started = params_of(&transport.frames(), "turn/start").expect("the turn started");
    assert_eq!(
        started["input"][1],
        json!({"type": "localImage", "path": "/tmp/screenshot.png"})
    );
}

#[tokio::test]
async fn a_denied_approval_is_declined_on_the_wire() {
    let transport = AppServer::new(Script::Approval);
    let responder = Responder::new(
        ApprovalDecision::Deny {
            reason: Some("not now".to_string()),
        },
        None,
    );
    let runtime = runtime(transport.clone());

    runtime
        .run(request(), &Collector::default(), Some(&responder))
        .await
        .unwrap();

    let decision = transport
        .frames()
        .into_iter()
        .find(|frame| frame.get("id").and_then(Value::as_str) == Some("server-1"))
        .expect("the approval was answered");
    assert_eq!(
        decision.pointer("/result/decision"),
        Some(&json!("decline"))
    );
}

#[tokio::test]
async fn a_blocking_question_waits_for_the_handler_answer() {
    let transport = AppServer::new(Script::BlockingQuestion);
    let events = Collector::default();
    let responder = Responder::new(
        ApprovalDecision::Allow,
        Some(QuestionAnswer::selected("Which fruit?", "Banana")),
    );
    let runtime = runtime(transport.clone());

    runtime
        .run(request(), &events, Some(&responder))
        .await
        .unwrap();

    let questions = responder.questions.lock().unwrap().clone();
    assert_eq!(questions.len(), 1);
    let prompts = questions[0].prompts().unwrap();
    assert_eq!(prompts[0].header, "Fruit");
    assert_eq!(prompts[0].question, "Which fruit?");
    assert_eq!(prompts[0].options[0].label, "Banana");
    let answer = transport
        .frames()
        .into_iter()
        .find(|frame| frame.get("id").and_then(Value::as_str) == Some("server-2"))
        .expect("the question was answered");
    assert_eq!(
        answer.pointer("/result/answers/q1/answers"),
        Some(&json!(["Banana"]))
    );
    assert!(events
        .events()
        .iter()
        .any(|event| matches!(event, TurnEvent::QuestionRequested(_))));
}

#[tokio::test]
async fn an_async_question_never_blocks_the_turn() {
    let transport = AppServer::new(Script::AsyncQuestion);
    let events = Collector::default();
    // Answering would be a bug: the turn must not wait on a non-blocking ask.
    let responder = Responder::new(ApprovalDecision::Allow, None);
    let runtime = runtime(transport.clone());

    let result = runtime
        .run(request(), &events, Some(&responder))
        .await
        .unwrap();

    assert_eq!(result.text, "Working");
    assert!(
        responder.questions.lock().unwrap().is_empty(),
        "a non-blocking question must not reach the interaction handler"
    );
    let open = events
        .events()
        .into_iter()
        .find_map(|event| match event {
            TurnEvent::AsyncQuestionRequested(request) => Some(request),
            _ => None,
        })
        .expect("the open question reached the application");
    assert_eq!(open.prompts().unwrap()[0].header, "Theme");
    let reply = transport
        .frames()
        .into_iter()
        .find(|frame| frame.get("id").and_then(Value::as_str) == Some("server-3"))
        .expect("the app server was answered immediately");
    let note = reply
        .pointer("/result/answers/q2/answers/0")
        .and_then(Value::as_str)
        .unwrap();
    assert!(note.contains("has not answered yet"), "{note}");
}

#[tokio::test]
async fn cancellation_sends_a_native_turn_interrupt() {
    let transport = AppServer::new(Script::Interrupt);
    let cancellation = CancellationToken::new();
    let mut request = request();
    request.cancellation = cancellation.clone();
    let runtime = runtime(transport.clone());
    let waiting = transport.clone();
    tokio::spawn(async move {
        // Cancel only once the turn is actually running.
        waiting.wait_for("turn/start").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
    });

    let error = runtime
        .run(request, &Collector::default(), None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeError::Cancelled {
            provider: Provider::Codex
        }
    ));
    let interrupt = transport
        .wait_for("turn/interrupt")
        .await
        .expect("the interrupt reached the app server");
    assert_eq!(
        interrupt.pointer("/params/turnId"),
        Some(&json!("turn-1")),
        "the interrupt must name the running turn"
    );
}

#[tokio::test]
async fn a_resumed_thread_reuses_its_identifier_without_restarting_the_session() {
    let transport = AppServer::new(Script::BlockingQuestion);
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Allow, None);
    let mut request = request();
    request.session_id = Some("thread-earlier".to_string());
    let runtime = runtime(transport.clone());

    let result = runtime
        .run(request, &events, Some(&responder))
        .await
        .unwrap();

    let frames = transport.frames();
    assert!(
        params_of(&frames, "thread/start").is_none(),
        "resuming must not open a second thread"
    );
    assert_eq!(
        params_of(&frames, "thread/resume").and_then(|params| params.get("threadId").cloned()),
        Some(json!("thread-earlier"))
    );
    assert_eq!(
        params_of(&frames, "turn/start").and_then(|params| params.get("threadId").cloned()),
        Some(json!("thread-earlier"))
    );
    assert_eq!(result.session_id.as_deref(), Some("thread-earlier"));
    assert!(
        !events
            .events()
            .iter()
            .any(|event| matches!(event, TurnEvent::SessionStarted { .. })),
        "a resumed thread is not a new session"
    );
}
