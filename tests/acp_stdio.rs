//! End-to-end ACP coverage against a scripted agent process.
//!
//! The fixture is a POSIX shell script that speaks ACP's JSON-RPC over its
//! real stdio: it answers `initialize`, `session/new` and `session/load`,
//! streams `session/update` notifications, asks for permission, honors
//! `session/cancel`, and can keep running after end-of-input the way some
//! Node-based agents do. Nothing here needs a real agent installed.

#![cfg(all(feature = "acp", unix))]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use temps_agent_runtime::providers::acp::{
    AcpAgentProbe, AcpAuthentication, AcpProbeErrorKind, AGENT_OPTION,
};
use temps_agent_runtime::providers::{Acp, AcpAgent, AcpAgents};
use temps_agent_runtime::{
    AgentRuntime, ApprovalDecision, ApprovalRequest, EventSink, InteractionHandler, PermissionMode,
    PlanEntryStatus, Provider, ProviderProcessErrorKind, QuestionAnswer, QuestionRequest, Result,
    RuntimeError, ToolCallStatus, TurnEvent, TurnRequest,
};

const AGENT: &str = r#"#!/bin/sh
# Scripted ACP agent. MOCK_ACP_MODE: ok, auth, hang, linger, stall (never
# reacts to a permission reply), split (streams a tool result, then completes
# with a bare status) or orphan (never answers, and leaves a child of its own
# process group that keeps stderr open).
log="$MOCK_ACP_LOG"
say() { printf '%s\n' "$1"; }
update() { say "{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"mock-session\",\"update\":$1}}"; }
if [ "$MOCK_ACP_MODE" = orphan ]; then
  if command -v perl >/dev/null 2>&1; then
    perl -MPOSIX -e 'POSIX::setsid(); exec "sleep", "20"' >/dev/null &
  else
    setsid sleep 20 >/dev/null &
  fi
  echo $! > "$log.orphan"
fi
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$log"
  case "$line" in
    *'"method":"initialize"'*)
      if [ "$MOCK_ACP_MODE" = orphan ]; then continue; fi
      say '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true},"authMethods":[{"id":"login","name":"Log in with Mock"}],"agentInfo":{"name":"mock-agent","version":"0.1.0"}}}' ;;
    *'"method":"session/new"'*)
      if [ "$MOCK_ACP_MODE" = auth ]; then
        say '{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Authentication required"}}'
      else
        echo 'Loaded cached credentials.'
        say '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"mock-session"}}'
      fi ;;
    *'"method":"session/load"'*)
      update '{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"replayed"}}'
      say '{"jsonrpc":"2.0","id":2,"result":null}' ;;
    *'"method":"session/prompt"'*)
      update '{"sessionUpdate":"plan","entries":[{"content":"Run the tests","priority":"high","status":"in_progress"}]}'
      update '{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Running tests."}}'
      if [ "$MOCK_ACP_MODE" = hang ]; then continue; fi
      update '{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"Run npm test","kind":"execute","status":"pending","rawInput":{"command":"npm test"}}'
      say '{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{"sessionId":"mock-session","toolCall":{"toolCallId":"call-1","title":"Run npm test","kind":"execute"},"options":[{"optionId":"allow","name":"Allow","kind":"allow_once"},{"optionId":"reject","name":"Reject","kind":"reject_once"}]}}' ;;
    *'"id":7'*)
      if [ "$MOCK_ACP_MODE" = stall ]; then continue; fi
      case "$line" in
        *'"optionId":"allow"'*)
          if [ "$MOCK_ACP_MODE" = split ]; then
            update '{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"in_progress","content":[{"type":"content","content":{"type":"text","text":"12 passed"}}]}'
            update '{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"completed"}'
          else
            update '{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"completed","content":[{"type":"content","content":{"type":"text","text":"12 passed"}}]}'
          fi
          update '{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":" All green."}}' ;;
        *)
          update '{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"failed","content":[{"type":"content","content":{"type":"text","text":"rejected by the user"}}]}'
          update '{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":" Skipped."}}' ;;
      esac
      update '{"sessionUpdate":"plan","entries":[{"content":"Run the tests","priority":"high","status":"completed"}]}'
      say '{"jsonrpc":"2.0","id":5,"result":{"stopReason":"end_turn"}}' ;;
    *'"method":"session/cancel"'*)
      say '{"jsonrpc":"2.0","id":5,"result":{"stopReason":"cancelled"}}' ;;
  esac
done
if [ "$MOCK_ACP_MODE" = linger ]; then
  while :; do sleep 1; done
fi
"#;

struct Fixture {
    _dir: tempfile::TempDir,
    script: PathBuf,
    log: PathBuf,
    workspace: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("mock-acp-agent");
        std::fs::write(&script, AGENT).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        Self {
            log: dir.path().join("frames.log"),
            script,
            workspace,
            _dir: dir,
        }
    }

    fn agent(&self, mode: &str) -> AcpAgent {
        let mut agent = AcpAgent::new("mock", "Mock Agent", &self.script, Vec::<String>::new());
        agent
            .environment
            .insert("MOCK_ACP_MODE".into(), mode.into());
        agent
            .environment
            .insert("MOCK_ACP_LOG".into(), self.log.to_string_lossy().into());
        agent
    }

    fn methods(&self) -> Vec<String> {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .map(|frame| {
                frame
                    .get("method")
                    .and_then(serde_json::Value::as_str)
                    .map_or_else(|| format!("reply:{}", frame["id"]), str::to_owned)
            })
            .collect()
    }

    fn runtime(&self, mode: &str) -> AgentRuntime {
        let agents = AcpAgents::new();
        agents.insert(self.agent(mode)).unwrap();
        let mut builder = AgentRuntime::builder();
        builder.register(Acp::new(agents));
        builder.build().unwrap()
    }

    fn request(&self, prompt: &str) -> TurnRequest {
        let mut request = TurnRequest::new(Provider::Acp, &self.workspace, prompt);
        request
            .harness_options
            .insert(AGENT_OPTION.into(), "mock".into());
        request.timeout = Duration::from_secs(30);
        request.interaction_timeout = Duration::from_secs(10);
        request
    }
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

#[tokio::test]
async fn runs_a_turn_with_an_approved_tool_call() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Allow);
    let result = fixture
        .runtime("ok")
        .run(fixture.request("run the tests"), &events, Some(&responder))
        .await
        .unwrap();

    assert_eq!(result.text, "Running tests. All green.");
    assert_eq!(result.session_id.as_deref(), Some("mock-session"));
    assert_eq!(
        fixture.methods(),
        ["initialize", "session/new", "session/prompt", "reply:7"]
    );
    let approvals = responder.approvals.lock().unwrap().clone();
    assert_eq!(approvals.len(), 1);
    assert_eq!(approvals[0].tool_name, "Run npm test");

    let events = events.events();
    assert!(events.iter().any(|event| matches!(
        event,
        TurnEvent::SessionStarted { session_id, .. } if session_id == "mock-session"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        TurnEvent::ToolCall { status: ToolCallStatus::Succeeded, output: Some(text), .. } if text == "12 passed"
    )));
    let plans = events
        .iter()
        .filter_map(|event| match event {
            TurnEvent::PlanUpdated { entries } => Some(entries[0].status),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        plans,
        [PlanEntryStatus::InProgress, PlanEntryStatus::Completed]
    );
    assert!(events.iter().any(|event| matches!(
        event,
        TurnEvent::Warning { message } if message.contains("non-ACP output")
    )));
}

#[tokio::test]
async fn a_denied_permission_is_reported_to_the_agent() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Deny { reason: None });
    let result = fixture
        .runtime("ok")
        .run(fixture.request("run the tests"), &events, Some(&responder))
        .await
        .unwrap();
    assert_eq!(result.text, "Running tests. Skipped.");
    assert!(events.events().iter().any(|event| matches!(
        event,
        TurnEvent::ToolCall {
            status: ToolCallStatus::Failed,
            ..
        }
    )));
}

#[tokio::test]
async fn full_access_answers_permission_requests_itself() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let mut request = fixture.request("run the tests");
    request.permission_mode = PermissionMode::FullAccess;
    let result = fixture
        .runtime("ok")
        .run(request, &events, None)
        .await
        .unwrap();
    assert_eq!(result.text, "Running tests. All green.");
}

#[tokio::test]
async fn resuming_loads_the_session_without_replaying_it() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let mut request = fixture.request("again");
    request.session_id = Some("mock-session".into());
    request.permission_mode = PermissionMode::FullAccess;
    let result = fixture
        .runtime("ok")
        .run(request, &events, None)
        .await
        .unwrap();
    assert_eq!(result.text, "Running tests. All green.");
    assert_eq!(fixture.methods()[1], "session/load");
    assert!(!events
        .events()
        .iter()
        .any(|event| matches!(event, TurnEvent::SessionStarted { .. })));
}

#[tokio::test]
async fn an_agent_that_needs_sign_in_fails_with_an_authentication_error() {
    let fixture = Fixture::new();
    let error = fixture
        .runtime("auth")
        .run(fixture.request("hi"), &Collector::default(), None)
        .await
        .unwrap_err();
    match error {
        RuntimeError::ProcessFailed { kind, .. } => {
            assert_eq!(kind, ProviderProcessErrorKind::AuthenticationFailed);
        }
        other => panic!("expected an authentication failure, got {other:?}"),
    }
    assert!(!fixture
        .methods()
        .iter()
        .any(|method| method == "session/prompt"));
}

#[tokio::test]
async fn an_agent_that_keeps_running_after_its_turn_is_stopped() {
    let fixture = Fixture::new();
    let mut request = fixture.request("run the tests");
    request.permission_mode = PermissionMode::FullAccess;
    let started = Instant::now();
    let result = fixture
        .runtime("linger")
        .run(request, &Collector::default(), None)
        .await
        .unwrap();
    assert_eq!(result.text, "Running tests. All green.");
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "the finished turn waited {:?} for the agent to exit",
        started.elapsed()
    );
}

#[tokio::test]
async fn cancellation_sends_session_cancel() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let request = fixture.request("run the tests");
    let cancellation = request.cancellation.clone();
    let runtime = fixture.runtime("hang");
    let turn = runtime.run(request, &events, None);
    let cancel = async {
        for _ in 0..200 {
            if events
                .events()
                .iter()
                .any(|event| matches!(event, TurnEvent::TextDelta { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(turn, cancel);
    assert!(
        matches!(result, Err(RuntimeError::Cancelled { .. })),
        "{result:?}"
    );
    assert!(fixture
        .methods()
        .iter()
        .any(|method| method == "session/cancel"));
}

#[tokio::test]
async fn probing_reports_capabilities_and_sign_in_state() {
    let fixture = Fixture::new();
    let probe: AcpAgentProbe = Acp::probe_agent(
        &fixture.agent("ok"),
        &fixture.workspace,
        true,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(probe.protocol_version, Some(1));
    assert_eq!(probe.agent_name.as_deref(), Some("mock-agent"));
    assert!(probe.load_session);
    assert_eq!(probe.auth_methods[0].name, "Log in with Mock");
    assert_eq!(probe.authentication, AcpAuthentication::Authenticated);

    let probe = Acp::probe_agent(
        &fixture.agent("auth"),
        &fixture.workspace,
        true,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(matches!(
        probe.authentication,
        AcpAuthentication::Required { .. }
    ));
}

#[tokio::test]
async fn probing_a_missing_or_silent_command_says_what_is_wrong() {
    let fixture = Fixture::new();
    let missing = AcpAgent::new("missing", "Missing", "temps-acp-not-installed", ["--acp"]);
    let error = Acp::probe_agent(&missing, &fixture.workspace, false, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert_eq!(error.kind, AcpProbeErrorKind::NotInstalled);

    // `sleep` never speaks ACP: the probe times out instead of hanging.
    let silent = AcpAgent::new("silent", "Silent", "sleep", ["30"]);
    let started = Instant::now();
    let error = Acp::probe_agent(&silent, Path::new("."), false, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(error.kind, AcpProbeErrorKind::Timeout);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn a_tool_result_streamed_before_the_final_status_is_kept() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Allow);
    let result = fixture
        .runtime("split")
        .run(fixture.request("run the tests"), &events, Some(&responder))
        .await
        .unwrap();
    assert_eq!(result.text, "Running tests. All green.");
    let completed = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            TurnEvent::ToolCall {
                status: ToolCallStatus::Succeeded,
                output,
                ..
            } => Some(output),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(completed, [Some("12 passed".to_string())]);
}

#[tokio::test]
async fn a_relative_working_directory_is_refused_before_the_agent_starts() {
    let fixture = Fixture::new();
    // `tests` exists relative to the test's directory, so the refusal is the
    // adapter's, not a missing directory.
    let mut request = TurnRequest::new(Provider::Acp, "tests", "hi");
    request
        .harness_options
        .insert(AGENT_OPTION.into(), "mock".into());
    let error = fixture
        .runtime("ok")
        .run(request, &Collector::default(), None)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("absolute working directory"),
        "{error}"
    );
    assert_eq!(fixture.methods().len(), 0, "the agent must not be started");

    let error = Acp::probe_agent(
        &fixture.agent("ok"),
        Path::new("workspace"),
        true,
        Duration::from_secs(10),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, AcpProbeErrorKind::Invalid);
    assert_eq!(fixture.methods().len(), 0, "the agent must not be started");
}

#[tokio::test]
async fn cancelling_after_the_decision_does_not_answer_the_request_twice() {
    let fixture = Fixture::new();
    let events = Collector::default();
    let responder = Responder::new(ApprovalDecision::Allow);
    let request = fixture.request("run the tests");
    let cancellation = request.cancellation.clone();
    let runtime = fixture.runtime("stall");
    let turn = runtime.run(request, &events, Some(&responder));
    let cancel = async {
        // The agent never reacts to the decision, so the permission stays
        // open in the adapter until the decision's reply is on the wire.
        for _ in 0..400 {
            if fixture.methods().iter().any(|method| method == "reply:7") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(turn, cancel);
    assert!(
        matches!(result, Err(RuntimeError::Cancelled { .. })),
        "{result:?}"
    );
    let methods = fixture.methods();
    assert!(methods.iter().any(|method| method == "session/cancel"));
    assert_eq!(
        methods
            .iter()
            .filter(|method| method.as_str() == "reply:7")
            .count(),
        1,
        "request 7 was answered more than once: {methods:?}"
    );
}

#[tokio::test]
async fn a_probe_does_not_wait_for_a_descendant_that_kept_stderr_open() {
    let fixture = Fixture::new();
    let started = Instant::now();
    let error = Acp::probe_agent(
        &fixture.agent("orphan"),
        &fixture.workspace,
        false,
        Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    let elapsed = started.elapsed();
    // The agent left its process group, so stopping the group does not close
    // the stderr pipe the descendant still holds.
    if let Ok(pid) = std::fs::read_to_string(format!("{}.orphan", fixture.log.display())) {
        let _ = std::process::Command::new("kill").arg(pid.trim()).status();
    }
    assert_eq!(error.kind, AcpProbeErrorKind::Timeout);
    assert!(
        elapsed < Duration::from_secs(10),
        "the probe waited {elapsed:?} for stderr to close"
    );
}
