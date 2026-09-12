//! Public API coverage for a remote, streaming execution transport.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use temps_agent_runtime::{
    AccountUsageProbeSpec, AccountUsageReport, AccountUsageSnapshot, AccountUsageStatus,
    AccountUsageWindow, AccountUsageWindowKind, AdapterOutput, AdapterState, AgentAdapter,
    AgentRuntime, ApprovalDecision, ApprovalRequest, AuthenticationProbeSpec, CommandSpec,
    ExecutionTransport, HarnessAuthentication, HarnessAuthenticationStatus, ManagedProcessEvent,
    ManagedProcessSpec, ManagedProcessStatus, ManagedProcessSupervisor, PermissionSupport,
    Provider, ProviderProbeContext, ProviderReadiness, QuestionAnswer, QuestionRequest, Result,
    RunStatus, RuntimeError, SandboxCapabilities, SandboxError, SecretString,
    TransportCapabilities, TransportError, TransportErrorKind, TransportExitStatus,
    TransportProcess, TransportProcessControl, TransportProcessHandle, TransportReader,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, TransportWriter, TurnEvent,
    TurnRequest,
};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Default)]
struct FixtureTransport {
    requests: Arc<Mutex<Vec<TransportSpawnRequest>>>,
    next_id: Arc<AtomicU64>,
}

impl FixtureTransport {
    fn requests(&self) -> Vec<TransportSpawnRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl ExecutionTransport for FixtureTransport {
    fn name(&self) -> &'static str {
        "fixture-remote"
    }

    fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            remote: true,
            interactive_stdin: true,
            reconnect: true,
            managed_processes: true,
            process_tree_termination: true,
            sandbox: SandboxCapabilities {
                filesystem: true,
                process_isolation: true,
                ..SandboxCapabilities::NONE
            },
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
            version: Some("remote-1.0".to_string()),
            detail: "available inside fixture sandbox".to_string(),
        })
    }

    async fn validate_working_directory(&self, working_directory: &Path) -> TransportResult<()> {
        if working_directory == Path::new("/remote/workspace") {
            Ok(())
        } else {
            Err(TransportError::new(
                TransportErrorKind::WorkingDirectoryNotFound,
                self.name(),
                "validate_working_directory",
                "remote workspace missing",
                false,
            ))
        }
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        let provider = request.command.program.ends_with("claude");
        let account_usage = request.command.program.ends_with("account-usage");
        let authentication = request.command.program.ends_with("auth-status");
        self.requests.lock().unwrap().push(request);
        let (sdk_stdin, mut remote_stdin) = duplex(8 * 1024);
        let (sdk_stdout, mut remote_stdout) = duplex(8 * 1024);
        let (sdk_stderr, remote_stderr) = duplex(8 * 1024);
        drop(remote_stderr);
        tokio::spawn(async move {
            let mut input = Vec::new();
            let _ = remote_stdin.read_to_end(&mut input).await;
        });
        tokio::spawn(async move {
            let output = if account_usage {
                b"{\"account_usage\":true}\n".as_slice()
            } else if authentication {
                b"authenticated\n".as_slice()
            } else if provider {
                b"{\"text\":\"remote turn\"}\n{\"terminal\":true}\n".as_slice()
            } else {
                b"service-ready\n".as_slice()
            };
            let _ = remote_stdout.write_all(output).await;
        });
        let native_id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        Ok(TransportProcess::new(
            TransportProcessHandle {
                transport: self.name().to_string(),
                native_id,
            },
            None,
            Some(Box::new(sdk_stdin) as TransportWriter),
            Box::new(sdk_stdout) as TransportReader,
            Box::new(sdk_stderr) as TransportReader,
            FixtureControl,
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
            "fixture attach is not exercised",
            false,
        ))
    }
}

struct FixtureControl;

#[async_trait]
impl TransportProcessControl for FixtureControl {
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

struct FixtureClaude;

#[async_trait]
impl AgentAdapter for FixtureClaude {
    fn provider(&self) -> Provider {
        Provider::Claude
    }

    fn executable(&self) -> PathBuf {
        PathBuf::from("/remote/bin/claude")
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            default: true,
            accept_edits: true,
            plan: true,
            full_access: true,
            custom: true,
            live_approvals: true,
            live_questions: true,
        }
    }

    fn account_usage_probe(&self) -> Option<AccountUsageProbeSpec> {
        Some(AccountUsageProbeSpec {
            command: CommandSpec::new("/remote/bin/account-usage"),
            expected_response_ids: None,
        })
    }

    fn authentication_probe(&self) -> Option<AuthenticationProbeSpec> {
        Some(AuthenticationProbeSpec {
            command: CommandSpec::new("/remote/bin/auth-status"),
        })
    }

    fn parse_authentication_probe(
        &self,
        stdout: &[u8],
        _stderr: &str,
        status: TransportExitStatus,
    ) -> Result<HarnessAuthentication> {
        assert!(status.success);
        assert_eq!(stdout, b"authenticated\n");
        Ok(HarnessAuthentication::authenticated("fixture_status"))
    }

    fn parse_account_usage_probe(&self, lines: &[String]) -> Result<AccountUsageReport> {
        assert_eq!(lines, ["{\"account_usage\":true}"]);
        Ok(AccountUsageReport::available(AccountUsageSnapshot {
            provider: Provider::Claude,
            plan: Some("fixture".into()),
            windows: vec![AccountUsageWindow {
                id: "weekly".into(),
                label: None,
                kind: AccountUsageWindowKind::Weekly,
                used_percent: 25.0,
                duration_minutes: Some(10_080),
                resets_at_unix_seconds: Some(1_780_000_000),
            }],
            credits: None,
        }))
    }

    async fn readiness(&self) -> ProviderReadiness {
        panic!("runtime readiness must be transport-scoped")
    }

    fn command(&self, request: &TurnRequest) -> Result<CommandSpec> {
        let mut command = CommandSpec::new(self.executable());
        command.initial_stdin = Some(request.prompt.as_bytes().to_vec());
        command.interactive_stdin = true;
        Ok(command)
    }

    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
        let value: Value = serde_json::from_str(line).unwrap();
        let mut output = AdapterOutput::default();
        if let Some(text) = value.get("text").and_then(Value::as_str) {
            state.result.text.push_str(text);
            output.events.push(TurnEvent::TextDelta {
                text: text.to_string(),
            });
        }
        if value.get("terminal").and_then(Value::as_bool) == Some(true) {
            state.result.status = RunStatus::Succeeded;
            output.terminal = true;
        }
        Ok(output)
    }

    fn approval_response(
        &self,
        _request: &ApprovalRequest,
        _original: &Value,
        _decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn question_response(
        &self,
        _request: &QuestionRequest,
        _original: &Value,
        _answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

#[tokio::test]
async fn provider_readiness_and_execution_happen_in_the_remote_transport() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(FixtureClaude);
    let runtime = builder.build().unwrap();

    let readiness = runtime.readiness(Provider::Claude).await.unwrap();
    assert!(readiness.installed);
    assert_eq!(
        readiness.executable,
        Some(PathBuf::from("/remote/bin/claude"))
    );

    let mut request = TurnRequest::new(Provider::Claude, "/remote/workspace", "run in the sandbox");
    request.required_sandbox_capabilities = SandboxCapabilities {
        filesystem: true,
        process_isolation: true,
        ..SandboxCapabilities::NONE
    };
    let result = runtime
        .run(request, &temps_agent_runtime::NoopEventSink, None)
        .await
        .unwrap();

    assert_eq!(result.text, "remote turn");
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].command.program, Path::new("/remote/bin/claude"));
    assert_eq!(
        requests[0].working_directory,
        Path::new("/remote/workspace")
    );
}

#[tokio::test]
async fn account_usage_is_fetched_inside_the_selected_transport() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(FixtureClaude);
    let runtime = builder.build().unwrap();

    let report = runtime.fetch_account_usage(Provider::Claude).await;

    assert_eq!(report.status, AccountUsageStatus::Available);
    assert_eq!(report.usage.unwrap().plan.as_deref(), Some("fixture"));
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].command.program,
        Path::new("/remote/bin/account-usage")
    );
    assert_eq!(requests[0].working_directory, Path::new("."));
}

#[tokio::test]
async fn account_usage_probe_uses_explicit_target_context_without_exposing_secrets() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(FixtureClaude);
    let runtime = builder.build().unwrap();
    let context = ProviderProbeContext::new("/remote/workspace")
        .with_environment("CLAUDE_ACCESS_TOKEN", SecretString::new("probe-secret"));
    assert!(!format!("{context:?}").contains("probe-secret"));

    let report = runtime
        .fetch_account_usage_with(Provider::Claude, context)
        .await
        .unwrap();

    assert_eq!(report.status, AccountUsageStatus::Available);
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].working_directory,
        Path::new("/remote/workspace")
    );
    assert_eq!(
        requests[0]
            .command
            .environment
            .get(std::ffi::OsStr::new("CLAUDE_ACCESS_TOKEN")),
        Some(&std::ffi::OsString::from("probe-secret"))
    );
    assert!(!format!("{:?}", requests[0].command).contains("probe-secret"));
}

#[tokio::test]
async fn invalid_probe_environment_is_rejected_before_spawn() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(FixtureClaude);
    let runtime = builder.build().unwrap();
    let context = ProviderProbeContext::new("/remote/workspace")
        .with_environment("INVALID=NAME", SecretString::new("secret"));

    let error = runtime.discover_harnesses_with(context).await.unwrap_err();

    assert!(matches!(
        error,
        RuntimeError::InvalidRequest {
            field: "probe_context.environment",
            ..
        }
    ));
    assert!(transport.requests().is_empty());
}

#[tokio::test]
async fn harness_inventory_is_probed_inside_the_selected_transport() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport);
    builder.register(FixtureClaude);
    let inventory = builder.build().unwrap().discover_harnesses().await;

    assert_eq!(inventory.transport, "fixture-remote");
    assert!(inventory.transport_capabilities.remote);
    assert_eq!(inventory.harnesses.len(), 3);
    assert_eq!(inventory.harnesses[0].provider, Provider::Claude);
    assert_eq!(
        inventory.harnesses[0].authentication.status,
        HarnessAuthenticationStatus::Authenticated
    );
    assert!(inventory.harnesses.iter().all(|harness| {
        harness.readiness.as_ref().is_some_and(|readiness| {
            readiness.installed && readiness.version.as_deref() == Some("remote-1.0")
        })
    }));
}

#[tokio::test]
async fn authentication_probe_uses_the_same_explicit_target_context() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(FixtureClaude);
    let context = ProviderProbeContext::new("/remote/workspace")
        .with_environment("PROBE_TOKEN", SecretString::new("target-secret"));

    let inventory = builder
        .build()
        .unwrap()
        .discover_harnesses_with(context)
        .await
        .unwrap();

    assert_eq!(
        inventory.harnesses[0].authentication.status,
        HarnessAuthenticationStatus::Authenticated
    );
    let requests = transport.requests();
    let authentication = requests
        .iter()
        .find(|request| request.command.program.ends_with("auth-status"))
        .unwrap();
    assert_eq!(
        authentication.working_directory,
        Path::new("/remote/workspace")
    );
    assert_eq!(
        authentication
            .command
            .environment
            .get(std::ffi::OsStr::new("PROBE_TOKEN")),
        Some(&std::ffi::OsString::from("target-secret"))
    );
    assert!(!format!("{:?}", authentication.command).contains("target-secret"));
}

#[tokio::test]
async fn missing_remote_sandbox_controls_fail_before_spawn() {
    let transport = FixtureTransport::default();
    let mut builder = AgentRuntime::builder().transport(transport.clone());
    builder.register(FixtureClaude);
    let runtime = builder.build().unwrap();
    let mut request = TurnRequest::new(Provider::Claude, "/remote/workspace", "test");
    request.required_sandbox_capabilities = SandboxCapabilities {
        network_allowlist: true,
        ..SandboxCapabilities::NONE
    };

    let error = runtime
        .run(request, &temps_agent_runtime::NoopEventSink, None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeError::Sandbox(SandboxError::MissingCapabilities { missing, .. })
            if missing == ["network_allowlist"]
    ));
    assert!(transport.requests().is_empty());
}

#[tokio::test]
async fn managed_processes_use_the_same_remote_transport_and_expose_its_handle() {
    let transport = FixtureTransport::default();
    let supervisor = ManagedProcessSupervisor::builder()
        .transport(transport.clone())
        .build()
        .unwrap();
    let mut process = supervisor
        .start(ManagedProcessSpec::background(
            "remote service",
            "/remote/bin/worker",
            "/remote/workspace",
        ))
        .await
        .unwrap();

    let line = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if let ManagedProcessEvent::Log { line, .. } = process.recv().await.unwrap() {
                break line;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(line.text, "service-ready");

    let snapshot = process.snapshot().await.unwrap();
    assert!(matches!(
        snapshot.status,
        ManagedProcessStatus::Running | ManagedProcessStatus::Succeeded
    ));
    let handle = snapshot.transport_handle.unwrap();
    assert_eq!(handle.transport, "fixture-remote");
    assert_eq!(transport.requests().len(), 1);
}
