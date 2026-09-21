use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value;

use crate::adapter::{inspect_executable, resolve_executable, AdapterState};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    AdapterOutput, AgentAdapter, ApprovalDecision, ApprovalRequest, CatalogProbeSpec, CommandSpec,
    HarnessCatalogStatus, HarnessControlGroup, HarnessControlKind, HarnessControlOption,
    HarnessModel, HarnessModelCatalog, PermissionMode, PermissionSupport, Provider,
    ProviderReadiness, ProviderTerminalFailure, QuestionAnswer, QuestionRequest, Result,
    RuntimeError, ToolCallStatus, TurnEvent, TurnRequest,
};

/// Transport used to run one OpenCode turn.
///
/// Both modes produce the same normalized [`TurnEvent`] stream. They differ in
/// whether the turn's permission policy is something the application controls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenCodeTurnMode {
    /// One-shot `opencode run --format json`.
    ///
    /// Output only, and with no permission enforcement an application can
    /// rely on: the CLI accepts `--auto` (approve everything) and
    /// `--agent plan`, so every other policy comes from whatever `opencode`
    /// configuration happens to exist on the machine. A caller cannot request
    /// "ask before running a shell command" here, nor learn that a tool call
    /// was refused.
    #[default]
    Run,
    /// `opencode serve`, driven over HTTP and Server-Sent Events.
    ///
    /// The permission policy is supplied per turn through
    /// `OPENCODE_CONFIG_CONTENT`, which the server reads instead of the
    /// ambient configuration, so the policy an application asked for is the
    /// one the harness actually runs under. Anything the policy marks `ask`
    /// arrives as a live approval.
    ///
    /// The server is reached on the SDK host's loopback interface, so this
    /// mode needs a transport that runs the provider on that host. Using it
    /// with a remote transport would require the port to be forwarded back,
    /// which the SDK does not arrange.
    Serve,
}

/// OpenCode CLI adapter using `opencode run --format json` or `opencode serve`.
#[derive(Debug, Clone, Default)]
pub struct OpenCode {
    executable: Option<PathBuf>,
    turn_mode: OpenCodeTurnMode,
}

impl OpenCode {
    /// Use an executable path meaningful inside the selected transport.
    pub fn with_executable(path: impl Into<PathBuf>) -> Self {
        Self {
            executable: Some(path.into()),
            turn_mode: OpenCodeTurnMode::default(),
        }
    }

    /// Drive turns through `opencode serve` instead of `opencode run`.
    ///
    /// This is the mode to use when the application — rather than whatever
    /// configuration exists on the machine — must decide what the agent is
    /// allowed to do.
    pub fn serve() -> Self {
        Self::default().with_turn_mode(OpenCodeTurnMode::Serve)
    }

    /// Select the transport used for turns.
    pub fn with_turn_mode(mut self, mode: OpenCodeTurnMode) -> Self {
        self.turn_mode = mode;
        self
    }

    /// Transport this adapter uses for turns.
    pub fn turn_mode(&self) -> OpenCodeTurnMode {
        self.turn_mode
    }

    fn serve_mode(&self) -> bool {
        self.turn_mode == OpenCodeTurnMode::Serve
    }

    /// Build the `opencode serve` invocation for one turn.
    fn serve_command(&self, request: &TurnRequest, port: u16) -> Result<CommandSpec> {
        let mut spec = CommandSpec::new(self.configured_executable());
        spec.args.extend([
            "serve".into(),
            "--hostname".into(),
            "127.0.0.1".into(),
            "--port".into(),
            port.to_string().into(),
        ]);
        // The policy travels in the environment rather than argv because it is
        // this turn's entire enforcement boundary, and `clear_environment`
        // means nothing reaches the child that was not put here deliberately.
        spec.environment.insert(
            "OPENCODE_CONFIG_CONTENT".into(),
            super::opencode_serve::permission_config(request)?.into(),
        );
        Ok(spec)
    }

    fn resolved(&self) -> Option<PathBuf> {
        resolve_executable(self.executable.as_ref(), "opencode")
    }

    fn configured_executable(&self) -> PathBuf {
        self.executable
            .clone()
            .unwrap_or_else(|| PathBuf::from("opencode"))
    }
}

#[async_trait]
impl AgentAdapter for OpenCode {
    fn provider(&self) -> Provider {
        Provider::OpenCode
    }

    fn executable(&self) -> PathBuf {
        self.configured_executable()
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            default: true,
            // `opencode run` has no flag that approves only edits; the served
            // policy expresses it as `edit: allow, bash: ask`.
            accept_edits: self.serve_mode(),
            // `--agent plan` selects a planning agent but does not guarantee
            // the absence of side effects. The served policy denies both
            // permission categories outright, which does.
            plan: self.serve_mode(),
            full_access: true,
            custom: true,
            // `opencode run` resolves permissions itself from whatever
            // configuration it finds and has no channel back into a running
            // turn; the served transport answers `permission.asked` over HTTP.
            live_approvals: self.serve_mode(),
            // OpenCode has no question channel in either mode.
            live_questions: false,
        }
    }

    fn launch_context_capabilities(&self) -> crate::LaunchContextCapabilities {
        if !self.serve_mode() {
            return crate::LaunchContextCapabilities::default();
        }
        crate::LaunchContextCapabilities {
            // Neither is a native field: OpenCode has no system-prompt or
            // tool-restriction input on its prompt body, so both are carried
            // as a prompt prefix. An *empty* allowlist is different — it is
            // enforced by a wildcard deny rule in the served policy.
            system_prompt_append: true,
            allowed_tools: true,
            stdio_mcp: true,
            http_mcp: true,
            ..crate::LaunchContextCapabilities::default()
        }
    }

    fn control_groups(&self) -> Vec<HarnessControlGroup> {
        vec![
            HarnessControlGroup {
                id: "permission_mode".into(),
                label: "Permission".into(),
                kind: HarnessControlKind::Permission,
                options: [
                    (
                        "default",
                        "Configured rules",
                        "Use OpenCode's configured permission rules.",
                        true,
                        false,
                    ),
                    (
                        "auto",
                        "Auto",
                        "Approve requests unless an explicit rule denies them.",
                        false,
                        true,
                    ),
                ]
                .into_iter()
                .map(
                    |(id, label, description, is_default, dangerous)| HarnessControlOption {
                        id: id.into(),
                        label: label.into(),
                        description: description.into(),
                        is_default,
                        dangerous,
                    },
                )
                .collect(),
            },
            HarnessControlGroup {
                id: "agent".into(),
                label: "Agent".into(),
                kind: HarnessControlKind::Agent,
                options: [
                    (
                        "build",
                        "Build",
                        "Use OpenCode's primary implementation agent.",
                        true,
                    ),
                    (
                        "plan",
                        "Plan",
                        "Use OpenCode's read-only planning agent.",
                        false,
                    ),
                ]
                .into_iter()
                .map(
                    |(id, label, description, is_default)| HarnessControlOption {
                        id: id.into(),
                        label: label.into(),
                        description: description.into(),
                        is_default,
                        dangerous: false,
                    },
                )
                .collect(),
            },
        ]
    }

    fn catalog_probe(&self) -> Option<CatalogProbeSpec> {
        let mut command = CommandSpec::new(self.configured_executable());
        command.args.push("models".into());
        Some(CatalogProbeSpec {
            command,
            expected_response_ids: None,
        })
    }

    fn parse_catalog(&self, lines: &[String]) -> Result<HarnessModelCatalog> {
        let models = lines
            .iter()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && line.contains('/'))
            .map(|id| HarnessModel {
                id: id.into(),
                label: id.into(),
                description: None,
                context_window_tokens: None,
                is_default: false,
                reasoning_efforts: Vec::new(),
                service_tiers: Vec::new(),
            })
            .collect::<Vec<_>>();
        if models.is_empty() {
            return Err(RuntimeError::Protocol {
                provider: Provider::OpenCode,
                message: "OpenCode models returned no configured models".into(),
            });
        }
        Ok(HarnessModelCatalog {
            status: HarnessCatalogStatus::Ready,
            source: "models_command".into(),
            models,
            error: None,
        })
    }

    async fn readiness(&self) -> ProviderReadiness {
        inspect_executable(Provider::OpenCode, self.resolved()).await
    }

    fn prepare_turn(&self, request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
        if !self.serve_mode() {
            return Ok(());
        }
        // Bind a throwaway listener so the OS picks a free port, then drop it
        // so the server can bind the same one. `opencode serve --port 0` does
        // not do this: it falls back to its fixed default port instead.
        let port = std::net::TcpListener::bind(("127.0.0.1", 0))
            .and_then(|listener| listener.local_addr())
            .map(|address| address.port())
            .map_err(|error| RuntimeError::Protocol {
                provider: Provider::OpenCode,
                message: format!("could not reserve a loopback port for OpenCode: {error}"),
            })?;
        super::opencode_serve::prepare_turn(request, state, port);
        Ok(())
    }

    fn command_for_turn(&self, request: &TurnRequest, state: &AdapterState) -> Result<CommandSpec> {
        match super::opencode_serve::turn_port(state) {
            Some(port) if self.serve_mode() => self.serve_command(request, port),
            _ => self.command(request),
        }
    }

    async fn attach(
        &self,
        _request: &TurnRequest,
        state: &AdapterState,
    ) -> Result<Option<crate::ProtocolStreams>> {
        Ok(super::opencode_serve::turn_port(state)
            .filter(|_| self.serve_mode())
            .map(super::opencode_http::connect))
    }

    fn interrupt_request(&self, state: &AdapterState) -> Option<Vec<u8>> {
        self.serve_mode()
            .then(|| super::opencode_serve::interrupt(state))
            .flatten()
    }

    fn command(&self, request: &TurnRequest) -> Result<CommandSpec> {
        let mut spec = CommandSpec::new(self.configured_executable());
        spec.args.push("run".into());
        if let Some(session) = request.session_id.as_deref() {
            spec.args.extend(["--session".into(), session.into()]);
        }
        let native_permission = request
            .harness_options
            .get("permission_mode")
            .map(String::as_str);
        if native_permission == Some("auto")
            || (native_permission.is_none()
                && matches!(request.permission_mode, PermissionMode::FullAccess))
        {
            spec.args.push("--auto".into());
        } else if native_permission.is_some_and(|permission| permission != "default") {
            return Err(RuntimeError::InvalidRequest {
                field: "harness_options.permission_mode",
                message: format!(
                    "unsupported OpenCode permission mode `{}`",
                    native_permission.unwrap_or_default()
                ),
            });
        }
        let agent = request.harness_options.get("agent").map(String::as_str).or(
            match &request.permission_mode {
                PermissionMode::Plan => Some("plan"),
                PermissionMode::Custom(agent) => Some(agent.as_str()),
                _ => None,
            },
        );
        if let Some(agent) = agent {
            spec.args.extend(["--agent".into(), agent.into()]);
        }
        if matches!(request.permission_mode, PermissionMode::AcceptEdits)
            && native_permission.is_none()
        {
            return Err(RuntimeError::InvalidRequest {
                    field: "permission_mode",
                    message: "OpenCode has no CLI mode that approves only edits; use Configured rules, Plan, FullAccess, or a custom configured agent".into(),
                });
        }
        if let Some(reasoning) = request.reasoning.as_deref() {
            if reasoning != "default" {
                spec.args.extend(["--variant".into(), reasoning.into()]);
            }
        }
        if let Some(model) = request.model.as_deref() {
            spec.args.extend(["--model".into(), model.into()]);
        }
        spec.args.extend([
            "--format".into(),
            "json".into(),
            request.prompt.clone().into(),
        ]);
        Ok(spec)
    }

    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
        if self.serve_mode() {
            return super::opencode_serve::parse_line(line, state);
        }
        let value: Value = serde_json::from_str(line).map_err(|error| RuntimeError::Protocol {
            provider: Provider::OpenCode,
            message: format!("invalid JSON event: {error}"),
        })?;
        let mut output = AdapterOutput::default();
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let part = value.get("part").unwrap_or(&Value::Null);
        match event_type {
            "text" => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    state.result.text.push_str(text);
                    output.events.push(TurnEvent::TextDelta {
                        text: text.to_string(),
                    });
                }
            }
            "reasoning" => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    state
                        .result
                        .reasoning
                        .get_or_insert_with(String::new)
                        .push_str(text);
                    output.events.push(TurnEvent::ReasoningDelta {
                        text: text.to_string(),
                    });
                }
            }
            "tool" => {
                let status = match part
                    .get("state")
                    .and_then(|state| state.get("status"))
                    .and_then(Value::as_str)
                {
                    Some("completed") => ToolCallStatus::Succeeded,
                    Some("error") => ToolCallStatus::Failed,
                    _ => ToolCallStatus::Started,
                };
                output.events.push(TurnEvent::ToolCall {
                    id: part
                        .get("callID")
                        .or_else(|| part.get("id"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    name: part
                        .get("tool")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_string(),
                    status,
                    input: part.pointer("/state/input").cloned(),
                    output: part
                        .pointer("/state/output")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    error: part
                        .pointer("/state/error")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    task_id: None,
                });
            }
            "step_start" => {
                if state.result.session_title.is_none() {
                    state.result.session_title = part
                        .pointer("/session/title")
                        .or_else(|| part.get("sessionTitle"))
                        .or_else(|| part.get("title"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|title| !title.is_empty())
                        .map(str::to_owned);
                }
                if state.result.model.is_none() {
                    let provider = part.get("providerID").and_then(Value::as_str);
                    let model = part.get("modelID").and_then(Value::as_str);
                    state.result.model = match (provider, model) {
                        (Some(provider), Some(model)) => Some(format!("{provider}/{model}")),
                        (_, Some(model)) => Some(model.to_string()),
                        _ => None,
                    };
                }
                if state.result.session_id.is_none() {
                    if let Some(session_id) = part.get("sessionID").and_then(Value::as_str) {
                        state.result.session_id = Some(session_id.to_owned());
                        output.events.push(TurnEvent::SessionStarted {
                            session_id: session_id.to_owned(),
                            title: state.result.session_title.clone(),
                        });
                    }
                }
            }
            "step_finish" => {
                let input = part
                    .pointer("/tokens/input")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let output_tokens = part
                    .pointer("/tokens/output")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if input > 0 || output_tokens > 0 {
                    let total_input = state.result.usage.input_tokens.unwrap_or(0) + input;
                    let total_output =
                        state.result.usage.output_tokens.unwrap_or(0) + output_tokens;
                    state.result.usage.input_tokens = Some(total_input);
                    state.result.usage.output_tokens = Some(total_output);
                    output
                        .events
                        .push(TurnEvent::Usage(state.result.usage.clone()));
                }
                if part
                    .get("reason")
                    .and_then(Value::as_str)
                    .is_some_and(|reason| reason == "stop")
                {
                    output.terminal = true;
                }
            }
            "error" => {
                let diagnostic = value
                    .pointer("/error/message")
                    .or_else(|| value.pointer("/error/data/message"))
                    .or_else(|| value.get("error"))
                    .or_else(|| value.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("OpenCode reported an error")
                    .to_string();
                let provider_code = value
                    .pointer("/error/code")
                    .or_else(|| value.pointer("/error/type"))
                    .or_else(|| value.get("code"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let kind = classify_provider_failure(&format!(
                    "{} {diagnostic}",
                    provider_code.as_deref().unwrap_or_default()
                ));
                let mut failure =
                    ProviderTerminalFailure::new(kind, diagnostic, DeliveryState::Accepted);
                if let Some(code) = provider_code {
                    failure = failure.with_provider_code(format!("opencode::{code}"));
                }
                state.terminal_failure = Some(failure);
                state.result.status = crate::RunStatus::Failed;
                output.terminal = true;
            }
            _ => {}
        }
        Ok(output)
    }

    fn approval_response(
        &self,
        _request: &ApprovalRequest,
        original: &Value,
        decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>> {
        if !self.serve_mode() {
            return Ok(None);
        }
        Ok(Some(super::opencode_serve::approval_response(
            original, decision,
        )?))
    }
    fn question_response(
        &self,
        request: &QuestionRequest,
        original: &Value,
        answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(super::opencode_serve::question_response(
            request, original, answer,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_step_usage() {
        let adapter = OpenCode::default();
        let mut state = AdapterState::default();
        for line in [
            r#"{"type":"step_finish","part":{"tokens":{"input":4,"output":2}}}"#,
            r#"{"type":"step_finish","part":{"tokens":{"input":3,"output":1},"reason":"stop"}}"#,
        ] {
            adapter.parse_line(line, &mut state).unwrap();
        }
        assert_eq!(state.result.usage.input_tokens, Some(7));
        assert_eq!(state.result.usage.output_tokens, Some(3));
    }

    #[test]
    fn retains_nested_native_terminal_failure() {
        let adapter = OpenCode::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"error","error":{"code":"model_not_found","message":"unknown model"}}"#,
                &mut state,
            )
            .unwrap();

        assert!(output.terminal);
        assert!(output.events.is_empty());
        let failure = state.terminal_failure.unwrap();
        assert_eq!(
            failure.kind,
            crate::ProviderProcessErrorKind::ModelUnavailable
        );
        assert_eq!(failure.delivery, DeliveryState::Accepted);
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("opencode::model_not_found")
        );
        assert_eq!(failure.diagnostic, "unknown model");
    }

    #[test]
    fn preserves_nested_missing_session_diagnostic() {
        let adapter = OpenCode::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"error","error":{"code":"session_not_found","message":"Session not found: old-session"}}"#,
                &mut state,
            )
            .expect("OpenCode error parses");

        assert!(output.terminal);
        let failure = state.terminal_failure.expect("terminal failure");
        assert_eq!(failure.diagnostic, "Session not found: old-session");
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("opencode::session_not_found")
        );
    }

    #[test]
    fn maps_safe_and_automatic_permission_modes() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("opencode");
        std::fs::write(&executable, "stub").unwrap();
        let adapter = OpenCode::with_executable(executable);

        let mut plan = TurnRequest::new(Provider::OpenCode, temp.path(), "test");
        plan.harness_options.insert("agent".into(), "plan".into());
        let command = adapter.command(&plan).unwrap();
        assert!(command
            .args
            .windows(2)
            .any(|args| args == ["--agent", "plan"]));

        let mut full = TurnRequest::new(Provider::OpenCode, temp.path(), "test");
        full.permission_mode = PermissionMode::FullAccess;
        let command = adapter.command(&full).unwrap();
        assert!(command.args.iter().any(|arg| arg == "--auto"));
    }

    #[test]
    fn rejects_accept_edits_instead_of_approving_every_tool() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("opencode");
        std::fs::write(&executable, "stub").unwrap();
        let mut request = TurnRequest::new(Provider::OpenCode, temp.path(), "test");
        request.permission_mode = PermissionMode::AcceptEdits;

        assert!(matches!(
            OpenCode::with_executable(executable).command(&request),
            Err(RuntimeError::InvalidRequest {
                field: "permission_mode",
                ..
            })
        ));
    }

    #[test]
    fn parses_models_reported_by_the_cli() {
        let catalog = OpenCode::default()
            .parse_catalog(&["opencode/free-model".into(), "openai/gpt-test".into()])
            .unwrap();
        assert_eq!(catalog.status, HarnessCatalogStatus::Ready);
        assert_eq!(catalog.models.len(), 2);
        assert_eq!(catalog.models[1].id, "openai/gpt-test");
    }

    #[test]
    fn step_start_emits_session_started_once_and_preserves_resume() {
        let adapter = OpenCode::default();
        let mut state = AdapterState::default();
        let line = r#"{"type":"step_start","part":{"sessionID":"session-1","title":"My session"}}"#;
        let first = adapter.parse_line(line, &mut state).unwrap();
        assert_eq!(
            first.events,
            vec![TurnEvent::SessionStarted {
                session_id: "session-1".into(),
                title: Some("My session".into())
            }]
        );
        assert!(adapter
            .parse_line(line, &mut state)
            .unwrap()
            .events
            .is_empty());
        let mut resumed = AdapterState::default();
        resumed.result.session_id = Some("session-1".into());
        assert!(adapter
            .parse_line(line, &mut resumed)
            .unwrap()
            .events
            .is_empty());
    }
}
