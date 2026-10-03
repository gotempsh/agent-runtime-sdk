//! pi coding agent adapter driven through `pi --mode rpc`.
//!
//! pi's RPC mode is a long-lived JSONL protocol on stdio: commands go to
//! stdin, and responses and session events come back on stdout. One turn is
//! one process:
//!
//! 1. The runtime writes `get_state`. Its response names the session pi
//!    opened, which is reported as [`TurnEvent::SessionStarted`] and checked
//!    against a requested resume before anything reaches the model.
//! 2. The adapter then writes the `prompt` command.
//! 3. Session events stream until `agent_settled`, pi's signal that no
//!    retry, compaction recovery or queued follow-up will continue the run.
//!    `agent_end` is not terminal: pi can retry after it.
//! 4. The runtime closes stdin, which pi treats as an orderly shutdown.
//!
//! pi itself never asks before running a tool. Approvals and questions
//! reach the application only when a pi extension asks through its UI
//! context (`confirm`, `select`, `input`, `editor`), which RPC mode forwards
//! as `extension_ui_request`.

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::adapter::{inspect_executable, resolve_executable, AdapterState, OversizedFrame};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    AdapterOutput, AgentAdapter, ApprovalDecision, ApprovalRequest, AuthenticationProbeSpec,
    CatalogProbeSpec, CommandSpec, CompactionTrigger, ContextCompaction, ContextWindowUsage,
    HarnessAuthentication, HarnessCatalogError, HarnessCatalogErrorKind, HarnessCatalogStatus,
    HarnessModel, HarnessModelCatalog, HarnessReasoningEffort, InteractionRequest,
    LaunchContextCapabilities, PermissionMode, PermissionSupport, Provider,
    ProviderProcessErrorKind, ProviderReadiness, ProviderTerminalFailure, QuestionAnswer,
    QuestionRequest, Result, RunStatus, RuntimeError, ToolCallStatus, TransportExitStatus,
    TurnCapabilities, TurnEvent, TurnRequest,
};

const STATE_KEY: &str = "pi";
/// The prompt command waits here, outside the per-line state, until pi has
/// reported its session. Keeping it separate means the state that is decoded
/// on every line never carries a copy of the prompt.
const PROMPT_KEY: &str = "pi_prompt";
const ID_STATE: &str = "temps-agent-runtime:state";
const ID_PROMPT: &str = "temps-agent-runtime:prompt";
/// Numeric because the runtime's catalog probe correlates numeric ids.
const ID_CATALOG_MODELS: u64 = 1;
const ID_CATALOG_STATE: u64 = 2;
/// Every pi thinking level, in increasing order of effort.
const THINKING_LEVELS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];
/// pi's default thinking level for new sessions.
const DEFAULT_THINKING_LEVEL: &str = "medium";
/// Built-in tools that cannot change the workspace or run commands.
const READ_ONLY_TOOLS: [&str; 4] = ["read", "grep", "find", "ls"];
const MAX_TOOL_OUTPUT_CHARS: usize = 4096;
const MAX_INTERACTION_TEXT_CHARS: usize = 2000;
const MAX_WARNING_CHARS: usize = 500;
const MAX_SESSION_TITLE_CHARS: usize = 200;
/// Bound on tracked in-flight tool calls; pi runs a handful in parallel.
const MAX_RUNNING_TOOLS: usize = 64;

/// pi coding agent adapter using `pi --mode rpc`.
///
/// pi runs every enabled tool without asking, so this adapter supports only
/// [`PermissionMode::FullAccess`], which leaves pi's tool set as configured,
/// and [`PermissionMode::Plan`], which restricts pi to its read-only built-in
/// tools. Run it inside an outer sandbox when the workspace must be protected.
#[derive(Debug, Clone, Default)]
pub struct Pi {
    executable: Option<PathBuf>,
    trust_project_resources: bool,
}

impl Pi {
    /// Use an executable path meaningful inside the selected transport.
    pub fn with_executable(path: impl Into<PathBuf>) -> Self {
        Self {
            executable: Some(path.into()),
            ..Self::default()
        }
    }

    /// Load trust-gated project resources (`.pi/settings.json`,
    /// `.pi/extensions`, `.pi/mcp.json`, project skills and prompts).
    ///
    /// Off by default: every turn passes `--no-approve`, so a repository
    /// cannot load executable pi extensions merely by being the working
    /// directory. Enabling this passes `--approve` instead.
    pub fn trust_project_resources(mut self, trust: bool) -> Self {
        self.trust_project_resources = trust;
        self
    }

    fn resolved(&self) -> Option<PathBuf> {
        resolve_executable(self.executable.as_ref(), "pi")
    }

    fn configured_executable(&self) -> PathBuf {
        self.executable
            .clone()
            .unwrap_or_else(|| PathBuf::from("pi"))
    }

    /// Base invocation shared by turns and probes.
    fn rpc_command(&self) -> CommandSpec {
        let mut spec = CommandSpec::new(self.configured_executable());
        spec.args.extend(["--mode".into(), "rpc".into()]);
        spec.args.push(if self.trust_project_resources {
            "--approve".into()
        } else {
            "--no-approve".into()
        });
        // The latest-version request is a network call and a stderr notice
        // on every start; neither belongs in an embedded turn.
        spec.environment
            .insert("PI_SKIP_VERSION_CHECK".into(), "1".into());
        spec
    }
}

/// Per-turn protocol state retained between output lines.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct TurnState {
    /// Session the caller asked to resume.
    resume: Option<String>,
    /// The prompt command was written to pi.
    prompt_sent: bool,
    /// Context window of the model pi selected, for context-usage snapshots.
    context_limit: Option<u64>,
    /// The current assistant message streamed text or reasoning deltas, so
    /// its authoritative `message_end` copy must not be emitted again.
    streamed_text: bool,
    streamed_reasoning: bool,
    /// Stop reason and error of the latest assistant message. pi may retry
    /// a failed request, so only the last one decides the turn.
    stop_reason: Option<String>,
    error_message: Option<String>,
    /// Trigger of the compaction currently in progress.
    open_compaction: Option<CompactionTrigger>,
    /// Tool calls started and not yet finished, as `(id, name)`.
    running_tools: Vec<(String, String)>,
}

fn load(state: &AdapterState) -> TurnState {
    state
        .extensions
        .get(STATE_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

fn store(state: &mut AdapterState, turn: &TurnState) {
    if let Ok(value) = serde_json::to_value(turn) {
        state.extensions.insert(STATE_KEY.to_string(), value);
    }
}

fn protocol(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Protocol {
        provider: Provider::Pi,
        message: message.into(),
    }
}

fn encode(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| protocol(format!("could not encode a pi command: {error}")))
}

fn invalid(field: &'static str, message: impl Into<String>) -> RuntimeError {
    RuntimeError::InvalidRequest {
        field,
        message: message.into(),
    }
}

fn bounded(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let mut bounded: String = text.chars().take(limit).collect();
    bounded.push_str("… [truncated]");
    bounded
}

/// pi accepts letters, digits, `.`, `_` and `-`, starting and ending with a
/// letter or digit. Checking here turns pi's startup error into a typed one.
fn valid_session_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 256
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// A value that pi's argument parser could read as another option.
fn option_like(value: &str) -> bool {
    value.starts_with('-')
}

/// Tools pi exposes for this turn, or `None` to keep its configured set.
fn tool_selection(request: &TurnRequest) -> Option<Vec<String>> {
    let allowed = request.launch_context.allowed_tools.as_ref();
    match &request.permission_mode {
        PermissionMode::Plan => Some(
            READ_ONLY_TOOLS
                .iter()
                .filter(|tool| {
                    allowed.is_none_or(|allowed| allowed.iter().any(|name| name == *tool))
                })
                .map(|tool| (*tool).to_string())
                .collect(),
        ),
        _ => allowed.cloned(),
    }
}

/// Text of every block of `kind` in a pi message's content array.
fn content_text(content: Option<&Value>, kind: &str, field: &str) -> String {
    match content {
        Some(Value::String(text)) if kind == "text" => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some(kind))
            .filter_map(|block| block.get(field).and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn model_id(provider: Option<&str>, model: Option<&str>) -> Option<String> {
    match (provider, model) {
        (Some(provider), Some(model)) if provider != "unknown" && model != "unknown" => {
            Some(format!("{provider}/{model}"))
        }
        _ => None,
    }
}

/// Thinking levels pi accepts for `model`, mirroring its own rule: a level
/// mapped to `null` is unavailable, and `xhigh` and `max` exist only when the
/// model maps them explicitly. Models without reasoning support only `off`.
fn supported_thinking_levels(model: &Value) -> Vec<&'static str> {
    if !model
        .get("reasoning")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return vec!["off"];
    }
    let map = model.get("thinkingLevelMap");
    THINKING_LEVELS
        .into_iter()
        .filter(|level| match map.and_then(|map| map.get(*level)) {
            Some(Value::Null) => false,
            Some(_) => true,
            None => !matches!(*level, "xhigh" | "max"),
        })
        .collect()
}

/// pi's own clamping: the requested level, else the next higher supported
/// one, else the highest supported one.
fn clamp_thinking_level<'a>(levels: &[&'a str], requested: &str) -> Option<&'a str> {
    if let Some(level) = levels.iter().find(|level| **level == requested) {
        return Some(level);
    }
    let start = THINKING_LEVELS
        .iter()
        .position(|level| *level == requested)?;
    THINKING_LEVELS[start..]
        .iter()
        .find_map(|candidate| levels.iter().find(|level| *level == candidate).copied())
        .or_else(|| levels.last().copied())
}

fn thinking_label(level: &str) -> &'static str {
    match level {
        "off" => "Off",
        "minimal" => "Minimal",
        "low" => "Low",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "Extra high",
        "max" => "Max",
        _ => "Custom",
    }
}

fn response_data(lines: &[Value], id: u64) -> Option<&Value> {
    lines
        .iter()
        .find(|line| {
            line.get("type").and_then(Value::as_str) == Some("response")
                && line.get("id").and_then(Value::as_u64) == Some(id)
                && line.get("success").and_then(Value::as_bool) == Some(true)
        })
        .and_then(|line| line.get("data"))
}

fn compaction_trigger(reason: Option<&str>) -> CompactionTrigger {
    match reason {
        Some("manual") => CompactionTrigger::Manual,
        Some("threshold" | "overflow") => CompactionTrigger::Automatic,
        _ => CompactionTrigger::Unknown,
    }
}

/// Classify a pi diagnostic, recognizing pi's own wording first.
fn failure_kind(diagnostic: &str) -> ProviderProcessErrorKind {
    let lowered = diagnostic.to_ascii_lowercase();
    if lowered.contains("no api key found") {
        ProviderProcessErrorKind::AuthenticationFailed
    } else {
        classify_provider_failure(diagnostic)
    }
}

fn fail(
    state: &mut AdapterState,
    output: &mut AdapterOutput,
    diagnostic: impl Into<String>,
    code: Option<&str>,
    delivery: DeliveryState,
) {
    let diagnostic = diagnostic.into();
    let mut failure = ProviderTerminalFailure::new(failure_kind(&diagnostic), diagnostic, delivery);
    if let Some(code) = code {
        failure = failure.with_provider_code(format!("pi::{code}"));
    }
    state.terminal_failure = Some(failure);
    state.result.status = RunStatus::Failed;
    output.terminal = true;
}

fn tool_result_text(result: Option<&Value>) -> Option<String> {
    let result = result?;
    let text = match result.get("content") {
        Some(content) => content_text(Some(content), "text", "text"),
        None => result.as_str().map(str::to_owned).unwrap_or_default(),
    };
    (!text.is_empty()).then(|| bounded(&text, MAX_TOOL_OUTPUT_CHARS))
}

fn interaction_text(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(|text| bounded(text, MAX_INTERACTION_TEXT_CHARS))
}

/// Translate one extension UI request into an application interaction, or a
/// display-only event for the fire-and-forget methods.
fn extension_ui_request(value: &Value, output: &mut AdapterOutput) -> Result<()> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| protocol("pi sent an extension UI request without an id"))?;
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let title = interaction_text(value, "title");
    match method {
        "confirm" => {
            let message = interaction_text(value, "message");
            let description = match (&title, &message) {
                (Some(title), Some(message)) => Some(format!("{title}\n\n{message}")),
                (Some(text), None) | (None, Some(text)) => Some(text.clone()),
                (None, None) => None,
            };
            let request = ApprovalRequest {
                id: id.to_string(),
                tool_name: "pi_extension_confirm".into(),
                input: json!({ "title": title, "message": message }),
                description,
            };
            output
                .events
                .push(TurnEvent::ApprovalRequested(request.clone()));
            output.interaction = Some(InteractionRequest::Approval {
                request,
                original: value.clone(),
            });
        }
        "select" | "input" | "editor" => {
            let question = title.unwrap_or_else(|| "pi needs your input".into());
            let options = if method == "select" {
                value
                    .get("options")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(|label| json!({ "label": bounded(label, MAX_INTERACTION_TEXT_CHARS), "description": "" }))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let header = bounded(&question, 80);
            let request = QuestionRequest::new(
                id,
                json!({ "questions": [{
                    "header": header,
                    "question": question,
                    "options": options,
                    "multiSelect": false,
                }]}),
            );
            output
                .events
                .push(TurnEvent::QuestionRequested(request.clone()));
            output.interaction = Some(InteractionRequest::Question {
                request,
                original: value.clone(),
            });
        }
        "notify" => {
            let severity = value.get("notifyType").and_then(Value::as_str);
            if matches!(severity, Some("warning" | "error")) {
                if let Some(message) = value.get("message").and_then(Value::as_str) {
                    output.events.push(TurnEvent::Warning {
                        message: bounded(message.trim(), MAX_WARNING_CHARS),
                    });
                }
            }
        }
        // Status lines, widgets, titles and editor text drive pi's terminal
        // UI and need no reply.
        _ => {}
    }
    Ok(())
}

#[async_trait]
impl AgentAdapter for Pi {
    fn provider(&self) -> Provider {
        Provider::Pi
    }

    fn executable(&self) -> PathBuf {
        self.configured_executable()
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            // pi has no mode that asks before consequential operations.
            default: false,
            accept_edits: false,
            // Enforced by restricting pi to its read-only built-in tools.
            plan: true,
            full_access: true,
            custom: false,
            // Both arrive only from pi extensions that ask through their UI
            // context; pi's own tools never do.
            live_approvals: true,
            live_questions: true,
        }
    }

    fn launch_context_capabilities(&self) -> LaunchContextCapabilities {
        LaunchContextCapabilities {
            system_prompt_append: true,
            allowed_tools: true,
            // pi reads MCP servers only from files and extensions; it has no
            // turn-scoped MCP argument.
            stdio_mcp: false,
            http_mcp: false,
            // Enforced with `--no-extensions`, which also disables every
            // other pi extension because pi has no MCP-only switch.
            strict_mcp_config: true,
        }
    }

    fn turn_capabilities(&self) -> TurnCapabilities {
        TurnCapabilities {
            context_window_usage: true,
            compaction_lifecycle: true,
            ..TurnCapabilities::default()
        }
    }

    fn catalog_probe(&self) -> Option<CatalogProbeSpec> {
        let mut command = self.rpc_command();
        command.args.push("--no-session".into());
        command.initial_stdin = Some(
            format!(
                "{}\n{}",
                json!({"id": ID_CATALOG_MODELS, "type": "get_available_models"}),
                json!({"id": ID_CATALOG_STATE, "type": "get_state"}),
            )
            .into_bytes(),
        );
        command.interactive_stdin = true;
        Some(CatalogProbeSpec {
            command,
            expected_response_ids: Some(vec![ID_CATALOG_MODELS, ID_CATALOG_STATE]),
        })
    }

    fn parse_catalog(&self, lines: &[String]) -> Result<HarnessModelCatalog> {
        let frames = lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect::<Vec<_>>();
        let models = response_data(&frames, ID_CATALOG_MODELS)
            .and_then(|data| data.get("models"))
            .and_then(Value::as_array)
            .ok_or_else(|| protocol("pi did not answer get_available_models"))?;
        let state = response_data(&frames, ID_CATALOG_STATE);
        let current = state
            .and_then(|state| state.get("model"))
            .and_then(|model| {
                model_id(
                    model.get("provider").and_then(Value::as_str),
                    model.get("id").and_then(Value::as_str),
                )
            });
        let current_level = state
            .and_then(|state| state.get("thinkingLevel"))
            .and_then(Value::as_str);
        let models = models
            .iter()
            .filter_map(|model| {
                let id = model_id(
                    model.get("provider").and_then(Value::as_str),
                    model.get("id").and_then(Value::as_str),
                )?;
                let is_default = current.as_deref() == Some(id.as_str());
                let levels = supported_thinking_levels(model);
                let default_level = clamp_thinking_level(
                    &levels,
                    current_level
                        .filter(|_| is_default)
                        .unwrap_or(DEFAULT_THINKING_LEVEL),
                );
                let reasoning_efforts = if levels.len() > 1 {
                    levels
                        .iter()
                        .map(|level| HarnessReasoningEffort {
                            id: (*level).to_string(),
                            label: thinking_label(level).to_string(),
                            description: None,
                            is_default: Some(*level) == default_level,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                Some(HarnessModel {
                    label: model
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or(&id)
                        .to_string(),
                    description: model
                        .get("provider")
                        .and_then(Value::as_str)
                        .map(|provider| format!("Provider: {provider}")),
                    context_window_tokens: model
                        .get("contextWindow")
                        .and_then(Value::as_u64)
                        .filter(|tokens| *tokens > 0),
                    is_default,
                    reasoning_efforts,
                    service_tiers: Vec::new(),
                    id,
                })
            })
            .collect::<Vec<_>>();
        if models.is_empty() {
            return Ok(HarnessModelCatalog {
                status: HarnessCatalogStatus::Failed,
                source: "rpc".into(),
                models,
                error: Some(HarnessCatalogError {
                    kind: HarnessCatalogErrorKind::Authentication,
                    message: "pi has no model with usable credentials; sign in with `/login` in pi or set a provider API key".into(),
                    retryable: false,
                }),
            });
        }
        Ok(HarnessModelCatalog {
            status: HarnessCatalogStatus::Ready,
            source: "rpc".into(),
            models,
            error: None,
        })
    }

    fn authentication_probe(&self) -> Option<AuthenticationProbeSpec> {
        // `--list-models` lists exactly the models whose provider has a
        // usable credential: a stored login, an environment key, or a
        // `models.json` key. It does not contact the provider.
        let mut command = CommandSpec::new(self.configured_executable());
        command.args.push("--list-models".into());
        command
            .environment
            .insert("PI_SKIP_VERSION_CHECK".into(), "1".into());
        Some(AuthenticationProbeSpec { command })
    }

    fn parse_authentication_probe(
        &self,
        stdout: &[u8],
        _stderr: &str,
        status: TransportExitStatus,
    ) -> Result<HarnessAuthentication> {
        let text = String::from_utf8_lossy(stdout);
        if !status.success {
            return Ok(HarnessAuthentication::unknown("list_models"));
        }
        if text.contains("No models available") {
            return Ok(HarnessAuthentication::required(
                "list_models",
                "pi has no provider credentials; sign in with `/login` in pi or set a provider API key such as ANTHROPIC_API_KEY",
            ));
        }
        let mut rows = text.lines().map(str::trim).filter(|line| !line.is_empty());
        let header = rows.next().unwrap_or_default();
        if header.starts_with("provider") && rows.next().is_some() {
            return Ok(HarnessAuthentication::authenticated("list_models"));
        }
        Ok(HarnessAuthentication::unknown("list_models"))
    }

    async fn readiness(&self) -> ProviderReadiness {
        inspect_executable(Provider::Pi, self.resolved()).await
    }

    fn command(&self, request: &TurnRequest) -> Result<CommandSpec> {
        match &request.permission_mode {
            PermissionMode::FullAccess | PermissionMode::Plan => {}
            mode => {
                return Err(invalid(
                    "permission_mode",
                    format!("pi runs tools without asking and supports only FullAccess or Plan, not {mode:?}"),
                ))
            }
        }
        let mut spec = self.rpc_command();
        if let Some(session) = request.session_id.as_deref() {
            if !valid_session_id(session) {
                return Err(invalid(
                    "session_id",
                    "pi session ids contain only letters, digits, `.`, `_` and `-`, and start and end with a letter or digit",
                ));
            }
            // Exact and local to the working directory. `--session` would
            // also match prefixes and offer, on stdout, to fork a session
            // from another project.
            spec.args.extend(["--session-id".into(), session.into()]);
        }
        if let Some(model) = request.model.as_deref().map(str::trim) {
            if model.is_empty() || option_like(model) {
                return Err(invalid(
                    "model",
                    "pi model ids must be non-empty `provider/id` values",
                ));
            }
            if model != "default" {
                spec.args.extend(["--model".into(), model.into()]);
            }
        }
        if let Some(reasoning) = request.reasoning.as_deref() {
            if reasoning != "default" {
                if !THINKING_LEVELS.contains(&reasoning) {
                    return Err(invalid(
                        "reasoning",
                        format!(
                            "pi thinking levels are {}, not `{reasoning}`",
                            THINKING_LEVELS.join(", ")
                        ),
                    ));
                }
                spec.args.extend(["--thinking".into(), reasoning.into()]);
            }
        }
        if let Some(append) = request.launch_context.system_prompt_append.as_deref() {
            if !append.trim().is_empty() {
                spec.args
                    .extend(["--append-system-prompt".into(), append.into()]);
            }
        }
        match tool_selection(request) {
            Some(tools) if tools.is_empty() => spec.args.push("--no-tools".into()),
            Some(tools) => {
                if let Some(tool) = tools
                    .iter()
                    .find(|tool| tool.is_empty() || option_like(tool) || tool.contains(','))
                {
                    return Err(invalid(
                        "launch_context.allowed_tools",
                        format!("`{tool}` is not a valid pi tool name"),
                    ));
                }
                spec.args.extend(["--tools".into(), tools.join(",").into()]);
            }
            None => {}
        }
        if request.launch_context.strict_mcp_config {
            spec.args.push("--no-extensions".into());
        }
        spec.initial_stdin = Some(encode(&json!({"id": ID_STATE, "type": "get_state"}))?);
        spec.interactive_stdin = true;
        Ok(spec)
    }

    fn prepare_turn(&self, request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
        let turn = TurnState {
            resume: request.session_id.clone(),
            ..TurnState::default()
        };
        store(state, &turn);
        state.extensions.insert(
            PROMPT_KEY.to_string(),
            json!({"id": ID_PROMPT, "type": "prompt", "message": request.prompt}),
        );
        Ok(())
    }

    fn interrupt_request(&self, _state: &AdapterState) -> Option<Vec<u8>> {
        encode(&json!({"type": "abort"})).ok()
    }

    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| protocol(format!("invalid JSON frame: {error}")))?;
        let mut output = AdapterOutput::default();
        let mut turn = load(state);
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "response" => response(&value, state, &mut turn, &mut output)?,
            "message_start" => {
                if value.pointer("/message/role").and_then(Value::as_str) == Some("assistant") {
                    turn.streamed_text = false;
                    turn.streamed_reasoning = false;
                }
            }
            "message_update" => {
                let event = value.get("assistantMessageEvent").unwrap_or(&Value::Null);
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match event.get("type").and_then(Value::as_str) {
                    Some("text_delta") if !delta.is_empty() => {
                        turn.streamed_text = true;
                        state.saw_text_delta = true;
                        state.result.text.push_str(delta);
                        output.events.push(TurnEvent::TextDelta {
                            text: delta.to_string(),
                        });
                    }
                    Some("thinking_delta") if !delta.is_empty() => {
                        turn.streamed_reasoning = true;
                        state
                            .result
                            .reasoning
                            .get_or_insert_with(String::new)
                            .push_str(delta);
                        output.events.push(TurnEvent::ReasoningDelta {
                            text: delta.to_string(),
                        });
                    }
                    _ => {}
                }
            }
            "message_end" => {
                let message = value.get("message").unwrap_or(&Value::Null);
                if message.get("role").and_then(Value::as_str) == Some("assistant") {
                    assistant_message(message, state, &mut turn, &mut output);
                }
            }
            "tool_execution_start" => {
                let id = value
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let name = value
                    .get("toolName")
                    .and_then(Value::as_str)
                    .unwrap_or("tool")
                    .to_string();
                if let Some(id) = &id {
                    if turn.running_tools.len() < MAX_RUNNING_TOOLS {
                        turn.running_tools.push((id.clone(), name.clone()));
                    }
                }
                output.events.push(TurnEvent::ToolCall {
                    id,
                    name,
                    status: ToolCallStatus::Started,
                    input: value.get("args").cloned(),
                    output: None,
                    error: None,
                    task_id: None,
                });
            }
            "tool_execution_end" => {
                if let Some(id) = value.get("toolCallId").and_then(Value::as_str) {
                    turn.running_tools.retain(|(running, _)| running != id);
                }
                let failed = value
                    .get("isError")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let text = tool_result_text(value.get("result"));
                output.events.push(TurnEvent::ToolCall {
                    id: value
                        .get("toolCallId")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    name: value
                        .get("toolName")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_string(),
                    status: if failed {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Succeeded
                    },
                    input: None,
                    output: if failed { None } else { text.clone() },
                    error: if failed {
                        Some(text.unwrap_or_else(|| "the tool reported an error".into()))
                    } else {
                        None
                    },
                    task_id: None,
                });
            }
            "compaction_start" => {
                let trigger = compaction_trigger(value.get("reason").and_then(Value::as_str));
                turn.open_compaction = Some(trigger);
                output.events.push(TurnEvent::CompactionStarted { trigger });
            }
            "compaction_end" => {
                let trigger = turn.open_compaction.take().unwrap_or_else(|| {
                    compaction_trigger(value.get("reason").and_then(Value::as_str))
                });
                let result = value.get("result").filter(|result| result.is_object());
                let aborted = value
                    .get("aborted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                match result {
                    Some(result) if !aborted => {
                        let pre_tokens = result.get("tokensBefore").and_then(Value::as_u64);
                        let post_tokens =
                            result.get("estimatedTokensAfter").and_then(Value::as_u64);
                        output.events.push(TurnEvent::CompactionCompleted {
                            compaction: ContextCompaction {
                                trigger,
                                pre_tokens,
                                post_tokens,
                                dropped_tokens: pre_tokens
                                    .zip(post_tokens)
                                    .map(|(pre, post)| pre.saturating_sub(post)),
                                cumulative_dropped_tokens: None,
                                duration_ms: None,
                            },
                        });
                    }
                    _ => output.events.push(TurnEvent::CompactionFailed {
                        trigger,
                        message: Some(if aborted {
                            "pi aborted the compaction".to_string()
                        } else {
                            "pi could not compact the conversation".to_string()
                        }),
                    }),
                }
            }
            "auto_retry_start" => {
                // The provider's error text can echo request details, so the
                // warning reports only the attempt.
                let attempt = value.get("attempt").and_then(Value::as_u64).unwrap_or(1);
                let maximum = value.get("maxAttempts").and_then(Value::as_u64);
                output.events.push(TurnEvent::Warning {
                    message: match maximum {
                        Some(maximum) => format!(
                            "pi is retrying the model request (attempt {attempt} of {maximum})"
                        ),
                        None => format!("pi is retrying the model request (attempt {attempt})"),
                    },
                });
            }
            "extension_error" => {
                let event = value
                    .get("event")
                    .and_then(Value::as_str)
                    .map_or_else(|| "an event".into(), |event| bounded(event, 64));
                output.events.push(TurnEvent::Warning {
                    message: format!("a pi extension failed while handling {event}"),
                });
            }
            "extension_ui_request" => extension_ui_request(&value, &mut output)?,
            "session_info_changed" => {
                state.result.session_title = value
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(|name| bounded(name, MAX_SESSION_TITLE_CHARS));
            }
            "agent_settled" if turn.prompt_sent => {
                output.terminal = true;
                match turn.stop_reason.as_deref() {
                    Some("error") => {
                        let diagnostic = turn
                            .error_message
                            .clone()
                            .unwrap_or_else(|| "pi reported a model error".into());
                        fail(
                            state,
                            &mut output,
                            diagnostic,
                            None,
                            DeliveryState::Accepted,
                        );
                    }
                    Some("aborted") => fail(
                        state,
                        &mut output,
                        "pi aborted the turn before it completed",
                        Some("aborted"),
                        DeliveryState::Accepted,
                    ),
                    _ => {}
                }
            }
            // `agent_end` can be followed by a retry, `turn_*`, queue and
            // entry events carry nothing a turn result needs, and records of
            // other roles repeat what tool events already reported.
            _ => {}
        }
        store(state, &turn);
        Ok(output)
    }

    fn accepts_oversized_frame(&self, prefix: &str) -> bool {
        oversized_frame_kind(prefix).is_some()
    }

    fn parse_oversized_frame(
        &self,
        frame: OversizedFrame<'_>,
        state: &mut AdapterState,
    ) -> Result<AdapterOutput> {
        match oversized_frame_kind(frame.prefix) {
            Some(OversizedKind::Redundant) => Ok(AdapterOutput::default()),
            Some(OversizedKind::ToolEnd) => Ok(oversized_tool_end(frame, state)),
            None => Err(protocol(
                "pi wrote an event larger than the event limit that cannot be skipped",
            )),
        }
    }

    fn approval_response(
        &self,
        request: &ApprovalRequest,
        _original: &Value,
        decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>> {
        let confirmed = matches!(
            decision,
            ApprovalDecision::Allow | ApprovalDecision::AllowForSession
        );
        Ok(Some(encode(&json!({
            "type": "extension_ui_response",
            "id": request.id,
            "confirmed": confirmed,
        }))?))
    }

    fn question_response(
        &self,
        request: &QuestionRequest,
        _original: &Value,
        answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>> {
        let value = answer.and_then(|answer| match answer.answers {
            Value::String(value) => Some(value),
            Value::Object(answers) => answers.into_values().find_map(|value| match value {
                Value::String(value) => Some(value),
                Value::Array(values) => values
                    .into_iter()
                    .find_map(|value| value.as_str().map(str::to_owned)),
                _ => None,
            }),
            _ => None,
        });
        let response = match value {
            Some(value) => {
                json!({"type": "extension_ui_response", "id": request.id, "value": value})
            }
            None => json!({"type": "extension_ui_response", "id": request.id, "cancelled": true}),
        };
        Ok(Some(encode(&response)?))
    }
}

/// An event pi wrote that is larger than the event limit and can be read
/// without.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OversizedKind {
    /// It repeats what the turn already received in smaller events.
    Redundant,
    /// A tool finished with a result too large to relay.
    ToolEnd,
}

fn oversized_frame_kind(prefix: &str) -> Option<OversizedKind> {
    // pi writes `type` first. These frames repeat what the turn already
    // received in smaller ones: `agent_end` and `turn_end` echo every message
    // of the run, non-assistant `message_*` records echo user input and tool
    // results, and tool progress is superseded by `tool_execution_end`.
    const REDUNDANT: [&str; 4] = [
        r#"{"type":"agent_end""#,
        r#"{"type":"turn_end""#,
        r#"{"type":"message_start""#,
        r#"{"type":"tool_execution_update""#,
    ];
    if REDUNDANT.iter().any(|frame| prefix.starts_with(frame))
        || (prefix.starts_with(r#"{"type":"message_end","message":{"role":""#)
            && !prefix.starts_with(r#"{"type":"message_end","message":{"role":"assistant""#))
    {
        return Some(OversizedKind::Redundant);
    }
    prefix
        .starts_with(r#"{"type":"tool_execution_end""#)
        .then_some(OversizedKind::ToolEnd)
}

/// Report a tool whose result (an image read, say) is too large to relay,
/// without that result.
fn oversized_tool_end(frame: OversizedFrame<'_>, state: &mut AdapterState) -> AdapterOutput {
    // pi writes the call id before the result, and the only call in flight
    // is the fallback.
    let field = |name: &str| {
        let start = frame.prefix.find(&format!(r#""{name}":""#))? + name.len() + 4;
        let end = frame.prefix[start..].find('"')? + start;
        Some(frame.prefix[start..end].to_string())
    };
    let mut turn = load(state);
    let running = match field("toolCallId") {
        Some(id) => turn
            .running_tools
            .iter()
            .position(|(running, _)| *running == id),
        None if turn.running_tools.len() == 1 => Some(0),
        None => None,
    }
    .map(|index| turn.running_tools.remove(index));
    store(state, &turn);
    let id = field("toolCallId").or_else(|| running.as_ref().map(|(id, _)| id.clone()));
    let name = field("toolName")
        .or_else(|| running.map(|(_, name)| name))
        .unwrap_or_else(|| "tool".into());
    let (status, output, error) = match oversized_tool_failed(frame.suffix) {
        Some(false) => (
            ToolCallStatus::Succeeded,
            Some("[pi tool output omitted: larger than the event limit]".to_string()),
            None,
        ),
        Some(true) => (
            ToolCallStatus::Failed,
            None,
            Some("[pi tool error omitted: larger than the event limit]".to_string()),
        ),
        // Never report success that pi did not confirm.
        None => (
            ToolCallStatus::Failed,
            None,
            Some(
                "pi reported a tool result larger than the event limit, and whether the tool \
                 succeeded could not be read"
                    .to_string(),
            ),
        ),
    };
    AdapterOutput {
        events: vec![TurnEvent::ToolCall {
            id,
            name,
            status,
            input: None,
            output,
            error,
            task_id: None,
        }],
        ..AdapterOutput::default()
    }
}

/// Whether an oversized `tool_execution_end` reports a failure. pi writes
/// `isError` last, after the result, so it survives in the frame's tail; a
/// tail that does not end with it is unknown.
fn oversized_tool_failed(suffix: &str) -> Option<bool> {
    let suffix = suffix.trim_end();
    if suffix.ends_with(r#","isError":true}"#) {
        Some(true)
    } else if suffix.ends_with(r#","isError":false}"#) {
        Some(false)
    } else {
        None
    }
}

/// Handle a response to one of the adapter's own commands.
fn response(
    value: &Value,
    state: &mut AdapterState,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) -> Result<()> {
    let success = value.get("success").and_then(Value::as_bool) == Some(true);
    let error = value
        .get("error")
        .and_then(Value::as_str)
        .map_or("pi rejected the command", str::trim)
        .to_string();
    match value.get("id").and_then(Value::as_str) {
        Some(ID_STATE) => {
            if !success {
                fail(
                    state,
                    output,
                    error,
                    Some("get_state"),
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            let data = value.get("data").unwrap_or(&Value::Null);
            // pi writes a session file only once it holds a message, so a
            // resumed session that reports none did not exist: `--session-id`
            // created an empty one. Fail rather than silently answer
            // without the conversation the caller meant to continue, and fail
            // too when pi does not say, since the session cannot be
            // confirmed.
            if let Some(resume) = turn.resume.as_deref() {
                match data.get("messageCount").and_then(Value::as_u64) {
                    Some(count) if count > 0 => {}
                    Some(_) => {
                        fail(
                            state,
                            output,
                            format!(
                                "pi session `{resume}` was not found for this working directory"
                            ),
                            Some("session_not_found"),
                            DeliveryState::NotSent,
                        );
                        return Ok(());
                    }
                    None => {
                        fail(
                            state,
                            output,
                            format!(
                                "pi did not report how many messages session `{resume}` holds, \
                                 so resuming it could not be confirmed"
                            ),
                            Some("session_unconfirmed"),
                            DeliveryState::NotSent,
                        );
                        return Ok(());
                    }
                }
            }
            let model = data.get("model");
            state.result.model = model_id(
                model
                    .and_then(|model| model.get("provider"))
                    .and_then(Value::as_str),
                model
                    .and_then(|model| model.get("id"))
                    .and_then(Value::as_str),
            );
            turn.context_limit = model
                .and_then(|model| model.get("contextWindow"))
                .and_then(Value::as_u64)
                .filter(|limit| *limit > 0);
            if let Some(name) = data
                .get("sessionName")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty())
            {
                state.result.session_title = Some(bounded(name, MAX_SESSION_TITLE_CHARS));
            }
            if let Some(session_id) = data.get("sessionId").and_then(Value::as_str) {
                if state.result.session_id.as_deref() != Some(session_id) {
                    state.result.session_id = Some(session_id.to_owned());
                    output.events.push(TurnEvent::SessionStarted {
                        session_id: session_id.to_owned(),
                        title: state.result.session_title.clone(),
                    });
                }
            }
            let prompt = state
                .extensions
                .remove(PROMPT_KEY)
                .ok_or_else(|| protocol("pi reported its session twice"))?;
            output.writes.push(encode(&prompt)?);
            turn.prompt_sent = true;
        }
        Some(ID_PROMPT) => {
            if !success {
                fail(
                    state,
                    output,
                    error,
                    Some("prompt_rejected"),
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            match value.pointer("/data/disposition").and_then(Value::as_str) {
                // An extension command consumed the prompt; no run starts,
                // so no `agent_settled` will follow.
                Some("handled") => output.terminal = true,
                _ => output.turn_submitted = true,
            }
        }
        _ => {}
    }
    Ok(())
}

/// Fold one completed assistant message into the turn result.
fn assistant_message(
    message: &Value,
    state: &mut AdapterState,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) {
    let content = message.get("content");
    // A provider that does not stream still delivers the message here.
    if !turn.streamed_reasoning {
        let reasoning = content_text(content, "thinking", "thinking");
        if !reasoning.is_empty() {
            state
                .result
                .reasoning
                .get_or_insert_with(String::new)
                .push_str(&reasoning);
            output
                .events
                .push(TurnEvent::ReasoningDelta { text: reasoning });
        }
    }
    if !turn.streamed_text {
        let text = content_text(content, "text", "text");
        if !text.is_empty() {
            state.result.text.push_str(&text);
            output.events.push(TurnEvent::TextDelta { text });
        }
    }
    if let Some(model) = model_id(
        message.get("provider").and_then(Value::as_str),
        message.get("model").and_then(Value::as_str),
    ) {
        state.result.model = Some(model);
    }
    turn.stop_reason = message
        .get("stopReason")
        .and_then(Value::as_str)
        .map(str::to_owned);
    turn.error_message = message
        .get("errorMessage")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let Some(usage) = message.get("usage").filter(|usage| usage.is_object()) else {
        return;
    };
    let count = |field: &str| usage.get(field).and_then(Value::as_u64).unwrap_or(0);
    let total = &mut state.result.usage;
    let add = |slot: &mut Option<u64>, value: u64| {
        if value > 0 || slot.is_some() {
            *slot = Some(slot.unwrap_or(0) + value);
        }
    };
    add(&mut total.input_tokens, count("input"));
    add(&mut total.output_tokens, count("output"));
    add(&mut total.cache_read_input_tokens, count("cacheRead"));
    add(&mut total.cache_creation_input_tokens, count("cacheWrite"));
    if let Some(cost) = usage.pointer("/cost/total").and_then(Value::as_f64) {
        if cost > 0.0 || total.cost_usd.is_some() {
            total.cost_usd = Some(total.cost_usd.unwrap_or(0.0) + cost);
        }
    }
    // `totalTokens` is the context this response leaves behind, which is
    // what pi itself reports as context usage.
    let occupied = count("totalTokens");
    if occupied > 0 {
        total.context_window = Some(ContextWindowUsage {
            used_tokens: Some(occupied),
            limit_tokens: turn.context_limit,
            model: state.result.model.clone(),
            estimated: false,
        });
    }
    if occupied > 0 || count("input") > 0 || count("output") > 0 {
        output
            .events
            .push(TurnEvent::Usage(state.result.usage.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(spec: &CommandSpec) -> Vec<String> {
        spec.args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect()
    }

    fn full_access(prompt: &str) -> TurnRequest {
        let mut request = TurnRequest::new(Provider::Pi, ".", prompt);
        request.permission_mode = PermissionMode::FullAccess;
        request
    }

    /// Run `request` through prepare, command and the `get_state` exchange,
    /// returning the state ready for session events.
    fn started(
        adapter: &Pi,
        request: &TurnRequest,
        session: &str,
    ) -> (AdapterState, AdapterOutput) {
        let mut state = AdapterState::default();
        state.result.session_id.clone_from(&request.session_id);
        adapter.prepare_turn(request, &mut state).unwrap();
        let output = adapter
            .parse_line(
                &json!({"id": ID_STATE, "type": "response", "command": "get_state", "success": true,
                    "data": {"sessionId": session, "messageCount": 3,
                        "model": {"provider": "anthropic", "id": "claude-sonnet-4-5", "contextWindow": 200_000}}})
                .to_string(),
                &mut state,
            )
            .unwrap();
        (state, output)
    }

    fn feed(adapter: &Pi, state: &mut AdapterState, frames: &[Value]) -> Vec<AdapterOutput> {
        frames
            .iter()
            .map(|frame| adapter.parse_line(&frame.to_string(), state).unwrap())
            .collect()
    }

    #[test]
    fn launches_rpc_mode_without_trusting_the_project() {
        let spec = Pi::default().command(&full_access("hi")).unwrap();
        assert_eq!(arguments(&spec), ["--mode", "rpc", "--no-approve"]);
        assert!(spec.interactive_stdin);
        assert!(spec.clear_environment);
        assert_eq!(
            spec.environment
                .get(std::ffi::OsStr::new("PI_SKIP_VERSION_CHECK")),
            Some(&"1".into())
        );
        let first: Value = serde_json::from_slice(spec.initial_stdin.as_ref().unwrap()).unwrap();
        assert_eq!(first["type"], "get_state");
        // The prompt is never part of argv or the first command.
        assert!(!arguments(&spec).iter().any(|argument| argument == "hi"));

        let trusted = Pi::default()
            .trust_project_resources(true)
            .command(&full_access("hi"))
            .unwrap();
        assert!(arguments(&trusted).contains(&"--approve".to_string()));
    }

    #[test]
    fn maps_model_thinking_session_and_launch_context() {
        let mut request = full_access("hi");
        request.model = Some("anthropic/claude-sonnet-4-5".into());
        request.reasoning = Some("high".into());
        request.session_id = Some("01a100a5-a169-758c-a71f-45422952f996".into());
        request.launch_context.system_prompt_append = Some("Follow the house style.".into());
        request.launch_context.allowed_tools = Some(vec!["read".into(), "bash".into()]);
        request.launch_context.strict_mcp_config = true;
        let arguments = arguments(&Pi::default().command(&request).unwrap());
        for pair in [
            ["--session-id", "01a100a5-a169-758c-a71f-45422952f996"],
            ["--model", "anthropic/claude-sonnet-4-5"],
            ["--thinking", "high"],
            ["--append-system-prompt", "Follow the house style."],
            ["--tools", "read,bash"],
        ] {
            assert!(
                arguments.windows(2).any(|window| window == pair),
                "{pair:?} in {arguments:?}"
            );
        }
        assert!(arguments.contains(&"--no-extensions".to_string()));
    }

    #[test]
    fn plan_mode_keeps_only_read_only_tools() {
        let mut request = TurnRequest::new(Provider::Pi, ".", "review");
        request.permission_mode = PermissionMode::Plan;
        let arguments = arguments(&Pi::default().command(&request).unwrap());
        assert!(arguments
            .windows(2)
            .any(|window| window == ["--tools", "read,grep,find,ls"]));

        request.launch_context.allowed_tools = Some(vec!["bash".into(), "read".into()]);
        let arguments = super::tests::arguments(&Pi::default().command(&request).unwrap());
        assert!(arguments
            .windows(2)
            .any(|window| window == ["--tools", "read"]));

        request.launch_context.allowed_tools = Some(vec!["bash".into()]);
        let arguments = super::tests::arguments(&Pi::default().command(&request).unwrap());
        assert!(arguments.contains(&"--no-tools".to_string()));
    }

    #[test]
    fn rejects_permission_modes_pi_cannot_enforce() {
        for mode in [
            PermissionMode::Default,
            PermissionMode::AcceptEdits,
            PermissionMode::Custom("yolo".into()),
        ] {
            let mut request = TurnRequest::new(Provider::Pi, ".", "hi");
            request.permission_mode = mode;
            assert!(matches!(
                Pi::default().command(&request),
                Err(RuntimeError::InvalidRequest {
                    field: "permission_mode",
                    ..
                })
            ));
        }
        let support = Pi::default().permission_support();
        assert!(!support.supports(&PermissionMode::Default));
        assert!(support.supports(&PermissionMode::FullAccess));
        assert!(support.supports(&PermissionMode::Plan));
    }

    #[test]
    fn rejects_values_pi_would_misread() {
        type Case = (&'static str, fn(&mut TurnRequest));
        let cases: [Case; 5] = [
            ("session_id", |request| {
                request.session_id = Some("../escape".into());
            }),
            ("session_id", |request| {
                request.session_id = Some("-flag".into());
            }),
            ("model", |request| request.model = Some("--approve".into())),
            ("reasoning", |request| {
                request.reasoning = Some("turbo".into());
            }),
            ("launch_context.allowed_tools", |request| {
                request.launch_context.allowed_tools = Some(vec!["read,bash".into()]);
            }),
        ];
        for (field, mutate) in cases {
            let mut request = full_access("hi");
            mutate(&mut request);
            match Pi::default().command(&request) {
                Err(RuntimeError::InvalidRequest {
                    field: rejected, ..
                }) => assert_eq!(rejected, field),
                other => panic!("expected {field} rejection, got {other:?}"),
            }
        }
    }

    #[test]
    fn writes_the_prompt_only_after_pi_reports_its_session() {
        let adapter = Pi::default();
        let request = full_access("summarize the repository");
        let (state, output) = started(&adapter, &request, "session-1");
        assert_eq!(
            output.events,
            vec![TurnEvent::SessionStarted {
                session_id: "session-1".into(),
                title: None
            }]
        );
        assert_eq!(output.writes.len(), 1);
        let prompt: Value = serde_json::from_slice(&output.writes[0]).unwrap();
        assert_eq!(prompt["type"], "prompt");
        assert_eq!(prompt["message"], "summarize the repository");
        assert_eq!(
            state.result.model.as_deref(),
            Some("anthropic/claude-sonnet-4-5")
        );
        assert!(!state.extensions.contains_key(PROMPT_KEY));
        assert!(!output.terminal);
    }

    #[test]
    fn a_resumed_session_is_not_announced_again() {
        let adapter = Pi::default();
        let mut request = full_access("continue");
        request.session_id = Some("session-1".into());
        let (_, output) = started(&adapter, &request, "session-1");
        assert!(output.events.is_empty(), "{:?}", output.events);
        assert_eq!(output.writes.len(), 1);
    }

    #[test]
    fn a_missing_resumed_session_fails_before_the_prompt_is_sent() {
        let adapter = Pi::default();
        let mut request = full_access("continue");
        request.session_id = Some("gone".into());
        let mut state = AdapterState::default();
        adapter.prepare_turn(&request, &mut state).unwrap();
        let output = adapter
            .parse_line(
                &json!({"id": ID_STATE, "type": "response", "command": "get_state", "success": true,
                    "data": {"sessionId": "gone", "messageCount": 0}})
                .to_string(),
                &mut state,
            )
            .unwrap();
        assert!(output.terminal);
        assert!(
            output.writes.is_empty(),
            "the prompt must not reach an empty session"
        );
        let failure = state.terminal_failure.unwrap();
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("pi::session_not_found")
        );
        assert_eq!(failure.delivery, DeliveryState::NotSent);
    }

    #[test]
    fn a_resume_pi_cannot_confirm_fails_before_the_prompt_is_sent() {
        let adapter = Pi::default();
        let mut request = full_access("continue");
        request.session_id = Some("kept".into());
        for data in [
            json!({"sessionId": "kept"}),
            json!({"sessionId": "kept", "messageCount": null}),
            json!({"sessionId": "kept", "messageCount": "3"}),
            json!({"sessionId": "kept", "messageCount": -1}),
            json!({"sessionId": "kept", "messageCount": 2.5}),
        ] {
            let mut state = AdapterState::default();
            adapter.prepare_turn(&request, &mut state).unwrap();
            let output = adapter
                .parse_line(
                    &json!({"id": ID_STATE, "type": "response", "command": "get_state",
                        "success": true, "data": data})
                    .to_string(),
                    &mut state,
                )
                .unwrap();
            assert!(output.terminal, "{data}");
            assert!(output.writes.is_empty(), "the prompt was sent for {data}");
            let failure = state.terminal_failure.unwrap();
            assert_eq!(
                failure.provider_code.as_deref(),
                Some("pi::session_unconfirmed"),
                "{data}"
            );
            assert_eq!(failure.delivery, DeliveryState::NotSent);
        }
    }

    #[test]
    fn a_rejected_prompt_is_an_authentication_failure_that_was_not_sent() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let output = adapter
            .parse_line(
                &json!({"id": ID_PROMPT, "type": "response", "command": "prompt", "success": false,
                    "error": "No API key found for anthropic.\n\nUse /login to log into a provider"})
                .to_string(),
                &mut state,
            )
            .unwrap();
        assert!(output.terminal);
        let failure = state.terminal_failure.unwrap();
        assert_eq!(failure.kind, ProviderProcessErrorKind::AuthenticationFailed);
        assert_eq!(failure.delivery, DeliveryState::NotSent);
    }

    #[test]
    fn streams_a_tool_using_run_until_agent_settled() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("THINK TOOL"), "session-1");
        let usage = json!({"input": 42, "output": 12, "cacheRead": 7, "cacheWrite": 3, "totalTokens": 64,
            "cost": {"input": 0.001, "output": 0.002, "cacheRead": 0, "cacheWrite": 0, "total": 0.003}});
        let outputs = feed(
            &adapter,
            &mut state,
            &[
                json!({"id": ID_PROMPT, "type": "response", "command": "prompt", "success": true, "data": {"disposition": "started"}}),
                json!({"type": "agent_start"}),
                json!({"type": "turn_start"}),
                json!({"type": "message_start", "message": {"role": "system", "content": ""}}),
                json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
                json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "Considering."}}),
                json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 1, "delta": "Running a command."}}),
                json!({"type": "message_update", "assistantMessageEvent": {"type": "toolcall_end", "contentIndex": 2,
                "toolCall": {"type": "toolCall", "id": "call-1", "name": "bash", "arguments": {"command": "echo hi"}}}}),
                json!({"type": "message_end", "message": {"role": "assistant", "provider": "anthropic", "model": "claude-sonnet-4-5",
                "content": [{"type": "thinking", "thinking": "Considering."}, {"type": "text", "text": "Running a command."},
                    {"type": "toolCall", "id": "call-1", "name": "bash", "arguments": {"command": "echo hi"}}],
                "usage": usage, "stopReason": "toolUse"}}),
                json!({"type": "tool_execution_start", "toolCallId": "call-1", "toolName": "bash", "args": {"command": "echo hi"}}),
                json!({"type": "tool_execution_update", "toolCallId": "call-1", "toolName": "bash", "partialResult": {"content": []}}),
                json!({"type": "tool_execution_end", "toolCallId": "call-1", "toolName": "bash",
                "result": {"content": [{"type": "text", "text": "hi\n"}]}, "isError": false}),
                json!({"type": "message_end", "message": {"role": "toolResult", "toolCallId": "call-1", "content": [{"type": "text", "text": "hi\n"}]}}),
                json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
                json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": " Done."}}),
                json!({"type": "message_end", "message": {"role": "assistant", "provider": "anthropic", "model": "claude-sonnet-4-5",
                "content": [{"type": "text", "text": " Done."}], "usage": usage, "stopReason": "stop"}}),
                json!({"type": "agent_end", "messages": [], "willRetry": false}),
            ],
        );
        assert!(outputs[0].turn_submitted);
        assert!(outputs.iter().all(|output| !output.terminal));
        let events = outputs
            .into_iter()
            .flat_map(|output| output.events)
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    TurnEvent::TextDelta { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>(),
            "Running a command. Done.",
            "message_end must not repeat streamed text"
        );
        assert!(events.contains(&TurnEvent::ReasoningDelta {
            text: "Considering.".into()
        }));
        assert!(events.contains(&TurnEvent::ToolCall {
            id: Some("call-1".into()),
            name: "bash".into(),
            status: ToolCallStatus::Started,
            input: Some(json!({"command": "echo hi"})),
            output: None,
            error: None,
            task_id: None,
        }));
        assert!(events.contains(&TurnEvent::ToolCall {
            id: Some("call-1".into()),
            name: "bash".into(),
            status: ToolCallStatus::Succeeded,
            input: None,
            output: Some("hi\n".into()),
            error: None,
            task_id: None,
        }));

        let settled = adapter
            .parse_line(&json!({"type": "agent_settled"}).to_string(), &mut state)
            .unwrap();
        assert!(settled.terminal);
        assert!(state.terminal_failure.is_none());
        assert_eq!(state.result.status, RunStatus::Succeeded);
        assert_eq!(state.result.text, "Running a command. Done.");
        assert_eq!(state.result.reasoning.as_deref(), Some("Considering."));
        let usage = &state.result.usage;
        assert_eq!(usage.input_tokens, Some(84));
        assert_eq!(usage.output_tokens, Some(24));
        assert_eq!(usage.cache_read_input_tokens, Some(14));
        assert_eq!(usage.cache_creation_input_tokens, Some(6));
        assert!((usage.cost_usd.unwrap() - 0.006).abs() < 1e-9);
        assert_eq!(
            usage.context_window,
            Some(ContextWindowUsage {
                used_tokens: Some(64),
                limit_tokens: Some(200_000),
                model: Some("anthropic/claude-sonnet-4-5".into()),
                estimated: false,
            })
        );
    }

    #[test]
    fn a_non_streaming_message_is_delivered_from_message_end() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let outputs = feed(
            &adapter,
            &mut state,
            &[
                json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
                json!({"type": "message_end", "message": {"role": "assistant",
                "content": [{"type": "text", "text": "Whole answer."}], "stopReason": "stop"}}),
            ],
        );
        assert_eq!(
            outputs[1].events,
            vec![TurnEvent::TextDelta {
                text: "Whole answer.".into()
            }]
        );
    }

    #[test]
    fn only_the_last_attempt_decides_the_turn() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let outputs = feed(
            &adapter,
            &mut state,
            &[
                json!({"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error",
                "errorMessage": "529 overloaded"}}),
                json!({"type": "agent_end", "messages": [], "willRetry": true}),
                json!({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 2000,
                "errorMessage": "529 overloaded sk-secret"}),
                json!({"type": "message_end", "message": {"role": "assistant",
                "content": [{"type": "text", "text": "Recovered."}], "stopReason": "stop"}}),
                json!({"type": "auto_retry_end", "success": true, "attempt": 2}),
            ],
        );
        let warning = outputs[2].events.first().unwrap();
        assert_eq!(
            warning,
            &TurnEvent::Warning {
                message: "pi is retrying the model request (attempt 1 of 3)".into()
            },
            "the provider error text must not reach a warning"
        );
        let settled = adapter
            .parse_line(&json!({"type": "agent_settled"}).to_string(), &mut state)
            .unwrap();
        assert!(settled.terminal);
        assert!(state.terminal_failure.is_none());
    }

    #[test]
    fn a_final_model_error_fails_the_turn_with_its_category() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        feed(
            &adapter,
            &mut state,
            &[
                json!({"type": "message_end", "message": {"role": "assistant",
            "content": [], "stopReason": "error",
            "errorMessage": "401 {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"invalid x-api-key\"}}"}}),
            ],
        );
        let settled = adapter
            .parse_line(&json!({"type": "agent_settled"}).to_string(), &mut state)
            .unwrap();
        assert!(settled.terminal);
        assert_eq!(state.result.status, RunStatus::Failed);
        let failure = state.terminal_failure.unwrap();
        assert_eq!(failure.kind, ProviderProcessErrorKind::AuthenticationFailed);
        assert_eq!(failure.delivery, DeliveryState::Accepted);
    }

    #[test]
    fn agent_settled_before_the_prompt_is_not_terminal() {
        let adapter = Pi::default();
        let mut state = AdapterState::default();
        adapter
            .prepare_turn(&full_access("hi"), &mut state)
            .unwrap();
        let output = adapter
            .parse_line(&json!({"type": "agent_settled"}).to_string(), &mut state)
            .unwrap();
        assert!(!output.terminal);
    }

    #[test]
    fn a_handled_prompt_ends_the_turn_without_a_run() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("/reload"), "session-1");
        let output = adapter
            .parse_line(
                &json!({"id": ID_PROMPT, "type": "response", "command": "prompt", "success": true,
                    "data": {"disposition": "handled"}})
                .to_string(),
                &mut state,
            )
            .unwrap();
        assert!(output.terminal);
        assert!(state.terminal_failure.is_none());
    }

    #[test]
    fn a_failed_tool_reports_its_error() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let output = adapter
            .parse_line(
                &json!({"type": "tool_execution_end", "toolCallId": "call-2", "toolName": "edit",
                    "result": {"content": [{"type": "text", "text": "oldText not found"}]}, "isError": true})
                .to_string(),
                &mut state,
            )
            .unwrap();
        assert_eq!(
            output.events,
            vec![TurnEvent::ToolCall {
                id: Some("call-2".into()),
                name: "edit".into(),
                status: ToolCallStatus::Failed,
                input: None,
                output: None,
                error: Some("oldText not found".into()),
                task_id: None,
            }]
        );
    }

    #[test]
    fn reports_the_compaction_lifecycle() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let outputs = feed(
            &adapter,
            &mut state,
            &[
                json!({"type": "compaction_start", "reason": "threshold"}),
                json!({"type": "compaction_end", "reason": "threshold", "aborted": false, "willRetry": false,
                "result": {"summary": "…", "tokensBefore": 150_000, "estimatedTokensAfter": 32000}}),
                json!({"type": "compaction_start", "reason": "manual"}),
                json!({"type": "compaction_end", "reason": "manual", "aborted": true, "willRetry": false}),
            ],
        );
        assert_eq!(
            outputs[0].events,
            vec![TurnEvent::CompactionStarted {
                trigger: CompactionTrigger::Automatic
            }]
        );
        assert_eq!(
            outputs[1].events,
            vec![TurnEvent::CompactionCompleted {
                compaction: ContextCompaction {
                    trigger: CompactionTrigger::Automatic,
                    pre_tokens: Some(150_000),
                    post_tokens: Some(32_000),
                    dropped_tokens: Some(118_000),
                    cumulative_dropped_tokens: None,
                    duration_ms: None,
                }
            }]
        );
        assert!(matches!(
            outputs[3].events.as_slice(),
            [TurnEvent::CompactionFailed {
                trigger: CompactionTrigger::Manual,
                ..
            }]
        ));
    }

    #[test]
    fn extension_confirm_round_trips_as_an_approval() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let output = adapter
            .parse_line(
                &json!({"type": "extension_ui_request", "id": "ui-1", "method": "confirm",
                    "title": "Run rm -rf build?", "message": "The command deletes files."})
                .to_string(),
                &mut state,
            )
            .unwrap();
        let Some(InteractionRequest::Approval { request, original }) = output.interaction else {
            panic!("expected an approval");
        };
        assert_eq!(request.id, "ui-1");
        assert_eq!(request.tool_name, "pi_extension_confirm");
        assert!(output
            .events
            .contains(&TurnEvent::ApprovalRequested(request.clone())));
        for (decision, confirmed) in [
            (ApprovalDecision::Allow, true),
            (ApprovalDecision::AllowForSession, true),
            (ApprovalDecision::Deny { reason: None }, false),
        ] {
            let response = adapter
                .approval_response(&request, &original, decision)
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&response).unwrap(),
                json!({"type": "extension_ui_response", "id": "ui-1", "confirmed": confirmed})
            );
        }
    }

    #[test]
    fn extension_select_round_trips_as_a_question() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let output = adapter
            .parse_line(
                &json!({"type": "extension_ui_request", "id": "ui-2", "method": "select",
                    "title": "Which environment?", "options": ["staging", "production"]})
                .to_string(),
                &mut state,
            )
            .unwrap();
        let Some(InteractionRequest::Question { request, original }) = output.interaction else {
            panic!("expected a question");
        };
        let prompts = request.prompts().unwrap();
        assert_eq!(prompts[0].question, "Which environment?");
        assert_eq!(
            prompts[0]
                .options
                .iter()
                .map(|option| option.label.as_str())
                .collect::<Vec<_>>(),
            ["staging", "production"]
        );
        let answered = adapter
            .question_response(
                &request,
                &original,
                Some(QuestionAnswer::selected("Which environment?", "staging")),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&answered).unwrap(),
            json!({"type": "extension_ui_response", "id": "ui-2", "value": "staging"})
        );
        let declined = adapter
            .question_response(&request, &original, None)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&declined).unwrap(),
            json!({"type": "extension_ui_response", "id": "ui-2", "cancelled": true})
        );
    }

    #[test]
    fn fire_and_forget_extension_ui_needs_no_reply() {
        let adapter = Pi::default();
        let (mut state, _) = started(&adapter, &full_access("hi"), "session-1");
        let outputs = feed(
            &adapter,
            &mut state,
            &[
                json!({"type": "extension_ui_request", "id": "ui-3", "method": "setStatus", "statusKey": "k", "statusText": "busy"}),
                json!({"type": "extension_ui_request", "id": "ui-4", "method": "notify", "message": "fyi", "notifyType": "info"}),
                json!({"type": "extension_ui_request", "id": "ui-5", "method": "notify", "message": "MCP server failed", "notifyType": "error"}),
            ],
        );
        assert!(outputs
            .iter()
            .all(|output| output.interaction.is_none() && output.writes.is_empty()));
        assert!(outputs[0].events.is_empty() && outputs[1].events.is_empty());
        assert_eq!(
            outputs[2].events,
            vec![TurnEvent::Warning {
                message: "MCP server failed".into()
            }]
        );
    }

    #[test]
    fn interrupts_with_abort() {
        let frame = Pi::default()
            .interrupt_request(&AdapterState::default())
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&frame).unwrap(),
            json!({"type": "abort"})
        );
    }

    #[test]
    fn redundant_oversized_frames_are_dropped_and_others_fail() {
        let adapter = Pi::default();
        let mut state = AdapterState::default();
        for prefix in [
            r#"{"type":"agent_end","messages":[{"role":"system""#,
            r#"{"type":"turn_end","message":{"role":"assistant""#,
            r#"{"type":"message_end","message":{"role":"toolResult","toolCallId":"x""#,
            r#"{"type":"message_start","message":{"role":"user""#,
            r#"{"type":"tool_execution_update","toolCallId":"x""#,
        ] {
            assert!(adapter.accepts_oversized_frame(prefix), "{prefix}");
            let output = adapter
                .parse_oversized_frame(OversizedFrame::new(prefix, "]}"), &mut state)
                .unwrap();
            assert!(output.events.is_empty() && !output.terminal, "{prefix}");
        }
        // The answer itself and unknown events cannot be done without.
        for prefix in [
            r#"{"type":"message_end","message":{"role":"assistant""#,
            r#"{"type":"agent_settled"}"#,
            r#"{"type":"message_update","assistantMessageEvent":{"#,
        ] {
            assert!(!adapter.accepts_oversized_frame(prefix), "{prefix}");
            assert!(adapter
                .parse_oversized_frame(OversizedFrame::new(prefix, "}"), &mut state)
                .is_err());
        }
    }

    #[test]
    fn an_oversized_tool_result_keeps_the_outcome_pi_reported() {
        let adapter = Pi::default();
        let prefix = r#"{"type":"tool_execution_end","toolCallId":"call-9","toolName":"read","result":{"content":[{"type":"image","data":"iVBOR"#;
        assert!(adapter.accepts_oversized_frame(prefix));
        for (suffix, status, failed_text) in [
            (
                r#"AAAA"}],"details":{}},"isError":false}"#,
                ToolCallStatus::Succeeded,
                None,
            ),
            (
                r#"too large"}],"details":{}},"isError":true}"#,
                ToolCallStatus::Failed,
                Some("[pi tool error omitted: larger than the event limit]"),
            ),
            // A tail that does not end with `isError` is never a success.
            (
                r#"AAAA"}],"details":{}}}"#,
                ToolCallStatus::Failed,
                Some("pi reported a tool result larger than the event limit, and whether the tool succeeded could not be read"),
            ),
            // `isError` inside the result's text is escaped and does not count.
            (
                r#"\",\"isError\":false}"}]}}"#,
                ToolCallStatus::Failed,
                Some("pi reported a tool result larger than the event limit, and whether the tool succeeded could not be read"),
            ),
        ] {
            let mut state = AdapterState::default();
            let output = adapter
                .parse_oversized_frame(OversizedFrame::new(prefix, suffix), &mut state)
                .unwrap();
            let [TurnEvent::ToolCall {
                id: Some(id),
                name,
                status: reported,
                output,
                error,
                ..
            }] = output.events.as_slice()
            else {
                panic!("expected one tool call for {suffix}: {:?}", output.events);
            };
            assert_eq!((id.as_str(), name.as_str()), ("call-9", "read"));
            assert_eq!(*reported, status, "{suffix}");
            assert_eq!(error.as_deref(), failed_text, "{suffix}");
            assert_eq!(output.is_some(), failed_text.is_none(), "{suffix}");
        }
    }

    #[test]
    fn an_oversized_tool_result_without_an_id_finishes_the_call_in_flight() {
        let adapter = Pi::default();
        let mut state = AdapterState::default();
        adapter
            .parse_line(
                &json!({"type": "tool_execution_start", "toolCallId": "call-10", "toolName": "read", "args": {}})
                    .to_string(),
                &mut state,
            )
            .unwrap();
        let output = adapter
            .parse_oversized_frame(
                OversizedFrame::new(
                    r#"{"type":"tool_execution_end","result":{"content":[{"#,
                    r#"]},"isError":true}"#,
                ),
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            output.events.as_slice(),
            [TurnEvent::ToolCall { id: Some(id), name, status: ToolCallStatus::Failed, .. }]
                if id == "call-10" && name == "read"
        ));
        assert!(
            load(&state).running_tools.is_empty(),
            "the finished call is no longer in flight"
        );
    }

    #[test]
    fn parses_the_rpc_model_catalog() {
        let lines = [
            json!({"id": ID_CATALOG_MODELS, "type": "response", "command": "get_available_models", "success": true,
                "data": {"models": [
                    {"id": "claude-sonnet-4-5", "name": "Claude Sonnet 4.5", "provider": "anthropic", "reasoning": true,
                        "contextWindow": 200_000,
                        "thinkingLevelMap": {"off": null, "minimal": null, "low": "low", "medium": "medium", "high": "high", "xhigh": null, "max": "max"}},
                    {"id": "gpt-4o-mini", "name": "GPT-4o mini", "provider": "openai", "reasoning": false, "contextWindow": 128_000},
                    {"id": "o-series", "name": "", "provider": "openai", "reasoning": true}
                ]}}),
            json!({"id": ID_CATALOG_STATE, "type": "response", "command": "get_state", "success": true,
                "data": {"model": {"provider": "anthropic", "id": "claude-sonnet-4-5"}, "thinkingLevel": "high"}}),
        ]
        .map(|value| value.to_string());
        let catalog = Pi::default().parse_catalog(&lines).unwrap();
        assert_eq!(catalog.status, HarnessCatalogStatus::Ready);
        let ids = catalog
            .models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                "anthropic/claude-sonnet-4-5",
                "openai/gpt-4o-mini",
                "openai/o-series"
            ]
        );
        let sonnet = &catalog.models[0];
        assert!(sonnet.is_default);
        assert_eq!(sonnet.context_window_tokens, Some(200_000));
        assert_eq!(
            sonnet
                .reasoning_efforts
                .iter()
                .map(|effort| effort.id.as_str())
                .collect::<Vec<_>>(),
            ["low", "medium", "high", "max"]
        );
        assert_eq!(
            sonnet
                .reasoning_efforts
                .iter()
                .find(|effort| effort.is_default)
                .map(|effort| effort.id.as_str()),
            Some("high")
        );
        let efforts = &catalog.models[1].reasoning_efforts;
        assert!(efforts.is_empty(), "{efforts:?}");
        let o_series = &catalog.models[2];
        assert_eq!(o_series.label, "openai/o-series");
        assert_eq!(
            o_series
                .reasoning_efforts
                .iter()
                .map(|effort| effort.id.as_str())
                .collect::<Vec<_>>(),
            ["off", "minimal", "low", "medium", "high"]
        );
        assert_eq!(
            o_series
                .reasoning_efforts
                .iter()
                .find(|effort| effort.is_default)
                .map(|effort| effort.id.as_str()),
            Some("medium")
        );
    }

    #[test]
    fn an_empty_catalog_means_no_credentials() {
        let lines = [
            json!({"id": ID_CATALOG_MODELS, "type": "response", "command": "get_available_models",
            "success": true, "data": {"models": []}})
            .to_string(),
        ];
        let catalog = Pi::default().parse_catalog(&lines).unwrap();
        assert_eq!(catalog.status, HarnessCatalogStatus::Failed);
        assert_eq!(
            catalog.error.unwrap().kind,
            HarnessCatalogErrorKind::Authentication
        );
    }

    #[test]
    fn the_catalog_probe_correlates_numeric_response_ids() {
        let probe = Pi::default().catalog_probe().unwrap();
        assert_eq!(
            probe.expected_response_ids,
            Some(vec![ID_CATALOG_MODELS, ID_CATALOG_STATE])
        );
        let stdin = String::from_utf8(probe.command.initial_stdin.clone().unwrap()).unwrap();
        let ids = stdin
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()["id"]
                    .as_u64()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, [ID_CATALOG_MODELS, ID_CATALOG_STATE]);
        assert!(arguments(&probe.command).contains(&"--no-session".to_string()));
    }

    #[test]
    fn reads_authentication_from_the_model_listing() {
        let adapter = Pi::default();
        let ok = TransportExitStatus {
            success: true,
            code: Some(0),
        };
        let listed = adapter
            .parse_authentication_probe(
                b"provider   model              context  max-out  thinking  images\nanthropic  claude-sonnet-4-5  200K     64K      yes       yes\n",
                "",
                ok,
            )
            .unwrap();
        assert_eq!(
            listed.status,
            crate::HarnessAuthenticationStatus::Authenticated
        );
        let missing = adapter
            .parse_authentication_probe(
                b"No models available. Use /login to log into a provider via OAuth or API key. See:\n",
                "",
                ok,
            )
            .unwrap();
        assert_eq!(missing.status, crate::HarnessAuthenticationStatus::Required);
        let failed = adapter
            .parse_authentication_probe(
                b"",
                "boom",
                TransportExitStatus {
                    success: false,
                    code: Some(1),
                },
            )
            .unwrap();
        assert_eq!(failed.status, crate::HarnessAuthenticationStatus::Unknown);
    }

    #[test]
    fn mirrors_pi_thinking_level_rules() {
        let none = json!({"reasoning": false});
        assert_eq!(supported_thinking_levels(&none), ["off"]);
        let plain = json!({"reasoning": true});
        assert_eq!(
            supported_thinking_levels(&plain),
            ["off", "minimal", "low", "medium", "high"]
        );
        let extended =
            json!({"reasoning": true, "thinkingLevelMap": {"xhigh": "xhigh", "medium": null}});
        let levels = supported_thinking_levels(&extended);
        assert_eq!(levels, ["off", "minimal", "low", "high", "xhigh"]);
        assert_eq!(clamp_thinking_level(&levels, "medium"), Some("high"));
        assert_eq!(clamp_thinking_level(&levels, "max"), Some("xhigh"));
    }

    #[test]
    fn rejects_non_json_stdout() {
        let error = Pi::default()
            .parse_line(
                "Session found in different project: /elsewhere",
                &mut AdapterState::default(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::Protocol {
                provider: Provider::Pi,
                ..
            }
        ));
    }
}
