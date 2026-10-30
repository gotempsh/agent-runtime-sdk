//! Generic adapter for agents that speak the Agent Client Protocol.
//!
//! [ACP](https://agentclientprotocol.com) is JSON-RPC 2.0 over the agent's
//! stdio, one message per line. Any agent that implements it — Gemini CLI
//! (`gemini --acp`), GitHub Copilot (`copilot --acp`), Cursor
//! (`cursor-agent acp`), Kimi, goose, Qwen Code and many more — runs through
//! this one adapter. The application registers each agent's command in an
//! [`AcpAgents`] registry and selects one per turn with the
//! [`AGENT_OPTION`] harness option.
//!
//! One turn is one process:
//!
//! 1. `initialize` with protocol version 1. The client offers neither file
//!    system nor terminal capabilities, so the agent uses its own tools in the
//!    turn's working directory, inside whatever sandbox wraps the process.
//! 2. `session/new`, or — to continue a conversation — `session/resume` when
//!    the agent supports it, else `session/load`, whose history replay is
//!    dropped because the application already has it. An agent that can do
//!    neither starts a new session and the turn says so in a warning.
//! 3. Optional `session/set_mode` (for [`PermissionMode::Custom`]) and model
//!    selection through the session's model config option.
//! 4. `session/prompt`. `session/update` notifications stream text, thoughts,
//!    tool calls, plans and usage until the prompt's response reports its stop
//!    reason, which ends the turn.
//!
//! `session/request_permission` reaches the application as an approval unless
//! the permission mode already decides it. Cancellation sends `session/cancel`
//! and answers every open permission request as cancelled, as the protocol
//! requires.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::adapter::AdapterState;
use crate::lifecycle::DeliveryState;
use crate::{
    AdapterOutput, AgentAdapter, ApprovalDecision, ApprovalRequest, CommandSpec,
    ContextWindowUsage, HarnessControlGroup, HarnessControlKind, HarnessControlOption,
    InteractionRequest, LaunchContextCapabilities, McpServerConfig, PermissionMode,
    PermissionSupport, PlanEntry, PlanEntryStatus, Provider, ProviderProcessErrorKind,
    ProviderReadiness, ProviderTerminalFailure, QuestionAnswer, QuestionRequest, Result, RunStatus,
    RuntimeError, SelectableExecutable, ToolCallStatus, TurnEvent, TurnRequest, Usage,
};

/// Harness option naming the registered [`AcpAgent::id`] a turn runs.
pub const AGENT_OPTION: &str = "agent";
/// ACP protocol version this adapter implements.
pub const PROTOCOL_VERSION: u64 = 1;
/// JSON-RPC error code ACP agents return when they need authentication.
pub const AUTH_REQUIRED_CODE: i64 = -32000;
/// JSON-RPC error code ACP agents return for an unknown resource.
pub const RESOURCE_NOT_FOUND_CODE: i64 = -32002;

const STATE_KEY: &str = "acp";
/// The prompt waits here, outside the per-line state, until the session is
/// ready, so the state decoded on every line never carries a copy of it.
const PROMPT_KEY: &str = "acp_prompt";

const ID_INITIALIZE: u64 = 1;
const ID_SESSION: u64 = 2;
/// Most session settings one turn may select; request ids `100..=131` carry them.
const MAX_REQUESTED_CONFIG: usize = 32;
const ID_SET_MODE: u64 = 3;
const ID_SET_MODEL: u64 = 4;
const ID_PROMPT: u64 = 5;

const MAX_TOOL_OUTPUT_CHARS: usize = 4096;
const MAX_TRACKED_TOOLS: usize = 256;
/// A tool input larger than this is not kept for the call's later updates.
const MAX_KEPT_INPUT_BYTES: usize = 16 * 1024;
const MAX_OPEN_PERMISSIONS: usize = 64;
const MAX_DIAGNOSTIC_CHARS: usize = 1000;
const MAX_TITLE_CHARS: usize = 200;
const MAX_LISTED_CHOICES: usize = 12;
const MAX_AGENT_ID_BYTES: usize = 64;
/// How long a finished agent may take to exit after its stdin closes before
/// the runtime stops it. Node-based agents commonly keep their event loop
/// alive after end-of-input.
const EXIT_GRACE: Duration = Duration::from_secs(3);
/// How long a probe waits for a stopped agent's stderr to close.
const STDERR_DRAIN: Duration = Duration::from_secs(2);

/// One ACP agent command the application makes available.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpAgent {
    /// Stable identifier selected through [`AGENT_OPTION`]: lowercase ASCII
    /// letters, digits, `.`, `_` and `-`.
    pub id: String,
    /// Human-readable name used in diagnostics.
    pub label: String,
    /// Executable meaningful inside the execution transport.
    pub command: PathBuf,
    /// Arguments that start the agent in ACP mode, such as `["--acp"]`.
    pub args: Vec<String>,
    /// Explicit environment additions for the agent process. The runtime
    /// otherwise passes only its safe allowlist, so provider API keys must be
    /// supplied here or through [`TurnRequest::environment`].
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

impl fmt::Debug for AcpAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcpAgent")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("command", &self.command)
            .field("args", &self.args)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl AcpAgent {
    /// Describe an agent started by `command` with `args`.
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        command: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            command: command.into(),
            args: args.into_iter().map(Into::into).collect(),
            environment: BTreeMap::new(),
        }
    }

    /// Check the identifier and command before the agent is registered.
    pub fn validate(&self) -> Result<()> {
        if !valid_agent_id(&self.id) {
            return Err(invalid(
                "acp_agent.id",
                "ACP agent ids are 1-64 lowercase letters, digits, `.`, `_` or `-`, starting with a letter or digit",
            ));
        }
        let command = self.command.as_os_str();
        if command.is_empty() || self.command.to_string_lossy().contains('\0') {
            return Err(invalid("acp_agent.command", "the agent command is empty"));
        }
        if self.args.iter().any(|arg| arg.contains('\0')) {
            return Err(invalid("acp_agent.args", "arguments cannot contain NUL"));
        }
        if self
            .environment
            .keys()
            .any(|name| name.is_empty() || name.contains('=') || name.contains('\0'))
        {
            return Err(invalid(
                "acp_agent.environment",
                "environment names must be non-empty and cannot contain `=` or NUL",
            ));
        }
        Ok(())
    }

    fn command_spec(&self) -> CommandSpec {
        let mut spec = CommandSpec::new(self.command.clone());
        spec.args = self.args.iter().map(Into::into).collect();
        for (name, value) in &self.environment {
            spec.environment.insert(name.into(), value.into());
        }
        spec.interactive_stdin = true;
        spec
    }
}

/// Whether `path` is absolute on the machine that will run the agent.
///
/// ACP requires `cwd` to be absolute, and the agent starts inside the working
/// directory, so a relative one would resolve a second time from there
/// (`workspace` becoming `workspace/workspace`). The adapter cannot resolve it
/// for a remote transport, so it refuses one instead. The host and the
/// transport may use different path syntaxes, hence the textual check.
fn is_absolute_directory(path: &Path) -> bool {
    if path.is_absolute() {
        return true;
    }
    let text = path.to_string_lossy();
    let bytes = text.as_bytes();
    text.starts_with('/')
        || text.starts_with("\\\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/'))
}

fn relative_directory_message(path: &Path) -> String {
    format!(
        "ACP agents need an absolute working directory, but `{}` is relative; pass its full path",
        path.display()
    )
}

fn valid_agent_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_AGENT_ID_BYTES
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

/// Shared, updatable set of ACP agents.
///
/// Cloning shares the registry, so an application can keep one handle and
/// change the available agents without rebuilding its runtimes.
#[derive(Debug, Clone, Default)]
pub struct AcpAgents {
    inner: Arc<RwLock<BTreeMap<String, AcpAgent>>>,
}

impl AcpAgents {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace every registered agent. Invalid agents are rejected and the
    /// registry is left unchanged.
    pub fn replace(&self, agents: impl IntoIterator<Item = AcpAgent>) -> Result<()> {
        let mut next = BTreeMap::new();
        for agent in agents {
            agent.validate()?;
            next.insert(agent.id.clone(), agent);
        }
        *self.write() = next;
        Ok(())
    }

    /// Register or replace one agent.
    pub fn insert(&self, agent: AcpAgent) -> Result<()> {
        agent.validate()?;
        self.write().insert(agent.id.clone(), agent);
        Ok(())
    }

    /// Remove one agent.
    pub fn remove(&self, id: &str) -> Option<AcpAgent> {
        self.write().remove(id)
    }

    /// Look up one agent.
    pub fn get(&self, id: &str) -> Option<AcpAgent> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// Every registered agent, ordered by id.
    pub fn list(&self) -> Vec<AcpAgent> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, AcpAgent>> {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Adapter for any agent that speaks the Agent Client Protocol over stdio.
///
/// Turns select an agent from the shared [`AcpAgents`] registry with the
/// [`AGENT_OPTION`] harness option.
#[derive(Debug, Clone, Default)]
pub struct Acp {
    agents: AcpAgents,
}

impl Acp {
    /// Run agents from `agents`.
    pub fn new(agents: AcpAgents) -> Self {
        Self { agents }
    }

    /// The registry this adapter reads.
    pub fn agents(&self) -> &AcpAgents {
        &self.agents
    }

    fn agent_for(&self, request: &TurnRequest) -> Result<AcpAgent> {
        let id = request
            .harness_options
            .get(AGENT_OPTION)
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                invalid(
                    "harness_options.agent",
                    "choose which ACP agent runs this turn",
                )
            })?;
        self.agents.get(id).ok_or_else(|| {
            invalid(
                "harness_options.agent",
                format!("no ACP agent named `{id}` is registered"),
            )
        })
    }

    /// Start `agent` locally, run the ACP handshake and report what it
    /// supports and whether it can open a session without signing in.
    ///
    /// With `check_session` the probe also calls `session/new` in
    /// `working_directory`, which must then be absolute because ACP requires
    /// an absolute `cwd` ([`AcpProbeErrorKind::Invalid`] otherwise); agents
    /// that require sign-in refuse it with
    /// [`AUTH_REQUIRED_CODE`]. Agents may record that empty session in their
    /// own history, so applications should run this only on an explicit user
    /// action. The process is always stopped before this returns.
    pub async fn probe_agent(
        agent: &AcpAgent,
        working_directory: &Path,
        check_session: bool,
        timeout: Duration,
    ) -> std::result::Result<AcpAgentProbe, AcpProbeError> {
        Self::probe_agent_with_progress(agent, working_directory, check_session, timeout, |_| {})
            .await
    }

    /// The same bounded probe, reporting concrete setup stages without prompting a model.
    pub async fn probe_agent_with_progress(
        agent: &AcpAgent,
        working_directory: &Path,
        check_session: bool,
        timeout: Duration,
        progress: impl Fn(&'static str) + Send + Sync,
    ) -> std::result::Result<AcpAgentProbe, AcpProbeError> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        progress("Starting the agent and negotiating ACP capabilities");
        agent
            .validate()
            .map_err(|error| AcpProbeError::new(AcpProbeErrorKind::Invalid, error.to_string()))?;
        if check_session && !is_absolute_directory(working_directory) {
            return Err(AcpProbeError::new(
                AcpProbeErrorKind::Invalid,
                relative_directory_message(working_directory),
            ));
        }
        let spec = agent.command_spec();
        let (mut child, mut guard) =
            crate::process::spawn(&spec, working_directory).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    AcpProbeError::new(
                        AcpProbeErrorKind::NotInstalled,
                        format!("`{}` was not found on PATH", agent.command.display()),
                    )
                } else {
                    AcpProbeError::new(
                        AcpProbeErrorKind::Spawn,
                        format!("could not start `{}`: {error}", agent.command.display()),
                    )
                }
            })?;
        let mut stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stderr_task =
            stderr.map(|stderr| crate::process::StderrCapture::spawn(stderr, MAX_DIAGNOSTIC_CHARS));
        let exchange = async {
            let mut lines = BufReader::new(stdout.ok_or_else(|| {
                AcpProbeError::new(AcpProbeErrorKind::Spawn, "stdout was not piped")
            })?)
            .lines();
            let writer = stdin.as_mut().ok_or_else(|| {
                AcpProbeError::new(AcpProbeErrorKind::Spawn, "stdin was not piped")
            })?;
            let send = |frame: Value| {
                let mut bytes = serde_json::to_vec(&frame).unwrap_or_default();
                bytes.push(b'\n');
                bytes
            };
            let initialize = send(initialize_request());
            writer
                .write_all(&initialize)
                .await
                .map_err(io_probe_error)?;
            writer.flush().await.map_err(io_probe_error)?;
            let initialized = read_response(&mut lines, ID_INITIALIZE).await?;
            if let Some(error) = initialized.get("error") {
                return Err(AcpProbeError::new(
                    AcpProbeErrorKind::Protocol,
                    format!("initialize failed: {}", error_message(error)),
                ));
            }
            let result = initialized.get("result").cloned().unwrap_or(Value::Null);
            let mut probe = AcpAgentProbe::from_initialize(&result);
            if !check_session {
                return Ok(probe);
            }
            progress("Opening a session and discovering models and settings");
            let session = send(request(
                ID_SESSION,
                "session/new",
                json!({"cwd": working_directory, "mcpServers": []}),
            ));
            writer.write_all(&session).await.map_err(io_probe_error)?;
            writer.flush().await.map_err(io_probe_error)?;
            let opened = read_response(&mut lines, ID_SESSION).await?;
            probe.config_options =
                session_config_options(opened.get("result").unwrap_or(&Value::Null));
            probe.authentication = match opened.get("error") {
                None => AcpAuthentication::Authenticated,
                Some(error) if error_code(error) == Some(AUTH_REQUIRED_CODE) => {
                    AcpAuthentication::Required {
                        message: bounded(&error_message(error), MAX_DIAGNOSTIC_CHARS),
                    }
                }
                Some(error) => AcpAuthentication::Unknown {
                    message: bounded(&error_message(error), MAX_DIAGNOSTIC_CHARS),
                },
            };
            Ok(probe)
        };
        let outcome = tokio::time::timeout(timeout, exchange).await;
        drop(stdin);
        let _ = guard.terminate_async().await;
        let _ = child.kill().await;
        // Stopping the agent's process group does not close stderr when a
        // descendant left the group and kept the pipe, so the read is bounded
        // and abandoned instead of holding the probe past its timeout.
        let stderr = match stderr_task {
            Some(task) => task.finish(STDERR_DRAIN).await.ok(),
            None => None,
        };
        let with_stderr = |mut error: AcpProbeError| {
            if let Some(stderr) = stderr.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                error.message = format!("{} ({})", error.message, bounded(stderr, 400));
            }
            error
        };
        match outcome {
            Ok(Ok(probe)) => Ok(probe),
            Ok(Err(error)) => Err(with_stderr(error)),
            Err(_) => Err(with_stderr(AcpProbeError::new(
                AcpProbeErrorKind::Timeout,
                format!(
                    "`{}` did not answer the ACP handshake within {}s; check that its arguments start ACP mode",
                    agent.command.display(),
                    timeout.as_secs()
                ),
            ))),
        }
    }
}

fn io_probe_error(error: std::io::Error) -> AcpProbeError {
    AcpProbeError::new(
        AcpProbeErrorKind::Protocol,
        format!("the agent closed its input: {error}"),
    )
}

async fn read_response<R>(
    lines: &mut tokio::io::Lines<R>,
    id: u64,
) -> std::result::Result<Value, AcpProbeError>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    while let Some(line) = lines.next_line().await.map_err(io_probe_error)? {
        let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
            continue;
        };
        if value.get("method").is_none() && value.get("id").and_then(Value::as_u64) == Some(id) {
            return Ok(value);
        }
    }
    Err(AcpProbeError::new(
        AcpProbeErrorKind::Protocol,
        "the agent exited before answering the ACP handshake; check that its arguments start ACP mode and that the installed version supports ACP",
    ))
}

/// A selectable value advertised by an ACP agent. IDs are opaque.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpConfigValue {
    /// Opaque wire value.
    pub value: String,
    /// Human-readable label.
    pub name: String,
    /// Agent-provided explanation.
    pub description: Option<String>,
}

/// ACP v1 select configuration, normalized from configOptions or legacy fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpConfigOption {
    /// Opaque config id.
    pub id: String,
    /// Human-readable label.
    pub name: String,
    /// Agent-provided explanation.
    pub description: Option<String>,
    /// Optional semantic category, such as model or mode.
    pub category: Option<String>,
    #[serde(rename = "type")]
    /// ACP control type; this client advertises select support.
    pub kind: String,
    /// Current agent-selected value.
    pub current_value: String,
    /// Choices advertised by the agent, flattened from groups.
    pub options: Vec<AcpConfigValue>,
}

/// Keep only protocol configuration, never arbitrary metadata or MCP credentials.
pub fn session_config_options(result: &Value) -> Vec<AcpConfigOption> {
    fn text(value: &Value, key: &str, max: usize) -> Option<String> {
        value
            .get(key)?
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= max)
            .map(str::to_owned)
    }
    fn values(entries: &[Value], out: &mut Vec<AcpConfigValue>, depth: usize) {
        if depth > 2 {
            return;
        }
        for entry in entries.iter().take(256) {
            if out.len() >= 256 {
                break;
            }
            if let (Some(value), Some(name)) =
                (text(entry, "value", 2048), text(entry, "name", 200))
            {
                if !out.iter().any(|v| v.value == value) {
                    out.push(AcpConfigValue {
                        value,
                        name,
                        description: text(entry, "description", 1000),
                    });
                }
            } else if let Some(nested) = entry.get("options").and_then(Value::as_array) {
                values(nested, out, depth + 1);
            }
        }
    }
    let mut options = Vec::new();
    for option in result
        .get("configOptions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(32)
    {
        if option.get("type").and_then(Value::as_str) != Some("select") {
            continue;
        }
        let (Some(id), Some(name), Some(current_value)) = (
            text(option, "id", 256),
            text(option, "name", 200),
            text(option, "currentValue", 2048),
        ) else {
            continue;
        };
        if options.iter().any(|o: &AcpConfigOption| o.id == id) {
            continue;
        }
        let mut choices = Vec::new();
        values(
            option
                .get("options")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            &mut choices,
            0,
        );
        options.push(AcpConfigOption {
            id,
            name,
            current_value,
            kind: "select".into(),
            description: text(option, "description", 1000),
            category: text(option, "category", 64),
            options: choices,
        });
    }
    for (category, field, list, current, value_key) in [
        (
            "model",
            "models",
            "availableModels",
            "currentModelId",
            "modelId",
        ),
        ("mode", "modes", "availableModes", "currentModeId", "id"),
    ] {
        if options
            .iter()
            .any(|o| o.category.as_deref() == Some(category) || o.id == category)
        {
            continue;
        }
        let Some(legacy) = result.get(field) else {
            continue;
        };
        let Some(current_value) = text(legacy, current, 2048) else {
            continue;
        };
        let choices = legacy
            .get(list)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(256)
            .filter_map(|v| {
                Some(AcpConfigValue {
                    value: text(v, value_key, 2048)?,
                    name: text(v, "name", 200)?,
                    description: text(v, "description", 1000),
                })
            })
            .collect();
        options.push(AcpConfigOption {
            id: category.into(),
            name: if category == "model" { "Model" } else { "Mode" }.into(),
            current_value,
            kind: "select".into(),
            category: Some(category.into()),
            description: None,
            options: choices,
        });
    }
    options
}

/// What a local ACP handshake reported about one agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpAgentProbe {
    /// Protocol version the agent negotiated.
    pub protocol_version: Option<u64>,
    /// Agent-reported name.
    pub agent_name: Option<String>,
    /// Agent-reported version.
    pub agent_version: Option<String>,
    /// Sign-in methods the agent offers.
    pub auth_methods: Vec<AcpAuthMethod>,
    /// The agent can replay a stored session (`session/load`).
    pub load_session: bool,
    /// The agent can continue a stored session without replay (`session/resume`).
    pub resume_session: bool,
    /// Whether a session could be opened without signing in.
    pub authentication: AcpAuthentication,
    /// Bounded, non-secret selectors reported by session setup.
    #[serde(default)]
    pub config_options: Vec<AcpConfigOption>,
}

impl AcpAgentProbe {
    fn from_initialize(result: &Value) -> Self {
        let info = result.get("agentInfo");
        let capabilities = result.get("agentCapabilities");
        Self {
            protocol_version: result.get("protocolVersion").and_then(Value::as_u64),
            agent_name: info
                .and_then(|info| info.get("title").or_else(|| info.get("name")))
                .and_then(Value::as_str)
                .map(|name| bounded(name, MAX_TITLE_CHARS)),
            agent_version: info
                .and_then(|info| info.get("version"))
                .and_then(Value::as_str)
                .map(|version| bounded(version, 64)),
            auth_methods: auth_methods(result),
            load_session: capabilities
                .and_then(|caps| caps.get("loadSession"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            resume_session: capabilities
                .and_then(|caps| caps.pointer("/sessionCapabilities/resume"))
                .is_some_and(|resume| !resume.is_null()),
            config_options: Vec::new(),
            authentication: AcpAuthentication::Unknown {
                message: "a session was not opened".into(),
            },
        }
    }
}

/// A sign-in method an ACP agent advertises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpAuthMethod {
    /// Protocol identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

/// Whether an ACP agent could open a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AcpAuthentication {
    /// `session/new` succeeded.
    Authenticated,
    /// The agent refused with [`AUTH_REQUIRED_CODE`].
    Required {
        /// Agent diagnostic.
        message: String,
    },
    /// Not checked, or the agent failed for another reason.
    Unknown {
        /// Why authentication is unknown.
        message: String,
    },
}

/// Why an ACP probe could not complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct AcpProbeError {
    /// Stable category.
    pub kind: AcpProbeErrorKind,
    /// Bounded diagnostic.
    pub message: String,
}

impl AcpProbeError {
    fn new(kind: AcpProbeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Category of an [`AcpProbeError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AcpProbeErrorKind {
    /// The agent definition is invalid.
    Invalid,
    /// The command was not found.
    NotInstalled,
    /// The command could not be started.
    Spawn,
    /// The agent did not answer in time.
    Timeout,
    /// The agent answered with something other than ACP.
    Protocol,
}

/// Per-turn protocol state retained between output lines.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct TurnState {
    agent_label: String,
    agent_id: String,
    working_directory: PathBuf,
    /// Session the caller asked to continue.
    resume: Option<String>,
    /// Session this turn runs in once opened.
    session_id: Option<String>,
    /// `session/load` is replaying history that the application already has.
    replaying: bool,
    /// Instructions delivered ahead of the prompt of a new session.
    instructions: Option<String>,
    /// `mcpServers` for session requests; HTTP entries are dropped when the
    /// agent cannot connect to them.
    mcp_servers: Vec<Value>,
    /// Mode requested through [`PermissionMode::Custom`].
    requested_mode: Option<String>,
    /// Model requested for this turn.
    requested_model: Option<String>,
    requested_config: BTreeMap<String, String>,
    session_config: Value,
    retained_start: Vec<u8>,
    retained_error: Option<String>,
    inflight_config: Option<Value>,
    permission: Policy,
    /// Requests still to send after the session opens, in order.
    pending: Vec<(u64, Value)>,
    prompt_sent: bool,
    supports_images: bool,
    /// Tool calls seen this turn.
    tools: Vec<TrackedTool>,
    /// Permission requests the client has not replied to, as
    /// `(request id, tool call id)`. An interrupt refuses these; a request
    /// leaves the list when its reply is written or its tool moves on.
    open_permissions: Vec<(Value, Option<String>)>,
    warned_noise: bool,
}

/// A tool call's stable name and, until it finishes, its latest input and
/// output: updates may omit fields they did not change, and every emitted
/// `ToolCall` event is a complete replacement.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct TrackedTool {
    id: String,
    title: String,
    input: Option<Value>,
    /// Latest bounded output an update carried; a final update that only
    /// reports the status completes the call with it.
    output: Option<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Policy {
    /// Every permission request reaches the application.
    #[default]
    Ask,
    /// File edits are allowed; everything else reaches the application.
    AcceptEdits,
    /// Every permission request is allowed.
    Allow,
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
        provider: Provider::Acp,
        message: message.into(),
    }
}

fn invalid(field: &'static str, message: impl Into<String>) -> RuntimeError {
    RuntimeError::InvalidRequest {
        field,
        message: message.into(),
    }
}

fn encode(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| protocol(format!("could not encode an ACP message: {error}")))
}

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn reply(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn reply_error(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn initialize_request() -> Value {
    request(
        ID_INITIALIZE,
        "initialize",
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false,
            },
            "clientInfo": {"name": "temps-agent-runtime", "version": env!("CARGO_PKG_VERSION")},
        }),
    )
}

fn bounded(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let mut bounded: String = text.chars().take(limit).collect();
    bounded.push_str("… [truncated]");
    bounded
}

fn error_code(error: &Value) -> Option<i64> {
    error.get("code").and_then(Value::as_i64)
}

fn error_message(error: &Value) -> String {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("the agent returned an error");
    match error.get("data") {
        Some(Value::String(data)) if !data.trim().is_empty() => format!("{message}: {data}"),
        Some(data) if data.is_object() => data
            .get("message")
            .or_else(|| data.get("details"))
            .and_then(Value::as_str)
            .map_or_else(|| message.to_string(), |data| format!("{message}: {data}")),
        _ => message.to_string(),
    }
}

fn auth_methods(result: &Value) -> Vec<AcpAuthMethod> {
    result
        .get("authMethods")
        .and_then(Value::as_array)
        .map(|methods| {
            methods
                .iter()
                .take(MAX_LISTED_CHOICES)
                .filter_map(|method| {
                    let id = method.get("id").and_then(Value::as_str)?;
                    Some(AcpAuthMethod {
                        id: bounded(id, 128),
                        name: bounded(
                            method.get("name").and_then(Value::as_str).unwrap_or(id),
                            MAX_TITLE_CHARS,
                        ),
                        description: method
                            .get("description")
                            .and_then(Value::as_str)
                            .map(|text| bounded(text, MAX_DIAGNOSTIC_CHARS)),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn fail(
    state: &mut AdapterState,
    output: &mut AdapterOutput,
    kind: ProviderProcessErrorKind,
    diagnostic: impl Into<String>,
    code: &str,
    delivery: DeliveryState,
) {
    let diagnostic = bounded(&diagnostic.into(), MAX_DIAGNOSTIC_CHARS);
    let kind = if kind == ProviderProcessErrorKind::Unknown {
        crate::error::classify_provider_failure(&diagnostic)
    } else {
        kind
    };
    state.terminal_failure = Some(
        ProviderTerminalFailure::new(kind, diagnostic, delivery)
            .with_provider_code(format!("acp::{code}")),
    );
    state.result.status = RunStatus::Failed;
    output.terminal = true;
}

/// Resolve the turn's MCP servers into ACP's literal `mcpServers` entries.
///
/// ACP sends variable values in the session request instead of naming
/// variables the agent should read, so every referenced variable must be part
/// of the turn's explicit environment; the runtime already rejects requests
/// that reference anything else.
fn mcp_servers(request: &TurnRequest) -> Result<Vec<Value>> {
    let lookup = |source: &str, field: &'static str| {
        request
            .environment
            .get(source)
            .map(|value| value.expose().to_string())
            .ok_or_else(|| {
                invalid(
                    field,
                    format!("references environment variable `{source}`, which was not supplied to the turn"),
                )
            })
    };
    let mut servers = Vec::with_capacity(request.launch_context.mcp_servers.len());
    for (name, server) in &request.launch_context.mcp_servers {
        servers.push(match server {
            McpServerConfig::Stdio {
                command,
                args,
                environment_from,
            } => {
                let mut env = Vec::with_capacity(environment_from.len());
                for (target, source) in environment_from {
                    env.push(json!({
                        "name": target,
                        "value": lookup(source, "launch_context.mcp_servers")?,
                    }));
                }
                json!({"name": name, "command": command, "args": args, "env": env})
            }
            McpServerConfig::Http { url, headers_from } => {
                let mut headers = Vec::with_capacity(headers_from.len());
                for (header, source) in headers_from {
                    headers.push(json!({
                        "name": header,
                        "value": lookup(source, "launch_context.mcp_servers")?,
                    }));
                }
                json!({"type": "http", "name": name, "url": url, "headers": headers})
            }
        });
    }
    Ok(servers)
}

#[async_trait]
impl AgentAdapter for Acp {
    fn provider(&self) -> Provider {
        Provider::Acp
    }

    fn supports_retained_process(&self) -> bool {
        true
    }

    fn retained_turn_start(&self, state: &AdapterState) -> Result<Option<Vec<u8>>> {
        let turn = load(state);
        if let Some(error) = turn.retained_error {
            return Err(protocol(&error));
        }
        if turn.retained_start.is_empty() {
            return Err(protocol(
                "the retained ACP session configuration is unavailable; restart the session",
            ));
        }
        Ok(Some(turn.retained_start))
    }

    fn inherit_retained_handoff(&self, previous: AdapterState, state: &mut AdapterState) {
        let prior = load(&previous);
        let mut turn = load(state);
        if turn.agent_id != prior.agent_id
            || turn.working_directory != prior.working_directory
            || turn.resume != prior.session_id
        {
            turn.retained_error = Some(
                "The retained ACP connection belongs to a different agent, directory or session"
                    .into(),
            );
            store(state, &turn);
            return;
        }
        if let Some(session) = turn.resume.clone() {
            turn.session_id = Some(session.clone());
            turn.session_config = prior.session_config.clone();
            turn.supports_images = prior.supports_images;
            let mut output = AdapterOutput::default();
            match configure_session(&session, &prior.session_config, state, &mut turn) {
                Ok(()) => match advance(state, &mut turn, &mut output) {
                    Ok(()) => turn.retained_start = output.writes.into_iter().flatten().collect(),
                    Err(error) => turn.retained_error = Some(error.to_string()),
                },
                Err(error) => turn.retained_error = Some(error),
            }
        }
        store(state, &turn);
    }

    fn classify_idle_frame(&self, line: &str) -> crate::adapter::IdleFrame {
        // JSON-RPC notifications, including unknown extension notifications,
        // are legal between prompts and must not retire a healthy connection.
        match serde_json::from_str::<Value>(line) {
            Ok(value) if value.get("method").and_then(Value::as_str).is_some() => {
                crate::adapter::IdleFrame::Background
            }
            _ => crate::adapter::IdleFrame::Unexpected,
        }
    }

    fn observe_idle_frame(&self, line: &str, inherited: &mut AdapterState) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return;
        };
        let is_notification = matches!(value.get("id"), None | Some(Value::Null));
        if !is_notification || value.get("method").and_then(Value::as_str) != Some("session/update")
        {
            return;
        }
        let mut turn = load(inherited);
        let params = &value["params"];
        if let (Some(ours), Some(theirs)) = (
            turn.session_id.as_deref(),
            params.get("sessionId").and_then(Value::as_str),
        ) {
            if ours != theirs {
                return;
            }
        }
        if apply_setting_update(&params["update"], &mut turn) {
            store(inherited, &turn);
        }
    }

    fn idle_frame_writes(&self, line: &str) -> Vec<Vec<u8>> {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return Vec::new();
        };
        let Some(id) = value.get("id").filter(|id| !id.is_null()) else {
            return Vec::new();
        };
        let frame =
            if value.get("method").and_then(Value::as_str) == Some("session/request_permission") {
                reply(id, json!({"outcome":{"outcome":"cancelled"}}))
            } else {
                reply_error(id, -32601, "No active turn can answer this request")
            };
        encode(&frame).into_iter().collect()
    }

    fn executable(&self) -> PathBuf {
        // Agents are chosen per turn; there is no single provider executable.
        PathBuf::from("acp")
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            default: true,
            // Decided from the tool kind the agent reports when it asks.
            accept_edits: true,
            // ACP has no client-enforceable read-only mode. An agent's own
            // planning mode is reachable through `Custom` when it offers one.
            plan: false,
            full_access: true,
            // Agent-native session modes through `session/set_mode`.
            custom: true,
            live_approvals: true,
            live_questions: false,
        }
    }

    fn launch_context_capabilities(&self) -> LaunchContextCapabilities {
        LaunchContextCapabilities {
            // Delivered as a leading text block of a new session's first
            // prompt, because ACP has no system-prompt field.
            system_prompt_append: true,
            allowed_tools: false,
            stdio_mcp: true,
            // Dropped with a warning when the agent reports no HTTP support.
            http_mcp: true,
            strict_mcp_config: false,
        }
    }

    fn terminal_exit_grace(&self) -> Option<Duration> {
        Some(EXIT_GRACE)
    }

    fn selectable_executables(&self) -> Option<Vec<SelectableExecutable>> {
        // The agent is chosen per turn, so readiness is about the registered
        // agents' own commands rather than the placeholder `acp` executable.
        Some(
            self.agents
                .list()
                .into_iter()
                .map(|agent| SelectableExecutable::new(agent.id, agent.label, agent.command))
                .collect(),
        )
    }

    fn control_groups(&self) -> Vec<HarnessControlGroup> {
        vec![HarnessControlGroup {
            id: AGENT_OPTION.into(),
            label: "Agent".into(),
            kind: HarnessControlKind::Agent,
            options: self
                .agents
                .list()
                .into_iter()
                .map(|agent| HarnessControlOption {
                    description: std::iter::once(agent.command.to_string_lossy().into_owned())
                        .chain(agent.args.iter().cloned())
                        .collect::<Vec<_>>()
                        .join(" "),
                    id: agent.id,
                    label: agent.label,
                    is_default: false,
                    dangerous: false,
                })
                .collect(),
        }]
    }

    async fn readiness(&self) -> ProviderReadiness {
        let agents = self.agents.list();
        ProviderReadiness {
            provider: Provider::Acp,
            installed: !agents.is_empty(),
            executable: None,
            version: None,
            detail: if agents.is_empty() {
                "No ACP agent is registered.".into()
            } else {
                format!(
                    "{} ACP agent(s) registered; check each with Acp::probe_agent.",
                    agents.len()
                )
            },
        }
    }

    fn command(&self, request: &TurnRequest) -> Result<CommandSpec> {
        let agent = self.agent_for(request)?;
        let mut spec = agent.command_spec();
        spec.initial_stdin = Some(encode(&initialize_request())?);
        Ok(spec)
    }

    fn prepare_turn(&self, request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
        let agent = self.agent_for(request)?;
        if !is_absolute_directory(&request.working_directory) {
            return Err(invalid(
                "working_directory",
                relative_directory_message(&request.working_directory),
            ));
        }
        let permission = match &request.permission_mode {
            PermissionMode::Default | PermissionMode::Custom(_) => Policy::Ask,
            PermissionMode::AcceptEdits => Policy::AcceptEdits,
            PermissionMode::FullAccess => Policy::Allow,
            mode => {
                return Err(invalid(
                    "permission_mode",
                    format!("ACP agents support Default, AcceptEdits, FullAccess or an agent mode, not {mode:?}"),
                ))
            }
        };
        let requested_mode = match &request.permission_mode {
            PermissionMode::Custom(mode) if !mode.trim().is_empty() => Some(mode.trim().into()),
            _ => None,
        };
        let requested_model = request
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty() && *model != "default")
            .map(str::to_owned);
        let requested_config: BTreeMap<String, String> = request
            .harness_options
            .get("config")
            .map(|value| serde_json::from_str(value))
            .transpose()
            .map_err(|_| {
                invalid(
                    "harness_options.config",
                    "expected an object of ACP config ids and string values",
                )
            })?
            .unwrap_or_default();
        // Rejected here, before the process starts, rather than after a session
        // is open; `configure_session` keeps the same bound for other callers.
        if requested_config.len() > MAX_REQUESTED_CONFIG {
            return Err(invalid(
                "harness_options.config",
                format!("at most {MAX_REQUESTED_CONFIG} ACP settings may be selected"),
            ));
        }
        let turn = TurnState {
            requested_config,
            agent_label: agent.label.clone(),
            agent_id: agent.id.clone(),
            working_directory: request.working_directory.clone(),
            resume: request
                .session_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_owned),
            instructions: request
                .launch_context
                .system_prompt_append
                .as_deref()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_owned),
            mcp_servers: mcp_servers(request)?,
            requested_mode,
            requested_model,
            permission,
            ..TurnState::default()
        };
        store(state, &turn);
        state.extensions.insert(
            PROMPT_KEY.to_string(),
            Value::String(request.prompt.clone()),
        );
        Ok(())
    }

    fn interrupt_request(&self, state: &AdapterState) -> Option<Vec<u8>> {
        let turn = load(state);
        let session = turn.session_id?;
        let mut frames = vec![notification(
            "session/cancel",
            json!({"sessionId": session}),
        )];
        frames.extend(
            turn.open_permissions
                .iter()
                .map(|(id, _)| reply(id, json!({"outcome": {"outcome": "cancelled"}}))),
        );
        let lines = frames
            .iter()
            .filter_map(|frame| serde_json::to_string(frame).ok())
            .collect::<Vec<_>>()
            .join("\n");
        Some(lines.into_bytes())
    }

    fn interaction_answered(&self, original: &Value, state: &mut AdapterState) {
        let Some(id) = original.get("id") else {
            return;
        };
        let mut turn = load(state);
        let before = turn.open_permissions.len();
        // The reply carries the user's decision, so a later cancellation must
        // not answer the same request again.
        turn.open_permissions.retain(|(open, _)| open != id);
        if turn.open_permissions.len() != before {
            store(state, &turn);
        }
    }

    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
        let mut output = AdapterOutput::default();
        let mut turn = load(state);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(output);
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            // Some agents log to stdout. Say so once instead of failing a turn
            // the protocol itself is still carrying.
            if !turn.warned_noise {
                turn.warned_noise = true;
                output.events.push(TurnEvent::Warning {
                    message: format!(
                        "{} wrote non-ACP output to stdout; it was ignored",
                        turn.agent_label
                    ),
                });
                store(state, &turn);
            }
            return Ok(output);
        };
        let method = value.get("method").and_then(Value::as_str);
        let id = value.get("id").filter(|id| !id.is_null());
        match (method, id) {
            (Some(method), Some(id)) => agent_request(method, id, &value, &mut turn, &mut output)?,
            (Some("session/update"), None) => {
                session_update(&value, state, &mut turn, &mut output);
            }
            (None, Some(_)) => response(&value, state, &mut turn, &mut output)?,
            (Some(_) | None, None) => {}
        }
        store(state, &turn);
        Ok(output)
    }

    fn approval_response(
        &self,
        _request: &ApprovalRequest,
        original: &Value,
        decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>> {
        let id = original
            .get("id")
            .ok_or_else(|| protocol("the permission request has no id"))?;
        let options = original
            .pointer("/params/options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let preferences: &[&str] = match decision {
            ApprovalDecision::Allow => &["allow_once", "allow_always"],
            ApprovalDecision::AllowForSession => &["allow_always", "allow_once"],
            _ => &["reject_once", "reject_always"],
        };
        let outcome = option_for(&options, preferences).map_or_else(
            || json!({"outcome": "cancelled"}),
            |option| json!({"outcome": "selected", "optionId": option}),
        );
        Ok(Some(encode(&reply(id, json!({"outcome": outcome})))?))
    }

    fn question_response(
        &self,
        _request: &QuestionRequest,
        _original: &Value,
        _answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>> {
        // This adapter never raises questions.
        Ok(None)
    }
}

/// What a permission request asks to do, phrased to follow "<tool> wants
/// to": `run \`npm test\``, `edit src/main.rs`. Built from the ACP tool kind
/// and whatever target the request names.
fn permission_action(kind: &str, tool: &Value, input: Option<&Value>) -> String {
    let text = |keys: &[&str]| {
        keys.iter()
            .find_map(|key| input?.get(key)?.as_str())
            .map(|text| bounded(text, MAX_TITLE_CHARS))
    };
    let path = || {
        tool.get("locations")
            .and_then(Value::as_array)
            .and_then(|locations| locations.first())
            .and_then(|location| location.get("path"))
            .and_then(Value::as_str)
            .map(|path| bounded(path, MAX_TITLE_CHARS))
            .or_else(|| text(&["filePath", "file_path", "path"]))
    };
    let on_path = |verb: &str, fallback: &str| {
        path().map_or_else(|| fallback.to_owned(), |path| format!("{verb} {path}"))
    };
    match kind {
        "execute" => text(&["command", "cmd"]).map_or_else(
            || "run a command".to_owned(),
            |command| format!("run `{command}`"),
        ),
        "read" => on_path("read", "read a file"),
        "edit" => on_path("edit", "edit files"),
        "delete" => on_path("delete", "delete files"),
        "move" => on_path("move", "move files"),
        "search" => "search the workspace".to_owned(),
        "fetch" => text(&["url"]).map_or_else(
            || "fetch from the network".to_owned(),
            |url| format!("fetch {url}"),
        ),
        _ => "use this tool".to_owned(),
    }
}

fn option_for(options: &[Value], kinds: &[&str]) -> Option<String> {
    kinds.iter().find_map(|kind| {
        options
            .iter()
            .find(|option| option.get("kind").and_then(Value::as_str) == Some(kind))
            .and_then(|option| option.get("optionId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn canonical_tool_name(name: &str, input: Option<&Value>) -> String {
    if let Some(input) = input.filter(|_| name == "MCP: tool") {
        if let (Some(server), Some(tool)) = (
            input.get("providerIdentifier").and_then(Value::as_str),
            input.get("toolName").and_then(Value::as_str),
        ) {
            let valid = |s: &str| {
                !s.is_empty()
                    && s.len() <= 128
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            };
            if valid(server) && !server.contains("__") && valid(tool) {
                return format!("mcp__{server}__{tool}");
            }
        }
    }
    bounded(name, MAX_TITLE_CHARS)
}

/// Answer a request the agent sent to the client.
fn agent_request(
    method: &str,
    id: &Value,
    value: &Value,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) -> Result<()> {
    if method != "session/request_permission" {
        // Neither file system, terminal nor elicitation capabilities were
        // offered, so a conforming agent never sends these.
        output.writes.push(encode(&reply_error(
            id,
            -32601,
            &format!("{method} is not supported by this client"),
        ))?);
        return Ok(());
    }
    let params = value.get("params").unwrap_or(&Value::Null);
    let tool = params.get("toolCall").unwrap_or(&Value::Null);
    let options = params
        .get("options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let kind = tool.get("kind").and_then(Value::as_str).unwrap_or("other");
    let automatic = match turn.permission {
        Policy::Allow => Some(&["allow_once", "allow_always"][..]),
        Policy::AcceptEdits if matches!(kind, "edit" | "delete" | "move" | "read" | "search") => {
            Some(&["allow_once", "allow_always"][..])
        }
        _ => None,
    };
    if let Some(option) = automatic.and_then(|kinds| option_for(&options, kinds)) {
        output.writes.push(encode(&reply(
            id,
            json!({"outcome": {"outcome": "selected", "optionId": option}}),
        ))?);
        return Ok(());
    }
    let tool_call_id = tool.get("toolCallId").and_then(Value::as_str);
    let tracked = tool_call_id.and_then(|id| turn.tools.iter().find(|tool| tool.id == id));
    // The name the tool call already streamed under, so the request and its
    // tool row read as the same thing.
    let title = tracked
        .map(|tool| tool.title.clone())
        .or_else(|| tool.get("title").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_else(|| format!("{kind} tool"));
    let input = tool
        .get("rawInput")
        .filter(|input| !input.is_null())
        .cloned()
        .or_else(|| tracked.and_then(|tool| tool.input.clone()));
    let title = canonical_tool_name(
        tool.get("name").and_then(Value::as_str).unwrap_or(&title),
        input.as_ref(),
    );
    let description = permission_action(kind, tool, input.as_ref());
    if turn.open_permissions.len() < MAX_OPEN_PERMISSIONS {
        turn.open_permissions
            .push((id.clone(), tool_call_id.map(str::to_owned)));
    }
    output.interaction = Some(InteractionRequest::Approval {
        request: ApprovalRequest {
            id: id.to_string(),
            tool_name: bounded(&title, MAX_TITLE_CHARS),
            input: input
                .unwrap_or_else(|| json!({"kind": kind, "locations": tool.get("locations")})),
            description: Some(description),
        },
        original: value.clone(),
    });
    Ok(())
}

/// Advance the handshake on a response to one of our requests.
fn response(
    value: &Value,
    state: &mut AdapterState,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) -> Result<()> {
    let Some(id) = value.get("id").and_then(Value::as_u64) else {
        return Ok(());
    };
    let error = value.get("error");
    let result = value.get("result").unwrap_or(&Value::Null);
    match id {
        ID_INITIALIZE => {
            // Cold process: discard preparation intended for a retained connection.
            turn.prompt_sent = false;
            turn.pending.clear();
            turn.session_id = None;
            turn.retained_error = None;
            if let Some(error) = error {
                fail(
                    state,
                    output,
                    ProviderProcessErrorKind::Unknown,
                    format!(
                        "{} failed to initialize: {}",
                        turn.agent_label,
                        error_message(error)
                    ),
                    "initialize",
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            let probe = AcpAgentProbe::from_initialize(result);
            if probe.protocol_version != Some(PROTOCOL_VERSION) {
                fail(
                    state,
                    output,
                    ProviderProcessErrorKind::Unknown,
                    format!(
                        "{} speaks ACP protocol version {}, but this client supports version {PROTOCOL_VERSION}",
                        turn.agent_label,
                        probe
                            .protocol_version
                            .map_or_else(|| "unknown".into(), |version| version.to_string())
                    ),
                    "protocol_version",
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            turn.supports_images = result
                .pointer("/agentCapabilities/promptCapabilities/image")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let http = result
                .pointer("/agentCapabilities/mcpCapabilities/http")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !http {
                let before = turn.mcp_servers.len();
                turn.mcp_servers
                    .retain(|server| server.get("type").and_then(Value::as_str) != Some("http"));
                if turn.mcp_servers.len() != before {
                    output.events.push(TurnEvent::Warning {
                        message: format!(
                            "{} cannot connect to HTTP MCP servers, so {} of them were left out of this turn",
                            turn.agent_label,
                            before - turn.mcp_servers.len()
                        ),
                    });
                }
            }
            let cwd = turn.working_directory.clone();
            let servers = Value::Array(turn.mcp_servers.clone());
            let open = match turn.resume.clone() {
                Some(session) if probe.resume_session => request(
                    ID_SESSION,
                    "session/resume",
                    json!({"sessionId": session, "cwd": cwd, "mcpServers": servers}),
                ),
                Some(session) if probe.load_session => {
                    turn.replaying = true;
                    request(
                        ID_SESSION,
                        "session/load",
                        json!({"sessionId": session, "cwd": cwd, "mcpServers": servers}),
                    )
                }
                Some(_) => {
                    output.events.push(TurnEvent::Warning {
                        message: format!(
                            "{} cannot resume ACP sessions, so this message starts a new session without the earlier conversation",
                            turn.agent_label
                        ),
                    });
                    turn.resume = None;
                    request(
                        ID_SESSION,
                        "session/new",
                        json!({"cwd": cwd, "mcpServers": servers}),
                    )
                }
                None => request(
                    ID_SESSION,
                    "session/new",
                    json!({"cwd": cwd, "mcpServers": servers}),
                ),
            };
            output.events.push(TurnEvent::ProviderProcessStatus {
                status: crate::ProviderProcessStatus::Checking,
                message: format!("Opening {}'s ACP session", turn.agent_label),
            });
            output.writes.push(encode(&open)?);
        }
        ID_SESSION => {
            turn.replaying = false;
            if let Some(error) = error {
                let message = error_message(error);
                let (kind, code, diagnostic) = match error_code(error) {
                    Some(AUTH_REQUIRED_CODE) => (
                        ProviderProcessErrorKind::AuthenticationFailed,
                        "auth_required",
                        format!(
                            "{} needs you to sign in before it can start a session ({message}). Sign in with the agent's own CLI, then send the message again.",
                            turn.agent_label
                        ),
                    ),
                    Some(RESOURCE_NOT_FOUND_CODE) if turn.resume.is_some() => (
                        ProviderProcessErrorKind::Unknown,
                        "session_not_found",
                        format!(
                            "{} session `{}` was not found: {message}",
                            turn.agent_label,
                            turn.resume.as_deref().unwrap_or_default()
                        ),
                    ),
                    _ => (
                        ProviderProcessErrorKind::Unknown,
                        "session",
                        format!("{} could not open a session: {message}", turn.agent_label),
                    ),
                };
                fail(
                    state,
                    output,
                    kind,
                    diagnostic,
                    code,
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            let session = match turn.resume.clone() {
                Some(session) => session,
                None => match result.get("sessionId").and_then(Value::as_str) {
                    Some(session) if !session.trim().is_empty() => session.to_string(),
                    _ => {
                        fail(
                            state,
                            output,
                            ProviderProcessErrorKind::Unknown,
                            format!("{} opened a session without an id", turn.agent_label),
                            "session",
                            DeliveryState::NotSent,
                        );
                        return Ok(());
                    }
                },
            };
            state.result.session_id = Some(session.clone());
            turn.session_config = session_snapshot(result);
            turn.session_config["_supportsImages"] = json!(turn.supports_images);
            {
                output.events.push(TurnEvent::SessionStarted {
                    session_id: session.clone(),
                    title: None,
                });
            }
            turn.session_id = Some(session.clone());
            if let Err(message) = configure_session(&session, result, state, turn) {
                fail(
                    state,
                    output,
                    ProviderProcessErrorKind::ModelUnavailable,
                    message,
                    "configure",
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            advance(state, turn, output)?;
        }
        ID_SET_MODE | ID_SET_MODEL | 100..=131 => {
            if let Some(error) = error {
                let what = refused_setting_label(id, turn.inflight_config.as_ref());
                fail(
                    state,
                    output,
                    if id == ID_SET_MODEL {
                        ProviderProcessErrorKind::ModelUnavailable
                    } else {
                        ProviderProcessErrorKind::Unknown
                    },
                    format!(
                        "{} refused the requested {what}: {}",
                        turn.agent_label,
                        error_message(error)
                    ),
                    "configure",
                    DeliveryState::NotSent,
                );
                return Ok(());
            }
            if result.get("configOptions").is_some() {
                turn.session_config = session_snapshot(result);
                turn.session_config["_supportsImages"] = json!(turn.supports_images);
            } else if let Some(frame) = turn.inflight_config.take() {
                let params = &frame["params"];
                if let Some(id) = params.get("configId").and_then(Value::as_str) {
                    if let Some(options) = turn.session_config["configOptions"].as_array_mut() {
                        if let Some(option) = options.iter_mut().find(|o| o["id"] == id) {
                            option["currentValue"] = params["value"].clone();
                        }
                    }
                } else if let Some(mode) = params.get("modeId") {
                    turn.session_config["modes"]["currentModeId"] = mode.clone();
                } else if let Some(model) = params.get("modelId") {
                    turn.session_config["models"]["currentModelId"] = model.clone();
                }
            }
            advance(state, turn, output)?;
        }
        ID_PROMPT => {
            output.terminal = true;
            // Only session metadata survives an idle process. Do not retain prompts,
            // tools or the per-turn MCP credential values in carried parser state.
            state.extensions.remove(PROMPT_KEY);
            turn.tools.clear();
            turn.open_permissions.clear();
            turn.mcp_servers.clear();
            turn.instructions = None;
            turn.retained_start.clear();
            if let Some(error) = error {
                let auth = error_code(error) == Some(AUTH_REQUIRED_CODE);
                fail(
                    state,
                    output,
                    if auth {
                        ProviderProcessErrorKind::AuthenticationFailed
                    } else {
                        ProviderProcessErrorKind::Unknown
                    },
                    format!("{}: {}", turn.agent_label, error_message(error)),
                    if auth { "auth_required" } else { "prompt" },
                    DeliveryState::Accepted,
                );
                return Ok(());
            }
            match result.get("stopReason").and_then(Value::as_str) {
                Some("end_turn") | None => {}
                Some("max_tokens") => output.events.push(TurnEvent::Warning {
                    message: format!("{} stopped at its output token limit", turn.agent_label),
                }),
                Some("max_turn_requests") => output.events.push(TurnEvent::Warning {
                    message: format!(
                        "{} stopped after reaching its limit of model requests for one turn",
                        turn.agent_label
                    ),
                }),
                Some("refusal") => fail(
                    state,
                    output,
                    ProviderProcessErrorKind::Unknown,
                    format!("{} refused to continue this turn", turn.agent_label),
                    "refusal",
                    DeliveryState::Accepted,
                ),
                Some("cancelled") => fail(
                    state,
                    output,
                    ProviderProcessErrorKind::Unknown,
                    format!(
                        "{} cancelled the turn before it completed",
                        turn.agent_label
                    ),
                    "cancelled",
                    DeliveryState::Accepted,
                ),
                Some(other) => output.events.push(TurnEvent::Warning {
                    message: format!("{} stopped: {}", turn.agent_label, bounded(other, 64)),
                }),
            }
        }
        _ => {}
    }
    Ok(())
}

/// What a refused configuration request was changing, for the error text: the
/// mode or model by request id, otherwise the setting named in the request
/// still in flight.
fn refused_setting_label(id: u64, inflight: Option<&Value>) -> String {
    match id {
        ID_SET_MODE => return "mode".into(),
        ID_SET_MODEL => return "model".into(),
        _ => {}
    }
    let params = inflight.map(|frame| &frame["params"]);
    match params {
        Some(p) if p.get("configId").is_some() => {
            format!("setting `{}`", p["configId"].as_str().unwrap_or("?"))
        }
        Some(p) if p.get("modeId").is_some() => "mode".into(),
        _ => "setting".into(),
    }
}

/// Queue mode and model selection for a freshly opened session.
fn configure_session(
    session: &str,
    result: &Value,
    state: &mut AdapterState,
    turn: &mut TurnState,
) -> std::result::Result<(), String> {
    if turn.requested_config.len() > MAX_REQUESTED_CONFIG {
        return Err(format!(
            "At most {MAX_REQUESTED_CONFIG} ACP settings may be selected"
        ));
    }
    let discovered = session_config_options(result);
    for (index, (id, value)) in turn.requested_config.clone().into_iter().enumerate() {
        let option = discovered
            .iter()
            .find(|o| o.id == id)
            .ok_or_else(|| format!("{} does not advertise setting `{id}`", turn.agent_label))?;
        if !option.options.iter().any(|v| v.value == value) {
            return Err(format!(
                "{} has no value `{value}` for {}",
                turn.agent_label, option.name
            ));
        }
        if option.current_value == value {
            continue;
        }
        let native = result
            .get("configOptions")
            .and_then(Value::as_array)
            .is_some_and(|opts| {
                opts.iter()
                    .any(|o| o.get("id").and_then(Value::as_str) == Some(&id))
            });
        let request_id = 100 + index as u64;
        let frame = if native {
            request(
                request_id,
                "session/set_config_option",
                json!({"sessionId":session,"configId":id,"value":value}),
            )
        } else if option.category.as_deref() == Some("mode") {
            request(
                request_id,
                "session/set_mode",
                json!({"sessionId":session,"modeId":value}),
            )
        } else if option.category.as_deref() == Some("model") {
            request(
                request_id,
                "session/set_model",
                json!({"sessionId":session,"modelId":value}),
            )
        } else {
            return Err(format!("{} cannot set `{id}`", turn.agent_label));
        };
        turn.pending.push((request_id, frame));
    }
    if let Some(mode) = turn.requested_mode.clone() {
        let modes = result
            .pointer("/modes/availableModes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let current = result
            .pointer("/modes/currentModeId")
            .and_then(Value::as_str);
        let available = modes
            .iter()
            .filter_map(|entry| entry.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>();
        if !available.contains(&mode.as_str()) {
            return Err(if available.is_empty() {
                format!(
                    "{} has no session modes, so mode `{mode}` cannot be selected",
                    turn.agent_label
                )
            } else {
                format!(
                    "{} has no mode `{mode}`; it offers {}",
                    turn.agent_label,
                    available
                        .iter()
                        .take(MAX_LISTED_CHOICES)
                        .copied()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            });
        }
        if current != Some(mode.as_str()) {
            turn.pending.push((
                ID_SET_MODE,
                request(
                    ID_SET_MODE,
                    "session/set_mode",
                    json!({"sessionId": session, "modeId": mode}),
                ),
            ));
        }
    }
    let config_model = result
        .get("configOptions")
        .and_then(Value::as_array)
        .and_then(|options| {
            options.iter().find(|option| {
                option.get("category").and_then(Value::as_str) == Some("model")
                    || option.get("id").and_then(Value::as_str) == Some("model")
            })
        })
        .cloned();
    let unstable_models = result.get("models").cloned();
    state.result.model = config_model
        .as_ref()
        .and_then(|option| option.get("currentValue"))
        .or_else(|| {
            unstable_models
                .as_ref()
                .and_then(|models| models.get("currentModelId"))
        })
        .and_then(Value::as_str)
        .map(str::to_owned);
    let Some(model) = turn.requested_model.clone() else {
        return Ok(());
    };
    if let Some(option) = config_model {
        let values = select_values(&option);
        if !values.iter().any(|value| value == &model) {
            return Err(choices_error(&turn.agent_label, &model, &values));
        }
        state.result.model = Some(model.clone());
        if option.get("currentValue").and_then(Value::as_str) != Some(model.as_str()) {
            let config_id = option.get("id").cloned().unwrap_or(Value::Null);
            turn.pending.push((
                ID_SET_MODEL,
                request(
                    ID_SET_MODEL,
                    "session/set_config_option",
                    json!({"sessionId": session, "configId": config_id, "value": model}),
                ),
            ));
        }
        return Ok(());
    }
    if let Some(models) = unstable_models {
        let values = models
            .get("availableModels")
            .and_then(Value::as_array)
            .map(|models| {
                models
                    .iter()
                    .filter_map(|model| model.get("modelId").and_then(Value::as_str))
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !values.iter().any(|value| value == &model) {
            return Err(choices_error(&turn.agent_label, &model, &values));
        }
        state.result.model = Some(model.clone());
        if models.get("currentModelId").and_then(Value::as_str) != Some(model.as_str()) {
            turn.pending.push((
                ID_SET_MODEL,
                request(
                    ID_SET_MODEL,
                    "session/set_model",
                    json!({"sessionId": session, "modelId": model}),
                ),
            ));
        }
        return Ok(());
    }
    Err(format!(
        "{} does not let clients choose a model, so `{model}` cannot be selected; choose the model in the agent's own settings",
        turn.agent_label
    ))
}

fn select_values(option: &Value) -> Vec<String> {
    let mut values = Vec::new();
    for entry in option
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(value) = entry.get("value").and_then(Value::as_str) {
            values.push(value.to_string());
        }
        for nested in entry
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(value) = nested.get("value").and_then(Value::as_str) {
                values.push(value.to_string());
            }
        }
    }
    values
}

fn session_snapshot(result: &Value) -> Value {
    let options = session_config_options(result);
    let native_ids = result
        .get("configOptions")
        .and_then(Value::as_array)
        .map(|v| {
            v.iter()
                .filter_map(|v| v.get("id").and_then(Value::as_str))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut snapshot = json!({"configOptions": options.iter().filter(|o| native_ids.contains(&o.id.as_str())).collect::<Vec<_>>()});
    for option in &options {
        if native_ids.contains(&option.id.as_str()) {
            continue;
        }
        if option.category.as_deref() == Some("model") {
            snapshot["models"] = json!({"currentModelId": option.current_value, "availableModels": option.options.iter().map(|v| json!({"modelId":v.value,"name":v.name})).collect::<Vec<_>>()});
        } else if option.category.as_deref() == Some("mode") {
            snapshot["modes"] = json!({"currentModeId": option.current_value, "availableModes": option.options.iter().map(|v| json!({"id":v.value,"name":v.name})).collect::<Vec<_>>()});
        }
    }
    snapshot
}

fn choices_error(agent: &str, model: &str, values: &[String]) -> String {
    format!(
        "{agent} has no model `{model}`; it offers {}",
        if values.is_empty() {
            "none".to_string()
        } else {
            values
                .iter()
                .take(MAX_LISTED_CHOICES)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        }
    )
}

/// Send the next queued configuration request, or the prompt once none remain.
fn advance(
    state: &mut AdapterState,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) -> Result<()> {
    if !turn.pending.is_empty() {
        let (_, next) = turn.pending.remove(0);
        output.events.push(TurnEvent::ProviderProcessStatus {
            status: crate::ProviderProcessStatus::Checking,
            message: format!("Applying {} settings", turn.agent_label),
        });
        turn.inflight_config = Some(next.clone());
        output.writes.push(encode(&next)?);
        return Ok(());
    }
    if turn.prompt_sent {
        return Ok(());
    }
    let Some(session) = turn.session_id.clone() else {
        return Err(protocol("the ACP session is not open"));
    };
    let prompt = state
        .extensions
        .get(PROMPT_KEY)
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default();
    let mut blocks = Vec::with_capacity(2);
    // A continued session already received the instructions.
    if turn.resume.is_none() {
        if let Some(instructions) = turn.instructions.take() {
            blocks.push(json!({"type": "text", "text": instructions}));
        }
    }
    blocks.push(json!({"type": "text", "text": prompt}));
    output.writes.push(encode(&request(
        ID_PROMPT,
        "session/prompt",
        json!({"sessionId": session, "prompt": blocks}),
    ))?);
    turn.prompt_sent = true;
    output.turn_submitted = true;
    Ok(())
}

/// Apply a session update that changes the agent's own settings to the
/// snapshot later turns compare their requested settings against. Returns
/// whether `update` was one.
fn apply_setting_update(update: &Value, turn: &mut TurnState) -> bool {
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("config_option_update") => {
            // An update carries the native options only: the legacy `modes`
            // and `models` the session opened with still hold until the agent
            // replaces them, so a later custom mode or model stays selectable.
            let mut snapshot = session_snapshot(update);
            for key in ["modes", "models", "_supportsImages"] {
                if snapshot.get(key).is_none() {
                    if let Some(kept) = turn.session_config.get(key) {
                        snapshot[key] = kept.clone();
                    }
                }
            }
            turn.session_config = snapshot;
            true
        }
        Some("current_mode_update") => {
            if let Some(mode) = update.get("currentModeId") {
                turn.session_config["modes"]["currentModeId"] = mode.clone();
            }
            true
        }
        _ => false,
    }
}

fn session_update(
    value: &Value,
    state: &mut AdapterState,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) {
    let params = value.get("params").unwrap_or(&Value::Null);
    if turn.replaying {
        return;
    }
    if let (Some(ours), Some(theirs)) = (
        turn.session_id.as_deref(),
        params.get("sessionId").and_then(Value::as_str),
    ) {
        if ours != theirs {
            return;
        }
    }
    let update = params.get("update").unwrap_or(&Value::Null);
    match update
        .get("sessionUpdate")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "agent_message_chunk" => {
            if let Some(text) = update.get("content").and_then(block_text) {
                if !text.is_empty() {
                    state.saw_text_delta = true;
                    state.result.text.push_str(&text);
                    output.events.push(TurnEvent::TextDelta { text });
                }
            }
        }
        "agent_thought_chunk" => {
            if let Some(text) = update.get("content").and_then(block_text) {
                if !text.is_empty() {
                    state
                        .result
                        .reasoning
                        .get_or_insert_with(String::new)
                        .push_str(&text);
                    output.events.push(TurnEvent::ReasoningDelta { text });
                }
            }
        }
        "tool_call" => tool_call(update, turn, output, true),
        "tool_call_update" => tool_call(update, turn, output, false),
        "plan" => {
            let entries = update
                .get("entries")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|entry| {
                            let content = entry.get("content").and_then(Value::as_str)?;
                            Some(PlanEntry {
                                content: bounded(content, MAX_DIAGNOSTIC_CHARS),
                                status: match entry.get("status").and_then(Value::as_str) {
                                    Some("completed") => PlanEntryStatus::Completed,
                                    Some("in_progress") => PlanEntryStatus::InProgress,
                                    _ => PlanEntryStatus::Pending,
                                },
                                priority: entry
                                    .get("priority")
                                    .and_then(Value::as_str)
                                    .map(|priority| bounded(priority, 16)),
                            })
                        })
                        .take(MAX_TRACKED_TOOLS)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            output.events.push(TurnEvent::PlanUpdated { entries });
        }
        "config_option_update" | "current_mode_update" => {
            apply_setting_update(update, turn);
        }
        "usage_update" => {
            let used = update.get("used").and_then(Value::as_u64);
            let size = update.get("size").and_then(Value::as_u64);
            let cost_usd = update
                .get("cost")
                .filter(|cost| {
                    cost.get("currency")
                        .and_then(Value::as_str)
                        .is_some_and(|currency| currency.eq_ignore_ascii_case("usd"))
                })
                .and_then(|cost| cost.get("amount"))
                .and_then(Value::as_f64);
            let usage = Usage {
                context_window: Some(ContextWindowUsage {
                    used_tokens: used,
                    limit_tokens: size,
                    model: state.result.model.clone(),
                    estimated: false,
                }),
                cost_usd,
                ..Usage::default()
            };
            state
                .result
                .usage
                .context_window
                .clone_from(&usage.context_window);
            if cost_usd.is_some() {
                state.result.usage.cost_usd = cost_usd;
            }
            output.events.push(TurnEvent::Usage(usage));
        }
        "session_info_update" => {
            if let Some(title) = update
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
            {
                state.result.session_title = Some(bounded(title, MAX_TITLE_CHARS));
            }
        }
        "notice" => {
            if let Some(title) = update.get("title").and_then(Value::as_str) {
                let description = update
                    .get("description")
                    .and_then(Value::as_str)
                    .map(|text| format!(": {}", bounded(text, MAX_DIAGNOSTIC_CHARS)))
                    .unwrap_or_default();
                output.events.push(TurnEvent::Warning {
                    message: format!("{}{description}", bounded(title, MAX_TITLE_CHARS)),
                });
            }
        }
        // User echoes, command lists, mode and config changes carry nothing a
        // turn result needs.
        _ => {}
    }
}

fn tool_call(update: &Value, turn: &mut TurnState, output: &mut AdapterOutput, created: bool) {
    let Some(id) = update.get("toolCallId").and_then(Value::as_str) else {
        return;
    };
    let reported_input = update
        .get("rawInput")
        .filter(|input| !input.is_null())
        .cloned();
    let index = if let Some(index) = turn.tools.iter().position(|tool| tool.id == id) {
        index
    } else {
        // The first title names the tool for the rest of the turn; agents
        // often retitle a call with its argument (a file path) later.
        let title = update
            .get("name")
            .or_else(|| update.get("title"))
            .and_then(Value::as_str)
            .map(|title| bounded(title, MAX_TITLE_CHARS))
            .or_else(|| {
                update
                    .get("kind")
                    .and_then(Value::as_str)
                    .map(|kind| format!("{kind} tool"))
            })
            .unwrap_or_else(|| "tool".into());
        if turn.tools.len() >= MAX_TRACKED_TOOLS {
            turn.tools.remove(0);
        }
        turn.tools.push(TrackedTool {
            id: id.to_string(),
            title: canonical_tool_name(&title, reported_input.as_ref()),
            input: None,
            output: None,
        });
        turn.tools.len() - 1
    };
    if let Some(input) = &reported_input {
        let small =
            serde_json::to_vec(input).is_ok_and(|bytes| bytes.len() <= MAX_KEPT_INPUT_BYTES);
        turn.tools[index].input = small.then(|| input.clone());
    }
    // An agent may stream output in `in_progress` updates and finish with a
    // bare `status: completed`; keep the latest so the final event is complete.
    let reported_output = update
        .get("content")
        .and_then(tool_content_text)
        .or_else(|| update.get("rawOutput").and_then(raw_output_text))
        .map(|text| bounded(&text, MAX_TOOL_OUTPUT_CHARS));
    if let Some(text) = &reported_output {
        turn.tools[index].output = Some(text.clone());
    }
    let title = turn.tools[index].title.clone();
    let input = reported_input.or_else(|| turn.tools[index].input.clone());
    let status = update.get("status").and_then(Value::as_str);
    if status.is_some_and(|status| status != "pending") {
        // A tool that moved on is no longer waiting for permission.
        turn.open_permissions
            .retain(|(_, tool)| tool.as_deref() != Some(id));
    }
    match status {
        Some("completed" | "failed") => {
            turn.tools[index].input = None;
            let failed = status == Some("failed");
            let text = turn.tools[index].output.take();
            output.events.push(TurnEvent::ToolCall {
                id: Some(id.to_string()),
                name: title,
                status: if failed {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Succeeded
                },
                input,
                output: if failed { None } else { text.clone() },
                error: if failed {
                    Some(text.unwrap_or_else(|| "the tool reported an error".into()))
                } else {
                    None
                },
                task_id: None,
                images: Vec::new(),
            });
        }
        _ if created || update.get("rawInput").is_some() || update.get("title").is_some() => {
            output.events.push(TurnEvent::ToolCall {
                id: Some(id.to_string()),
                name: title,
                status: ToolCallStatus::Started,
                input,
                output: None,
                error: None,
                task_id: None,
                images: Vec::new(),
            });
        }
        _ => {}
    }
}

fn block_text(block: &Value) -> Option<String> {
    match block.get("type").and_then(Value::as_str)? {
        "text" => block.get("text").and_then(Value::as_str).map(str::to_owned),
        "resource_link" => block
            .get("uri")
            .or_else(|| block.get("name"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        "resource" => block
            .pointer("/resource/text")
            .or_else(|| block.pointer("/resource/uri"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        "image" => Some("[image]".into()),
        "audio" => Some("[audio]".into()),
        _ => None,
    }
}

fn tool_content_text(content: &Value) -> Option<String> {
    let parts = content
        .as_array()?
        .iter()
        .filter_map(|item| match item.get("type").and_then(Value::as_str)? {
            "content" => item.get("content").and_then(block_text),
            "diff" => {
                let path = item.get("path").and_then(Value::as_str).unwrap_or("a file");
                let new_text = item
                    .get("newText")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                Some(if item.get("oldText").is_some_and(|old| !old.is_null()) {
                    format!("Edited {path}\n{new_text}")
                } else {
                    format!("Created {path}\n{new_text}")
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn raw_output_text(raw: &Value) -> Option<String> {
    match raw {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agents() -> AcpAgents {
        let agents = AcpAgents::new();
        agents
            .insert(AcpAgent::new("gemini", "Gemini CLI", "gemini", ["--acp"]))
            .unwrap();
        agents
    }

    fn turn(prompt: &str) -> TurnRequest {
        let mut request = TurnRequest::new(Provider::Acp, std::env::temp_dir(), prompt);
        request
            .harness_options
            .insert(AGENT_OPTION.into(), "gemini".into());
        request
    }

    fn started(adapter: &Acp, request: &TurnRequest) -> AdapterState {
        let mut state = AdapterState::default();
        adapter.prepare_turn(request, &mut state).unwrap();
        state
    }

    fn feed(adapter: &Acp, state: &mut AdapterState, frame: Value) -> AdapterOutput {
        adapter.parse_line(&frame.to_string(), state).unwrap()
    }

    fn written(output: &AdapterOutput) -> Vec<Value> {
        output
            .writes
            .iter()
            .flat_map(|bytes| {
                String::from_utf8_lossy(bytes)
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<Value>>()
            })
            .collect()
    }

    fn initialized(capabilities: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": ID_INITIALIZE, "result": {
            "protocolVersion": 1, "agentCapabilities": capabilities, "authMethods": []
        }})
    }

    #[test]
    fn idle_notifications_keep_the_connection_and_requests_are_answered_without_approval() {
        let adapter = Acp::new(agents());
        let notice = json!({"jsonrpc":"2.0","method":"vendor/activity","params":{}}).to_string();
        assert_eq!(
            adapter.classify_idle_frame(&notice),
            crate::adapter::IdleFrame::Background
        );
        assert_eq!(adapter.idle_frame_writes(&notice), Vec::<Vec<u8>>::new());
        let permission =
            json!({"jsonrpc":"2.0","id":"p","method":"session/request_permission","params":{}})
                .to_string();
        let replies = adapter.idle_frame_writes(&permission);
        let reply: Value = serde_json::from_slice(&replies[0]).unwrap();
        assert_eq!(reply["result"]["outcome"]["outcome"], "cancelled");
    }

    #[test]
    fn config_discovery_prefers_native_grouped_values_and_falls_back_to_legacy() {
        let result = json!({"configOptions":[{"id":"choose-model","name":"Model","category":"model","type":"select","currentValue":"vendor/a","options":[{"group":"Vendor","options":[{"value":"vendor/a","name":"A"}]}]}],"models":{"currentModelId":"old","availableModels":[{"modelId":"old","name":"Old"}]},"modes":{"currentModeId":"ask","availableModes":[{"id":"ask","name":"Ask"}]}});
        let options = session_config_options(&result);
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].id, "choose-model");
        assert_eq!(options[0].options[0].value, "vendor/a");
        assert_eq!(options[1].category.as_deref(), Some("mode"));
        assert_eq!(session_config_options(&json!({})), Vec::new());
        assert_eq!(
            session_config_options(&json!({"configOptions":[{"id":"bad","type":"unknown"}]})),
            Vec::new()
        );
    }

    #[test]
    fn resumed_sessions_announce_ready_and_retained_turns_skip_handshake_and_load() {
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(
            &adapter,
            &turn("first"),
            json!({"sessionId":"s","configOptions":[{"id":"mode","name":"Mode","category":"mode","type":"select","currentValue":"agent","options":[{"value":"agent","name":"Agent"},{"value":"ask","name":"Ask"}]}]}),
        );
        feed(
            &adapter,
            &mut state,
            json!({"jsonrpc":"2.0","id":ID_PROMPT,"result":{"stopReason":"end_turn"}}),
        );
        let mut request = turn("second");
        request.session_id = Some("s".into());
        request
            .harness_options
            .insert("config".into(), r#"{"mode":"ask"}"#.into());
        let mut next = started(&adapter, &request);
        adapter.inherit_retained_handoff(state, &mut next);
        let bytes = adapter.retained_turn_start(&next).unwrap().unwrap();
        let first: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(first["method"], "session/set_config_option");
        assert_eq!(first["params"]["value"], "ask");
        let out = feed(
            &adapter,
            &mut next,
            json!({"jsonrpc":"2.0","id":100,"result":{}}),
        );
        assert_eq!(written(&out)[0]["method"], "session/prompt");
        assert_eq!(written(&out)[0]["params"]["prompt"][0]["text"], "second");
        // Cold fallback still opens the provider session and never loses the prompt.
        let mut cold = started(&adapter, &request);
        adapter.mark_retained_turn(&mut cold);
        feed(
            &adapter,
            &mut cold,
            initialized(json!({"loadSession":true})),
        );
        let out = feed(
            &adapter,
            &mut cold,
            json!({"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}),
        );
        assert!(out
            .events
            .iter()
            .any(|e| matches!(e, TurnEvent::SessionStarted { .. })));
    }

    #[test]
    fn a_setting_the_agent_changed_while_idle_is_not_skipped_as_already_current() {
        let adapter = Acp::new(agents());
        let session = json!({"sessionId":"s","configOptions":[{"id":"effort","name":"Effort","category":"thought_level","type":"select","currentValue":"low","options":[{"value":"low","name":"Low"},{"value":"high","name":"High"}]}]});
        let finished = || {
            let (mut state, _) = opened(&adapter, &turn("first"), session.clone());
            feed(
                &adapter,
                &mut state,
                json!({"jsonrpc":"2.0","id":ID_PROMPT,"result":{"stopReason":"end_turn"}}),
            );
            state
        };
        let mut request = turn("second");
        request.session_id = Some("s".into());
        request
            .harness_options
            .insert("config".into(), r#"{"effort":"low"}"#.into());
        let update = |value: &str, id: &str| {
            json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":id,
                "update":{"sessionUpdate":"config_option_update","configOptions":[
                    {"id":"effort","name":"Effort","category":"thought_level","type":"select","currentValue":value,
                     "options":[{"value":"low","name":"Low"},{"value":"high","name":"High"}]}]}}})
            .to_string()
        };

        // Nothing changed while idle: `low` is already current, so the next
        // turn goes straight to the prompt.
        let mut next = started(&adapter, &request);
        adapter.inherit_retained_handoff(finished(), &mut next);
        let first: Value =
            serde_json::from_slice(&adapter.retained_turn_start(&next).unwrap().unwrap()).unwrap();
        assert_eq!(first["method"], "session/prompt");

        // The agent switched itself to `high` while idle: `low` is requested again.
        let mut inherited = finished();
        adapter.observe_idle_frame(&update("high", "s"), &mut inherited);
        let mut next = started(&adapter, &request);
        adapter.inherit_retained_handoff(inherited, &mut next);
        let first: Value =
            serde_json::from_slice(&adapter.retained_turn_start(&next).unwrap().unwrap()).unwrap();
        assert_eq!(first["method"], "session/set_config_option");
        assert_eq!(first["params"]["value"], "low");

        // A frame for another session, a request, or other noise changes nothing.
        let mut inherited = finished();
        adapter.observe_idle_frame(&update("high", "other"), &mut inherited);
        adapter.observe_idle_frame(
            &json!({"jsonrpc":"2.0","id":9,"method":"session/update","params":{"sessionId":"s"}})
                .to_string(),
            &mut inherited,
        );
        adapter.observe_idle_frame("not json", &mut inherited);
        let mut next = started(&adapter, &request);
        adapter.inherit_retained_handoff(inherited, &mut next);
        let first: Value =
            serde_json::from_slice(&adapter.retained_turn_start(&next).unwrap().unwrap()).unwrap();
        assert_eq!(first["method"], "session/prompt");
    }

    #[test]
    fn a_native_config_update_keeps_the_legacy_modes_the_session_opened_with() {
        let adapter = Acp::new(agents());
        let session = json!({"sessionId":"s",
            "modes":{"currentModeId":"code","availableModes":[{"id":"code","name":"Code"},{"id":"architect","name":"Architect"}]},
            "configOptions":[{"id":"effort","name":"Effort","category":"thought_level","type":"select","currentValue":"low","options":[{"value":"low","name":"Low"},{"value":"high","name":"High"}]}]});
        let (mut state, _) = opened(&adapter, &turn("first"), session);
        feed(
            &adapter,
            &mut state,
            json!({"jsonrpc":"2.0","id":ID_PROMPT,"result":{"stopReason":"end_turn"}}),
        );
        // The update lists the native options only, as agents send it.
        adapter.observe_idle_frame(
            &json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s",
                "update":{"sessionUpdate":"config_option_update","configOptions":[
                    {"id":"effort","name":"Effort","category":"thought_level","type":"select","currentValue":"high",
                     "options":[{"value":"low","name":"Low"},{"value":"high","name":"High"}]}]}}})
            .to_string(),
            &mut state,
        );
        let snapshot = load(&state).session_config;
        assert_eq!(snapshot["configOptions"][0]["currentValue"], "high");
        assert_eq!(snapshot["modes"]["currentModeId"], "code");

        // The next turn can still select the custom mode the session offers.
        let mut request = turn("second");
        request.session_id = Some("s".into());
        request.permission_mode = PermissionMode::Custom("architect".into());
        let mut next = started(&adapter, &request);
        adapter.inherit_retained_handoff(state, &mut next);
        let first: Value = serde_json::from_slice(
            &adapter
                .retained_turn_start(&next)
                .expect("the custom mode is still advertised")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first["method"], "session/set_mode");
        assert_eq!(first["params"]["modeId"], "architect");
    }

    #[test]
    fn permission_names_use_standard_names_or_explicit_mcp_wrapper_metadata() {
        assert_eq!(
            canonical_tool_name(
                "MCP: tool",
                Some(&json!({"providerIdentifier":"temps_fleet","toolName":"list_daemons"}))
            ),
            "mcp__temps_fleet__list_daemons"
        );
        assert_eq!(
            canonical_tool_name(
                "execute",
                Some(&json!({"providerIdentifier":"temps_fleet","toolName":"list_daemons"}))
            ),
            "execute"
        );
        assert_eq!(
            canonical_tool_name(
                "MCP: tool",
                Some(&json!({"providerIdentifier":"other/invalid","toolName":"list_daemons"}))
            ),
            "MCP: tool"
        );
    }

    #[test]
    fn launches_the_selected_agent_and_starts_with_initialize() {
        let adapter = Acp::new(agents());
        let spec = adapter.command(&turn("hi")).unwrap();
        assert_eq!(spec.program, PathBuf::from("gemini"));
        assert_eq!(spec.args, vec![std::ffi::OsString::from("--acp")]);
        assert!(spec.interactive_stdin);
        let first: Value = serde_json::from_slice(spec.initial_stdin.as_ref().unwrap()).unwrap();
        assert_eq!(first["method"], "initialize");
        assert_eq!(first["params"]["protocolVersion"], 1);
        assert_eq!(first["params"]["clientCapabilities"]["terminal"], false);
    }

    #[test]
    fn an_unknown_or_missing_agent_is_rejected_before_spawning() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request.harness_options.clear();
        assert!(adapter
            .command(&request)
            .unwrap_err()
            .to_string()
            .contains("choose which ACP agent"));
        request
            .harness_options
            .insert(AGENT_OPTION.into(), "copilot".into());
        assert!(adapter
            .command(&request)
            .unwrap_err()
            .to_string()
            .contains("`copilot`"));
    }

    #[test]
    fn a_tool_keeps_its_name_and_input_when_later_updates_omit_them() {
        // `opencode acp` names a call by its tool, then retitles it with the
        // file path and completes it without `rawInput`.
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(&adapter, &turn("hi"), json!({"sessionId": "s"}));
        let update = |update: Value| {
            json!({"jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": "s", "update": update}})
        };
        feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call", "toolCallId": "w", "title": "write", "kind": "edit", "status": "pending", "rawInput": {}}),
            ),
        );
        feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "w", "status": "in_progress",
            "rawInput": {"filePath": "/repo/hello.txt", "content": "hi"}}),
            ),
        );
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "w", "status": "completed",
            "title": "hello.txt", "content": [{"type": "content", "content": {"type": "text", "text": "Wrote file successfully."}}]}),
            ),
        );
        let Some(TurnEvent::ToolCall {
            name,
            input,
            status,
            ..
        }) = out.events.first()
        else {
            panic!("expected the completed call");
        };
        assert_eq!(name, "write");
        assert_eq!(*status, ToolCallStatus::Succeeded);
        assert_eq!(input.as_ref().unwrap()["filePath"], "/repo/hello.txt");
    }

    #[test]
    fn registered_agents_are_advertised_as_the_agent_control() {
        let adapter = Acp::new(agents());
        let groups = adapter.control_groups();
        assert_eq!(groups[0].id, AGENT_OPTION);
        assert_eq!(groups[0].kind, HarnessControlKind::Agent);
        assert_eq!(groups[0].options[0].id, "gemini");
        assert_eq!(groups[0].options[0].description, "gemini --acp");
        // The registry is shared: a later registration is advertised at once.
        adapter
            .agents()
            .insert(AcpAgent::new("kimi", "Kimi CLI", "kimi", ["acp"]))
            .unwrap();
        assert_eq!(adapter.control_groups()[0].options.len(), 2);
    }

    #[test]
    fn agent_ids_are_validated() {
        assert!(AcpAgent::new("Gemini", "x", "gemini", ["--acp"])
            .validate()
            .is_err());
        assert!(AcpAgent::new("", "x", "gemini", ["--acp"])
            .validate()
            .is_err());
        assert!(AcpAgent::new("-x", "x", "gemini", ["--acp"])
            .validate()
            .is_err());
        assert!(AcpAgent::new("kimi-cli.2", "x", "kimi", ["acp"])
            .validate()
            .is_ok());
        assert!(AcpAgent::new("ok", "x", "", Vec::<String>::new())
            .validate()
            .is_err());
    }

    #[test]
    fn a_new_session_streams_until_the_prompt_returns() {
        let adapter = Acp::new(agents());
        let mut request = turn("fix it");
        request.launch_context.system_prompt_append = Some("Fleet rules".into());
        let mut state = started(&adapter, &request);
        let out = feed(&adapter, &mut state, initialized(json!({})));
        let frames = written(&out);
        assert_eq!(frames[0]["method"], "session/new");
        assert_eq!(frames[0]["params"]["mcpServers"], json!([]));

        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION, "result": {"sessionId": "s-1"}}),
        );
        assert!(
            matches!(&out.events[0], TurnEvent::SessionStarted { session_id, .. } if session_id == "s-1")
        );
        let frames = written(&out);
        assert_eq!(frames[0]["method"], "session/prompt");
        let blocks = frames[0]["params"]["prompt"].as_array().unwrap();
        assert_eq!(blocks[0]["text"], "Fleet rules");
        assert_eq!(blocks[1]["text"], "fix it");
        assert!(out.turn_submitted);

        let update = |update: Value| {
            json!({"jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": "s-1", "update": update}})
        };
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "agent_thought_chunk", "content": {"type": "text", "text": "thinking"}}),
            ),
        );
        assert!(matches!(&out.events[0], TurnEvent::ReasoningDelta { text } if text == "thinking"));
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "Read main.rs", "kind": "read", "status": "pending", "rawInput": {"path": "main.rs"}}),
            ),
        );
        assert!(
            matches!(&out.events[0], TurnEvent::ToolCall { status: ToolCallStatus::Started, name, .. } if name == "Read main.rs")
        );
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed",
            "content": [{"type": "content", "content": {"type": "text", "text": "fn main() {}"}}]}),
            ),
        );
        assert!(
            matches!(&out.events[0], TurnEvent::ToolCall { status: ToolCallStatus::Succeeded, name, output: Some(text), .. } if name == "Read main.rs" && text == "fn main() {}")
        );
        let out = feed(
            &adapter,
            &mut state,
            update(json!({"sessionUpdate": "plan", "entries": [
            {"content": "Read", "priority": "high", "status": "completed"},
            {"content": "Fix", "priority": "medium", "status": "in_progress"}]})),
        );
        assert!(
            matches!(&out.events[0], TurnEvent::PlanUpdated { entries } if entries.len() == 2 && entries[1].status == PlanEntryStatus::InProgress)
        );
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "Done."}}),
            ),
        );
        assert!(matches!(&out.events[0], TurnEvent::TextDelta { text } if text == "Done."));
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "usage_update", "used": 1200, "size": 200_000, "cost": {"amount": 0.01, "currency": "USD"}}),
            ),
        );
        assert!(
            matches!(&out.events[0], TurnEvent::Usage(usage) if usage.context_window.as_ref().unwrap().used_tokens == Some(1200))
        );
        // Another session's updates are not this turn's.
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": "other", "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "x"}}}}),
        );
        assert_eq!(out.events.len(), 0);

        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_PROMPT, "result": {"stopReason": "end_turn"}}),
        );
        assert!(out.terminal);
        assert!(state.terminal_failure.is_none());
        assert_eq!(state.result.text, "Done.");
        assert_eq!(state.result.session_id.as_deref(), Some("s-1"));
        assert_eq!(state.result.usage.cost_usd, Some(0.01));
    }

    #[test]
    fn resume_prefers_session_resume_and_skips_instructions() {
        let adapter = Acp::new(agents());
        let mut request = turn("again");
        request.session_id = Some("s-1".into());
        request.launch_context.system_prompt_append = Some("Fleet rules".into());
        let mut state = started(&adapter, &request);
        let out = feed(
            &adapter,
            &mut state,
            initialized(json!({"loadSession": true, "sessionCapabilities": {"resume": {}}})),
        );
        assert_eq!(written(&out)[0]["method"], "session/resume");
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION, "result": {}}),
        );
        assert!(
            out.events
                .iter()
                .any(|e| matches!(e, TurnEvent::SessionStarted { .. })),
            "a resumed session announces readiness"
        );
        let prompt = &written(&out)[0];
        assert_eq!(prompt["params"]["sessionId"], "s-1");
        assert_eq!(prompt["params"]["prompt"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn session_load_replay_is_dropped() {
        let adapter = Acp::new(agents());
        let mut request = turn("again");
        request.session_id = Some("s-1".into());
        let mut state = started(&adapter, &request);
        let out = feed(
            &adapter,
            &mut state,
            initialized(json!({"loadSession": true})),
        );
        assert_eq!(written(&out)[0]["method"], "session/load");
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": "s-1", "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "old"}}}}),
        );
        assert_eq!(out.events.len(), 0);
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION, "result": null}),
        );
        assert_eq!(written(&out)[0]["method"], "session/prompt");
        assert_eq!(state.result.text.len(), 0);
    }

    #[test]
    fn an_agent_that_cannot_resume_starts_fresh_and_says_so() {
        let adapter = Acp::new(agents());
        let mut request = turn("again");
        request.session_id = Some("s-1".into());
        request.launch_context.system_prompt_append = Some("Fleet rules".into());
        let mut state = started(&adapter, &request);
        let out = feed(&adapter, &mut state, initialized(json!({})));
        assert!(
            matches!(&out.events[0], TurnEvent::Warning { message } if message.contains("cannot resume"))
        );
        assert_eq!(written(&out)[0]["method"], "session/new");
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION, "result": {"sessionId": "s-2"}}),
        );
        assert!(
            matches!(&out.events[0], TurnEvent::SessionStarted { session_id, .. } if session_id == "s-2")
        );
        assert_eq!(
            written(&out)[0]["params"]["prompt"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn auth_required_fails_before_the_prompt_with_a_sign_in_hint() {
        let adapter = Acp::new(agents());
        let mut state = started(&adapter, &turn("hi"));
        feed(&adapter, &mut state, initialized(json!({})));
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION,
            "error": {"code": -32000, "message": "Authentication required"}}),
        );
        assert!(out.terminal);
        let failure = state.terminal_failure.as_ref().unwrap();
        assert_eq!(failure.kind, ProviderProcessErrorKind::AuthenticationFailed);
        assert_eq!(failure.delivery, DeliveryState::NotSent);
        assert_eq!(failure.provider_code.as_deref(), Some("acp::auth_required"));
        assert!(failure
            .diagnostic
            .contains("Gemini CLI needs you to sign in"));
    }

    #[test]
    fn a_missing_resumed_session_fails_instead_of_starting_over() {
        let adapter = Acp::new(agents());
        let mut request = turn("again");
        request.session_id = Some("gone".into());
        let mut state = started(&adapter, &request);
        feed(
            &adapter,
            &mut state,
            initialized(json!({"loadSession": true})),
        );
        feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION,
            "error": {"code": -32002, "message": "Resource not found"}}),
        );
        let failure = state.terminal_failure.as_ref().unwrap();
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("acp::session_not_found")
        );
    }

    #[test]
    fn a_newer_protocol_version_is_refused() {
        let adapter = Acp::new(agents());
        let mut state = started(&adapter, &turn("hi"));
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": 2}}),
        );
        assert!(out.terminal);
        assert!(state
            .terminal_failure
            .as_ref()
            .unwrap()
            .diagnostic
            .contains("version 2"));
    }

    fn opened(
        adapter: &Acp,
        request: &TurnRequest,
        session: Value,
    ) -> (AdapterState, AdapterOutput) {
        let mut state = started(adapter, request);
        feed(adapter, &mut state, initialized(json!({})));
        let out = feed(
            adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SESSION, "result": session}),
        );
        (state, out)
    }

    #[test]
    fn a_custom_mode_and_model_are_selected_before_the_prompt() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request.permission_mode = PermissionMode::Custom("architect".into());
        request.model = Some("pro".into());
        let session = json!({"sessionId": "s",
            "modes": {"currentModeId": "code", "availableModes": [{"id": "code", "name": "Code"}, {"id": "architect", "name": "Architect"}]},
            "configOptions": [{"id": "model", "name": "Model", "category": "model", "type": "select", "currentValue": "flash",
                "options": [{"value": "flash", "name": "Flash"}, {"value": "pro", "name": "Pro"}]}]});
        let (mut state, out) = opened(&adapter, &request, session);
        assert_eq!(written(&out)[0]["method"], "session/set_mode");
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SET_MODE, "result": {}}),
        );
        let frame = &written(&out)[0];
        assert_eq!(frame["method"], "session/set_config_option");
        assert_eq!(frame["params"]["value"], "pro");
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_SET_MODEL, "result": {"configOptions": []}}),
        );
        assert_eq!(written(&out)[0]["method"], "session/prompt");
        assert_eq!(state.result.model.as_deref(), Some("pro"));
    }

    #[test]
    fn a_refused_setting_is_named_not_reported_as_the_model() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request
            .harness_options
            .insert("config".into(), r#"{"effort":"high"}"#.into());
        let session = json!({"sessionId": "s", "configOptions": [
            {"id": "effort", "name": "Effort", "category": "thought_level", "type": "select", "currentValue": "low",
                "options": [{"value": "low", "name": "Low"}, {"value": "high", "name": "High"}]}]});
        let (mut state, out) = opened(&adapter, &request, session);
        let frame = &written(&out)[0];
        assert_eq!(frame["method"], "session/set_config_option");
        let id = frame["id"].as_u64().unwrap();
        assert!((100..=131).contains(&id));
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": "not now"}}),
        );
        assert!(out.terminal);
        let failure = state.terminal_failure.as_ref().unwrap();
        assert!(
            failure
                .diagnostic
                .contains("refused the requested setting `effort`"),
            "{}",
            failure.diagnostic
        );
        assert!(!failure.diagnostic.contains("requested model"));
    }

    #[test]
    fn refused_setting_labels_follow_the_request() {
        let config =
            json!({"method": "session/set_config_option", "params": {"configId": "effort"}});
        let mode = json!({"method": "session/set_mode", "params": {"modeId": "plan"}});
        assert_eq!(refused_setting_label(ID_SET_MODE, None), "mode");
        assert_eq!(refused_setting_label(ID_SET_MODEL, Some(&config)), "model");
        assert_eq!(
            refused_setting_label(100, Some(&config)),
            "setting `effort`"
        );
        assert_eq!(refused_setting_label(101, Some(&mode)), "mode");
        assert_eq!(refused_setting_label(102, None), "setting");
    }

    #[test]
    fn too_many_settings_are_rejected_before_the_process_starts() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        let config: BTreeMap<String, String> = (0..=MAX_REQUESTED_CONFIG)
            .map(|index| (format!("setting-{index}"), "on".to_string()))
            .collect();
        request
            .harness_options
            .insert("config".into(), serde_json::to_string(&config).unwrap());
        let error = adapter
            .prepare_turn(&request, &mut AdapterState::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains("at most 32 ACP settings"), "{error}");

        // The limit itself is allowed.
        let mut allowed = turn("hi");
        let config: BTreeMap<String, String> = (0..MAX_REQUESTED_CONFIG)
            .map(|index| (format!("setting-{index}"), "on".to_string()))
            .collect();
        allowed
            .harness_options
            .insert("config".into(), serde_json::to_string(&config).unwrap());
        adapter
            .prepare_turn(&allowed, &mut AdapterState::default())
            .unwrap();
    }

    #[test]
    fn an_unknown_model_fails_naming_the_choices() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request.model = Some("ultra".into());
        let session = json!({"sessionId": "s", "models": {"currentModelId": "a",
            "availableModels": [{"modelId": "a", "name": "A"}, {"modelId": "b", "name": "B"}]}});
        let (state, out) = opened(&adapter, &request, session);
        assert!(out.terminal);
        let failure = state.terminal_failure.as_ref().unwrap();
        assert_eq!(failure.kind, ProviderProcessErrorKind::ModelUnavailable);
        assert!(
            failure.diagnostic.contains("offers a, b"),
            "{}",
            failure.diagnostic
        );
    }

    fn permission_request(kind: &str) -> Value {
        json!({"jsonrpc": "2.0", "id": 7, "method": "session/request_permission", "params": {
            "sessionId": "s",
            "toolCall": {"toolCallId": "t1", "title": "Run npm test", "kind": kind, "rawInput": {"command": "npm test"}},
            "options": [
                {"optionId": "yes", "name": "Allow", "kind": "allow_once"},
                {"optionId": "always", "name": "Always allow", "kind": "allow_always"},
                {"optionId": "no", "name": "Reject", "kind": "reject_once"}]}})
    }

    #[test]
    fn permission_requests_become_approvals_and_round_trip() {
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(&adapter, &turn("hi"), json!({"sessionId": "s"}));
        let out = feed(&adapter, &mut state, permission_request("execute"));
        let Some(InteractionRequest::Approval { request, original }) = out.interaction else {
            panic!("expected an approval");
        };
        assert_eq!(request.tool_name, "Run npm test");
        assert_eq!(request.input["command"], "npm test");
        // Reads after "<tool> wants to".
        assert_eq!(request.description.as_deref(), Some("run `npm test`"));
        let answer = |decision| {
            let bytes = adapter
                .approval_response(&request, &original, decision)
                .unwrap()
                .unwrap();
            serde_json::from_slice::<Value>(&bytes).unwrap()
        };
        assert_eq!(
            answer(ApprovalDecision::Allow)["result"]["outcome"]["optionId"],
            "yes"
        );
        assert_eq!(
            answer(ApprovalDecision::AllowForSession)["result"]["outcome"]["optionId"],
            "always"
        );
        let denied = answer(ApprovalDecision::Deny { reason: None });
        assert_eq!(denied["id"], 7);
        assert_eq!(denied["result"]["outcome"]["optionId"], "no");
    }

    #[test]
    fn a_permission_request_says_what_it_would_do() {
        let located = json!({"locations": [{"path": "/repo/src/main.rs"}]});
        assert_eq!(
            permission_action("edit", &located, None),
            "edit /repo/src/main.rs"
        );
        assert_eq!(
            permission_action("read", &Value::Null, Some(&json!({"filePath": "a.txt"}))),
            "read a.txt"
        );
        assert_eq!(
            permission_action("delete", &Value::Null, None),
            "delete files"
        );
        assert_eq!(
            permission_action("execute", &Value::Null, None),
            "run a command"
        );
        assert_eq!(
            permission_action(
                "fetch",
                &Value::Null,
                Some(&json!({"url": "https://example.com"}))
            ),
            "fetch https://example.com"
        );
        assert_eq!(
            permission_action("think", &Value::Null, None),
            "use this tool"
        );
    }

    #[test]
    fn permission_modes_answer_what_they_cover() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request.permission_mode = PermissionMode::AcceptEdits;
        let (mut state, _) = opened(&adapter, &request, json!({"sessionId": "s"}));
        let out = feed(&adapter, &mut state, permission_request("edit"));
        assert!(out.interaction.is_none());
        assert_eq!(written(&out)[0]["result"]["outcome"]["optionId"], "yes");
        let out = feed(&adapter, &mut state, permission_request("execute"));
        assert!(
            out.interaction.is_some(),
            "commands still ask under AcceptEdits"
        );

        request.permission_mode = PermissionMode::FullAccess;
        let (mut state, _) = opened(&adapter, &request, json!({"sessionId": "s"}));
        let out = feed(&adapter, &mut state, permission_request("execute"));
        assert!(out.interaction.is_none());
        assert_eq!(written(&out)[0]["result"]["outcome"]["optionId"], "yes");

        request.permission_mode = PermissionMode::Plan;
        let mut state = AdapterState::default();
        assert!(adapter.prepare_turn(&request, &mut state).is_err());
    }

    #[test]
    fn cancellation_cancels_the_session_and_open_permissions() {
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(&adapter, &turn("hi"), json!({"sessionId": "s"}));
        feed(&adapter, &mut state, permission_request("execute"));
        let bytes = adapter.interrupt_request(&state).unwrap();
        let frames = String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(frames[0]["method"], "session/cancel");
        assert_eq!(frames[0]["params"]["sessionId"], "s");
        assert_eq!(frames[1]["id"], 7);
        assert_eq!(frames[1]["result"]["outcome"]["outcome"], "cancelled");
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": ID_PROMPT, "result": {"stopReason": "cancelled"}}),
        );
        assert!(out.terminal);
        assert_eq!(state.result.status, RunStatus::Failed);
    }

    #[test]
    fn unsupported_client_methods_get_an_error_reply() {
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(&adapter, &turn("hi"), json!({"sessionId": "s"}));
        let out = feed(
            &adapter,
            &mut state,
            json!({"jsonrpc": "2.0", "id": "x", "method": "fs/read_text_file",
            "params": {"sessionId": "s", "path": "/etc/hosts"}}),
        );
        let frame = &written(&out)[0];
        assert_eq!(frame["id"], "x");
        assert_eq!(frame["error"]["code"], -32601);
    }

    #[test]
    fn stdout_noise_warns_once_without_failing() {
        let adapter = Acp::new(agents());
        let mut state = started(&adapter, &turn("hi"));
        let out = adapter
            .parse_line("Loaded cached credentials.", &mut state)
            .unwrap();
        assert_eq!(out.events.len(), 1);
        let out = adapter.parse_line("more noise", &mut state).unwrap();
        assert_eq!(out.events.len(), 0);
        assert!(!out.terminal);
    }

    #[test]
    fn mcp_servers_are_sent_with_resolved_values() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request
            .environment
            .insert("FLEET_TOKEN".into(), crate::SecretString::new("secret"));
        request.launch_context.mcp_servers.insert(
            "fleet".into(),
            McpServerConfig::Stdio {
                command: "fleet-mcp".into(),
                args: vec!["serve".into()],
                environment_from: BTreeMap::from([("TOKEN".into(), "FLEET_TOKEN".into())]),
            },
        );
        request.launch_context.mcp_servers.insert(
            "web".into(),
            McpServerConfig::Http {
                url: "http://127.0.0.1:1/mcp".into(),
                headers_from: BTreeMap::new(),
            },
        );
        let mut state = started(&adapter, &request);
        let out = feed(&adapter, &mut state, initialized(json!({})));
        assert!(
            matches!(&out.events[0], TurnEvent::Warning { message } if message.contains("HTTP MCP"))
        );
        let servers = written(&out)[0]["params"]["mcpServers"].clone();
        assert_eq!(servers.as_array().unwrap().len(), 1);
        assert_eq!(servers[0]["name"], "fleet");
        assert_eq!(
            servers[0]["env"][0],
            json!({"name": "TOKEN", "value": "secret"})
        );
    }

    #[test]
    fn a_tool_completed_without_content_keeps_the_output_it_streamed() {
        // Some agents stream the result in `in_progress` updates and finish
        // with a bare status.
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(&adapter, &turn("hi"), json!({"sessionId": "s"}));
        let update = |update: Value| {
            json!({"jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": "s", "update": update}})
        };
        feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call", "toolCallId": "t", "title": "Run tests", "kind": "execute", "status": "pending"}),
            ),
        );
        feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "t", "status": "in_progress",
            "content": [{"type": "content", "content": {"type": "text", "text": "first"}}]}),
            ),
        );
        feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "t", "status": "in_progress",
            "content": [{"type": "content", "content": {"type": "text", "text": "12 passed, 1 failed"}}]}),
            ),
        );
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "t", "status": "failed"}),
            ),
        );
        let Some(TurnEvent::ToolCall {
            status,
            output,
            error,
            ..
        }) = out.events.first()
        else {
            panic!("expected the failed call");
        };
        assert_eq!(*status, ToolCallStatus::Failed);
        assert_eq!(output, &None);
        assert_eq!(error.as_deref(), Some("12 passed, 1 failed"));

        // The kept output belongs to the call that finished.
        feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call", "toolCallId": "u", "title": "Read", "kind": "read", "status": "pending"}),
            ),
        );
        let out = feed(
            &adapter,
            &mut state,
            update(
                json!({"sessionUpdate": "tool_call_update", "toolCallId": "u", "status": "completed"}),
            ),
        );
        assert!(matches!(
            out.events.first(),
            Some(TurnEvent::ToolCall {
                status: ToolCallStatus::Succeeded,
                output: None,
                ..
            })
        ));
    }

    #[test]
    fn a_relative_working_directory_is_refused_before_spawning() {
        let adapter = Acp::new(agents());
        let mut request = turn("hi");
        request.working_directory = PathBuf::from("workspace");
        let error = adapter
            .prepare_turn(&request, &mut AdapterState::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains("absolute working directory"), "{error}");
        assert!(error.contains("`workspace`"), "{error}");

        // Remote transports use their own path syntax.
        for absolute in [
            std::env::temp_dir(),
            "/workspace/a".into(),
            "C:\\repo".into(),
            "\\\\host\\share".into(),
        ] {
            assert!(is_absolute_directory(&absolute), "{}", absolute.display());
        }
        for relative in [".", "workspace", "../repo", "C:repo", ""] {
            assert!(!is_absolute_directory(Path::new(relative)), "{relative}");
        }
    }

    #[test]
    fn an_answered_permission_is_not_cancelled_again() {
        let adapter = Acp::new(agents());
        let (mut state, _) = opened(&adapter, &turn("hi"), json!({"sessionId": "s"}));
        let out = feed(&adapter, &mut state, permission_request("execute"));
        let Some(InteractionRequest::Approval { original, .. }) = out.interaction else {
            panic!("expected an approval");
        };
        // Cancelling while the user is still deciding refuses the request.
        let frames = |state: &AdapterState| {
            String::from_utf8(adapter.interrupt_request(state).unwrap())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(frames(&state).len(), 2);
        // Once the decision was sent, the agent has its answer; only the
        // session is cancelled.
        adapter.interaction_answered(&original, &mut state);
        let frames = frames(&state);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["method"], "session/cancel");
    }
}
