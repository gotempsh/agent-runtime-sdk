use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Semaphore;

use crate::adapter::{AdapterState, AgentAdapter, CommandSpec, InteractionRequest};
use crate::error::classify_provider_failure;
use crate::startup::{StartupObserver, StartupStage, StartupTrace};
use crate::{
    AccountUsageReport, DenyAll, EventSink, ExecutionTransport, HarnessAuthentication,
    HarnessCatalogError, HarnessCatalogErrorKind, HarnessCatalogStatus, HarnessControlGroup,
    HarnessExtensionAccess, HarnessExtensionDenialKind, HarnessExtensionInventory,
    HarnessExtensionQuery, HarnessExtensionScope, HarnessExtensionWarning, HarnessInventory,
    HarnessLimitation, HarnessMcpDefinition, HarnessMcpServer, HarnessModelCatalog,
    HarnessReadiness, HarnessSkill, HarnessStatus, InteractionHandler, LocalTransport,
    McpServerConfig, McpServerManagementRequest, Provider, ProviderProbeContext,
    ProviderProcessErrorKind, Result, RuntimeError, SandboxContext, SandboxError,
    SandboxProfileUpdate, SandboxRecoveryDecision, SandboxRecoveryHandler, SandboxRecoveryPolicy,
    SandboxRecoveryRequest, SandboxViolation, SecretString, SkillManagementRequest, TransportError,
    TransportErrorKind, TransportExitStatus, TransportReadinessRequest, TransportSpawnRequest,
    TurnEvent, TurnRequest, TurnResult,
};

const DEFAULT_MAX_PROMPT_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_EVENT_LINE_BYTES: usize = 2 * 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 32 * 1024;
const MAX_PROVIDER_CODE_CHARS: usize = 256;
const CATALOG_MAX_BYTES: usize = 1024 * 1024;
const CATALOG_TIMEOUT: Duration = Duration::from_secs(15);
const EXTENSION_TIMEOUT: Duration = Duration::from_secs(5);
const EXTENSION_MAX_BYTES: usize = 1024 * 1024;
const ACCOUNT_USAGE_TIMEOUT: Duration = Duration::from_secs(15);
const ACCOUNT_USAGE_MAX_BYTES: usize = 256 * 1024;
const AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(5);
const AUTHENTICATION_MAX_BYTES: usize = 64 * 1024;
const AUTHENTICATION_SOURCE_MAX_CHARS: usize = 128;
const AUTHENTICATION_REASON_MAX_CHARS: usize = 4_096;
const MAX_SANDBOX_RECOVERY_RETRIES: u8 = 8;
const MAX_ALLOWED_TOOLS: usize = 256;
const MAX_TOOL_NAME_BYTES: usize = 256;
const MAX_ATTACHMENTS: usize = 64;
const MAX_MCP_SERVERS: usize = 64;
const MAX_MCP_SERVER_NAME_BYTES: usize = 128;
const MAX_MCP_ARGUMENTS: usize = 256;
const MAX_ENVIRONMENT_VARIABLES: usize = 128;
const MAX_ENVIRONMENT_NAME_BYTES: usize = 256;
const MAX_ENVIRONMENT_VALUE_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_TOTAL_BYTES: usize = 256 * 1024;
const SANDBOX_RETRY_PROMPT: &str = "The sandbox profile was updated with the approved access. Retry only the previously blocked operation, then continue the task.";
/// How long a cancelled provider may keep running after acknowledging an
/// adapter-encoded interrupt, before the process tree is terminated anyway.
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// How long to wait for a terminated provider to be reaped when the turn ran
/// over adapter-supplied protocol streams. The process is already being
/// stopped; this only bounds how long the turn waits to observe it.
const ATTACHED_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Write newline-terminated provider frames to an interactive stdin.
async fn write_provider_frames(
    provider: Provider,
    stdin: Option<&mut crate::TransportWriter>,
    frames: &[Vec<u8>],
    stream: &'static str,
) -> Result<()> {
    if frames.is_empty() {
        return Ok(());
    }
    let writer = stdin.ok_or_else(|| RuntimeError::Protocol {
        provider,
        message: format!("adapter produced a {stream} without interactive stdin"),
    })?;
    for frame in frames {
        for bytes in [frame.as_slice(), b"\n".as_slice()] {
            writer
                .write_all(bytes)
                .await
                .map_err(|source| RuntimeError::ProcessIo {
                    provider,
                    stream,
                    source,
                })?;
        }
    }
    writer
        .flush()
        .await
        .map_err(|source| RuntimeError::ProcessIo {
            provider,
            stream,
            source,
        })
}

/// Stop a running provider, preferring its own cooperative interrupt.
///
/// An adapter that encodes an interrupt gets a bounded moment to unwind its
/// tool processes and persist session state; the turn is cancelled either way.
async fn cancel_running_process(
    provider: Provider,
    adapter: &dyn AgentAdapter,
    state: &AdapterState,
    stdin: &mut Option<crate::TransportWriter>,
    process: &mut crate::TransportProcess,
) -> RuntimeError {
    if stdin.is_some() {
        if let Some(frame) = adapter.interrupt_request(state) {
            let delivered = write_provider_frames(
                provider,
                stdin.as_mut(),
                std::slice::from_ref(&frame),
                "interrupt",
            )
            .await
            .is_ok();
            // End-of-input lets a stdio protocol server exit on its own once
            // it has acknowledged the interrupt.
            stdin.take();
            if delivered {
                let _ = tokio::time::timeout(INTERRUPT_GRACE, process.wait()).await;
            }
        }
    }
    let _ = process.terminate().await;
    RuntimeError::Cancelled { provider }
}

const SKILL_DISCOVERY_SCRIPT: &str = r#"
provider=$1
limit=$2
marker=$(printf '\036')
emit_access() {
  scope=$1
  target=$2
  probe=$target
  depth=0
  while test ! -e "$probe" && test "$depth" -lt 4; do
    next=${probe%/*}
    test -n "$next" || next=/
    test "$next" = "$probe" && break
    probe=$next
    depth=$((depth + 1))
  done
  if test -e "$probe" && test -w "$probe"; then
    printf 'access\0%s\0%s\0%s\0' "$scope" '1' ''
  else
    printf 'access\0%s\0%s\0%s\0' "$scope" '0' 'The execution identity cannot write this provider extension location.'
  fi
}
emit_root() {
  scope=$1
  source=$2
  root=$3
  test -d "$root" || return
  for entry in "$root"/*; do
    test -f "$entry/SKILL.md" || continue
    description=$(sed -n '/^---$/,/^---$/s/^description:[[:space:]]*//p' "$entry/SKILL.md" 2>/dev/null | sed -n '1p')
    printf 'skill\0%s\0%s\0%s\0%s\0' "$scope" "$source" "$entry/SKILL.md" "$description"
  done
}
printf '%sTEMPS_AGENT_RUNTIME_EXTENSIONS\0' "$marker"
case "$provider" in
  claude)
    user_base=$HOME/.claude
    project_base=$PWD/.claude
    emit_access user "$user_base/skills"
    emit_access project "$project_base/skills"
    emit_access user_mcp "$HOME/.claude.json"
    emit_access project_mcp "$PWD/.mcp.json"
    emit_root user claude "$user_base/skills"
    ;;
  codex)
    user_base=$HOME/.agents
    project_base=$PWD/.agents
    emit_access user "$user_base/skills"
    emit_access project "$project_base/skills"
    emit_access user_mcp "$HOME/.codex/config.toml"
    emit_root user codex "$HOME/.codex/skills"
    emit_root user agents "$HOME/.agents/skills"
    ;;
  open_code)
    user_base=${XDG_CONFIG_HOME:-$HOME/.config}/opencode
    project_base=$PWD/.opencode
    emit_access user "$user_base/skills"
    emit_access project "$project_base/skills"
    emit_root user opencode "$user_base/skills"
    emit_root user claude "$HOME/.claude/skills"
    emit_root user agents "$HOME/.agents/skills"
    ;;
  *) exit 64 ;;
esac
cursor=$PWD
depth=0
project_root=$PWD
if command -v git >/dev/null 2>&1; then
  detected=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null) && project_root=$detected
fi
while test "$depth" -lt 32; do
  case "$provider" in
    claude) emit_root project claude "$cursor/.claude/skills" ;;
    codex) emit_root project agents "$cursor/.agents/skills" ;;
    open_code)
      emit_root project claude "$cursor/.claude/skills"
      emit_root project agents "$cursor/.agents/skills"
      emit_root project opencode "$cursor/.opencode/skills"
      ;;
  esac
  test "$cursor" = "$project_root" && break
  next=${cursor%/*}
  test -n "$next" || next=/
  test "$next" = "$cursor" && break
  cursor=$next
  depth=$((depth + 1))
done
test "$limit" -gt 0
"#;

const SKILL_MANAGEMENT_SCRIPT: &str = r#"
provider=$1
scope=$2
name=$3
action=$4
case "$scope:$provider" in
  user:claude) base=$HOME/.claude/skills ;;
  user:codex) base=$HOME/.agents/skills ;;
  user:open_code) base=${XDG_CONFIG_HOME:-$HOME/.config}/opencode/skills ;;
  project:claude) base=$PWD/.claude/skills ;;
  project:codex) base=$PWD/.agents/skills ;;
  project:open_code) base=$PWD/.opencode/skills ;;
  *) exit 64 ;;
esac
target=$base/$name
reject_symlink_components() {
  inspected=$1
  case "$inspected" in
    /*) current=/; remaining=${inspected#/} ;;
    *) printf '%s\n' 'extension path must be absolute' >&2; exit 78 ;;
  esac
  while test -n "$remaining"; do
    case "$remaining" in
      */*) component=${remaining%%/*}; remaining=${remaining#*/} ;;
      *) component=$remaining; remaining= ;;
    esac
    test -n "$component" || continue
    current=${current%/}/$component
    if test -L "$current"; then
      printf '%s\n' "refusing extension path with symlink component: $current" >&2
      exit 78
    fi
  done
}
reject_symlink_components "$target"
case "$action" in
  write)
    umask 077
    mkdir -p -- "$target" || exit 73
    reject_symlink_components "$target"
    temp=$(mktemp "$target/.SKILL.md.temps-agent-runtime.XXXXXXXXXX") || exit 74
    cat > "$temp" || { rm -f -- "$temp"; exit 74; }
    mv -f -- "$temp" "$target/SKILL.md" || { rm -f -- "$temp"; exit 74; }
    ;;
  remove)
    test -e "$target" || exit 0
    rm -rf -- "$target" || exit 77
    ;;
  *) exit 64 ;;
esac
"#;

struct SandboxEventSink<'a> {
    inner: &'a dyn EventSink,
    sandbox: &'a crate::sandbox::ResolvedSandbox,
    violation: Mutex<Option<SandboxViolation>>,
}

impl<'a> SandboxEventSink<'a> {
    fn new(inner: &'a dyn EventSink, sandbox: &'a crate::sandbox::ResolvedSandbox) -> Self {
        Self {
            inner,
            sandbox,
            violation: Mutex::new(None),
        }
    }

    fn violation(&self) -> Option<SandboxViolation> {
        self.violation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait::async_trait]
impl EventSink for SandboxEventSink<'_> {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        let violation = self.sandbox.classify_event(&event);
        self.inner.emit(event).await?;
        if let Some(violation) = violation {
            let should_emit = {
                let mut current = self
                    .violation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if current.is_none() {
                    *current = Some(violation.clone());
                    true
                } else {
                    false
                }
            };
            if should_emit {
                self.inner
                    .emit(TurnEvent::SandboxAccessDenied {
                        profile: self.sandbox.profile.clone(),
                        violation,
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

/// Builder for a bounded agent runtime.
pub struct AgentRuntimeBuilder {
    adapters: HashMap<Provider, Arc<dyn AgentAdapter>>,
    transport: Arc<dyn ExecutionTransport>,
    concurrency_limit: usize,
    max_prompt_bytes: usize,
    max_event_line_bytes: usize,
    startup_observer: Option<Arc<dyn StartupObserver>>,
}

impl AgentRuntimeBuilder {
    /// Start with all adapters enabled by Cargo features.
    pub fn new() -> Self {
        #[allow(unused_mut)]
        let mut builder = Self {
            adapters: HashMap::new(),
            transport: Arc::new(LocalTransport),
            concurrency_limit: 2,
            max_prompt_bytes: DEFAULT_MAX_PROMPT_BYTES,
            max_event_line_bytes: DEFAULT_MAX_EVENT_LINE_BYTES,
            startup_observer: None,
        };
        #[cfg(feature = "claude")]
        builder.register(crate::providers::Claude::default());
        #[cfg(feature = "codex")]
        builder.register(crate::providers::Codex::default());
        #[cfg(feature = "opencode")]
        builder.register(crate::providers::OpenCode::default());
        builder
    }

    /// Replace or add an adapter.
    pub fn register(&mut self, adapter: impl AgentAdapter + 'static) -> &mut Self {
        self.adapters.insert(adapter.provider(), Arc::new(adapter));
        self
    }

    /// Execute provider processes through a configured local or remote transport.
    pub fn transport(mut self, transport: impl ExecutionTransport + 'static) -> Self {
        self.transport = Arc::new(transport);
        self
    }

    /// Execute provider processes through an already shared transport.
    pub fn transport_from_arc(mut self, transport: Arc<dyn ExecutionTransport>) -> Self {
        self.transport = transport;
        self
    }

    /// Limit concurrently running provider processes.
    pub fn concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }

    /// Limit prompt bytes before any process is spawned.
    pub fn max_prompt_bytes(mut self, limit: usize) -> Self {
        self.max_prompt_bytes = limit;
        self
    }

    /// Limit one provider NDJSON record.
    pub fn max_event_line_bytes(mut self, limit: usize) -> Self {
        self.max_event_line_bytes = limit;
        self
    }

    /// Observe payload-free startup boundaries. Disabled by default.
    ///
    /// Callbacks must return promptly; see [`StartupObserver`]. Observations
    /// do not add provider events, change the remote protocol, or start extra
    /// processes. A resumed session still starts a fresh process in this driver.
    pub fn startup_observer(mut self, observer: Arc<dyn StartupObserver>) -> Self {
        self.startup_observer = Some(observer);
        self
    }

    /// Validate limits and construct the runtime.
    pub fn build(self) -> Result<AgentRuntime> {
        if self.concurrency_limit == 0 {
            return Err(RuntimeError::InvalidRequest {
                field: "concurrency_limit",
                message: "must be greater than zero".to_string(),
            });
        }
        if self.max_prompt_bytes == 0 || self.max_event_line_bytes == 0 {
            return Err(RuntimeError::InvalidRequest {
                field: "limits",
                message: "prompt and event-line limits must be greater than zero".to_string(),
            });
        }
        Ok(AgentRuntime {
            adapters: self.adapters,
            transport: self.transport,
            permits: Arc::new(Semaphore::new(self.concurrency_limit)),
            max_prompt_bytes: self.max_prompt_bytes,
            max_event_line_bytes: self.max_event_line_bytes,
            startup_observer: self.startup_observer,
        })
    }
}

impl Default for AgentRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn valid_environment_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['=', '\0'])
}

fn validate_explicit_environment(
    environment: &BTreeMap<String, SecretString>,
    field: &'static str,
) -> Result<()> {
    if environment.len() > MAX_ENVIRONMENT_VARIABLES {
        return Err(RuntimeError::InvalidRequest {
            field,
            message: format!("may contain at most {MAX_ENVIRONMENT_VARIABLES} explicit variables"),
        });
    }
    let mut environment_bytes = 0_usize;
    for (name, value) in environment {
        if name.is_empty() || name.len() > MAX_ENVIRONMENT_NAME_BYTES || name.contains(['=', '\0'])
        {
            return Err(RuntimeError::InvalidRequest {
                field,
                message: format!(
                    "variable names must be non-empty, at most {MAX_ENVIRONMENT_NAME_BYTES} bytes, and cannot contain `=` or NUL"
                ),
            });
        }
        if value.expose().len() > MAX_ENVIRONMENT_VALUE_BYTES || value.expose().contains('\0') {
            return Err(RuntimeError::InvalidRequest {
                field,
                message: format!(
                    "variable values must be at most {MAX_ENVIRONMENT_VALUE_BYTES} bytes and cannot contain NUL"
                ),
            });
        }
        environment_bytes = environment_bytes
            .saturating_add(name.len())
            .saturating_add(value.expose().len());
    }
    if environment_bytes > MAX_ENVIRONMENT_TOTAL_BYTES {
        return Err(RuntimeError::InvalidRequest {
            field,
            message: format!(
                "explicit variable names and values exceed the {MAX_ENVIRONMENT_TOTAL_BYTES} byte aggregate limit"
            ),
        });
    }
    Ok(())
}

fn apply_probe_context(command: &mut CommandSpec, context: &ProviderProbeContext) {
    for (name, value) in &context.environment {
        command
            .environment
            .insert(name.into(), value.expose().into());
    }
}

fn bounded_probe_diagnostic(
    diagnostic: &str,
    environment: &BTreeMap<String, SecretString>,
) -> String {
    redact_secrets(diagnostic, environment)
        .chars()
        .take(AUTHENTICATION_REASON_MAX_CHARS)
        .collect()
}

fn catalog_failure_kind(
    diagnostic: &str,
    fallback: HarnessCatalogErrorKind,
) -> HarnessCatalogErrorKind {
    match classify_provider_failure(diagnostic) {
        ProviderProcessErrorKind::AuthenticationFailed => HarnessCatalogErrorKind::Authentication,
        ProviderProcessErrorKind::PermissionDenied => HarnessCatalogErrorKind::Permission,
        ProviderProcessErrorKind::ModelUnavailable => HarnessCatalogErrorKind::ModelUnavailable,
        ProviderProcessErrorKind::RateLimited => HarnessCatalogErrorKind::RateLimited,
        ProviderProcessErrorKind::Network => HarnessCatalogErrorKind::Network,
        ProviderProcessErrorKind::Unknown => fallback,
    }
}

fn validate_environment_reference(
    request: &TurnRequest,
    field: &'static str,
    target: &str,
    source: &str,
) -> Result<()> {
    if !valid_environment_name(target) || !valid_environment_name(source) {
        return Err(RuntimeError::InvalidRequest {
            field,
            message: "environment target and source names must be non-empty and cannot contain `=` or NUL"
                .to_string(),
        });
    }
    if !request.environment.contains_key(source) {
        return Err(RuntimeError::InvalidRequest {
            field,
            message: format!(
                "references environment variable `{source}`, but that variable was not supplied to the turn"
            ),
        });
    }
    Ok(())
}

fn validate_launch_context(
    request: &TurnRequest,
    capabilities: crate::LaunchContextCapabilities,
) -> Result<()> {
    let context = &request.launch_context;
    if context.system_prompt_append.is_some() && !capabilities.system_prompt_append {
        return Err(RuntimeError::InvalidRequest {
            field: "launch_context.system_prompt_append",
            message: format!(
                "{} does not support system-prompt additions",
                request.provider
            ),
        });
    }
    if context.allowed_tools.is_some() && !capabilities.allowed_tools {
        return Err(RuntimeError::InvalidRequest {
            field: "launch_context.allowed_tools",
            message: format!(
                "{} does not support an exact tool allowlist",
                request.provider
            ),
        });
    }
    if context.strict_mcp_config && !capabilities.strict_mcp_config {
        return Err(RuntimeError::InvalidRequest {
            field: "launch_context.strict_mcp_config",
            message: format!(
                "{} cannot exclude ambient MCP configuration",
                request.provider
            ),
        });
    }
    if let Some(system_prompt) = context.system_prompt_append.as_deref() {
        if system_prompt.len() > DEFAULT_MAX_PROMPT_BYTES || system_prompt.contains('\0') {
            return Err(RuntimeError::InvalidRequest {
                field: "launch_context.system_prompt_append",
                message: format!(
                    "must be at most {DEFAULT_MAX_PROMPT_BYTES} bytes and cannot contain NUL"
                ),
            });
        }
    }
    if let Some(tools) = context.allowed_tools.as_deref() {
        if tools.len() > MAX_ALLOWED_TOOLS {
            return Err(RuntimeError::InvalidRequest {
                field: "launch_context.allowed_tools",
                message: format!("may contain at most {MAX_ALLOWED_TOOLS} tools"),
            });
        }
        for tool in tools {
            if tool.is_empty()
                || tool.len() > MAX_TOOL_NAME_BYTES
                || tool.contains(['\0', '\n', '\r', ','])
            {
                return Err(RuntimeError::InvalidRequest {
                    field: "launch_context.allowed_tools",
                    message: format!(
                        "tool names must be non-empty, at most {MAX_TOOL_NAME_BYTES} bytes, and cannot contain commas, NUL, or newlines"
                    ),
                });
            }
        }
    }
    if context.mcp_servers.len() > MAX_MCP_SERVERS {
        return Err(RuntimeError::InvalidRequest {
            field: "launch_context.mcp_servers",
            message: format!("may contain at most {MAX_MCP_SERVERS} servers"),
        });
    }
    for (name, server) in &context.mcp_servers {
        if name.is_empty()
            || name.len() > MAX_MCP_SERVER_NAME_BYTES
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(RuntimeError::InvalidRequest {
                field: "launch_context.mcp_servers",
                message: format!(
                    "server names must use only ASCII letters, numbers, `-`, or `_` and be at most {MAX_MCP_SERVER_NAME_BYTES} bytes"
                ),
            });
        }
        match server {
            McpServerConfig::Stdio {
                command,
                args,
                environment_from,
            } => {
                if !capabilities.stdio_mcp {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers",
                        message: format!(
                            "{} does not support turn-scoped stdio MCP servers",
                            request.provider
                        ),
                    });
                }
                let Some(command) = command.to_str() else {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers.command",
                        message: "stdio commands must be valid UTF-8".to_string(),
                    });
                };
                if command.is_empty() || command.contains('\0') {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers.command",
                        message: "stdio commands must be non-empty and cannot contain NUL"
                            .to_string(),
                    });
                }
                if args.len() > MAX_MCP_ARGUMENTS || args.iter().any(|arg| arg.contains('\0')) {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers.args",
                        message: format!(
                            "stdio servers may have at most {MAX_MCP_ARGUMENTS} arguments and arguments cannot contain NUL"
                        ),
                    });
                }
                for (target, source) in environment_from {
                    validate_environment_reference(
                        request,
                        "launch_context.mcp_servers.environment_from",
                        target,
                        source,
                    )?;
                }
            }
            McpServerConfig::Http { url, headers_from } => {
                if !capabilities.http_mcp {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers",
                        message: format!(
                            "{} does not support turn-scoped HTTP MCP servers",
                            request.provider
                        ),
                    });
                }
                if let Err(message) = crate::url_security::validate_http_endpoint(url) {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers.url",
                        message: format!("HTTP MCP URL {message}"),
                    });
                }
                for (header, source) in headers_from {
                    if header.is_empty()
                        || !header
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                    {
                        return Err(RuntimeError::InvalidRequest {
                            field: "launch_context.mcp_servers.headers_from",
                            message: "HTTP header names must use only ASCII letters, numbers, `-`, or `_`"
                                .to_string(),
                        });
                    }
                    validate_environment_reference(
                        request,
                        "launch_context.mcp_servers.headers_from",
                        header,
                        source,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Reject attachment references an adapter could not place in argv or a
/// provider request without corrupting it.
fn validate_attachments(request: &TurnRequest) -> Result<()> {
    if request.attachments.len() > MAX_ATTACHMENTS {
        return Err(RuntimeError::InvalidRequest {
            field: "attachments",
            message: format!("a turn may reference at most {MAX_ATTACHMENTS} attachments"),
        });
    }
    for attachment in &request.attachments {
        let valid = attachment
            .path
            .to_str()
            .is_some_and(|path| !path.is_empty() && !path.contains(['\0', '\n', '\r']));
        if !valid {
            return Err(RuntimeError::InvalidRequest {
                field: "attachments.path",
                message: "attachment paths must be non-empty UTF-8 without NUL or newlines"
                    .to_string(),
            });
        }
    }
    Ok(())
}

/// Bounded process runtime shared by all provider adapters.
#[derive(Clone)]
pub struct AgentRuntime {
    adapters: HashMap<Provider, Arc<dyn AgentAdapter>>,
    transport: Arc<dyn ExecutionTransport>,
    permits: Arc<Semaphore>,
    max_prompt_bytes: usize,
    max_event_line_bytes: usize,
    startup_observer: Option<Arc<dyn StartupObserver>>,
}

impl AgentRuntime {
    /// Create a runtime builder.
    pub fn builder() -> AgentRuntimeBuilder {
        AgentRuntimeBuilder::new()
    }

    /// Inspect one compiled adapter inside the configured execution transport.
    pub async fn readiness(&self, provider: Provider) -> Result<crate::ProviderReadiness> {
        let adapter = self
            .adapters
            .get(&provider)
            .ok_or(RuntimeError::AdapterUnavailable { provider })?;
        self.transport
            .readiness(TransportReadinessRequest {
                provider,
                program: adapter.executable(),
            })
            .await
            .map_err(|source| RuntimeError::Transport { provider, source })
    }

    /// Autocomplete working directories inside this runtime's transport.
    pub async fn suggest_working_directories(
        &self,
        input: impl Into<String>,
        limit: usize,
    ) -> crate::TransportResult<crate::WorkingDirectoryCandidates> {
        let input = input.into();
        if input.len() > 4_096 || input.contains('\0') {
            return Err(crate::TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                self.transport.name(),
                "suggest_working_directories",
                "directory input must be at most 4096 bytes and cannot contain NUL bytes",
                false,
            ));
        }
        if !(1..=50).contains(&limit) {
            return Err(crate::TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                self.transport.name(),
                "suggest_working_directories",
                "directory suggestion limit must be between 1 and 50",
                false,
            ));
        }
        self.transport
            .suggest_working_directories(crate::WorkingDirectoryQuery { input, limit })
            .await
    }

    /// Fetch provider-account quota for the identity authenticated on this execution host.
    ///
    /// The query is explicit and bounded. It does not start an agent turn and it is separate
    /// from [`crate::Usage::context_window`], which belongs to one provider session. Applications
    /// should cache successful reports briefly and refresh when the usage surface is opened.
    pub async fn fetch_account_usage(&self, provider: Provider) -> AccountUsageReport {
        self.fetch_account_usage_inner(provider, ProviderProbeContext::default())
            .await
    }

    /// Fetch provider-account quota using explicit target-local probe context.
    ///
    /// Secret environment values are bounded, excluded from `Debug`, and
    /// injected only into the provider probe process. The working directory is
    /// interpreted by the configured execution transport.
    pub async fn fetch_account_usage_with(
        &self,
        provider: Provider,
        context: ProviderProbeContext,
    ) -> Result<AccountUsageReport> {
        validate_explicit_environment(&context.environment, "probe_context.environment")?;
        Ok(self.fetch_account_usage_inner(provider, context).await)
    }

    async fn fetch_account_usage_inner(
        &self,
        provider: Provider,
        context: ProviderProbeContext,
    ) -> AccountUsageReport {
        let Some(adapter) = self.adapters.get(&provider) else {
            return AccountUsageReport::unsupported(
                provider,
                format!("the {provider} adapter is not registered"),
            );
        };
        let Some(probe) = adapter.account_usage_probe() else {
            return AccountUsageReport::unsupported(
                provider,
                format!("the {provider} adapter does not expose account usage"),
            );
        };
        let Ok(_permit) = self.permits.acquire().await else {
            return AccountUsageReport::unavailable(
                provider,
                "the runtime stopped before account usage could be queried",
                true,
            );
        };
        let mut command = probe.command;
        apply_probe_context(&mut command, &context);
        let lines = match if let Some(expected) = probe.expected_response_ids {
            self.execute_response_probe(
                command,
                context.working_directory,
                "fetch_account_usage",
                expected,
                ACCOUNT_USAGE_TIMEOUT,
                ACCOUNT_USAGE_MAX_BYTES,
            )
            .await
        } else {
            self.execute_bounded_command(
                command,
                context.working_directory,
                "fetch_account_usage",
                ACCOUNT_USAGE_TIMEOUT,
                ACCOUNT_USAGE_MAX_BYTES,
            )
            .await
            .map(|output| {
                String::from_utf8_lossy(&output)
                    .lines()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
        } {
            Ok(lines) => lines,
            Err(error) => {
                let message = redact_secrets(&error.message, &context.environment);
                return AccountUsageReport::unavailable(
                    provider,
                    format!("account usage could not be fetched: {message}"),
                    error.retryable,
                );
            }
        };
        match adapter.parse_account_usage_probe(&lines) {
            Ok(mut report) if report.provider == provider => {
                if let Some(reason) = report.reason.take() {
                    report.reason = Some(redact_secrets(&reason, &context.environment));
                }
                report
            }
            Ok(_) => AccountUsageReport::unavailable(
                provider,
                "the account-usage response named another provider",
                false,
            ),
            Err(error) => {
                let message = redact_secrets(&error.to_string(), &context.environment);
                AccountUsageReport::unavailable(
                    provider,
                    format!("account usage could not be parsed: {message}"),
                    false,
                )
            }
        }
    }

    /// Search skills and inspect MCP servers applicable to one harness on this execution host.
    ///
    /// This is an explicit, bounded operation and is intentionally separate from
    /// [`Self::discover_harnesses`] so remote filesystem and provider configuration probes never
    /// delay the initial harness picker.
    pub async fn discover_harness_extensions(
        &self,
        query: HarnessExtensionQuery,
    ) -> crate::TransportResult<HarnessExtensionInventory> {
        validate_extension_query(&query, self.transport.name())?;
        let adapter = self.adapters.get(&query.provider).ok_or_else(|| {
            TransportError::new(
                TransportErrorKind::Unsupported,
                self.transport.name(),
                "discover_harness_extensions",
                format!("the {} adapter is not registered", query.provider),
                false,
            )
        })?;
        self.transport
            .validate_working_directory(&query.working_directory)
            .await?;

        let skills = self.run_skill_probe(&query);
        let mcp_servers = self.run_mcp_probe(
            query.provider,
            Arc::clone(adapter),
            &query.working_directory,
        );
        let (skills, mcp_servers) = tokio::join!(skills, mcp_servers);
        let mut warnings = Vec::new();
        let (
            skills,
            user_skill_access,
            project_skill_access,
            user_mcp_write_access,
            project_mcp_write_access,
        ) = match skills {
            Ok(result) => result,
            Err(error) => {
                warnings.push(HarnessExtensionWarning {
                    stage: "skills".into(),
                    message: error.message.clone(),
                    retryable: error.retryable,
                });
                let access =
                    HarnessExtensionAccess::denied(denial_from_transport(&error), error.message);
                (
                    Vec::new(),
                    access.clone(),
                    access.clone(),
                    access.clone(),
                    access,
                )
            }
        };
        let mut mcp_probe_denial = None;
        let mcp_servers = match mcp_servers {
            Ok(servers) => servers,
            Err(error) => {
                mcp_probe_denial = Some(HarnessExtensionAccess::denied(
                    denial_from_transport(&error),
                    error.message.clone(),
                ));
                warnings.push(HarnessExtensionWarning {
                    stage: "mcp_servers".into(),
                    message: error.message,
                    retryable: error.retryable,
                });
                Vec::new()
            }
        };
        let (mut manage_user_mcp_servers, mut manage_project_mcp_servers) =
            mcp_management_access(query.provider);
        if manage_user_mcp_servers.allowed {
            manage_user_mcp_servers = user_mcp_write_access;
        }
        if manage_project_mcp_servers.allowed {
            manage_project_mcp_servers = project_mcp_write_access;
        }
        if let Some(denial) = mcp_probe_denial {
            if manage_user_mcp_servers.allowed {
                manage_user_mcp_servers = denial.clone();
            }
            if manage_project_mcp_servers.allowed {
                manage_project_mcp_servers = denial;
            }
        }

        Ok(HarnessExtensionInventory {
            provider: query.provider,
            transport: self.transport.name().to_string(),
            working_directory: query.working_directory,
            skills,
            mcp_servers,
            manage_user_skills: user_skill_access,
            manage_project_skills: project_skill_access,
            manage_user_mcp_servers,
            manage_project_mcp_servers,
            warnings,
        })
    }

    /// Install, replace, or remove a host-scoped `SKILL.md` definition.
    pub async fn manage_skill(
        &self,
        request: SkillManagementRequest,
    ) -> crate::TransportResult<()> {
        validate_extension_name(&request.name, self.transport.name(), "manage_skill")?;
        if request
            .content
            .as_ref()
            .is_some_and(|content| content.len() > EXTENSION_MAX_BYTES)
        {
            return Err(invalid_extension_request(
                self.transport.name(),
                "manage_skill",
                "SKILL.md content must not exceed 1 MiB",
            ));
        }
        self.transport
            .validate_working_directory(&request.working_directory)
            .await?;
        let mut command = CommandSpec::new("/bin/sh");
        command.args.extend([
            "-c".into(),
            SKILL_MANAGEMENT_SCRIPT.into(),
            "runtime-skill-management".into(),
            provider_argument(request.provider).into(),
            scope_argument(request.scope).into(),
            request.name.clone().into(),
            if request.content.is_some() {
                "write"
            } else {
                "remove"
            }
            .into(),
        ]);
        command.initial_stdin = request.content.map(String::into_bytes);
        self.execute_extension_command(command, request.working_directory, "manage_skill")
            .await
            .map(|_| ())
    }

    /// Add or remove an MCP server through the provider's supported CLI.
    pub async fn manage_mcp_server(
        &self,
        request: McpServerManagementRequest,
    ) -> crate::TransportResult<()> {
        validate_extension_name(&request.name, self.transport.name(), "manage_mcp_server")?;
        self.transport
            .validate_working_directory(&request.working_directory)
            .await?;
        let adapter = self.adapters.get(&request.provider).ok_or_else(|| {
            invalid_extension_request(
                self.transport.name(),
                "manage_mcp_server",
                format!("the {} adapter is not registered", request.provider),
            )
        })?;
        let command = build_mcp_management_command(
            request.provider,
            adapter.executable(),
            request.scope,
            &request.name,
            request.definition,
            self.transport.name(),
        )?;
        self.execute_extension_command(command, request.working_directory, "manage_mcp_server")
            .await
            .map(|_| ())
    }

    async fn run_skill_probe(
        &self,
        query: &HarnessExtensionQuery,
    ) -> crate::TransportResult<(
        Vec<HarnessSkill>,
        HarnessExtensionAccess,
        HarnessExtensionAccess,
        HarnessExtensionAccess,
        HarnessExtensionAccess,
    )> {
        let mut command = CommandSpec::new("/bin/sh");
        command.args.extend([
            "-c".into(),
            SKILL_DISCOVERY_SCRIPT.into(),
            "runtime-skill-discovery".into(),
            provider_argument(query.provider).into(),
            query.limit.to_string().into(),
        ]);
        let output = self
            .execute_extension_command(command, query.working_directory.clone(), "discover_skills")
            .await?;
        parse_skill_probe(
            &output,
            query.skill_query.as_deref(),
            query.limit,
            self.transport.name(),
        )
    }

    async fn run_mcp_probe(
        &self,
        provider: Provider,
        adapter: Arc<dyn AgentAdapter>,
        working_directory: &std::path::Path,
    ) -> crate::TransportResult<Vec<HarnessMcpServer>> {
        let mut command = CommandSpec::new(adapter.executable());
        command.args.extend(["mcp".into(), "list".into()]);
        if provider == Provider::Codex {
            command.args.push("--json".into());
        }
        let output = self
            .execute_extension_command(command, working_directory.to_path_buf(), "discover_mcp")
            .await?;
        parse_mcp_servers(provider, &output, self.transport.name())
    }

    async fn execute_extension_command(
        &self,
        command: CommandSpec,
        working_directory: PathBuf,
        operation: &'static str,
    ) -> crate::TransportResult<Vec<u8>> {
        self.execute_bounded_command(
            command,
            working_directory,
            operation,
            EXTENSION_TIMEOUT,
            EXTENSION_MAX_BYTES,
        )
        .await
    }

    async fn execute_bounded_command(
        &self,
        command: CommandSpec,
        working_directory: PathBuf,
        operation: &'static str,
        timeout: Duration,
        max_bytes: usize,
    ) -> crate::TransportResult<Vec<u8>> {
        let initial_stdin = command.initial_stdin.clone();
        let mut process = self
            .transport
            .spawn(TransportSpawnRequest {
                command,
                working_directory,
            })
            .await?;
        // A rejecting command may close stdin before reading the content.
        // Keep its exit status and stderr authoritative rather than returning
        // a timing-dependent broken-pipe error and hiding the rejection.
        let stdin = process.take_stdin();
        let write_input = async move {
            if let Some(initial) = initial_stdin {
                let Some(mut stdin) = stdin else {
                    return Err("the extension command did not expose stdin".to_string());
                };
                stdin
                    .write_all(&initial)
                    .await
                    .map_err(|error| format!("could not write extension content: {error}"))?;
                stdin
                    .flush()
                    .await
                    .map_err(|error| format!("could not flush extension content: {error}"))?;
            }
            Ok::<_, String>(())
        };
        let Some(stdout) = process.take_stdout() else {
            let _ = process.terminate().await;
            return Err(extension_transport_error(
                self.transport.name(),
                operation,
                "the extension probe did not expose stdout",
                false,
            ));
        };
        let stderr_task = process
            .take_stderr()
            .map(|stderr| tokio::spawn(crate::process::bounded_stderr(stderr, STDERR_TAIL_BYTES)));
        let run = async {
            let read_output = async {
                let mut output = Vec::new();
                stdout
                    .take((max_bytes + 1) as u64)
                    .read_to_end(&mut output)
                    .await
                    .map_err(|error| error.to_string())?;
                if output.len() > max_bytes {
                    return Err(format!("command output exceeded {max_bytes} bytes"));
                }
                Ok::<_, String>(output)
            };
            let (output, input_result) = tokio::join!(read_output, write_input);
            let output = output?;
            let status = process.wait().await.map_err(|error| error.message)?;
            if status.success {
                input_result?;
            }
            Ok::<_, String>((output, status))
        };
        let result = tokio::time::timeout(timeout, run).await;
        match result {
            Ok(Ok((output, status))) if status.success => {
                if let Some(task) = stderr_task {
                    let _ = task.await;
                }
                Ok(output)
            }
            Ok(Ok((_, _))) => {
                let stderr = match stderr_task {
                    Some(task) => task
                        .await
                        .ok()
                        .and_then(std::result::Result::ok)
                        .unwrap_or_default(),
                    None => String::new(),
                };
                Err(extension_transport_error(
                    self.transport.name(),
                    operation,
                    if stderr.trim().is_empty() {
                        "the provider extension command exited unsuccessfully".to_string()
                    } else {
                        format!("the provider extension command failed: {}", stderr.trim())
                    },
                    false,
                ))
            }
            Ok(Err(message)) => {
                if let Some(task) = stderr_task {
                    task.abort();
                }
                let _ = process.terminate().await;
                Err(extension_transport_error(
                    self.transport.name(),
                    operation,
                    message,
                    true,
                ))
            }
            Err(_) => {
                if let Some(task) = stderr_task {
                    task.abort();
                }
                let _ = process.terminate().await;
                Err(TransportError::new(
                    TransportErrorKind::ConnectionTimedOut,
                    self.transport.name(),
                    operation,
                    format!("{operation} exceeded {} seconds", timeout.as_secs()),
                    true,
                ))
            }
        }
    }

    async fn execute_response_probe(
        &self,
        command: CommandSpec,
        working_directory: PathBuf,
        operation: &'static str,
        expected_response_ids: Vec<u64>,
        timeout: Duration,
        max_bytes: usize,
    ) -> crate::TransportResult<Vec<String>> {
        let initial_stdin = command.initial_stdin.clone();
        let mut process = self
            .transport
            .spawn(TransportSpawnRequest {
                command,
                working_directory,
            })
            .await?;
        let mut stdin_guard = if let Some(initial) = initial_stdin {
            let Some(mut stdin) = process.take_stdin() else {
                let _ = process.terminate().await;
                return Err(extension_transport_error(
                    self.transport.name(),
                    operation,
                    "the provider probe did not expose stdin",
                    false,
                ));
            };
            stdin.write_all(&initial).await.map_err(|error| {
                extension_transport_error(
                    self.transport.name(),
                    operation,
                    format!("could not write provider probe input: {error}"),
                    true,
                )
            })?;
            if !initial.ends_with(b"\n") {
                stdin.write_all(b"\n").await.map_err(|error| {
                    extension_transport_error(
                        self.transport.name(),
                        operation,
                        format!("could not terminate provider probe input: {error}"),
                        true,
                    )
                })?;
            }
            stdin.flush().await.map_err(|error| {
                extension_transport_error(
                    self.transport.name(),
                    operation,
                    format!("could not flush provider probe input: {error}"),
                    true,
                )
            })?;
            Some(stdin)
        } else {
            drop(process.take_stdin());
            None
        };
        let Some(stdout) = process.take_stdout() else {
            let _ = process.terminate().await;
            return Err(extension_transport_error(
                self.transport.name(),
                operation,
                "the provider probe did not expose stdout",
                false,
            ));
        };
        let stderr_task = process
            .take_stderr()
            .map(|stderr| tokio::spawn(crate::process::bounded_stderr(stderr, STDERR_TAIL_BYTES)));
        let read = async {
            let mut reader = BufReader::new(stdout).lines();
            let mut lines = Vec::new();
            let mut seen = Vec::with_capacity(expected_response_ids.len());
            let mut bytes = 0usize;
            while let Some(line) = reader
                .next_line()
                .await
                .map_err(|error| error.to_string())?
            {
                bytes = bytes.saturating_add(line.len().saturating_add(1));
                if bytes > max_bytes {
                    return Err(format!("command output exceeded {max_bytes} bytes"));
                }
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                    if let Some(id) = value.get("id").and_then(serde_json::Value::as_u64) {
                        if expected_response_ids.contains(&id) && !seen.contains(&id) {
                            seen.push(id);
                        }
                    }
                }
                lines.push(line);
                if expected_response_ids.iter().all(|id| seen.contains(id)) {
                    return Ok(lines);
                }
            }
            Err("provider probe ended before every expected response arrived".to_string())
        };
        let result = tokio::time::timeout(timeout, read).await;
        drop(stdin_guard.take());
        let _ = process.terminate().await;
        if let Some(task) = stderr_task {
            task.abort();
        }
        match result {
            Ok(Ok(lines)) => Ok(lines),
            Ok(Err(message)) => Err(extension_transport_error(
                self.transport.name(),
                operation,
                message,
                true,
            )),
            Err(_) => Err(TransportError::new(
                TransportErrorKind::ConnectionTimedOut,
                self.transport.name(),
                operation,
                format!("{operation} exceeded {} seconds", timeout.as_secs()),
                true,
            )),
        }
    }

    /// Probe every registered provider inside this runtime's configured transport.
    ///
    /// Probes run concurrently with an upper bound equal to the number of known
    /// provider variants. A transport failure is retained on the corresponding
    /// harness instead of aborting the rest of the inventory.
    pub async fn discover_harnesses(&self) -> HarnessInventory {
        self.discover_harnesses_inner(ProviderProbeContext::default())
            .await
    }

    /// Probe every registered provider with explicit target-local context.
    ///
    /// Use this when authentication or project configuration is supplied by
    /// the embedding application rather than ambient target state. Invalid
    /// environment input is rejected before any probe process is spawned.
    pub async fn discover_harnesses_with(
        &self,
        context: ProviderProbeContext,
    ) -> Result<HarnessInventory> {
        validate_explicit_environment(&context.environment, "probe_context.environment")?;
        Ok(self.discover_harnesses_inner(context).await)
    }

    async fn discover_harnesses_inner(&self, context: ProviderProbeContext) -> HarnessInventory {
        let capabilities = self.transport.capabilities();
        let mut tasks = tokio::task::JoinSet::new();
        for provider in [Provider::Claude, Provider::Codex, Provider::OpenCode] {
            if self.adapters.contains_key(&provider) {
                let runtime = self.clone();
                let context = context.clone();
                tasks.spawn(async move { runtime.probe_harness(provider, context).await });
            }
        }
        let mut harnesses = Vec::with_capacity(tasks.len());
        while let Some(result) = tasks.join_next().await {
            if let Ok(harness) = result {
                harnesses.push(harness);
            }
        }
        harnesses.sort_by_key(|harness| provider_order(harness.provider));
        HarnessInventory {
            transport: self.transport.name().to_string(),
            transport_capabilities: capabilities,
            harnesses,
        }
    }

    async fn probe_harness(
        &self,
        provider: Provider,
        context: ProviderProbeContext,
    ) -> HarnessReadiness {
        let adapter = self
            .adapters
            .get(&provider)
            .expect("discovery only schedules registered adapters");
        let permissions = adapter.permission_support();
        let launch_context = adapter.launch_context_capabilities();
        let capabilities = self.transport.capabilities();
        let mut limitations = Vec::with_capacity(2);
        if (permissions.live_approvals || permissions.live_questions)
            && !capabilities.interactive_stdin
        {
            limitations.push(HarnessLimitation::InteractiveStdinUnavailable);
        }
        if !capabilities.process_tree_termination {
            limitations.push(HarnessLimitation::ProcessTreeTerminationUnavailable);
        }
        match self
            .transport
            .readiness(TransportReadinessRequest {
                provider,
                program: adapter.executable(),
            })
            .await
        {
            Ok(readiness) => {
                let (authentication, control_groups, models, account_usage) = if readiness.installed
                {
                    let authentication =
                        self.probe_harness_authentication(Arc::clone(adapter), context.clone());
                    let metadata = self.probe_harness_metadata(Arc::clone(adapter), context);
                    let (authentication, metadata) = tokio::join!(authentication, metadata);
                    let (control_groups, models, account_usage) = metadata;
                    (authentication, control_groups, models, account_usage)
                } else {
                    (
                        HarnessAuthentication::unknown("executable_not_installed"),
                        adapter.control_groups(),
                        HarnessModelCatalog::unsupported("executable_not_installed"),
                        None,
                    )
                };
                HarnessReadiness {
                    provider,
                    status: if !readiness.installed {
                        HarnessStatus::NotInstalled
                    } else if limitations.is_empty() {
                        HarnessStatus::Ready
                    } else {
                        HarnessStatus::Incompatible
                    },
                    readiness: Some(readiness),
                    authentication,
                    permissions,
                    launch_context,
                    control_groups,
                    models,
                    account_usage,
                    limitations,
                    error: None,
                }
            }
            Err(error) => {
                let authentication = HarnessAuthentication::unavailable(
                    "transport_readiness",
                    error.message.clone(),
                    error.retryable,
                );
                HarnessReadiness {
                    provider,
                    status: HarnessStatus::Unavailable,
                    readiness: None,
                    authentication,
                    permissions,
                    launch_context,
                    control_groups: adapter.control_groups(),
                    models: HarnessModelCatalog::unsupported("transport_unavailable"),
                    account_usage: None,
                    limitations,
                    error: Some(error),
                }
            }
        }
    }

    async fn probe_harness_authentication(
        &self,
        adapter: Arc<dyn AgentAdapter>,
        context: ProviderProbeContext,
    ) -> HarnessAuthentication {
        let Some(probe) = adapter.authentication_probe() else {
            return HarnessAuthentication::unknown("unsupported");
        };
        let mut command = probe.command;
        apply_probe_context(&mut command, &context);
        let provider = adapter.provider();
        match self
            .execute_authentication_command(command, context.working_directory.clone())
            .await
        {
            Ok((stdout, stderr, status)) => {
                match adapter.parse_authentication_probe(&stdout, &stderr, status) {
                    Ok(mut authentication) => {
                        authentication.source =
                            bounded_probe_diagnostic(&authentication.source, &context.environment)
                                .chars()
                                .take(AUTHENTICATION_SOURCE_MAX_CHARS)
                                .collect();
                        if authentication.source.is_empty() {
                            authentication.source = "authentication_probe".into();
                        }
                        if let Some(reason) = authentication.reason.take() {
                            authentication.reason =
                                Some(bounded_probe_diagnostic(&reason, &context.environment));
                        }
                        authentication
                    }
                    Err(error) => HarnessAuthentication::unavailable(
                        "authentication_probe",
                        bounded_probe_diagnostic(&error.to_string(), &context.environment),
                        false,
                    ),
                }
            }
            Err(error) => HarnessAuthentication::unavailable(
                "authentication_probe",
                bounded_probe_diagnostic(
                    &format!(
                        "{provider} authentication could not be inspected: {}",
                        error.message
                    ),
                    &context.environment,
                ),
                error.retryable,
            ),
        }
    }

    async fn execute_authentication_command(
        &self,
        command: CommandSpec,
        working_directory: PathBuf,
    ) -> crate::TransportResult<(Vec<u8>, String, TransportExitStatus)> {
        let initial_stdin = command.initial_stdin.clone();
        let mut process = self
            .transport
            .spawn(TransportSpawnRequest {
                command,
                working_directory,
            })
            .await?;
        if let Some(initial) = initial_stdin {
            let Some(mut stdin) = process.take_stdin() else {
                let _ = process.terminate().await;
                return Err(extension_transport_error(
                    self.transport.name(),
                    "probe_authentication",
                    "the authentication probe did not expose stdin",
                    false,
                ));
            };
            if let Err(error) = stdin.write_all(&initial).await {
                let _ = process.terminate().await;
                return Err(extension_transport_error(
                    self.transport.name(),
                    "probe_authentication",
                    format!("could not write authentication probe input: {error}"),
                    true,
                ));
            }
            drop(stdin);
        } else {
            drop(process.take_stdin());
        }
        let Some(stdout) = process.take_stdout() else {
            let _ = process.terminate().await;
            return Err(extension_transport_error(
                self.transport.name(),
                "probe_authentication",
                "the authentication probe did not expose stdout",
                false,
            ));
        };
        let stderr = process.take_stderr();
        let run = async {
            let read_stdout = async {
                let mut output = Vec::new();
                stdout
                    .take((AUTHENTICATION_MAX_BYTES + 1) as u64)
                    .read_to_end(&mut output)
                    .await
                    .map_err(|error| error.to_string())?;
                if output.len() > AUTHENTICATION_MAX_BYTES {
                    return Err(format!(
                        "authentication output exceeded {AUTHENTICATION_MAX_BYTES} bytes"
                    ));
                }
                Ok(output)
            };
            let read_stderr = async {
                match stderr {
                    Some(stderr) => crate::process::bounded_stderr(stderr, STDERR_TAIL_BYTES)
                        .await
                        .map_err(|error| error.to_string()),
                    None => Ok(String::new()),
                }
            };
            let (status, stdout, stderr) = tokio::join!(process.wait(), read_stdout, read_stderr);
            Ok::<_, String>((stdout?, stderr?, status.map_err(|error| error.message)?))
        };
        match tokio::time::timeout(AUTHENTICATION_TIMEOUT, run).await {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(message)) => {
                let _ = process.terminate().await;
                Err(extension_transport_error(
                    self.transport.name(),
                    "probe_authentication",
                    message,
                    true,
                ))
            }
            Err(_) => {
                let _ = process.terminate().await;
                Err(TransportError::new(
                    TransportErrorKind::ConnectionTimedOut,
                    self.transport.name(),
                    "probe_authentication",
                    format!(
                        "authentication probe exceeded {} seconds",
                        AUTHENTICATION_TIMEOUT.as_secs()
                    ),
                    true,
                ))
            }
        }
    }

    async fn probe_harness_metadata(
        &self,
        adapter: Arc<dyn AgentAdapter>,
        context: ProviderProbeContext,
    ) -> (
        Vec<HarnessControlGroup>,
        HarnessModelCatalog,
        Option<crate::AccountUsageSnapshot>,
    ) {
        let static_controls = adapter.control_groups();
        let Some(probe) = adapter.catalog_probe() else {
            return (
                static_controls,
                HarnessModelCatalog::unsupported("unsupported"),
                None,
            );
        };
        let provider = adapter.provider();
        let mut command = probe.command;
        apply_probe_context(&mut command, &context);
        let program = command.program.clone();
        let expected = probe.expected_response_ids.clone().unwrap_or_default();
        let mut process = match self
            .transport
            .spawn(TransportSpawnRequest {
                command: command.clone(),
                working_directory: context.working_directory,
            })
            .await
        {
            Ok(process) => process,
            Err(error) => {
                let message = redact_secrets(&error.message, &context.environment);
                return (
                    static_controls,
                    failed_catalog(
                        HarnessCatalogErrorKind::Transport,
                        format!(
                            "could not start {} metadata probe: {}",
                            program.display(),
                            message
                        ),
                        error.retryable,
                    ),
                    None,
                );
            }
        };
        let mut stdin_guard = None;
        if let Some(initial) = &command.initial_stdin {
            let Some(mut stdin) = process.take_stdin() else {
                let _ = process.terminate().await;
                return (
                    static_controls,
                    failed_catalog(
                        HarnessCatalogErrorKind::Transport,
                        "metadata probe did not expose stdin".into(),
                        false,
                    ),
                    None,
                );
            };
            if stdin.write_all(initial).await.is_err()
                || stdin.write_all(b"\n").await.is_err()
                || stdin.flush().await.is_err()
            {
                let _ = process.terminate().await;
                return (
                    static_controls,
                    failed_catalog(
                        HarnessCatalogErrorKind::Transport,
                        "could not write metadata probe request".into(),
                        true,
                    ),
                    None,
                );
            }
            if !expected.is_empty() {
                stdin_guard = Some(stdin);
            }
        }
        let Some(stdout) = process.take_stdout() else {
            let _ = process.terminate().await;
            return (
                static_controls,
                failed_catalog(
                    HarnessCatalogErrorKind::Transport,
                    "metadata probe did not expose stdout".into(),
                    false,
                ),
                None,
            );
        };
        let stderr_task = process
            .take_stderr()
            .map(|stderr| tokio::spawn(crate::process::bounded_stderr(stderr, STDERR_TAIL_BYTES)));
        let mut seen = Vec::with_capacity(expected.len());
        let read_lines = async {
            let mut reader = BufReader::new(stdout).lines();
            let mut lines = Vec::new();
            let mut bytes = 0usize;
            while let Some(line) = reader
                .next_line()
                .await
                .map_err(|error| error.to_string())?
            {
                bytes = bytes.saturating_add(line.len());
                if bytes > CATALOG_MAX_BYTES {
                    return Err(format!(
                        "metadata output exceeded {CATALOG_MAX_BYTES} bytes"
                    ));
                }
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                    if let Some(id) = value.get("id").and_then(serde_json::Value::as_u64) {
                        if expected.contains(&id) && !seen.contains(&id) {
                            seen.push(id);
                        }
                    }
                }
                lines.push(line);
                if !expected.is_empty() && expected.iter().all(|id| seen.contains(id)) {
                    break;
                }
            }
            Ok::<_, String>(lines)
        };
        let lines = match tokio::time::timeout(CATALOG_TIMEOUT, read_lines).await {
            Ok(Ok(lines)) => lines,
            Ok(Err(message)) => {
                let _ = process.terminate().await;
                if let Some(task) = stderr_task {
                    task.abort();
                }
                return (
                    static_controls,
                    failed_catalog(HarnessCatalogErrorKind::Transport, message, true),
                    None,
                );
            }
            Err(_) => {
                let _ = process.terminate().await;
                if let Some(task) = stderr_task {
                    task.abort();
                }
                return (
                    static_controls,
                    failed_catalog(
                        HarnessCatalogErrorKind::Timeout,
                        format!(
                            "{provider} metadata probe exceeded {} seconds",
                            CATALOG_TIMEOUT.as_secs()
                        ),
                        true,
                    ),
                    None,
                );
            }
        };
        let completed_early = !expected.is_empty() && expected.iter().all(|id| seen.contains(id));
        drop(stdin_guard);
        let status = if completed_early {
            let _ = process.terminate().await;
            None
        } else {
            process.wait().await.ok()
        };
        let stderr = match stderr_task {
            Some(task) => task
                .await
                .ok()
                .and_then(std::result::Result::ok)
                .unwrap_or_default(),
            None => String::new(),
        };
        if status.is_some_and(|status| !status.success) {
            let diagnostic = if stderr.trim().is_empty() {
                format!("{provider} metadata command exited unsuccessfully")
            } else {
                redact_secrets(stderr.trim(), &context.environment)
            };
            return (
                static_controls,
                failed_catalog(
                    catalog_failure_kind(&diagnostic, HarnessCatalogErrorKind::CommandFailed),
                    diagnostic,
                    false,
                ),
                None,
            );
        }
        let controls = adapter
            .parse_control_groups(&lines)
            .unwrap_or_else(|_| static_controls.clone());
        let models = adapter.parse_catalog(&lines).unwrap_or_else(|error| {
            let diagnostic = redact_secrets(&error.to_string(), &context.environment);
            failed_catalog(
                catalog_failure_kind(&diagnostic, HarnessCatalogErrorKind::Protocol),
                diagnostic,
                false,
            )
        });
        let account_usage = adapter.parse_account_usage(&lines).ok().flatten();
        (controls, models, account_usage)
    }

    /// Inspect static and interactive permission behavior without starting a turn.
    pub fn permission_support(&self, provider: Provider) -> Result<crate::PermissionSupport> {
        let adapter = self
            .adapters
            .get(&provider)
            .ok_or(RuntimeError::AdapterUnavailable { provider })?;
        Ok(adapter.permission_support())
    }

    /// Inspect provider-neutral launch-context support without starting a turn.
    pub fn launch_context_capabilities(
        &self,
        provider: Provider,
    ) -> Result<crate::LaunchContextCapabilities> {
        let adapter = self
            .adapters
            .get(&provider)
            .ok_or(RuntimeError::AdapterUnavailable { provider })?;
        Ok(adapter.launch_context_capabilities())
    }

    /// Inspect optional per-turn provider behaviors without starting a turn.
    pub fn turn_capabilities(&self, provider: Provider) -> Result<crate::TurnCapabilities> {
        let adapter = self
            .adapters
            .get(&provider)
            .ok_or(RuntimeError::AdapterUnavailable { provider })?;
        Ok(adapter.turn_capabilities())
    }

    /// Run one turn, streaming normalized events with backpressure.
    pub async fn run(
        &self,
        request: TurnRequest,
        events: &dyn EventSink,
        interactions: Option<&dyn InteractionHandler>,
    ) -> Result<TurnResult> {
        let mut trace = StartupTrace::new(request.provider, self.startup_observer.clone());
        let result = self.run_inner(request, events, interactions, &trace).await;
        trace.finish(&result);
        result
    }

    async fn run_inner(
        &self,
        request: TurnRequest,
        events: &dyn EventSink,
        interactions: Option<&dyn InteractionHandler>,
        trace: &StartupTrace,
    ) -> Result<TurnResult> {
        self.validate(&request)?;
        self.validate_working_directory(&request).await?;
        let provider = request.provider;
        let adapter = self
            .adapters
            .get(&provider)
            .cloned()
            .ok_or(RuntimeError::AdapterUnavailable { provider })?;
        if !adapter
            .permission_support()
            .supports(&request.permission_mode)
        {
            return Err(RuntimeError::InvalidRequest {
                field: "permission_mode",
                message: format!(
                    "{:?} is not supported by the configured {provider} adapter",
                    request.permission_mode
                ),
            });
        }
        let control_groups = adapter.control_groups();
        for (key, value) in &request.harness_options {
            // Codex's credential-free relay descriptor is an embedder-only
            // option validated by its adapter, not a user-selectable control.
            // Do not advertise its free-form JSON as a control-group choice.
            if key == "model_relay" && provider == Provider::Codex {
                continue;
            }
            if key == "service_tier" && provider == Provider::Codex {
                continue;
            }
            let Some(group) = control_groups.iter().find(|group| group.id == *key) else {
                return Err(RuntimeError::InvalidRequest {
                    field: "harness_options",
                    message: format!("{provider} does not advertise the `{key}` control"),
                });
            };
            if !group.options.iter().any(|option| option.id == *value) {
                return Err(RuntimeError::InvalidRequest {
                    field: "harness_options",
                    message: format!("{provider} does not advertise `{value}` for `{key}`"),
                });
            }
        }
        trace.record(StartupStage::Validated);
        // A pre-cancelled turn must never race an immediately available permit
        // into spawning a provider executable.
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Cancelled { provider });
        }
        trace.record(StartupStage::WaitingForPermit);
        let permit = tokio::select! {
            _ = request.cancellation.cancelled() => {
                return Err(RuntimeError::Cancelled { provider });
            }
            permit = self.permits.clone().acquire_owned() => permit.map_err(|_| RuntimeError::Cancelled { provider })?,
        };
        trace.record(StartupStage::PermitAcquired);
        let timeout = request.timeout;
        let result = tokio::time::timeout(
            timeout,
            self.run_process(
                adapter,
                &request,
                events,
                interactions.unwrap_or(&DenyAll),
                trace,
            ),
        )
        .await;
        drop(permit);
        match result {
            Ok(result) => result,
            Err(_) => Err(RuntimeError::Timeout {
                provider,
                seconds: timeout.as_secs(),
            }),
        }
    }

    /// Run a turn with opt-in, bounded recovery for an application-managed
    /// sandbox profile.
    ///
    /// A denied tool step is never approved automatically. The handler first
    /// chooses an exact least-privilege change, the manager durably commits a
    /// new profile revision, and only then does the runtime resume the same
    /// provider session with a narrow retry instruction. The runtime never
    /// retries without a sandbox or changes provider permission mode.
    pub async fn run_with_sandbox_recovery(
        &self,
        mut request: TurnRequest,
        events: &dyn EventSink,
        interactions: Option<&dyn InteractionHandler>,
        recovery: &dyn SandboxRecoveryHandler,
        policy: SandboxRecoveryPolicy,
    ) -> Result<TurnResult> {
        if policy.max_retries > MAX_SANDBOX_RECOVERY_RETRIES {
            return Err(RuntimeError::InvalidRequest {
                field: "sandbox_recovery.max_retries",
                message: format!("must be at most {MAX_SANDBOX_RECOVERY_RETRIES}"),
            });
        }
        self.validate(&request)?;
        self.validate_working_directory(&request).await?;
        let provider = request.provider;
        let adapter = self
            .adapters
            .get(&provider)
            .cloned()
            .ok_or(RuntimeError::AdapterUnavailable { provider })?;
        if !adapter
            .permission_support()
            .supports(&request.permission_mode)
        {
            return Err(RuntimeError::InvalidRequest {
                field: "permission_mode",
                message: format!(
                    "{:?} is not supported by the configured {provider} adapter",
                    request.permission_mode
                ),
            });
        }
        let sandbox = request
            .sandbox
            .as_ref()
            .ok_or_else(|| RuntimeError::InvalidRequest {
                field: "sandbox",
                message: "managed sandbox recovery requires a sandbox profile".to_string(),
            })?;
        let (manager, mut profile) =
            sandbox
                .managed_parts()
                .ok_or_else(|| RuntimeError::InvalidRequest {
                    field: "sandbox",
                    message: "managed sandbox recovery requires SandboxRequest::managed"
                        .to_string(),
                })?;
        let required = sandbox.required_capabilities();
        let context = SandboxContext {
            provider,
            working_directory: request.working_directory.clone(),
        };
        let _permit = tokio::select! {
            _ = request.cancellation.cancelled() => {
                return Err(RuntimeError::Cancelled { provider });
            }
            permit = self.permits.clone().acquire_owned() => permit.map_err(|_| RuntimeError::Cancelled { provider })?,
        };

        for retry_index in 0..=policy.max_retries {
            let resolved = tokio::select! {
                _ = request.cancellation.cancelled() => {
                    return Err(RuntimeError::Cancelled { provider });
                }
                resolved = tokio::time::timeout(
                    request.interaction_timeout,
                    manager.resolve(&profile, &context),
                ) => resolved.map_err(|_| {
                    RuntimeError::Sandbox(SandboxError::ProfileTimeout {
                        manager: manager.name().to_string(),
                        operation: "profile resolution",
                        seconds: request.interaction_timeout.as_secs(),
                    })
                })??,
            };
            if resolved.profile.id != profile.id || resolved.profile.revision.is_none() {
                return Err(RuntimeError::Sandbox(SandboxError::InvalidProfile {
                    manager: manager.name().to_string(),
                    profile: profile.id,
                    message: "resolution must preserve identity and return an exact revision"
                        .to_string(),
                }));
            }
            let exact_profile = resolved.profile.clone();
            let resolved = crate::sandbox::ResolvedSandbox {
                backend: resolved.backend,
                profile: Some(exact_profile.clone()),
                required,
            };
            if !resolved.backend.capabilities().satisfies(required) {
                return Err(RuntimeError::Sandbox(SandboxError::MissingCapabilities {
                    backend: resolved.backend.name().to_string(),
                    missing: resolved.backend.capabilities().missing(required),
                }));
            }
            profile = exact_profile;
            let mut attempt = request.clone();
            attempt.sandbox = Some(resolved.as_request());
            let tracking_events = SandboxEventSink::new(events, &resolved);
            let timeout = attempt.timeout;
            let mut trace = StartupTrace::new(provider, self.startup_observer.clone());
            let result = tokio::time::timeout(
                timeout,
                self.run_process(
                    adapter.clone(),
                    &attempt,
                    &tracking_events,
                    interactions.unwrap_or(&DenyAll),
                    &trace,
                ),
            )
            .await
            .map_err(|_| RuntimeError::Timeout {
                provider,
                seconds: timeout.as_secs(),
            })
            .and_then(std::convert::identity);
            trace.finish(&result);
            let result = result?;
            let Some(violation) = tracking_events.violation() else {
                return Ok(result);
            };
            if retry_index == policy.max_retries {
                return Ok(result);
            }
            let Some(session_id) = result.session_id.clone() else {
                events
                    .emit(TurnEvent::Warning {
                        message: "sandbox access was denied, but the provider supplied no resumable session id; the step was not retried".to_string(),
                    })
                    .await?;
                return Ok(result);
            };
            let attempt_number = retry_index.saturating_add(1);
            let decision = tokio::select! {
                _ = request.cancellation.cancelled() => {
                    return Err(RuntimeError::Cancelled { provider });
                }
                decision = tokio::time::timeout(
                    request.interaction_timeout,
                    recovery.recover(SandboxRecoveryRequest {
                        profile: profile.clone(),
                        violation: violation.clone(),
                        attempt: attempt_number,
                    }),
                ) => decision.unwrap_or(SandboxRecoveryDecision::Deny {
                    reason: Some("sandbox recovery approval timed out".to_string()),
                }),
            };
            let SandboxRecoveryDecision::Retry { change } = decision else {
                return Ok(result);
            };
            let updated = tokio::select! {
                _ = request.cancellation.cancelled() => {
                    return Err(RuntimeError::Cancelled { provider });
                }
                updated = tokio::time::timeout(
                    request.interaction_timeout,
                    manager.update(
                        SandboxProfileUpdate {
                            profile: profile.clone(),
                            change: change.clone(),
                            violation: violation.clone(),
                        },
                        &context,
                    ),
                ) => updated.map_err(|_| {
                    RuntimeError::Sandbox(SandboxError::ProfileTimeout {
                        manager: manager.name().to_string(),
                        operation: "profile update",
                        seconds: request.interaction_timeout.as_secs(),
                    })
                })??,
            };
            if updated.profile.id != profile.id || updated.profile.revision.is_none() {
                return Err(RuntimeError::Sandbox(SandboxError::InvalidProfile {
                    manager: manager.name().to_string(),
                    profile: profile.id,
                    message: "update must preserve identity and return an exact revision"
                        .to_string(),
                }));
            }
            profile = updated.profile;
            events
                .emit(TurnEvent::SandboxProfileUpdated {
                    profile: profile.clone(),
                    change,
                })
                .await?;
            events
                .emit(TurnEvent::SandboxStepRetrying {
                    step_id: violation.step_id,
                    attempt: attempt_number,
                })
                .await?;
            request.session_id = Some(session_id);
            request.prompt = SANDBOX_RETRY_PROMPT.to_string();
        }
        unreachable!("bounded sandbox recovery loop always returns")
    }

    fn validate(&self, request: &TurnRequest) -> Result<()> {
        if request.prompt.trim().is_empty() {
            return Err(RuntimeError::InvalidRequest {
                field: "prompt",
                message: "must not be empty".to_string(),
            });
        }
        if request.prompt.len() > self.max_prompt_bytes {
            return Err(RuntimeError::InvalidRequest {
                field: "prompt",
                message: format!("exceeds the {} byte limit", self.max_prompt_bytes),
            });
        }
        if request.timeout.is_zero() || request.interaction_timeout.is_zero() {
            return Err(RuntimeError::InvalidRequest {
                field: "timeout",
                message: "turn and interaction timeouts must be greater than zero".to_string(),
            });
        }
        match request.auto_compaction {
            crate::AutoCompactionPolicy::Automatic
            | crate::AutoCompactionPolicy::TokenThreshold { .. }
                if request.provider != Provider::Claude =>
            {
                return Err(RuntimeError::InvalidRequest {
                    field: "auto_compaction",
                    message: format!(
                        "{} does not support configurable automatic compaction",
                        request.provider
                    ),
                });
            }
            crate::AutoCompactionPolicy::TokenThreshold { tokens }
                if !(100_000..=1_000_000).contains(&tokens) =>
            {
                return Err(RuntimeError::InvalidRequest {
                    field: "auto_compaction",
                    message: "Claude automatic compaction threshold must be between 100000 and 1000000 tokens"
                        .to_string(),
                });
            }
            crate::AutoCompactionPolicy::ProviderDefault
            | crate::AutoCompactionPolicy::Automatic
            | crate::AutoCompactionPolicy::TokenThreshold { .. } => {}
        }
        validate_attachments(request)?;
        validate_explicit_environment(&request.environment, "environment")?;
        let capabilities = self
            .adapters
            .get(&request.provider)
            .ok_or(RuntimeError::AdapterUnavailable {
                provider: request.provider,
            })?
            .launch_context_capabilities();
        validate_launch_context(request, capabilities)?;
        if let Some(sandbox) = &request.sandbox {
            sandbox.validate()?;
        }
        let available_sandbox =
            self.transport
                .capabilities()
                .sandbox
                .union(request.sandbox.as_ref().map_or(
                    crate::SandboxCapabilities::NONE,
                    crate::SandboxRequest::capabilities,
                ));
        if !available_sandbox.satisfies(request.required_sandbox_capabilities) {
            return Err(RuntimeError::Sandbox(SandboxError::MissingCapabilities {
                backend: format!("{} execution transport", self.transport.name()),
                missing: available_sandbox.missing(request.required_sandbox_capabilities),
            }));
        }
        Ok(())
    }

    async fn validate_working_directory(&self, request: &TurnRequest) -> Result<()> {
        self.transport
            .validate_working_directory(&request.working_directory)
            .await
            .map_err(|source| RuntimeError::Transport {
                provider: request.provider,
                source,
            })
    }

    async fn run_process(
        &self,
        adapter: Arc<dyn AgentAdapter>,
        request: &TurnRequest,
        events: &dyn EventSink,
        interactions: &dyn InteractionHandler,
        trace: &StartupTrace,
    ) -> Result<TurnResult> {
        let provider = request.provider;
        let mut state = AdapterState::default();
        // A resumed provider process commonly repeats its native session ID in
        // the startup handshake. Seed the parser with the ID the caller is
        // already attached to so adapters do not project that handshake as a
        // second `SessionStarted` lifecycle event.
        state.result.session_id.clone_from(&request.session_id);
        // Seeded before the command is built so an adapter that must agree
        // with itself about a per-turn value — the loopback port an
        // `opencode serve` child is told to bind, which `attach` later
        // connects to — decides it once, here.
        adapter.prepare_turn(request, &mut state)?;
        let mut spec = adapter.command_for_turn(request, &state)?;
        for (name, value) in &request.environment {
            spec.environment.insert(name.into(), value.expose().into());
        }
        trace.record(StartupStage::CommandPrepared);
        if let Some(sandbox) = &request.sandbox {
            let context = SandboxContext {
                provider,
                working_directory: request.working_directory.clone(),
            };
            spec = tokio::select! {
                _ = request.cancellation.cancelled() => {
                    return Err(RuntimeError::Cancelled { provider });
                }
                prepared = sandbox.prepare(context, spec) => prepared?,
            };
            trace.record(StartupStage::SandboxPrepared);
        }
        let capabilities = self.transport.capabilities();
        if spec.interactive_stdin && !capabilities.interactive_stdin {
            return Err(RuntimeError::TransportCapabilityUnavailable {
                provider,
                transport: self.transport.name().to_string(),
                capability: "interactive_stdin",
                message: "select an SSH transport or a raw terminal transport for live approvals"
                    .to_string(),
            });
        }
        if !capabilities.process_tree_termination {
            return Err(RuntimeError::TransportCapabilityUnavailable {
                provider,
                transport: self.transport.name().to_string(),
                capability: "process_tree_termination",
                message: "the transport must terminate provider tool descendants".to_string(),
            });
        }
        let program = spec.program.clone();
        let mut process = self
            .transport
            .spawn(TransportSpawnRequest {
                command: spec.clone(),
                working_directory: request.working_directory.clone(),
            })
            .await
            .map_err(|source| {
                if source.kind == TransportErrorKind::ExecutableNotFound {
                    RuntimeError::ExecutableNotFound {
                        provider,
                        executable: program.display().to_string(),
                    }
                } else {
                    RuntimeError::Transport {
                        provider,
                        source: redact_transport_error(source, &request.environment),
                    }
                }
            })?;
        trace.record(StartupStage::ProcessSpawned);
        let stdin = process
            .take_stdin()
            .ok_or_else(|| RuntimeError::ProcessIo {
                provider,
                stream: "stdin setup",
                source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stdin was not piped"),
            })?;
        let mut stdin = Some(stdin);
        if let Some(initial) = &spec.initial_stdin {
            let writer = stdin.as_mut().expect("stdin initialized");
            writer
                .write_all(initial)
                .await
                .map_err(|source| RuntimeError::ProcessIo {
                    provider,
                    stream: "stdin write",
                    source,
                })?;
            writer
                .write_all(b"\n")
                .await
                .map_err(|source| RuntimeError::ProcessIo {
                    provider,
                    stream: "stdin write",
                    source,
                })?;
            writer
                .flush()
                .await
                .map_err(|source| RuntimeError::ProcessIo {
                    provider,
                    stream: "stdin flush",
                    source,
                })?;
        }
        if spec.initial_stdin.is_some() {
            trace.record(StartupStage::InitialInputWritten);
        }
        if !spec.interactive_stdin {
            stdin.take();
        }
        let stdout = process
            .take_stdout()
            .ok_or_else(|| RuntimeError::ProcessIo {
                provider,
                stream: "stdout setup",
                source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stdout was not piped"),
            })?;
        let stderr = process
            .take_stderr()
            .ok_or_else(|| RuntimeError::ProcessIo {
                provider,
                stream: "stderr setup",
                source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stderr was not piped"),
            })?;
        let stderr_task = tokio::spawn(crate::process::bounded_stderr(stderr, STDERR_TAIL_BYTES));
        // A provider whose protocol is not carried by its own stdio replaces
        // both halves here. The child stays spawned, supervised and
        // stderr-drained exactly as before; only the frame carrier differs,
        // so everything below this point — cancellation, interrupts,
        // interaction timeouts, line bounding — is shared by both kinds of
        // provider instead of growing a second turn loop.
        let mut attached = false;
        let reader: crate::TransportReader = match adapter.attach(request, &state).await {
            Ok(Some(streams)) => {
                attached = true;
                stdin = Some(streams.writer);
                streams.reader
            }
            Ok(None) => stdout,
            Err(error) => {
                let _ = process.terminate().await;
                stderr_task.abort();
                return Err(error);
            }
        };
        trace.record(StartupStage::StreamsAttached);
        let mut first_output = true;
        let mut first_text = true;
        let mut lines = BufReader::new(reader).lines();
        let mut protocol_completed = false;
        loop {
            let line = tokio::select! {
                _ = request.cancellation.cancelled() => {
                    let error = cancel_running_process(
                        provider,
                        adapter.as_ref(),
                        &state,
                        &mut stdin,
                        &mut process,
                    )
                    .await;
                    stderr_task.abort();
                    return Err(error);
                }
                line = lines.next_line() => line.map_err(|source| RuntimeError::ProcessIo {
                    provider,
                    stream: "stdout read",
                    source,
                })?,
            };
            let Some(line) = line else { break };
            if first_output {
                first_output = false;
                trace.record(StartupStage::FirstOutput);
            }
            if line.len() > self.max_event_line_bytes {
                let _ = process.terminate().await;
                stderr_task.abort();
                return Err(RuntimeError::Protocol {
                    provider,
                    message: format!("event line exceeded {} bytes", self.max_event_line_bytes),
                });
            }
            let output = adapter.parse_line(&line, &mut state)?;
            for event in output.events {
                if first_text && matches!(&event, TurnEvent::TextDelta { text } if !text.is_empty())
                {
                    first_text = false;
                    trace.record(StartupStage::FirstText);
                }
                events.emit(event).await?;
            }
            write_provider_frames(provider, stdin.as_mut(), &output.writes, "provider write")
                .await?;
            if let Some(interaction) = output.interaction {
                let response = match interaction {
                    InteractionRequest::Approval {
                        request: approval,
                        original,
                    } => {
                        let decision = tokio::select! {
                            _ = request.cancellation.cancelled() => {
                                let error = cancel_running_process(
                                    provider,
                                    adapter.as_ref(),
                                    &state,
                                    &mut stdin,
                                    &mut process,
                                )
                                .await;
                                stderr_task.abort();
                                return Err(error);
                            }
                            decision = tokio::time::timeout(
                                request.interaction_timeout,
                                interactions.approve(approval.clone()),
                            ) => decision.unwrap_or_else(|_| {
                                crate::ApprovalDecision::Deny {
                                    reason: Some("Approval timed out".to_string()),
                                }
                            }),
                        };
                        adapter.approval_response(&approval, &original, decision)?
                    }
                    InteractionRequest::Question {
                        request: question,
                        original,
                    } => {
                        let answer = tokio::select! {
                            _ = request.cancellation.cancelled() => {
                                let error = cancel_running_process(
                                    provider,
                                    adapter.as_ref(),
                                    &state,
                                    &mut stdin,
                                    &mut process,
                                )
                                .await;
                                stderr_task.abort();
                                return Err(error);
                            }
                            answer = tokio::time::timeout(
                                request.interaction_timeout,
                                interactions.answer(question.clone()),
                            ) => answer.ok().flatten(),
                        };
                        adapter.question_response(&question, &original, answer)?
                    }
                };
                if let Some(response) = response {
                    write_provider_frames(
                        provider,
                        stdin.as_mut(),
                        std::slice::from_ref(&response),
                        "interaction response",
                    )
                    .await?;
                }
            }
            if output.terminal {
                protocol_completed = true;
                stdin.take();
                if attached {
                    // A stdio provider is read to end-of-output because
                    // closing its stdin is what makes it exit, and trailing
                    // lines can still arrive. An adapter-supplied carrier has
                    // no such contract: the protocol is over, and waiting for
                    // the adapter to close its own reader would hand a third
                    // party the ability to hang the turn.
                    break;
                }
            }
        }
        drop(stdin);
        let status = if attached {
            // A provider driven over adapter-supplied streams has no reason to
            // exit when the protocol ends: `opencode serve` is a server, and
            // nothing closes it because a turn finished. Waiting for a natural
            // exit would hang until the turn deadline, so stopping it *is* the
            // normal shutdown here. A turn that failed has already recorded
            // why, so the exit status this synthesizes is never what decides
            // the outcome.
            process
                .terminate()
                .await
                .map_err(|source| RuntimeError::Transport { provider, source })?;
            match tokio::time::timeout(ATTACHED_SHUTDOWN_GRACE, process.wait()).await {
                Ok(status) => {
                    let status =
                        status.map_err(|source| RuntimeError::Transport { provider, source })?;
                    // An intentional server shutdown can exit by signal (Unix)
                    // or a nonzero termination code (Windows). Only a terminal
                    // protocol event makes that expected; EOF alone is not success.
                    if protocol_completed {
                        TransportExitStatus {
                            success: true,
                            code: None,
                        }
                    } else {
                        status
                    }
                }
                Err(_) => TransportExitStatus {
                    success: protocol_completed,
                    code: None,
                },
            }
        } else {
            process
                .wait()
                .await
                .map_err(|source| RuntimeError::Transport { provider, source })?
        };
        match request.tool_process_policy {
            crate::ToolProcessPolicy::PreserveOnCompletion => process.disarm(),
            crate::ToolProcessPolicy::TerminateOnCompletion if !attached => {
                process
                    .terminate()
                    .await
                    .map_err(|source| RuntimeError::Transport { provider, source })?;
            }
            crate::ToolProcessPolicy::TerminateOnCompletion => {}
        }
        let stderr = stderr_task
            .await
            .map_err(|error| RuntimeError::Protocol {
                provider,
                message: format!("stderr reader task failed: {error}"),
            })?
            .map_err(|source| RuntimeError::ProcessIo {
                provider,
                stream: "stderr read",
                source,
            })?;
        let terminal_failure = state.terminal_failure.take();
        if !status.success
            || terminal_failure.is_some()
            || state.result.status == crate::RunStatus::Failed
        {
            let (diagnostic, explicit_kind, provider_code, delivery) =
                if let Some(failure) = terminal_failure {
                    let diagnostic = if failure.diagnostic.trim().is_empty() {
                        stderr.trim().to_string()
                    } else {
                        failure.diagnostic.trim().to_string()
                    };
                    (
                        diagnostic,
                        Some(failure.kind),
                        failure.provider_code,
                        failure.delivery,
                    )
                } else {
                    let diagnostic = if !stderr.trim().is_empty() {
                        stderr.trim().to_string()
                    } else if !state.result.text.trim().is_empty() {
                        state.result.text.trim().to_string()
                    } else {
                        "provider returned no diagnostic output".to_string()
                    };
                    let delivery = if state.result.status == crate::RunStatus::Failed {
                        crate::lifecycle::DeliveryState::Accepted
                    } else {
                        crate::lifecycle::DeliveryState::PossiblySent
                    };
                    (diagnostic, None, None, delivery)
                };
            let diagnostic = redact_secrets(&diagnostic, &request.environment)
                .chars()
                .take(STDERR_TAIL_BYTES)
                .collect::<String>();
            let provider_code = provider_code.map(|code| {
                redact_secrets(&code, &request.environment)
                    .chars()
                    .take(MAX_PROVIDER_CODE_CHARS)
                    .collect::<String>()
            });
            let classified = classify_provider_failure(&format!(
                "{} {diagnostic}",
                provider_code.as_deref().unwrap_or_default()
            ));
            let kind = explicit_kind
                .filter(|kind| *kind != ProviderProcessErrorKind::Unknown)
                .unwrap_or(classified);
            return Err(RuntimeError::ProcessFailed {
                provider,
                kind,
                exit_code: status.code,
                stderr: diagnostic,
                provider_code,
                delivery,
            });
        }
        if state.result.text.is_empty() {
            events
                .emit(TurnEvent::Warning {
                    message: format!("{provider} completed without a text response"),
                })
                .await?;
        }
        Ok(state.result)
    }
}

fn redact_transport_error(
    mut error: TransportError,
    environment: &std::collections::BTreeMap<String, crate::SecretString>,
) -> TransportError {
    error.message = redact_secrets(&error.message, environment);
    error
}

fn redact_secrets(
    value: &str,
    environment: &std::collections::BTreeMap<String, crate::SecretString>,
) -> String {
    let mut secrets = environment
        .values()
        .map(crate::SecretString::expose)
        .filter(|secret| !secret.is_empty() && value.contains(secret))
        .collect::<Vec<_>>();
    secrets.sort_unstable_by_key(|secret| std::cmp::Reverse(secret.len()));
    secrets.dedup();
    secrets
        .into_iter()
        .fold(value.to_string(), |redacted, secret| {
            redacted.replace(secret, "[REDACTED]")
        })
}

const fn provider_order(provider: Provider) -> u8 {
    match provider {
        Provider::Claude => 0,
        Provider::Codex => 1,
        Provider::OpenCode => 2,
    }
}

fn provider_argument(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
        Provider::OpenCode => "open_code",
    }
}

fn scope_argument(scope: HarnessExtensionScope) -> &'static str {
    match scope {
        HarnessExtensionScope::User => "user",
        HarnessExtensionScope::Project => "project",
    }
}

fn validate_extension_query(
    query: &HarnessExtensionQuery,
    transport: &str,
) -> crate::TransportResult<()> {
    if !(1..=200).contains(&query.limit) {
        return Err(invalid_extension_request(
            transport,
            "discover_harness_extensions",
            "skill result limit must be between 1 and 200",
        ));
    }
    if query
        .skill_query
        .as_ref()
        .is_some_and(|value| value.len() > 256 || value.contains('\0'))
    {
        return Err(invalid_extension_request(
            transport,
            "discover_harness_extensions",
            "skill search must be at most 256 bytes and cannot contain NUL bytes",
        ));
    }
    Ok(())
}

fn validate_extension_name(
    name: &str,
    transport: &str,
    operation: &'static str,
) -> crate::TransportResult<()> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(invalid_extension_request(
            transport,
            operation,
            "extension names must be 1-64 lowercase alphanumeric characters with single hyphen separators",
        ))
    }
}

fn invalid_extension_request(
    transport: &str,
    operation: &'static str,
    message: impl Into<String>,
) -> TransportError {
    TransportError::new(
        TransportErrorKind::InvalidConfiguration,
        transport,
        operation,
        message,
        false,
    )
}

fn extension_transport_error(
    transport: &str,
    operation: &'static str,
    message: impl Into<String>,
    retryable: bool,
) -> TransportError {
    TransportError::new(
        TransportErrorKind::ProcessControlFailed,
        transport,
        operation,
        message,
        retryable,
    )
}

fn denial_from_transport(error: &TransportError) -> HarnessExtensionDenialKind {
    match error.kind {
        TransportErrorKind::PermissionDenied => HarnessExtensionDenialKind::PermissionDenied,
        TransportErrorKind::ExecutableNotFound => HarnessExtensionDenialKind::HarnessUnavailable,
        _ => HarnessExtensionDenialKind::TransportUnsupported,
    }
}

fn parse_skill_probe(
    output: &[u8],
    query: Option<&str>,
    limit: usize,
    transport: &str,
) -> crate::TransportResult<(
    Vec<HarnessSkill>,
    HarnessExtensionAccess,
    HarnessExtensionAccess,
    HarnessExtensionAccess,
    HarnessExtensionAccess,
)> {
    const MARKER: &[u8] = b"\x1eTEMPS_AGENT_RUNTIME_EXTENSIONS\0";
    let marker = output
        .windows(MARKER.len())
        .rposition(|window| window == MARKER)
        .ok_or_else(|| {
            TransportError::new(
                TransportErrorKind::Protocol,
                transport,
                "discover_skills",
                "the execution host omitted its extension response marker",
                false,
            )
        })?;
    let fields = output[marker + MARKER.len()..]
        .split(|byte| *byte == 0)
        .map(|field| String::from_utf8_lossy(field).into_owned())
        .collect::<Vec<_>>();
    let mut index = 0;
    let mut skills = std::collections::BTreeMap::new();
    let mut user_access = None;
    let mut project_access = None;
    let mut user_mcp_access = None;
    let mut project_mcp_access = None;
    while index < fields.len() {
        if fields[index].is_empty() {
            break;
        }
        match fields[index].as_str() {
            "access" if index + 2 < fields.len() => {
                let scope = fields[index + 1].as_str();
                let allowed = fields[index + 2] == "1";
                let reason = fields
                    .get(index + 3)
                    .cloned()
                    .filter(|value| !value.is_empty());
                let access = if allowed {
                    HarnessExtensionAccess::allowed()
                } else {
                    HarnessExtensionAccess::denied(
                        HarnessExtensionDenialKind::PermissionDenied,
                        reason.unwrap_or_else(|| {
                            "The execution identity cannot write this skill scope.".into()
                        }),
                    )
                };
                if scope == "user" {
                    user_access = Some(access);
                } else if scope == "project" {
                    project_access = Some(access);
                } else if scope == "user_mcp" {
                    user_mcp_access = Some(access);
                } else if scope == "project_mcp" {
                    project_mcp_access = Some(access);
                }
                index += 4;
            }
            "skill" if index + 4 < fields.len() => {
                let scope = if fields[index + 1] == "user" {
                    HarnessExtensionScope::User
                } else {
                    HarnessExtensionScope::Project
                };
                let path = PathBuf::from(&fields[index + 3]);
                let id = path
                    .parent()
                    .and_then(std::path::Path::file_name)
                    .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
                let description = fields[index + 4].trim().to_string();
                let matches = query.is_none_or(|needle| {
                    let needle = needle.to_ascii_lowercase();
                    id.to_ascii_lowercase().contains(&needle)
                        || description.to_ascii_lowercase().contains(&needle)
                });
                if matches && !id.is_empty() {
                    skills.insert(
                        id.clone(),
                        HarnessSkill {
                            id,
                            description: (!description.is_empty()).then_some(description),
                            path,
                            scope,
                            source: fields[index + 2].clone(),
                        },
                    );
                }
                index += 5;
            }
            _ => {
                return Err(TransportError::new(
                    TransportErrorKind::Protocol,
                    transport,
                    "discover_skills",
                    "the execution host returned malformed extension metadata",
                    false,
                ));
            }
        }
    }
    Ok((
        skills.into_values().take(limit).collect(),
        user_access.unwrap_or_else(|| {
            HarnessExtensionAccess::denied(
                HarnessExtensionDenialKind::PermissionDenied,
                "The execution host did not confirm user skill write access.",
            )
        }),
        project_access.unwrap_or_else(|| {
            HarnessExtensionAccess::denied(
                HarnessExtensionDenialKind::PermissionDenied,
                "The execution host did not confirm project skill write access.",
            )
        }),
        user_mcp_access.unwrap_or_else(|| {
            HarnessExtensionAccess::denied(
                HarnessExtensionDenialKind::PermissionDenied,
                "The execution host did not confirm user MCP configuration write access.",
            )
        }),
        project_mcp_access.unwrap_or_else(|| {
            HarnessExtensionAccess::denied(
                HarnessExtensionDenialKind::PermissionDenied,
                "The execution host did not confirm project MCP configuration write access.",
            )
        }),
    ))
}

fn parse_mcp_servers(
    provider: Provider,
    output: &[u8],
    transport: &str,
) -> crate::TransportResult<Vec<HarnessMcpServer>> {
    if provider == Provider::Codex {
        let values = serde_json::from_slice::<serde_json::Value>(output).map_err(|_| {
            TransportError::new(
                TransportErrorKind::Protocol,
                transport,
                "discover_mcp",
                "Codex returned malformed MCP metadata",
                false,
            )
        })?;
        return Ok(values
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|server| {
                Some(HarnessMcpServer {
                    name: server.get("name")?.as_str()?.to_string(),
                    enabled: server.get("enabled").and_then(serde_json::Value::as_bool),
                    transport: server
                        .pointer("/transport/type")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                    detail: server
                        .get("disabled_reason")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                })
            })
            .take(200)
            .collect());
    }
    let text = String::from_utf8_lossy(output);
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("Checking"))
        .filter_map(|line| {
            let name = line
                .split([':', ' ', '\t'])
                .find(|part| !part.is_empty() && part.chars().any(char::is_alphanumeric))?
                .trim_matches(|character: char| {
                    !character.is_alphanumeric() && character != '-' && character != '_'
                })
                .to_string();
            (!name.is_empty()).then_some(HarnessMcpServer {
                name,
                enabled: None,
                transport: None,
                detail: ["connected", "failed", "pending", "disabled"]
                    .into_iter()
                    .find(|status| line.to_ascii_lowercase().contains(status))
                    .map(str::to_string),
            })
        })
        .take(200)
        .collect())
}

fn mcp_management_access(provider: Provider) -> (HarnessExtensionAccess, HarnessExtensionAccess) {
    match provider {
        Provider::Claude => (
            HarnessExtensionAccess::allowed(),
            HarnessExtensionAccess::allowed(),
        ),
        Provider::Codex => (
            HarnessExtensionAccess::allowed(),
            HarnessExtensionAccess::denied(
                HarnessExtensionDenialKind::ScopeUnsupported,
                "Codex CLI MCP management writes user configuration and has no project-scope add/remove command.",
            ),
        ),
        Provider::OpenCode => {
            let denied = HarnessExtensionAccess::denied(
                HarnessExtensionDenialKind::ProviderUnsupported,
                "OpenCode manages MCP servers through JSON/JSONC configuration; this SDK does not rewrite those files because comments and secrets must be preserved.",
            );
            (denied.clone(), denied)
        }
    }
}

fn build_mcp_management_command(
    provider: Provider,
    executable: PathBuf,
    scope: HarnessExtensionScope,
    name: &str,
    definition: Option<HarnessMcpDefinition>,
    transport: &str,
) -> crate::TransportResult<CommandSpec> {
    let mut command = CommandSpec::new(executable);
    match (provider, definition) {
        (Provider::Claude, Some(definition)) => {
            let definition = match definition {
                HarnessMcpDefinition::Stdio {
                    mut command,
                    environment,
                } => {
                    if !environment.is_empty() {
                        return Err(TransportError::new(
                            TransportErrorKind::Unsupported,
                            transport,
                            "manage_mcp_server",
                            "Claude CLI MCP management persists stdio environment values; secret-bearing definitions cannot be added safely",
                            false,
                        ));
                    }
                    if command.is_empty() {
                        return Err(invalid_extension_request(
                            transport,
                            "manage_mcp_server",
                            "an stdio MCP server command cannot be empty",
                        ));
                    }
                    let program = command.remove(0);
                    serde_json::json!({
                        "type": "stdio",
                        "command": program,
                        "args": command,
                        "env": environment,
                    })
                }
                HarnessMcpDefinition::Http {
                    url,
                    bearer_token_env_var,
                } => {
                    let mut definition = serde_json::json!({ "type": "http", "url": url });
                    if let Some(variable) = bearer_token_env_var {
                        definition["headers"] = serde_json::json!({
                            "Authorization": format!("Bearer ${{{variable}}}"),
                        });
                    }
                    definition
                }
            };
            command.args.extend([
                "mcp".into(),
                "add-json".into(),
                name.into(),
                serde_json::to_string(&definition)
                    .map_err(|error| {
                        invalid_extension_request(transport, "manage_mcp_server", error.to_string())
                    })?
                    .into(),
                "--scope".into(),
                scope_argument(scope).into(),
            ]);
        }
        (Provider::Claude, None) => {
            command.args.extend([
                "mcp".into(),
                "remove".into(),
                name.into(),
                "--scope".into(),
                scope_argument(scope).into(),
            ]);
        }
        (Provider::Codex, _) if scope == HarnessExtensionScope::Project => {
            return Err(TransportError::new(
                TransportErrorKind::Unsupported,
                transport,
                "manage_mcp_server",
                "Codex CLI does not expose project-scoped MCP management",
                false,
            ));
        }
        (
            Provider::Codex,
            Some(HarnessMcpDefinition::Stdio {
                command: server,
                environment,
            }),
        ) => {
            if !environment.is_empty() {
                return Err(TransportError::new(
                    TransportErrorKind::Unsupported,
                    transport,
                    "manage_mcp_server",
                    "Codex CLI MCP management persists stdio environment values; secret-bearing definitions cannot be added safely",
                    false,
                ));
            }
            if server.is_empty() {
                return Err(invalid_extension_request(
                    transport,
                    "manage_mcp_server",
                    "an stdio MCP server command cannot be empty",
                ));
            }
            command
                .args
                .extend(["mcp".into(), "add".into(), name.into()]);
            for (key, value) in environment {
                command
                    .args
                    .extend(["--env".into(), format!("{key}={value}").into()]);
            }
            command.args.push("--".into());
            command.args.extend(server.into_iter().map(Into::into));
        }
        (
            Provider::Codex,
            Some(HarnessMcpDefinition::Http {
                url,
                bearer_token_env_var,
            }),
        ) => {
            command.args.extend([
                "mcp".into(),
                "add".into(),
                name.into(),
                "--url".into(),
                url.into(),
            ]);
            if let Some(variable) = bearer_token_env_var {
                command
                    .args
                    .extend(["--bearer-token-env-var".into(), variable.into()]);
            }
        }
        (Provider::Codex, None) => {
            command
                .args
                .extend(["mcp".into(), "remove".into(), name.into()]);
        }
        (Provider::OpenCode, _) => {
            return Err(TransportError::new(
                TransportErrorKind::Unsupported,
                transport,
                "manage_mcp_server",
                "OpenCode MCP JSON/JSONC management is not safely supported",
                false,
            ));
        }
    }
    Ok(command)
}

fn failed_catalog(
    kind: HarnessCatalogErrorKind,
    message: String,
    retryable: bool,
) -> HarnessModelCatalog {
    HarnessModelCatalog {
        status: HarnessCatalogStatus::Failed,
        source: "runtime_probe".into(),
        models: Vec::new(),
        error: Some(HarnessCatalogError {
            kind,
            message,
            retryable,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_launch_context_capabilities() -> crate::LaunchContextCapabilities {
        crate::LaunchContextCapabilities {
            system_prompt_append: true,
            allowed_tools: true,
            stdio_mcp: true,
            http_mcp: true,
            strict_mcp_config: true,
        }
    }

    fn http_mcp_launch_context_capabilities() -> crate::LaunchContextCapabilities {
        crate::LaunchContextCapabilities {
            http_mcp: true,
            ..crate::LaunchContextCapabilities::default()
        }
    }

    #[test]
    fn explicit_environment_is_bounded_before_spawn() {
        let runtime = AgentRuntime::builder().build().unwrap();
        let mut request = TurnRequest::new(Provider::Claude, ".", "test");
        request.environment.insert(
            "TOKEN".into(),
            crate::SecretString::new("x".repeat(MAX_ENVIRONMENT_VALUE_BYTES + 1)),
        );

        let error = runtime.validate(&request).unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::InvalidRequest {
                field: "environment",
                ..
            }
        ));
    }

    #[test]
    fn diagnostic_redaction_prefers_longer_overlapping_secrets() {
        let environment = std::collections::BTreeMap::from([
            ("SHORT".into(), crate::SecretString::new("token")),
            ("LONG".into(), crate::SecretString::new("token-sensitive")),
        ]);

        assert_eq!(
            redact_secrets("failed with token-sensitive and token", &environment),
            "failed with [REDACTED] and [REDACTED]"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn project_skill_management_rejects_symlinked_scope_roots() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), workspace.path().join(".claude")).unwrap();
        let runtime = AgentRuntime::builder().build().unwrap();

        let error = runtime
            .manage_skill(SkillManagementRequest {
                provider: Provider::Claude,
                scope: HarnessExtensionScope::Project,
                name: "review".into(),
                working_directory: workspace.path().to_owned(),
                content: Some("---\ndescription: Review\n---\n".into()),
            })
            .await
            .expect_err("a project-controlled symlink must not redirect skill writes");

        assert_eq!(error.kind, TransportErrorKind::ProcessControlFailed);
        assert!(error.message.contains("symlink component"), "{error:?}");
        assert!(!outside.path().join("skills/review/SKILL.md").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn extension_rejection_preserves_stderr_when_stdin_is_not_consumed() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = AgentRuntime::builder().build().unwrap();
        let mut command = CommandSpec::new("/bin/sh");
        command.args = vec![
            "-c".into(),
            "printf 'specific rejection' >&2; exit 78".into(),
        ];
        command.initial_stdin = Some(vec![b'x'; 1024 * 1024]);
        let error = runtime
            .execute_extension_command(command, workspace.path().into(), "test_rejection")
            .await
            .expect_err("the command rejects input");
        assert!(error.message.contains("specific rejection"), "{error:?}");
        assert!(!error.retryable);
    }

    #[test]
    fn launch_context_requires_explicit_mcp_secret_sources() {
        let mut request = TurnRequest::new(Provider::Claude, ".", "test");
        request.launch_context.mcp_servers.insert(
            "temps_fleet".into(),
            McpServerConfig::Stdio {
                command: "temps-fleet".into(),
                args: vec!["mcp-server".into()],
                environment_from: std::collections::BTreeMap::from([(
                    "TEMPS_FLEET_TOKEN".into(),
                    "FLEET_SECRET".into(),
                )]),
            },
        );

        let error =
            validate_launch_context(&request, all_launch_context_capabilities()).unwrap_err();

        assert!(matches!(
            &error,
            RuntimeError::InvalidRequest {
                field: "launch_context.mcp_servers.environment_from",
                ..
            }
        ));
        assert!(error.to_string().contains("FLEET_SECRET"));
    }

    #[test]
    fn launch_context_accepts_scoped_mcp_capability_environment() {
        let mut request = TurnRequest::new(Provider::Claude, ".", "test");
        request.environment.insert(
            "FLEET_SECRET".into(),
            crate::SecretString::new("redacted-value"),
        );
        request.launch_context.system_prompt_append = Some("Be concise.".into());
        request.launch_context.allowed_tools = Some(vec!["Read".into()]);
        request.launch_context.mcp_servers.insert(
            "temps_fleet".into(),
            McpServerConfig::Stdio {
                command: "temps-fleet".into(),
                args: vec!["mcp-server".into()],
                environment_from: std::collections::BTreeMap::from([(
                    "TEMPS_FLEET_TOKEN".into(),
                    "FLEET_SECRET".into(),
                )]),
            },
        );

        validate_launch_context(&request, all_launch_context_capabilities()).unwrap();
        assert!(!format!("{request:?}").contains("redacted-value"));
    }

    #[test]
    fn codex_accepts_only_the_launch_context_fields_it_enforces() {
        let capabilities = http_mcp_launch_context_capabilities();
        let mut request = TurnRequest::new(Provider::Codex, ".", "test");
        request.environment.insert(
            "TURN_TOKEN".into(),
            crate::SecretString::new("redacted-value"),
        );
        request.launch_context.mcp_servers.insert(
            "platform".into(),
            McpServerConfig::Http {
                url: "https://relay.example.test/mcp".into(),
                headers_from: std::collections::BTreeMap::from([(
                    "Authorization".into(),
                    "TURN_TOKEN".into(),
                )]),
            },
        );
        validate_launch_context(&request, capabilities).unwrap();

        request.launch_context.system_prompt_append = Some("extra policy".into());
        let error = validate_launch_context(&request, capabilities).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::InvalidRequest {
                field: "launch_context.system_prompt_append",
                ..
            }
        ));
    }

    #[cfg(feature = "codex")]
    #[tokio::test]
    async fn codex_temps_relay_turn_passes_runtime_control_validation() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = AgentRuntime::builder().build().unwrap();
        let mut request = TurnRequest::new(Provider::Codex, directory.path(), "test");
        request.environment.insert(
            "TEMPS_MODEL_RELAY_TOKEN".into(),
            crate::SecretString::new("test-secret"),
        );
        request.environment.insert(
            "TEMPS_CHAT_MCP_AUTHORIZATION".into(),
            crate::SecretString::new("Bearer test-secret"),
        );
        request.harness_options.insert(
            "model_relay".into(),
            r#"{"base_url":"http://127.0.0.1:8000/v1","token_env":"TEMPS_MODEL_RELAY_TOKEN"}"#
                .into(),
        );
        request.launch_context.mcp_servers.insert(
            "temps-chat".into(),
            McpServerConfig::Http {
                url: "http://127.0.0.1:8000/mcp".into(),
                headers_from: std::collections::BTreeMap::from([(
                    "Authorization".into(),
                    "TEMPS_CHAT_MCP_AUTHORIZATION".into(),
                )]),
            },
        );
        request.cancellation.cancel();
        let error = runtime
            .run(request, &crate::NoopEventSink, None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::Cancelled {
                provider: Provider::Codex
            }
        ));
    }

    #[cfg(feature = "codex")]
    #[tokio::test]
    async fn codex_relay_allowance_does_not_accept_arbitrary_controls() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = AgentRuntime::builder().build().unwrap();
        let mut request = TurnRequest::new(Provider::Codex, directory.path(), "test");
        request
            .harness_options
            .insert("arbitrary_config".into(), "unsafe".into());
        request.cancellation.cancel();
        let error = runtime
            .run(request, &crate::NoopEventSink, None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::InvalidRequest {
                field: "harness_options",
                ..
            }
        ));
    }

    #[test]
    fn codex_rejects_unsupported_stdio_mcp_precisely() {
        let capabilities = http_mcp_launch_context_capabilities();
        let mut request = TurnRequest::new(Provider::Codex, ".", "test");
        request.launch_context.mcp_servers.insert(
            "local".into(),
            McpServerConfig::Stdio {
                command: "server".into(),
                args: Vec::new(),
                environment_from: std::collections::BTreeMap::new(),
            },
        );

        let error = validate_launch_context(&request, capabilities).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::InvalidRequest {
                field: "launch_context.mcp_servers",
                ..
            }
        ));
        assert!(error.to_string().contains("stdio MCP"));
    }

    #[test]
    fn parses_host_scoped_skills_and_write_access_without_recursive_scanning() {
        let output = b"startup\n\x1eTEMPS_AGENT_RUNTIME_EXTENSIONS\x00access\x00user\x001\x00\x00access\x00project\x000\x00read only\x00access\x00user_mcp\x001\x00\x00access\x00project_mcp\x000\x00config is read only\x00skill\x00user\x00agents\x00/Users/agent/.agents/skills/review/SKILL.md\x00Review changes\x00skill\x00project\x00claude\x00/work/.claude/skills/deploy/SKILL.md\x00Deploy safely\x00";
        let (skills, user, project, user_mcp, project_mcp) =
            parse_skill_probe(output, Some("deploy"), 20, "ssh").unwrap();

        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].id, "deploy");
        assert_eq!(skills[0].scope, HarnessExtensionScope::Project);
        assert!(user.allowed);
        assert!(!project.allowed);
        assert_eq!(
            project.denial_kind,
            Some(HarnessExtensionDenialKind::PermissionDenied)
        );
        assert_eq!(project.reason.as_deref(), Some("read only"));
        assert!(user_mcp.allowed);
        assert!(!project_mcp.allowed);
        assert_eq!(project_mcp.reason.as_deref(), Some("config is read only"));
    }

    #[test]
    fn codex_mcp_discovery_discards_commands_environment_and_headers() {
        let output = br#"[{"name":"docs","enabled":true,"transport":{"type":"streamable_http","url":"https://example.test?token=secret","http_headers":{"Authorization":"secret"}}}]"#;
        let servers = parse_mcp_servers(Provider::Codex, output, "local").unwrap();

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "docs");
        assert_eq!(servers[0].transport.as_deref(), Some("streamable_http"));
        assert_eq!(servers[0].detail, None);
        assert!(!format!("{servers:?}").contains("secret"));
    }

    #[test]
    fn codex_project_mcp_management_returns_a_typed_scope_denial() {
        let error = build_mcp_management_command(
            Provider::Codex,
            PathBuf::from("codex"),
            HarnessExtensionScope::Project,
            "docs",
            None,
            "ssh",
        )
        .unwrap_err();

        assert_eq!(error.kind, TransportErrorKind::Unsupported);
        assert!(error.message.contains("project-scoped"));
    }

    #[test]
    fn managed_mcp_command_debug_never_exposes_definition_values() {
        let command = build_mcp_management_command(
            Provider::Codex,
            PathBuf::from("codex"),
            HarnessExtensionScope::User,
            "private-server",
            Some(HarnessMcpDefinition::Http {
                url: "https://example.test/mcp?token=private-value".into(),
                bearer_token_env_var: Some("PRIVATE_TOKEN".into()),
            }),
            "local",
        )
        .unwrap();
        let debug = format!("{command:?}");

        assert!(!debug.contains("private-value"));
        assert!(!debug.contains("PRIVATE_TOKEN"));
        assert!(debug.contains("argument_count"));
    }

    #[test]
    fn stdio_mcp_management_rejects_secret_values_in_persistent_cli_arguments() {
        for provider in [Provider::Claude, Provider::Codex] {
            let error = build_mcp_management_command(
                provider,
                PathBuf::from("provider"),
                HarnessExtensionScope::User,
                "private-server",
                Some(HarnessMcpDefinition::Stdio {
                    command: vec!["server".into()],
                    environment: BTreeMap::from([("TOKEN".into(), "private-value".into())]),
                }),
                "local",
            )
            .unwrap_err();
            assert_eq!(error.kind, TransportErrorKind::Unsupported);
            assert!(!format!("{error:?}").contains("private-value"));
        }
    }

    #[test]
    fn extension_names_cannot_escape_provider_owned_directories() {
        for name in ["../escape", "UPPER", "double--dash", ""] {
            assert!(validate_extension_name(name, "local", "manage_skill").is_err());
        }
        assert!(validate_extension_name("review-pr", "local", "manage_skill").is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn manages_project_skill_inside_the_selected_execution_directory() {
        let runtime = AgentRuntime::builder().build().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let skill_path = directory.path().join(".claude/skills/review-pr/SKILL.md");
        let content = "---\ndescription: Review a pull request\n---\n\nReview the diff.\n";

        runtime
            .manage_skill(SkillManagementRequest {
                provider: Provider::Claude,
                scope: HarnessExtensionScope::Project,
                name: "review-pr".into(),
                working_directory: directory.path().into(),
                content: Some(content.into()),
            })
            .await
            .unwrap();

        assert_eq!(std::fs::read_to_string(&skill_path).unwrap(), content);

        runtime
            .manage_skill(SkillManagementRequest {
                provider: Provider::Claude,
                scope: HarnessExtensionScope::Project,
                name: "review-pr".into(),
                working_directory: directory.path().into(),
                content: None,
            })
            .await
            .unwrap();

        assert!(!skill_path.exists());
    }

    #[test]
    fn classifies_provider_failures_for_recovery_ui() {
        assert_eq!(
            classify_provider_failure("HTTP error: 401 Unauthorized"),
            ProviderProcessErrorKind::AuthenticationFailed
        );
        assert_eq!(
            classify_provider_failure("rate limit exceeded (429)"),
            ProviderProcessErrorKind::RateLimited
        );
        assert_eq!(
            classify_provider_failure("invalid peer certificate: UnknownIssuer"),
            ProviderProcessErrorKind::Network
        );
        assert_eq!(
            catalog_failure_kind(
                "authentication_error: invalid api key",
                HarnessCatalogErrorKind::Protocol,
            ),
            HarnessCatalogErrorKind::Authentication
        );
        assert_eq!(
            catalog_failure_kind("malformed frame", HarnessCatalogErrorKind::Protocol),
            HarnessCatalogErrorKind::Protocol
        );
    }

    #[test]
    fn rejects_zero_concurrency() {
        let error = AgentRuntime::builder().concurrency_limit(0).build();
        assert!(matches!(
            error,
            Err(RuntimeError::InvalidRequest {
                field: "concurrency_limit",
                ..
            })
        ));
    }

    #[cfg(unix)]
    mod unix {
        use std::ffi::OsString;
        use std::path::PathBuf;
        use std::sync::Mutex;
        use std::time::Duration;

        use async_trait::async_trait;
        use serde_json::Value;

        use crate::{
            AdapterOutput, AdapterState, AgentAdapter, ApprovalDecision, ApprovalRequest,
            CommandSpec, PermissionSupport, ProviderReadiness, QuestionAnswer, QuestionRequest,
            ResolvedSandboxProfile, RunStatus, SandboxBackend, SandboxCapabilities, SandboxContext,
            SandboxError, SandboxPathAccess, SandboxProfileChange, SandboxProfileManager,
            SandboxProfileRef, SandboxProfileUpdate, SandboxRecoveryDecision,
            SandboxRecoveryHandler, SandboxRecoveryPolicy, SandboxRecoveryRequest, SandboxRequest,
            SandboxResource, SandboxViolation, ToolCallStatus,
        };

        use super::*;

        #[derive(Clone)]
        struct ShellAdapter {
            script: String,
            args: Vec<OsString>,
        }

        #[async_trait]
        impl AgentAdapter for ShellAdapter {
            fn provider(&self) -> Provider {
                Provider::Claude
            }

            fn permission_support(&self) -> PermissionSupport {
                PermissionSupport {
                    default: true,
                    accept_edits: true,
                    plan: true,
                    full_access: true,
                    custom: true,
                    live_approvals: false,
                    live_questions: false,
                }
            }

            async fn readiness(&self) -> ProviderReadiness {
                ProviderReadiness {
                    provider: Provider::Claude,
                    installed: true,
                    executable: Some(PathBuf::from("/bin/sh")),
                    version: None,
                    detail: "test adapter".into(),
                }
            }

            fn command(&self, _request: &TurnRequest) -> Result<CommandSpec> {
                let mut command = CommandSpec::new("/bin/sh");
                command.args = vec!["-c".into(), self.script.clone().into()];
                command.args.extend(self.args.clone());
                Ok(command)
            }

            fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
                let value: Value =
                    serde_json::from_str(line).map_err(|error| RuntimeError::Protocol {
                        provider: Provider::Claude,
                        message: error.to_string(),
                    })?;
                let mut output = AdapterOutput::default();
                if let Some(text) = value.get("text").and_then(Value::as_str) {
                    state.result.text.push_str(text);
                    output
                        .events
                        .push(TurnEvent::TextDelta { text: text.into() });
                }
                if value.get("tool_failed").and_then(Value::as_bool) == Some(true) {
                    output.events.push(TurnEvent::ToolCall {
                        id: Some("step-1".into()),
                        name: "shell".into(),
                        status: ToolCallStatus::Failed,
                        input: None,
                        output: None,
                        error: Some("cat: /private/input: Operation not permitted".into()),
                        task_id: None,
                    });
                }
                if let Some(session_id) = value.get("session").and_then(Value::as_str) {
                    if state.result.session_id.as_deref() != Some(session_id) {
                        state.result.session_id = Some(session_id.to_string());
                        output.events.push(TurnEvent::SessionStarted {
                            session_id: session_id.to_string(),
                            title: value
                                .get("title")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        });
                    }
                }
                if value.get("terminal").and_then(Value::as_bool) == Some(true) {
                    output.terminal = true;
                    state.result.status = RunStatus::Succeeded;
                }
                if value.get("failed").and_then(Value::as_bool) == Some(true) {
                    state.result.status = RunStatus::Failed;
                }
                if let Some(diagnostic) = value.get("native_failure").and_then(Value::as_str) {
                    state.terminal_failure = Some(
                        crate::ProviderTerminalFailure::new(
                            classify_provider_failure(diagnostic),
                            diagnostic,
                            crate::lifecycle::DeliveryState::Accepted,
                        )
                        .with_provider_code("test::native_failure"),
                    );
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

        #[derive(Default)]
        struct CollectEvents(Mutex<Vec<TurnEvent>>);

        #[async_trait]
        impl EventSink for CollectEvents {
            async fn emit(&self, event: TurnEvent) -> Result<()> {
                self.0.lock().unwrap().push(event);
                Ok(())
            }
        }

        struct ApprovalAdapter;

        #[async_trait]
        impl AgentAdapter for ApprovalAdapter {
            fn provider(&self) -> Provider {
                Provider::Claude
            }

            fn permission_support(&self) -> PermissionSupport {
                PermissionSupport {
                    default: true,
                    accept_edits: true,
                    plan: true,
                    full_access: true,
                    custom: false,
                    live_approvals: true,
                    live_questions: false,
                }
            }

            async fn readiness(&self) -> ProviderReadiness {
                ProviderReadiness {
                    provider: Provider::Claude,
                    installed: true,
                    executable: Some(PathBuf::from("/bin/sh")),
                    version: None,
                    detail: "approval test adapter".into(),
                }
            }

            fn command(&self, _request: &TurnRequest) -> Result<CommandSpec> {
                let mut command = CommandSpec::new("/bin/sh");
                command.args = vec![
                    "-c".into(),
                    "printf '%s\\n' '{\"approval\":true}'; read response".into(),
                ];
                command.interactive_stdin = true;
                Ok(command)
            }

            fn parse_line(&self, line: &str, _state: &mut AdapterState) -> Result<AdapterOutput> {
                let value: Value = serde_json::from_str(line).unwrap();
                let approval = ApprovalRequest {
                    id: "approval-1".into(),
                    tool_name: "shell".into(),
                    input: Value::Null,
                    description: None,
                };
                Ok(AdapterOutput {
                    events: vec![TurnEvent::ApprovalRequested(approval.clone())],
                    interaction: Some(InteractionRequest::Approval {
                        request: approval,
                        original: value,
                    }),
                    writes: Vec::new(),
                    terminal: false,
                })
            }

            fn approval_response(
                &self,
                _request: &ApprovalRequest,
                _original: &Value,
                _decision: ApprovalDecision,
            ) -> Result<Option<Vec<u8>>> {
                Ok(Some(b"allow".to_vec()))
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

        struct PendingApproval {
            entered: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl InteractionHandler for PendingApproval {
            async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
                self.entered.notify_one();
                std::future::pending().await
            }

            async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
                None
            }
        }

        struct EnvironmentSandbox;

        #[async_trait]
        impl SandboxBackend for EnvironmentSandbox {
            fn name(&self) -> &'static str {
                "environment-test"
            }

            fn capabilities(&self) -> SandboxCapabilities {
                SandboxCapabilities {
                    filesystem: true,
                    process_isolation: true,
                    ..SandboxCapabilities::NONE
                }
            }

            async fn prepare(
                &self,
                context: SandboxContext,
                mut command: CommandSpec,
            ) -> std::result::Result<CommandSpec, SandboxError> {
                assert_eq!(context.provider, Provider::Claude);
                command
                    .environment
                    .insert("SANDBOX_MARKER".into(), "prepared".into());
                Ok(command)
            }
        }

        struct PendingSandbox;

        #[async_trait]
        impl SandboxBackend for PendingSandbox {
            fn name(&self) -> &'static str {
                "pending-test"
            }

            fn capabilities(&self) -> SandboxCapabilities {
                SandboxCapabilities::NONE
            }

            async fn prepare(
                &self,
                _context: SandboxContext,
                _command: CommandSpec,
            ) -> std::result::Result<CommandSpec, SandboxError> {
                std::future::pending().await
            }
        }

        #[derive(Clone)]
        struct RecoverableSandbox {
            revision: String,
        }

        #[async_trait]
        impl SandboxBackend for RecoverableSandbox {
            fn name(&self) -> &'static str {
                "recoverable-test"
            }

            fn capabilities(&self) -> SandboxCapabilities {
                SandboxCapabilities {
                    filesystem: true,
                    process_isolation: true,
                    ..SandboxCapabilities::NONE
                }
            }

            async fn prepare(
                &self,
                _context: SandboxContext,
                mut command: CommandSpec,
            ) -> std::result::Result<CommandSpec, SandboxError> {
                command
                    .environment
                    .insert("PROFILE_REVISION".into(), self.revision.clone().into());
                Ok(command)
            }

            fn classify_event(&self, event: &TurnEvent) -> Option<SandboxViolation> {
                let TurnEvent::ToolCall {
                    id,
                    name,
                    status: ToolCallStatus::Failed,
                    ..
                } = event
                else {
                    return None;
                };
                Some(SandboxViolation {
                    step_id: id.clone(),
                    tool_name: Some(name.clone()),
                    resource: SandboxResource::Path {
                        path: PathBuf::from("/private/input"),
                        access: Some(SandboxPathAccess::Read),
                    },
                    message: "sandbox denied /private/input".into(),
                })
            }
        }

        struct TestProfileManager {
            revision: Mutex<String>,
        }

        #[async_trait]
        impl SandboxProfileManager for TestProfileManager {
            fn name(&self) -> &'static str {
                "test-profiles"
            }

            fn capabilities(&self) -> SandboxCapabilities {
                RecoverableSandbox {
                    revision: String::new(),
                }
                .capabilities()
            }

            async fn resolve(
                &self,
                profile: &SandboxProfileRef,
                _context: &SandboxContext,
            ) -> std::result::Result<ResolvedSandboxProfile, SandboxError> {
                let revision = self.revision.lock().unwrap().clone();
                if let Some(expected) = &profile.revision {
                    assert_eq!(expected, &revision);
                }
                Ok(ResolvedSandboxProfile::new(
                    SandboxProfileRef::at_revision(&profile.id, &revision),
                    RecoverableSandbox { revision },
                ))
            }

            async fn update(
                &self,
                request: SandboxProfileUpdate,
                _context: &SandboxContext,
            ) -> std::result::Result<ResolvedSandboxProfile, SandboxError> {
                assert_eq!(request.profile.revision.as_deref(), Some("1"));
                assert!(matches!(
                    request.change,
                    SandboxProfileChange::GrantPath {
                        access: SandboxPathAccess::Read,
                        ..
                    }
                ));
                *self.revision.lock().unwrap() = "2".into();
                Ok(ResolvedSandboxProfile::new(
                    SandboxProfileRef::at_revision(request.profile.id, "2"),
                    RecoverableSandbox {
                        revision: "2".into(),
                    },
                ))
            }
        }

        struct ApproveSandboxRecovery;

        #[async_trait]
        impl SandboxRecoveryHandler for ApproveSandboxRecovery {
            async fn recover(&self, request: SandboxRecoveryRequest) -> SandboxRecoveryDecision {
                let SandboxResource::Path { path, .. } = request.violation.resource else {
                    panic!("expected path violation")
                };
                SandboxRecoveryDecision::Retry {
                    change: SandboxProfileChange::GrantPath {
                        path,
                        access: SandboxPathAccess::Read,
                        bypass_protection: false,
                    },
                }
            }
        }

        #[derive(Default)]
        struct TimingCollector {
            samples: Mutex<Vec<crate::StartupTiming>>,
            waiting: tokio::sync::Notify,
        }

        impl crate::StartupObserver for TimingCollector {
            fn observe(&self, sample: crate::StartupTiming) {
                self.samples.lock().unwrap().push(sample);
                if sample.stage == StartupStage::WaitingForPermit {
                    self.waiting.notify_one();
                }
            }
        }

        impl TimingCollector {
            fn stages(&self) -> Vec<StartupStage> {
                self.samples
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|sample| sample.stage)
                    .collect()
            }
        }

        fn observed_runtime(
            script: &str,
            observer: Arc<dyn crate::StartupObserver>,
        ) -> AgentRuntime {
            let mut builder = AgentRuntime::builder()
                .startup_observer(observer)
                .concurrency_limit(1);
            builder.register(ShellAdapter {
                script: script.into(),
                args: Vec::new(),
            });
            builder.build().unwrap()
        }

        #[tokio::test]
        async fn startup_timings_separate_output_from_text_without_changing_events() {
            let samples = Arc::new(TimingCollector::default());
            let runtime = observed_runtime(
                r#"printf '%s\n' '{"session":"private-session"}' '{"text":""}' '{"text":"hello"}' '{"text":"world"}' '{"terminal":true}'"#,
                samples.clone(),
            );
            let directory = tempfile::tempdir().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "private prompt");
            let events = CollectEvents::default();
            let result = runtime.run(request, &events, None).await.unwrap();
            assert_eq!(result.text, "helloworld");
            assert_eq!(events.0.lock().unwrap().len(), 4);
            assert_eq!(
                samples.stages(),
                vec![
                    StartupStage::Started,
                    StartupStage::Validated,
                    StartupStage::WaitingForPermit,
                    StartupStage::PermitAcquired,
                    StartupStage::CommandPrepared,
                    StartupStage::ProcessSpawned,
                    StartupStage::StreamsAttached,
                    StartupStage::FirstOutput,
                    StartupStage::FirstText,
                    StartupStage::Succeeded,
                ]
            );
            let recorded = samples.samples.lock().unwrap();
            assert!(recorded
                .windows(2)
                .all(|pair| pair[0].elapsed <= pair[1].elapsed));
            assert!(recorded
                .iter()
                .all(|sample| sample.observation_id == recorded[0].observation_id));
            let diagnostic = format!("{recorded:?}");
            for private in [
                "private prompt",
                "private-session",
                "helloworld",
                directory.path().to_str().unwrap(),
            ] {
                assert!(!diagnostic.contains(private));
            }
        }

        #[tokio::test]
        async fn startup_timings_distinguish_queue_wait_and_abandoned_execution() {
            let samples = Arc::new(TimingCollector::default());
            let runtime = observed_runtime("exit 0", samples.clone());
            let permit = runtime.permits.clone().acquire_owned().await.unwrap();
            let directory = tempfile::tempdir().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            let task =
                tokio::spawn(
                    async move { runtime.run(request, &crate::NoopEventSink, None).await },
                );
            tokio::time::timeout(Duration::from_secs(5), samples.waiting.notified())
                .await
                .unwrap();
            assert_eq!(
                samples.stages(),
                vec![
                    StartupStage::Started,
                    StartupStage::Validated,
                    StartupStage::WaitingForPermit
                ]
            );
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(permit);
            assert_eq!(samples.stages().last(), Some(&StartupStage::Abandoned));
            assert!(!samples.stages().contains(&StartupStage::ProcessSpawned));
        }

        #[tokio::test]
        async fn startup_timings_report_failure_cancellation_and_timeout_once() {
            let samples = Arc::new(TimingCollector::default());
            let runtime = observed_runtime("exec sleep 60", samples.clone());
            let directory = tempfile::tempdir().unwrap();
            let cancelled = TurnRequest::new(Provider::Claude, directory.path(), "cancel");
            cancelled.cancellation.cancel();
            assert!(matches!(
                runtime.run(cancelled, &crate::NoopEventSink, None).await,
                Err(RuntimeError::Cancelled { .. })
            ));
            let mut timeout = TurnRequest::new(Provider::Claude, directory.path(), "timeout");
            timeout.timeout = Duration::from_millis(25);
            assert!(matches!(
                runtime.run(timeout, &crate::NoopEventSink, None).await,
                Err(RuntimeError::Timeout { .. })
            ));
            let mut invalid = TurnRequest::new(Provider::Claude, directory.path(), "invalid");
            invalid.timeout = Duration::ZERO;
            assert!(runtime
                .run(invalid, &crate::NoopEventSink, None)
                .await
                .is_err());
            let samples = samples.samples.lock().unwrap();
            let mut observations = std::collections::BTreeMap::new();
            for sample in samples.iter() {
                observations
                    .entry(sample.observation_id)
                    .or_insert_with(Vec::new)
                    .push(sample.stage);
            }
            assert_eq!(observations.len(), 3);
            let terminal: Vec<_> = observations
                .values()
                .map(|stages| *stages.last().unwrap())
                .collect();
            assert_eq!(
                terminal,
                vec![
                    StartupStage::Cancelled,
                    StartupStage::TimedOut,
                    StartupStage::Failed
                ]
            );
            assert!(!samples
                .iter()
                .any(|sample| sample.stage == StartupStage::Abandoned
                    || sample.stage == StartupStage::FirstText));
        }

        #[tokio::test]
        async fn startup_observer_failure_cannot_fail_a_provider_turn() {
            struct Panics;
            impl crate::StartupObserver for Panics {
                fn observe(&self, _: crate::StartupTiming) {
                    panic!("observer unavailable");
                }
            }
            let runtime = observed_runtime(
                r#"printf '%s\n' '{"text":"done"}' '{"terminal":true}'"#,
                Arc::new(Panics),
            );
            let directory = tempfile::tempdir().unwrap();
            let result = runtime
                .run(
                    TurnRequest::new(Provider::Claude, directory.path(), "test"),
                    &crate::NoopEventSink,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(result.text, "done");
        }

        #[tokio::test]
        async fn streams_normalized_events_and_returns_result() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"printf '%s\n' '{"text":"done"}' '{"terminal":true}'"#.into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            let events = CollectEvents::default();

            let result = runtime.run(request, &events, None).await.unwrap();

            assert_eq!(result.text, "done");
            assert_eq!(
                events.0.lock().unwrap().as_slice(),
                [TurnEvent::TextDelta {
                    text: "done".into()
                }]
            );
        }

        #[tokio::test]
        async fn resumed_session_handshake_does_not_emit_session_started_again() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"printf '%s\n' '{"session":"session-existing"}' '{"terminal":true}'"#
                    .into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "continue");
            request.session_id = Some("session-existing".into());
            let events = CollectEvents::default();

            let result = runtime.run(request, &events, None).await.unwrap();

            assert_eq!(result.session_id.as_deref(), Some("session-existing"));
            assert!(!events
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, TurnEvent::SessionStarted { .. })));
        }

        #[tokio::test]
        async fn nonzero_exit_uses_provider_result_when_stderr_is_empty() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"printf '%s\n' '{"text":"authentication failed","failed":true}'; exit 7"#
                    .into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "test");

            let error = runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap_err();

            assert!(matches!(
                error,
                RuntimeError::ProcessFailed { stderr, .. }
                    if stderr == "authentication failed"
            ));
        }

        #[tokio::test]
        async fn provider_failure_redacts_explicit_environment_values() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"printf 'request failed for %s\n' "$PRIVATE_TOKEN" >&2; exit 7"#.into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.environment.insert(
                "PRIVATE_TOKEN".into(),
                crate::SecretString::new("do-not-surface-this"),
            );

            let error = runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap_err();

            assert!(matches!(
                error,
                RuntimeError::ProcessFailed { stderr, .. }
                    if stderr == "request failed for [REDACTED]"
            ));
        }

        #[tokio::test]
        async fn native_failure_is_returned_even_when_the_process_exits_successfully() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"printf '{"native_failure":"rate limit for %s"}\n' "$PRIVATE_TOKEN""#
                    .into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.environment.insert(
                "PRIVATE_TOKEN".into(),
                crate::SecretString::new("do-not-surface-this"),
            );

            let error = runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap_err();

            assert!(matches!(
                error,
                RuntimeError::ProcessFailed {
                    kind: ProviderProcessErrorKind::RateLimited,
                    exit_code: Some(0),
                    stderr,
                    provider_code,
                    delivery: crate::lifecycle::DeliveryState::Accepted,
                    ..
                } if stderr == "rate limit for [REDACTED]"
                    && provider_code.as_deref() == Some("test::native_failure")
            ));
        }

        #[tokio::test]
        async fn custom_sandbox_prepares_the_provider_command() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"printf '{"text":"%s"}\n' "$SANDBOX_MARKER"; printf '%s\n' '{"terminal":true}'"#.into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.sandbox = Some(SandboxRequest::new(EnvironmentSandbox).requiring(
                SandboxCapabilities {
                    filesystem: true,
                    process_isolation: true,
                    ..SandboxCapabilities::NONE
                },
            ));

            let result = runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap();

            assert_eq!(result.text, "prepared");
        }

        #[tokio::test]
        async fn updates_managed_profile_and_retries_the_failed_step_in_session() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: concat!(
                    "if [ \"$PROFILE_REVISION\" = 1 ]; then ",
                    "printf '%s\\n' '{\"session\":\"session-1\"}' '{\"tool_failed\":true}' '{\"terminal\":true}'; ",
                    "else printf '%s\\n' '{\"text\":\"retried\"}' '{\"terminal\":true}'; fi"
                )
                .into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let manager = TestProfileManager {
                revision: Mutex::new("1".into()),
            };
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.sandbox = Some(
                SandboxRequest::managed(manager, SandboxProfileRef::current("profile-1"))
                    .requiring(SandboxCapabilities {
                        filesystem: true,
                        process_isolation: true,
                        ..SandboxCapabilities::NONE
                    }),
            );
            let events = CollectEvents::default();

            let result = runtime
                .run_with_sandbox_recovery(
                    request,
                    &events,
                    None,
                    &ApproveSandboxRecovery,
                    SandboxRecoveryPolicy::default(),
                )
                .await
                .unwrap();

            assert_eq!(result.text, "retried");
            let events = events.0.lock().unwrap();
            assert!(events.iter().any(|event| matches!(
                event,
                TurnEvent::SandboxAccessDenied { violation, .. }
                    if violation.step_id.as_deref() == Some("step-1")
            )));
            assert!(events.iter().any(|event| matches!(
                event,
                TurnEvent::SandboxProfileUpdated { profile, .. }
                    if profile.revision.as_deref() == Some("2")
            )));
            assert!(events
                .iter()
                .any(|event| matches!(event, TurnEvent::SandboxStepRetrying { attempt: 1, .. })));
        }

        #[tokio::test]
        async fn missing_sandbox_capability_fails_before_execution() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: "exit 99".into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.sandbox = Some(SandboxRequest::new(PendingSandbox).requiring(
                SandboxCapabilities {
                    network_allowlist: true,
                    ..SandboxCapabilities::NONE
                },
            ));

            let error = runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap_err();

            assert!(matches!(
                error,
                RuntimeError::Sandbox(SandboxError::MissingCapabilities { .. })
            ));
        }

        #[tokio::test]
        async fn cancellation_interrupts_sandbox_preparation() {
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: "exit 99".into(),
                args: Vec::new(),
            });
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.sandbox = Some(SandboxRequest::new(PendingSandbox));
            let cancellation = request.cancellation.clone();
            let task =
                tokio::spawn(
                    async move { runtime.run(request, &crate::NoopEventSink, None).await },
                );
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancellation.cancel();

            assert!(matches!(
                task.await.unwrap(),
                Err(RuntimeError::Cancelled {
                    provider: Provider::Claude
                })
            ));
        }

        #[tokio::test]
        async fn cancellation_interrupts_pending_approval() {
            let mut builder = AgentRuntime::builder();
            builder.register(ApprovalAdapter);
            let runtime = builder.build().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            let cancellation = request.cancellation.clone();
            let entered = Arc::new(tokio::sync::Notify::new());
            let entered_wait = entered.notified();
            let interactions = PendingApproval {
                entered: Arc::clone(&entered),
            };
            let task = tokio::spawn(async move {
                runtime
                    .run(request, &crate::NoopEventSink, Some(&interactions))
                    .await
            });
            entered_wait.await;
            cancellation.cancel();

            let result = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("cancellation must not wait for the approval timeout")
                .unwrap();
            assert!(matches!(
                result,
                Err(RuntimeError::Cancelled {
                    provider: Provider::Claude
                })
            ));
        }

        #[tokio::test]
        async fn completed_turn_and_runtime_drop_preserve_tool_processes_by_default() {
            let directory = tempfile::tempdir().unwrap();
            let pid_path = directory.path().join("preserved.pid");
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"(trap '' HUP; exec sleep 60) </dev/null >/dev/null 2>&1 & echo $! > "$1"; printf '%s\n' '{"text":"done","terminal":true}'"#.into(),
                args: vec!["runtime-test".into(), pid_path.as_os_str().to_owned()],
            });
            let runtime = builder.build().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "test");

            let result = runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap();
            drop(runtime);

            assert_eq!(result.text, "done");
            let pid = std::fs::read_to_string(&pid_path)
                .unwrap()
                .trim()
                .parse::<i32>()
                .unwrap();
            assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok());

            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }

        #[tokio::test]
        async fn turn_scoped_policy_terminates_tool_processes_on_completion() {
            let directory = tempfile::tempdir().unwrap();
            let pid_path = directory.path().join("terminated.pid");
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: r#"(trap '' HUP; exec sleep 60) </dev/null >/dev/null 2>&1 & echo $! > "$1"; printf '%s\n' '{"text":"done","terminal":true}'"#.into(),
                args: vec!["runtime-test".into(), pid_path.as_os_str().to_owned()],
            });
            let runtime = builder.build().unwrap();
            let mut request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            request.tool_process_policy = crate::ToolProcessPolicy::TerminateOnCompletion;

            runtime
                .run(request, &crate::NoopEventSink, None)
                .await
                .unwrap();

            let pid = std::fs::read_to_string(&pid_path)
                .unwrap()
                .trim()
                .parse::<i32>()
                .unwrap();
            for _ in 0..200 {
                if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                    == Err(nix::errno::Errno::ESRCH)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
            panic!("tool process {pid} survived turn-scoped completion");
        }

        #[tokio::test]
        async fn cancellation_terminates_provider_grandchildren() {
            let directory = tempfile::tempdir().unwrap();
            let pid_path = directory.path().join("grandchild.pid");
            let mut builder = AgentRuntime::builder();
            builder.register(ShellAdapter {
                script: "sleep 60 & echo $! > \"$1\"; wait".into(),
                args: vec!["runtime-test".into(), pid_path.as_os_str().to_owned()],
            });
            let runtime = builder.build().unwrap();
            let request = TurnRequest::new(Provider::Claude, directory.path(), "test");
            let cancellation = request.cancellation.clone();
            let task =
                tokio::spawn(
                    async move { runtime.run(request, &crate::NoopEventSink, None).await },
                );
            let mut pid = None;
            for _ in 0..100 {
                pid = std::fs::read_to_string(&pid_path)
                    .ok()
                    .and_then(|value| value.trim().parse::<i32>().ok());
                if pid.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let pid = pid.expect("provider grandchild PID was not written completely");
            cancellation.cancel();
            assert!(matches!(
                task.await.unwrap(),
                Err(RuntimeError::Cancelled {
                    provider: Provider::Claude
                })
            ));

            for _ in 0..200 {
                if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                    == Err(nix::errno::Errno::ESRCH)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("provider grandchild {pid} survived cancellation");
        }
    }
}
