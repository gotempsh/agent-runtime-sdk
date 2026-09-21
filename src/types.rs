use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::Result;

/// Additional provider-neutral context applied without changing the visible user prompt.
///
/// Applications remain responsible for higher-level concepts such as personas. They compile
/// those policies into this launch context before dispatching a turn.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchContext {
    /// Instructions appended to the harness's native system prompt.
    pub system_prompt_append: Option<String>,
    /// Exact tools exposed to the agent. `None` keeps the harness default; an empty list disables
    /// all tools.
    pub allowed_tools: Option<Vec<String>>,
    /// MCP servers made available for this runtime or turn, keyed by provider-visible name.
    pub mcp_servers: BTreeMap<String, McpServerConfig>,
    /// Ignore MCP servers from provider/user/project configuration and use only `mcp_servers`.
    pub strict_mcp_config: bool,
}

/// Provider-adapter support for individual [`LaunchContext`] fields.
///
/// Capabilities are deliberately field-level: an application can offer only
/// the controls a selected adapter can enforce, and a provider can add support
/// incrementally without claiming compatibility with the entire launch
/// context contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchContextCapabilities {
    /// The adapter can append application instructions to the native system prompt.
    pub system_prompt_append: bool,
    /// The adapter can enforce an exact provider-visible tool allowlist.
    pub allowed_tools: bool,
    /// The adapter can configure turn-scoped stdio MCP servers, including
    /// environment references.
    pub stdio_mcp: bool,
    /// The adapter can configure turn-scoped HTTP MCP servers, including
    /// header environment references.
    pub http_mcp: bool,
    /// The adapter can exclude ambient provider MCP configuration for a turn.
    pub strict_mcp_config: bool,
}

impl fmt::Debug for LaunchContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LaunchContext")
            .field(
                "system_prompt_append_bytes",
                &self.system_prompt_append.as_ref().map(String::len),
            )
            .field("allowed_tools", &self.allowed_tools)
            .field(
                "mcp_server_names",
                &self.mcp_servers.keys().collect::<Vec<_>>(),
            )
            .field("strict_mcp_config", &self.strict_mcp_config)
            .finish()
    }
}

/// One provider-neutral MCP server launched for an agent runtime.
///
/// Secret values must be supplied through [`TurnRequest::environment`]. The environment/header
/// maps below contain only source variable names, allowing adapters to reference credentials
/// without serializing their values into provider command arguments.
///
/// The provider harness receives those source variables in its environment so it can expand the
/// references. This prevents accidental serialization but is not process isolation: the harness
/// and tools it launches can read the values. Use narrowly scoped, short-lived credentials or an
/// external credential broker when the harness itself must not possess a credential.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
#[non_exhaustive]
pub enum McpServerConfig {
    /// A subprocess speaking MCP over standard input/output.
    Stdio {
        /// Executable path meaningful on the selected execution host.
        command: PathBuf,
        /// Ordered executable arguments. Arguments must not contain secrets.
        args: Vec<String>,
        /// MCP-child variable name to harness-visible source variable name.
        environment_from: BTreeMap<String, String>,
    },
    /// A streamable HTTP MCP endpoint.
    Http {
        /// Endpoint URL. Credentials must not be embedded in the URL.
        url: String,
        /// HTTP header name to harness-visible source variable name.
        headers_from: BTreeMap<String, String>,
    },
}

impl fmt::Debug for McpServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdio {
                command,
                args,
                environment_from,
            } => formatter
                .debug_struct("Stdio")
                .field("command", command)
                .field("argument_count", &args.len())
                .field(
                    "environment_keys",
                    &environment_from.keys().collect::<Vec<_>>(),
                )
                .finish(),
            Self::Http { headers_from, .. } => formatter
                .debug_struct("Http")
                .field("url", &"[REDACTED]")
                .field("header_names", &headers_from.keys().collect::<Vec<_>>())
                .finish(),
        }
    }
}

/// Agent implementation used for a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Provider {
    /// Anthropic Claude Code CLI.
    Claude,
    /// OpenAI Codex CLI.
    Codex,
    /// OpenCode CLI.
    OpenCode,
}

impl fmt::Display for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Claude => "Claude Code",
            Self::Codex => "Codex",
            Self::OpenCode => "OpenCode",
        })
    }
}

/// Provider-neutral permission intent.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PermissionMode {
    /// Ask before consequential operations.
    #[default]
    Default,
    /// Allow ordinary workspace edits while retaining other checks.
    AcceptEdits,
    /// Disable modifying tools where the provider supports it.
    Plan,
    /// Let the provider run without its internal sandbox.
    ///
    /// This is appropriate when a stronger outer boundary such as Nono owns
    /// enforcement. Without an outer boundary it is intentionally dangerous.
    FullAccess,
    /// Pass a provider-native permission/agent mode through unchanged.
    Custom(String),
}

/// What the runtime does with provider descendants after a natural provider exit.
///
/// Cancellation, timeout, a dropped turn future, or an event-sink failure always
/// terminates the supervised process tree. This policy applies only after the
/// provider process exits on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolProcessPolicy {
    /// Leave detached tool processes running after the provider exits.
    ///
    /// This is the default so development servers and other intentionally
    /// long-running commands do not die merely because an agent turn ended.
    #[default]
    PreserveOnCompletion,
    /// Terminate remaining descendants in the provider process group/tree.
    TerminateOnCompletion,
}

/// Permission behavior implemented by one configured provider adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionSupport {
    /// Safe provider-default behavior.
    pub default: bool,
    /// A mode that permits ordinary workspace edits.
    pub accept_edits: bool,
    /// A read-only or planning mode.
    pub plan: bool,
    /// A mode that automatically approves otherwise permitted operations.
    pub full_access: bool,
    /// Provider-native custom modes or policies.
    pub custom: bool,
    /// Permission requests can be delivered to [`InteractionHandler`].
    pub live_approvals: bool,
    /// Agent questions can be delivered to [`InteractionHandler`].
    pub live_questions: bool,
}

impl PermissionSupport {
    /// Return whether a static permission mode is supported.
    pub fn supports(self, mode: &PermissionMode) -> bool {
        match mode {
            PermissionMode::Default => self.default,
            PermissionMode::AcceptEdits => self.accept_edits,
            PermissionMode::Plan => self.plan,
            PermissionMode::FullAccess => self.full_access,
            PermissionMode::Custom(_) => self.custom,
        }
    }
}

/// A secret-bearing string whose `Debug` output is always redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Expose the secret for explicit process-environment injection.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([REDACTED])")
    }
}

/// Typed origin of a provider turn.
///
/// Both variants are delivered to provider adapters as user-level input. Agent
/// provenance never promotes relay content into system or developer instructions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "provenance", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TurnProvenance {
    /// Input originated from an application-authenticated human or ordinary chat flow.
    #[default]
    User,
    /// Input originated from an application-authorized Agent Relay envelope.
    Agent(crate::relay::AgentMessageProvenance),
}

/// Provider-neutral automatic context-compaction policy for one runtime turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AutoCompactionPolicy {
    /// Keep the harness's configured default.
    #[default]
    ProviderDefault,
    /// Ask the harness to choose and maintain an automatic compaction threshold.
    Automatic,
    /// Ask the harness to compact when the active context reaches this token count.
    TokenThreshold {
        /// Provider-visible token threshold.
        tokens: u64,
    },
}

/// Everything needed to run one agent turn.
#[derive(Clone)]
pub struct TurnRequest {
    /// Provider adapter to use.
    pub provider: Provider,
    /// Workspace visible to the agent.
    pub working_directory: PathBuf,
    /// User prompt. The runtime never logs it.
    pub prompt: String,
    /// Typed user-versus-agent provenance for this user-level prompt.
    pub provenance: TurnProvenance,
    /// Optional provider model identifier.
    pub model: Option<String>,
    /// Optional provider reasoning-effort or variant identifier.
    pub reasoning: Option<String>,
    /// Permission policy mapped by the provider adapter.
    pub permission_mode: PermissionMode,
    /// Provider-native selections advertised by harness discovery.
    ///
    /// Keys are [`crate::HarnessControlGroup::id`] values. Keeping these
    /// orthogonal prevents concepts such as Codex collaboration mode or
    /// service tier from being collapsed into a generic permission enum.
    pub harness_options: BTreeMap<String, String>,
    /// Provider-neutral system, tool, and MCP launch context.
    pub launch_context: LaunchContext,
    /// Automatic context-compaction policy for this turn.
    pub auto_compaction: AutoCompactionPolicy,
    /// Provider-native session identifier to resume.
    pub session_id: Option<String>,
    /// Optional provider turn limit.
    pub max_turns: Option<u32>,
    /// Hard wall-clock deadline for the process and interactions.
    pub timeout: Duration,
    /// Maximum time to wait for one approval or question response.
    pub interaction_timeout: Duration,
    /// Lifetime policy for tool processes after the provider exits naturally.
    pub tool_process_policy: ToolProcessPolicy,
    /// Explicit harness environment additions. Values are redacted from `Debug` and surfaced
    /// diagnostics, but remain readable by the harness and its descendants.
    pub environment: BTreeMap<String, SecretString>,
    /// Cooperative cancellation owned by the caller.
    pub cancellation: CancellationToken,
    /// Optional pluggable outer sandbox.
    pub sandbox: Option<crate::SandboxRequest>,
    /// Controls that must be enforced by the execution transport, the optional
    /// sandbox backend, or their combination.
    pub required_sandbox_capabilities: crate::SandboxCapabilities,
}

impl fmt::Debug for TurnRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("TurnRequest");
        debug
            .field("provider", &self.provider)
            .field("working_directory", &self.working_directory)
            .field("prompt_bytes", &self.prompt.len())
            .field("provenance", &self.provenance)
            .field("model", &self.model)
            .field("reasoning", &self.reasoning)
            .field("permission_mode", &self.permission_mode)
            .field("harness_options", &self.harness_options)
            .field("launch_context", &self.launch_context)
            .field("auto_compaction", &self.auto_compaction)
            .field("session_id", &self.session_id)
            .field("max_turns", &self.max_turns)
            .field("timeout", &self.timeout)
            .field("interaction_timeout", &self.interaction_timeout)
            .field("tool_process_policy", &self.tool_process_policy)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            );
        debug.field("sandbox", &self.sandbox);
        debug.field(
            "required_sandbox_capabilities",
            &self.required_sandbox_capabilities,
        );
        debug.finish_non_exhaustive()
    }
}

impl TurnRequest {
    /// Create a request with safe defaults.
    pub fn new(
        provider: Provider,
        working_directory: impl Into<PathBuf>,
        prompt: impl Into<String>,
    ) -> Self {
        Self {
            provider,
            working_directory: working_directory.into(),
            prompt: prompt.into(),
            provenance: TurnProvenance::default(),
            model: None,
            reasoning: None,
            permission_mode: PermissionMode::Default,
            harness_options: BTreeMap::new(),
            launch_context: LaunchContext::default(),
            auto_compaction: AutoCompactionPolicy::default(),
            session_id: None,
            max_turns: None,
            timeout: Duration::from_secs(30 * 60),
            interaction_timeout: Duration::from_secs(10 * 60),
            tool_process_policy: ToolProcessPolicy::default(),
            environment: BTreeMap::new(),
            cancellation: CancellationToken::new(),
            sandbox: None,
            required_sandbox_capabilities: crate::SandboxCapabilities::NONE,
        }
    }
}

/// Result of provider discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderReadiness {
    /// Provider inspected.
    pub provider: Provider,
    /// Whether its executable could be resolved.
    pub installed: bool,
    /// Resolved executable when installed.
    pub executable: Option<PathBuf>,
    /// Best-effort version output.
    pub version: Option<String>,
    /// Actionable installation or authentication guidance.
    pub detail: String,
}

/// Lifecycle of a normalized tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolCallStatus {
    /// Tool execution was announced.
    Started,
    /// Tool execution completed successfully.
    Succeeded,
    /// Tool execution failed.
    Failed,
}

/// Accumulated model usage when reported by the provider.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    /// Input tokens.
    pub input_tokens: Option<u64>,
    /// Output tokens.
    pub output_tokens: Option<u64>,
    /// Input tokens written to the provider prompt cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    /// Input tokens served from the provider prompt cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    /// Latest active context-window occupancy when the provider exposes enough data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<ContextWindowUsage>,
    /// Provider-reported estimated cost in USD.
    pub cost_usd: Option<f64>,
}

/// Semantic scope of one provider account-usage window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AccountUsageWindowKind {
    /// A short rolling provider session window, commonly five hours.
    Session,
    /// A rolling or fixed weekly provider allocation.
    Weekly,
    /// A provider window that does not map to a stable cross-provider scope.
    Other,
}

/// Point-in-time utilization for one provider account quota window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountUsageWindow {
    /// Stable provider-native identifier, such as `five_hour` or `secondary`.
    pub id: String,
    /// Provider-supplied display label when the identifier is not user friendly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Provider-neutral semantic scope.
    pub kind: AccountUsageWindowKind,
    /// Provider-reported usage percentage. Values above 100 are preserved.
    pub used_percent: f64,
    /// Window duration when the provider reports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_minutes: Option<u64>,
    /// Unix timestamp in seconds for the next reset when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_unix_seconds: Option<u64>,
}

/// Optional account credits associated with a provider quota snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountCredits {
    /// Whether this account can use an unbounded credit balance.
    pub unlimited: bool,
    /// Provider-formatted remaining balance, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
    /// ISO 4217 currency when the provider defines the balance as money.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

/// Provider-account usage independent from one conversation's token usage.
///
/// Providers expose these values differently: Claude emits them while a
/// session is running, while Codex also supports a metadata-only account
/// query. Consumers should replace an older snapshot from the same provider
/// rather than add percentages together.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountUsageSnapshot {
    /// Provider whose account limits were observed.
    pub provider: Provider,
    /// Provider-native subscription or plan label when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    /// Independently resetting quota windows.
    pub windows: Vec<AccountUsageWindow>,
    /// Optional prepaid or metered credits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits: Option<AccountCredits>,
}

/// Availability of a fetch-on-demand provider account-usage query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AccountUsageStatus {
    /// The provider returned a current quota snapshot.
    Available,
    /// The provider supports quota discovery but it could not be read now.
    Unavailable,
    /// The provider adapter does not implement account quota discovery.
    Unsupported,
}

/// Fetch-on-demand provider account usage for one execution host.
///
/// This report is independent from a conversation's [`Usage`] and belongs to
/// the provider identity authenticated inside the selected execution transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountUsageReport {
    /// Provider account that was queried.
    pub provider: Provider,
    /// Whether a current snapshot was returned.
    pub status: AccountUsageStatus,
    /// Current quota snapshot when [`AccountUsageStatus::Available`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<AccountUsageSnapshot>,
    /// Bounded, non-secret explanation when usage is unavailable or unsupported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether retrying later without changing configuration may succeed.
    pub retryable: bool,
}

impl AccountUsageReport {
    /// Construct an available account-usage report.
    pub fn available(usage: AccountUsageSnapshot) -> Self {
        Self {
            provider: usage.provider,
            status: AccountUsageStatus::Available,
            usage: Some(usage),
            reason: None,
            retryable: false,
        }
    }

    /// Construct a temporarily or permanently unavailable report.
    pub fn unavailable(provider: Provider, reason: impl Into<String>, retryable: bool) -> Self {
        Self {
            provider,
            status: AccountUsageStatus::Unavailable,
            usage: None,
            reason: Some(reason.into()),
            retryable,
        }
    }

    /// Construct a report for an adapter without account-usage support.
    pub fn unsupported(provider: Provider, reason: impl Into<String>) -> Self {
        Self {
            provider,
            status: AccountUsageStatus::Unsupported,
            usage: None,
            reason: Some(reason.into()),
            retryable: false,
        }
    }
}

/// Point-in-time occupancy of the provider's active context window.
///
/// This is distinct from cumulative billing usage. `used_tokens` describes the
/// context expected to be carried into the next model step; cached tokens still
/// occupy context even when they are billed differently.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWindowUsage {
    /// Tokens currently occupying the active context window.
    pub used_tokens: Option<u64>,
    /// Maximum model context size when advertised by the harness.
    pub limit_tokens: Option<u64>,
    /// Model that produced this snapshot when reported.
    pub model: Option<String>,
    /// Whether occupancy was derived from provider usage components rather than reported directly.
    pub estimated: bool,
}

impl ContextWindowUsage {
    /// Remaining tokens when both occupancy and the limit are known.
    pub fn remaining_tokens(&self) -> Option<u64> {
        Some(self.limit_tokens?.saturating_sub(self.used_tokens?))
    }
}

/// Cause reported for one context compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CompactionTrigger {
    /// The provider crossed its automatic threshold.
    Automatic,
    /// An application or user explicitly requested compaction.
    Manual,
    /// The provider did not expose a recognized cause.
    Unknown,
}

/// Provider-neutral completed context-compaction boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCompaction {
    /// Why compaction occurred.
    pub trigger: CompactionTrigger,
    /// Active context tokens immediately before compaction.
    pub pre_tokens: Option<u64>,
    /// Active context tokens immediately after compaction.
    pub post_tokens: Option<u64>,
    /// Tokens removed by this compaction.
    pub dropped_tokens: Option<u64>,
    /// Provider-reported cumulative tokens removed across the session.
    pub cumulative_dropped_tokens: Option<u64>,
    /// Provider-reported compaction duration.
    pub duration_ms: Option<u64>,
}

/// Latest provider-native state for one task or subagent.
///
/// Claude Code currently supplies this signal. Other adapters can use the same
/// shape when their native protocols expose equivalent lifecycle information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTask {
    /// Provider-native task identifier.
    pub id: String,
    /// Provider-neutral category such as `subagent`, `shell`, or `workflow`.
    pub kind: String,
    /// Current bounded task description.
    pub description: String,
    /// Provider-native lifecycle status.
    pub status: String,
    /// Provider-native subagent type when reported.
    pub agent_type: Option<String>,
    /// Terminal failure summary when reported.
    pub error: Option<String>,
    /// Latest bounded progress or terminal summary.
    pub summary: Option<String>,
}

/// Usage reported for one native task or subagent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskUsage {
    /// Total tokens attributed to the task.
    pub total_tokens: u64,
    /// Number of tool uses attributed to the task.
    pub tool_uses: u64,
    /// Task duration in milliseconds.
    pub duration_ms: u64,
}

/// Ordered lifecycle transition for a native task or subagent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentTaskActivityKind {
    /// The provider created the task.
    Started,
    /// Task metadata or state changed.
    Updated,
    /// The provider reported intermediate progress.
    Progress,
    /// The task completed successfully.
    Completed,
    /// The task failed.
    Failed,
    /// The task was stopped before completion.
    Stopped,
}

/// One append-only native task/subagent lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskActivity {
    /// Provider-native task identifier.
    pub task_id: String,
    /// Lifecycle transition kind.
    pub kind: AgentTaskActivityKind,
    /// Updated description when supplied.
    pub description: Option<String>,
    /// Provider-native status when supplied.
    pub status: Option<String>,
    /// Provider-native subagent type when supplied.
    pub agent_type: Option<String>,
    /// Latest progress or terminal summary.
    pub summary: Option<String>,
    /// Most recently active tool name.
    pub last_tool_name: Option<String>,
    /// Provider-reported nesting depth.
    pub spawn_depth: Option<u32>,
    /// Provider-reported task usage.
    pub usage: Option<AgentTaskUsage>,
}

/// Provider-neutral streaming event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TurnEvent {
    /// Session identifier became available.
    SessionStarted {
        /// Provider-native resumable identifier.
        session_id: String,
        /// Provider-native display title when the harness reports one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    /// Incremental assistant prose.
    TextDelta {
        /// Newly generated prose.
        text: String,
    },
    /// Incremental or consolidated reasoning text.
    ReasoningDelta {
        /// Newly reported reasoning text.
        text: String,
    },
    /// The provider's effective permission mode changed during the session.
    ///
    /// This is distinct from the mode requested when launching the turn. Some
    /// harnesses can enter and leave a restricted planning mode themselves.
    PermissionModeChanged {
        /// Permission mode currently enforced by the provider harness.
        mode: PermissionMode,
    },
    /// Tool lifecycle update.
    ToolCall {
        /// Provider-native tool call identifier.
        id: Option<String>,
        /// Tool name.
        name: String,
        /// Current lifecycle status.
        status: ToolCallStatus,
        /// Provider-native input; treat as sensitive.
        input: Option<Value>,
        /// Bounded tool output when reported.
        output: Option<String>,
        /// Bounded tool failure when reported.
        error: Option<String>,
        /// Native task/subagent that owns this tool call, when reported.
        task_id: Option<String>,
    },
    /// Replaceable snapshot of provider-native tasks and subagents.
    TasksChanged {
        /// Complete current task set, bounded by the adapter.
        tasks: Vec<AgentTask>,
    },
    /// Append-only native task/subagent lifecycle activity.
    TaskActivity {
        /// Ordered activity record.
        activity: AgentTaskActivity,
    },
    /// An explicit manual compaction invocation began.
    CompactionStarted {
        /// Requested compaction cause.
        trigger: CompactionTrigger,
    },
    /// The provider completed an automatic or manual context compaction.
    CompactionCompleted {
        /// Bounded provider-neutral compaction metadata.
        compaction: ContextCompaction,
    },
    /// Content-free Agent Relay lifecycle activity emitted by a host bridge.
    AgentRelayActivity {
        /// Normalized relay operation, receipt, or safety-limit result.
        activity: crate::relay::AgentRelayActivity,
    },
    /// A human approval is required.
    ApprovalRequested(ApprovalRequest),
    /// The agent proposed a plan and requires an explicit accept/reject decision.
    ///
    /// Applications resolve this through the same [`InteractionHandler::approve`]
    /// boundary as other approvals, using the request identifier for correlation.
    PlanApprovalRequested(ApprovalRequest),
    /// The agent asked the user a question.
    QuestionRequested(QuestionRequest),
    /// The agent asked a question the turn did not wait on.
    ///
    /// Some harnesses can ask a question without blocking their own turn
    /// (Codex app-server sends `item/tool/requestUserInput` with
    /// `isBlocking: false`). The runtime tells the provider immediately that
    /// no answer is available yet and keeps the turn running, so this event is
    /// never resolved through [`InteractionHandler::answer`]. Applications
    /// should render it as an open question and deliver the user's eventual
    /// answer as a follow-up prompt in the next turn.
    AsyncQuestionRequested(QuestionRequest),
    /// Token or cost update.
    Usage(Usage),
    /// Provider-account quota usage changed or was refreshed.
    AccountUsageUpdated {
        /// Replaceable point-in-time account quota snapshot.
        usage: AccountUsageSnapshot,
    },
    /// Recoverable diagnostic safe to show to the user.
    Warning {
        /// Human-readable diagnostic.
        message: String,
    },
    /// A sandbox backend identified a denied provider tool step.
    SandboxAccessDenied {
        /// Exact managed profile revision, when the turn used one.
        profile: Option<crate::SandboxProfileRef>,
        /// Structured failure used by approval UIs and audit storage.
        violation: crate::SandboxViolation,
    },
    /// A managed sandbox profile was durably updated after approval.
    SandboxProfileUpdated {
        /// Newly activated exact revision.
        profile: crate::SandboxProfileRef,
        /// Approved change applied to that revision.
        change: crate::SandboxProfileChange,
    },
    /// The runtime is resuming the provider session to retry a denied step.
    SandboxStepRetrying {
        /// Provider-native failed step identifier, when available.
        step_id: Option<String>,
        /// One-based retry attempt number.
        attempt: u8,
    },
}

/// Human approval requested by an adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    /// Provider request identifier.
    pub id: String,
    /// Tool being considered.
    pub tool_name: String,
    /// Provider-native input. Applications should treat it as sensitive.
    pub input: Value,
    /// Optional human-readable description.
    pub description: Option<String>,
}

/// Question requested by an adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuestionRequest {
    /// Provider request identifier.
    pub id: String,
    /// Provider-neutral prompts ready for a UI.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prompts: Vec<QuestionPrompt>,
    /// Provider-native question payload.
    pub questions: Value,
}

impl QuestionRequest {
    /// Create a request and normalize any recognized prompt payload.
    pub fn new(id: impl Into<String>, questions: Value) -> Self {
        let value = questions
            .get("questions")
            .cloned()
            .unwrap_or_else(|| questions.clone());
        let prompts = serde_json::from_value(value).unwrap_or_default();
        Self {
            id: id.into(),
            prompts,
            questions,
        }
    }

    /// Decode the provider payload into portable prompts suitable for a UI.
    ///
    /// Providers may add fields to the native payload. Unknown fields are
    /// ignored while malformed prompts return a descriptive serialization
    /// error instead of being presented as an unanswered question.
    pub fn prompts(&self) -> serde_json::Result<Vec<QuestionPrompt>> {
        if !self.prompts.is_empty() {
            return Ok(self.prompts.clone());
        }
        let value = self
            .questions
            .get("questions")
            .cloned()
            .unwrap_or_else(|| self.questions.clone());
        serde_json::from_value(value)
    }
}

/// One portable prompt within an agent question request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionPrompt {
    /// Short label used to associate the answer with the prompt.
    pub header: String,
    /// The full question shown to the user.
    pub question: String,
    /// Choices offered by the harness.
    #[serde(default)]
    pub options: Vec<QuestionOption>,
    /// Whether more than one option may be selected.
    #[serde(default, rename = "multiSelect", alias = "multi_select")]
    pub multi_select: bool,
}

/// One selectable answer to an agent question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionOption {
    /// Concise answer label returned to the harness when selected.
    pub label: String,
    /// Additional context describing the effect of this answer.
    #[serde(default)]
    pub description: String,
}

/// Response to a tool or plan approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ApprovalDecision {
    /// Permit the operation.
    Allow,
    /// Permit the operation and comparable ones for the rest of the session.
    ///
    /// Providers that expose a session-scoped approval (Codex app-server's
    /// `acceptForSession`) use it; the remaining adapters treat this exactly
    /// like [`ApprovalDecision::Allow`] for this one operation.
    AllowForSession,
    /// Reject the operation with an optional explanation.
    Deny {
        /// Explanation returned to the agent.
        reason: Option<String>,
    },
}

/// Response to an agent question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionAnswer {
    /// Provider-native answer map.
    pub answers: Value,
}

impl QuestionAnswer {
    /// Build a single-entry provider answer map keyed by question text.
    pub fn selected(question: impl Into<String>, value: impl Into<String>) -> Self {
        let mut answers = serde_json::Map::new();
        answers.insert(question.into(), Value::String(value.into()));
        Self {
            answers: Value::Object(answers),
        }
    }
}

/// Terminal status of a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStatus {
    /// Provider reported successful completion.
    Succeeded,
    /// Provider reported a terminal model-level error.
    Failed,
}

/// Provider-neutral terminal result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnResult {
    /// Terminal status.
    pub status: RunStatus,
    /// Final answer assembled by the adapter.
    pub text: String,
    /// Consolidated reasoning when available.
    pub reasoning: Option<String>,
    /// Provider session identifier for continuation.
    pub session_id: Option<String>,
    /// Provider-native session display title when reported by the harness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_title: Option<String>,
    /// Model actually reported by the provider.
    pub model: Option<String>,
    /// Final usage totals.
    pub usage: Usage,
}

impl Default for TurnResult {
    fn default() -> Self {
        Self {
            status: RunStatus::Succeeded,
            text: String::new(),
            reasoning: None,
            session_id: None,
            session_title: None,
            model: None,
            usage: Usage::default(),
        }
    }
}

/// Backpressured destination for normalized events.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Consume one event. Returning an error cancels the turn.
    async fn emit(&self, event: TurnEvent) -> Result<()>;
}

/// Sink that discards all events.
pub struct NoopEventSink;

#[async_trait]
impl EventSink for NoopEventSink {
    async fn emit(&self, _event: TurnEvent) -> Result<()> {
        Ok(())
    }
}

/// Application-owned bridge for approvals and user questions.
#[async_trait]
pub trait InteractionHandler: Send + Sync {
    /// Resolve one approval request.
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision;
    /// Resolve one question.
    async fn answer(&self, request: QuestionRequest) -> Option<QuestionAnswer>;
}

/// Interaction handler that fails closed immediately.
pub struct DenyAll;

#[async_trait]
impl InteractionHandler for DenyAll {
    async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::Deny {
            reason: Some("No interaction handler is configured".to_string()),
        }
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn question_request_decodes_portable_prompts() {
        let request = QuestionRequest {
            id: "question-1".to_string(),
            prompts: Vec::new(),
            questions: serde_json::json!({
                "questions": [{
                    "header": "Fruit",
                    "question": "Which fruit do you prefer?",
                    "multiSelect": false,
                    "options": [
                        {"label": "Plantain", "description": "Choose plantain."},
                        {"label": "Banana", "description": "Choose banana."}
                    ]
                }]
            }),
        };

        let prompts = request.prompts().unwrap();
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0].header, "Fruit");
        assert_eq!(prompts[0].options[1].label, "Banana");
        assert!(!prompts[0].multi_select);
    }

    #[test]
    fn question_answer_builds_the_provider_answer_map() {
        assert_eq!(
            QuestionAnswer::selected("Fruit", "Banana").answers,
            serde_json::json!({"Fruit": "Banana"})
        );
    }

    #[test]
    fn request_debug_redacts_prompt_and_environment_values() {
        let mut request = TurnRequest::new(Provider::Claude, ".", "secret prompt");
        request
            .environment
            .insert("API_KEY".into(), SecretString::new("secret value"));
        let debug = format!("{request:?}");
        assert!(!debug.contains("secret prompt"));
        assert!(!debug.contains("secret value"));
        assert!(debug.contains("API_KEY"));
    }
}
