use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value;

use crate::lifecycle::DeliveryState;
use crate::{
    AccountUsageReport, AccountUsageSnapshot, ApprovalDecision, ApprovalRequest,
    HarnessAuthentication, HarnessControlGroup, HarnessModelCatalog, LaunchContextCapabilities,
    PermissionSupport, Provider, ProviderReadiness, QuestionAnswer, QuestionRequest, Result,
    TransportExitStatus, TurnCapabilities, TurnEvent, TurnRequest, TurnResult,
};

/// Fully separated process invocation produced by an adapter.
///
/// Programs and arguments remain separate values throughout execution; this
/// crate never constructs a shell command string.
#[derive(Clone)]
pub struct CommandSpec {
    /// Executable path.
    pub program: PathBuf,
    /// Ordered argument vector.
    pub args: Vec<OsString>,
    /// Explicit environment additions.
    pub environment: BTreeMap<OsString, OsString>,
    /// Remove the host environment before applying the runtime allowlist.
    pub clear_environment: bool,
    /// Initial bytes written to stdin, followed by a newline.
    pub initial_stdin: Option<Vec<u8>>,
    /// Whether stdin must remain available for interaction responses.
    pub interactive_stdin: bool,
}

impl fmt::Debug for CommandSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandSpec")
            .field("program", &self.program)
            .field("argument_count", &self.args.len())
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("clear_environment", &self.clear_environment)
            .field(
                "initial_stdin_bytes",
                &self.initial_stdin.as_ref().map(Vec::len),
            )
            .field("interactive_stdin", &self.interactive_stdin)
            .finish()
    }
}

/// Metadata-only provider command used during harness discovery.
#[derive(Debug, Clone)]
pub struct CatalogProbeSpec {
    /// Process invocation executed inside the configured transport.
    pub command: CommandSpec,
    /// Stop reading and terminate the probe after this many JSON response ids
    /// have been observed. `None` waits for normal process exit.
    pub expected_response_ids: Option<Vec<u64>>,
}

/// Provider command used to fetch account quota without starting an agent turn.
#[derive(Debug, Clone)]
pub struct AccountUsageProbeSpec {
    /// Process invocation executed inside the configured transport.
    pub command: CommandSpec,
    /// Keep stdin open and stop after these JSON response IDs arrive.
    /// `None` closes stdin and waits for normal process exit.
    pub expected_response_ids: Option<Vec<u64>>,
}

/// Provider command used to inspect whether the selected target can authenticate.
#[derive(Debug, Clone)]
pub struct AuthenticationProbeSpec {
    /// Process invocation executed inside the configured transport.
    pub command: CommandSpec,
}

impl CommandSpec {
    /// Create a process specification for an executable.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            clear_environment: true,
            initial_stdin: None,
            interactive_stdin: false,
        }
    }

    /// Wrap this command with an outer executable while preserving its stdin
    /// and environment contract.
    ///
    /// `prefix_args` belong to the wrapper. `argument_separator` is commonly
    /// `Some("--".into())`, but remains explicit because wrapper CLIs differ.
    pub fn wrap_with(
        self,
        program: impl Into<PathBuf>,
        mut prefix_args: Vec<OsString>,
        argument_separator: Option<OsString>,
    ) -> Self {
        if let Some(separator) = argument_separator {
            prefix_args.push(separator);
        }
        prefix_args.push(self.program.into_os_string());
        prefix_args.extend(self.args);
        Self {
            program: program.into(),
            args: prefix_args,
            environment: self.environment,
            clear_environment: self.clear_environment,
            initial_stdin: self.initial_stdin,
            interactive_stdin: self.interactive_stdin,
        }
    }
}

/// Mutable provider state retained while parsing a turn.
#[derive(Debug, Default)]
pub struct AdapterState {
    /// Normalized terminal result assembled from provider frames.
    pub result: TurnResult,
    /// Lossless provider-native terminal failure observed on stdout.
    ///
    /// Some CLIs exit successfully after emitting a failed terminal frame, so
    /// process status and stderr alone cannot represent the turn outcome.
    pub terminal_failure: Option<ProviderTerminalFailure>,
    /// Whether a real incremental text event was observed.
    pub saw_text_delta: bool,
    /// Provider extension state.
    pub extensions: BTreeMap<String, Value>,
}

/// Provider-native terminal failure retained by an adapter until process exit.
///
/// Applications receive this through [`crate::RuntimeError::ProcessFailed`];
/// the runtime bounds and redacts the diagnostic before it crosses that
/// boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTerminalFailure {
    /// Stable provider-neutral category.
    pub kind: crate::ProviderProcessErrorKind,
    /// Provider diagnostic before runtime redaction and output bounding.
    pub diagnostic: String,
    /// Optional provider-native error code.
    pub provider_code: Option<String>,
    /// Whether the provider acknowledged or may have received the turn.
    pub delivery: DeliveryState,
}

impl ProviderTerminalFailure {
    /// Create a typed terminal failure without a provider-native code.
    pub fn new(
        kind: crate::ProviderProcessErrorKind,
        diagnostic: impl Into<String>,
        delivery: DeliveryState,
    ) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
            provider_code: None,
            delivery,
        }
    }

    /// Attach a provider-native code suitable for application diagnostics.
    pub fn with_provider_code(mut self, code: impl Into<String>) -> Self {
        self.provider_code = Some(code.into());
        self
    }
}

/// Interaction surfaced by a provider protocol.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InteractionRequest {
    /// Tool or plan approval.
    Approval {
        /// Normalized request delivered to the application.
        request: ApprovalRequest,
        /// Original provider frame needed to encode a response.
        original: Value,
    },
    /// User-facing question.
    Question {
        /// Normalized request delivered to the application.
        request: QuestionRequest,
        /// Original provider frame needed to encode a response.
        original: Value,
    },
}

/// Result of parsing one provider output line.
#[derive(Debug, Default)]
pub struct AdapterOutput {
    /// Zero or more normalized events.
    pub events: Vec<TurnEvent>,
    /// Optional blocking interaction.
    pub interaction: Option<InteractionRequest>,
    /// Provider-native frames the runtime writes to stdin immediately, without
    /// waiting for an application decision. Each is terminated with a newline.
    ///
    /// Bidirectional protocols need this to advance their own handshake: a
    /// JSON-RPC client answering a server request it resolves itself, or
    /// issuing the next call once the previous response arrived. An adapter
    /// that uses this field must set [`CommandSpec::interactive_stdin`].
    pub writes: Vec<Vec<u8>>,
    /// True after a provider terminal frame. The runtime then closes stdin.
    pub terminal: bool,
}

/// Adapter between a provider-native CLI protocol and normalized events.
#[async_trait]
pub trait AgentAdapter: Send + Sync {
    /// Provider implemented by this adapter.
    fn provider(&self) -> Provider;

    /// Executable name or path meaningful inside the selected execution transport.
    fn executable(&self) -> PathBuf {
        PathBuf::from(match self.provider() {
            Provider::Claude => "claude",
            Provider::Codex => "codex",
            Provider::OpenCode => "opencode",
        })
    }

    /// Static and interactive permission behavior implemented by this adapter.
    fn permission_support(&self) -> PermissionSupport;

    /// Provider-neutral launch-context fields this adapter can enforce.
    ///
    /// The default is deny-all so custom adapters must opt into each field
    /// explicitly. The runtime validates requests against this contract before
    /// spawning a provider process.
    fn launch_context_capabilities(&self) -> LaunchContextCapabilities {
        LaunchContextCapabilities::default()
    }

    /// Optional per-turn behaviors this adapter implements.
    ///
    /// The default denies every capability, so an application can tell a
    /// provider that consumes [`TurnRequest::attachments`] natively apart from
    /// one that needs the files described in the prompt instead.
    fn turn_capabilities(&self) -> TurnCapabilities {
        TurnCapabilities::default()
    }

    /// Provider-native runtime controls supported by this adapter and CLI.
    fn control_groups(&self) -> Vec<HarnessControlGroup> {
        Vec::new()
    }

    /// Build a metadata-only model/control catalog probe.
    fn catalog_probe(&self) -> Option<CatalogProbeSpec> {
        None
    }

    /// Parse collected stdout lines from [`Self::catalog_probe`].
    fn parse_catalog(&self, _lines: &[String]) -> Result<HarnessModelCatalog> {
        Ok(HarnessModelCatalog::unsupported("unsupported"))
    }

    /// Refine provider controls with metadata returned by the catalog probe.
    fn parse_control_groups(&self, _lines: &[String]) -> Result<Vec<HarnessControlGroup>> {
        Ok(self.control_groups())
    }

    /// Parse an optional account-usage snapshot observed by the catalog probe.
    ///
    /// A missing snapshot is not an error: some harnesses expose quota usage
    /// only while a real turn is running or only for subscription-based auth.
    fn parse_account_usage(&self, _lines: &[String]) -> Result<Option<AccountUsageSnapshot>> {
        Ok(None)
    }

    /// Build an explicit, metadata-only provider account-usage query.
    ///
    /// The command runs inside the selected execution transport so credentials
    /// never need to move from an SSH or sandbox target to the SDK host.
    fn account_usage_probe(&self) -> Option<AccountUsageProbeSpec> {
        None
    }

    /// Parse output from [`Self::account_usage_probe`].
    fn parse_account_usage_probe(&self, lines: &[String]) -> Result<AccountUsageReport> {
        Ok(match self.parse_account_usage(lines)? {
            Some(usage) => AccountUsageReport::available(usage),
            None => AccountUsageReport::unavailable(
                self.provider(),
                "the provider returned no account-usage metadata",
                true,
            ),
        })
    }

    /// Build a bounded provider-native authentication-status query.
    ///
    /// Returning `None` leaves authentication status `Unknown`. Adapters must
    /// not claim authentication merely because an executable or model catalog
    /// is available.
    fn authentication_probe(&self) -> Option<AuthenticationProbeSpec> {
        None
    }

    /// Interpret a completed authentication-status query.
    fn parse_authentication_probe(
        &self,
        _stdout: &[u8],
        _stderr: &str,
        _status: TransportExitStatus,
    ) -> Result<HarnessAuthentication> {
        Ok(HarnessAuthentication::unknown("unsupported"))
    }

    /// Legacy adapter-local availability probe.
    ///
    /// Applications should call [`crate::AgentRuntime::readiness`], which
    /// probes inside the configured execution transport instead.
    async fn readiness(&self) -> ProviderReadiness;

    /// Build the provider process for one validated request.
    fn command(&self, request: &TurnRequest) -> Result<CommandSpec>;

    /// Seed per-turn parser state from the validated request.
    ///
    /// The runtime calls this once, after [`Self::command`] and before the
    /// first output line. Adapters whose protocol issues requests of its own
    /// (rather than encoding the whole turn in argv and stdin) use it to
    /// retain the turn parameters that [`Self::parse_line`] later needs.
    fn prepare_turn(&self, request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
        let _ = (request, state);
        Ok(())
    }

    /// Translate one stdout line and update accumulated state.
    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput>;

    /// Encode a provider-native cooperative interrupt for the running turn.
    ///
    /// Returning `Some` makes the runtime write the frame on cancellation and
    /// give the provider a bounded moment to stop on its own before the
    /// process tree is terminated. The turn still fails with
    /// [`crate::RuntimeError::Cancelled`]; this only lets the harness unwind
    /// its own tool processes and persist session state first. Requires
    /// [`CommandSpec::interactive_stdin`].
    fn interrupt_request(&self, state: &AdapterState) -> Option<Vec<u8>> {
        let _ = state;
        None
    }

    /// Encode a provider-native approval response.
    fn approval_response(
        &self,
        request: &ApprovalRequest,
        original: &Value,
        decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>>;

    /// Encode a provider-native question response.
    fn question_response(
        &self,
        request: &QuestionRequest,
        original: &Value,
        answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>>;
}

/// Resolve an executable override or search common user-local `PATH` roots.
#[cfg(any(
    feature = "claude",
    feature = "codex",
    feature = "opencode",
    feature = "nono"
))]
pub(crate) fn resolve_executable(override_path: Option<&PathBuf>, name: &str) -> Option<PathBuf> {
    if let Some(path) = override_path {
        return path.is_file().then(|| path.clone());
    }
    let mut candidates = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        candidates.extend([
            home.join(".local/bin"),
            home.join(".cargo/bin"),
            home.join(".bun/bin"),
            home.join(".npm-global/bin"),
        ]);
    }
    #[cfg(windows)]
    let names = [
        format!("{name}.exe"),
        format!("{name}.cmd"),
        name.to_string(),
    ];
    #[cfg(not(windows))]
    let names = [name.to_string()];
    candidates
        .into_iter()
        .flat_map(|directory| names.iter().map(move |name| directory.join(name)))
        .find(|path| path.is_file())
}

#[cfg(any(feature = "claude", feature = "codex", feature = "opencode"))]
pub(crate) async fn inspect_executable(
    provider: Provider,
    path: Option<PathBuf>,
) -> ProviderReadiness {
    let Some(path) = path else {
        return ProviderReadiness {
            provider,
            installed: false,
            executable: None,
            version: None,
            detail: format!("Install the {provider} CLI and authenticate it as the runtime user."),
        };
    };
    let version = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::process::Command::new(&path)
            .arg("--version")
            .output(),
    )
    .await
    .ok()
    .and_then(std::result::Result::ok)
    .filter(|output| output.status.success())
    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
    .filter(|version| !version.is_empty());
    ProviderReadiness {
        provider,
        installed: true,
        executable: Some(path),
        version,
        detail: "The CLI executable is available; authentication is checked when a turn starts."
            .to_string(),
    }
}
