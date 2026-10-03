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
#[derive(Clone, PartialEq, Eq)]
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

/// A provider frame longer than the runtime's event-line limit.
///
/// The runtime never buffers such a frame. It keeps only the frame's first
/// and last few hundred bytes, so an adapter can identify the frame from its
/// head and read fields its provider writes after a large payload from its
/// tail. The two overlap when the frame is shorter than both together.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct OversizedFrame<'a> {
    /// The frame's first bytes.
    pub prefix: &'a str,
    /// The frame's last bytes, without its line terminator.
    pub suffix: &'a str,
}

impl<'a> OversizedFrame<'a> {
    /// Describe an oversized frame by its first and last bytes.
    #[must_use]
    pub const fn new(prefix: &'a str, suffix: &'a str) -> Self {
        Self { prefix, suffix }
    }
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
    /// The provider acknowledged the submitted prompt for this turn.
    pub turn_submitted: bool,
}

/// How a retained process frame read between turns is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleFrame {
    /// Output from background work the process still owns, such as a running
    /// terminal session. Dropped without affecting the process.
    Background,
    /// Answer to [`AgentAdapter::retained_keepalive_probe`]. `active` keeps the
    /// process alive for another idle period; otherwise it is terminated.
    KeepaliveResult {
        /// Whether the process still owns live background work.
        active: bool,
    },
    /// A frame that cannot be assigned to any turn. Retires the process.
    Unexpected,
}

/// Protocol carrier an adapter supplies in place of the provider's own stdio.
///
/// Most provider CLIs speak their protocol over stdout and stdin, so the
/// runtime reads frames from the child and writes [`AdapterOutput::writes`]
/// back to it. A provider whose protocol is *not* carried by its own stdio —
/// `opencode serve`, which exposes HTTP and Server-Sent Events on a loopback
/// port — returns these streams from [`AgentAdapter::attach`] instead.
///
/// The frame contract is deliberately unchanged: the runtime still reads
/// newline-delimited frames from [`Self::reader`] and still writes
/// newline-terminated frames to [`Self::writer`]. [`AgentAdapter::parse_line`]
/// therefore stays one synchronous, fully testable state machine no matter
/// what actually moves the bytes, and cancellation, interrupts, interaction
/// timeouts and line bounding keep working without a second code path.
///
/// The child process is still spawned, supervised and torn down by the
/// runtime. An adapter that returns streams here must keep the turn's
/// liveness tied to that child: the runtime fails the turn when the process
/// exits before a terminal frame arrives, so a server that dies mid-turn
/// surfaces immediately instead of hanging until the turn deadline.
pub struct ProtocolStreams {
    /// Newline-delimited frames parsed by [`AgentAdapter::parse_line`].
    pub reader: crate::TransportReader,
    /// Sink for [`AdapterOutput::writes`] and encoded interaction responses.
    pub writer: crate::TransportWriter,
}

impl fmt::Debug for ProtocolStreams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ProtocolStreams").finish()
    }
}

/// Adapter between a provider-native CLI protocol and normalized events.
#[async_trait]
pub trait AgentAdapter: Send + Sync {
    /// Provider implemented by this adapter.
    fn provider(&self) -> Provider;

    /// Whether this exact adapter supports retaining one native process across turns.
    ///
    /// Custom adapters remain disabled unless they explicitly implement the
    /// complete lifecycle contract.
    fn supports_retained_process(&self) -> bool {
        false
    }

    /// Executable name or path meaningful inside the selected execution transport.
    fn executable(&self) -> PathBuf {
        PathBuf::from(match self.provider() {
            Provider::Claude => "claude",
            Provider::Codex => "codex",
            Provider::OpenCode => "opencode",
            Provider::Pi => "pi",
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

    /// Build the provider process using state seeded by [`Self::prepare_turn`].
    ///
    /// The default ignores the state and defers to [`Self::command`], which is
    /// what a provider that encodes its whole turn in argv and stdin needs.
    /// An adapter that must agree with itself about a value chosen per turn —
    /// the loopback port `opencode serve` is told to bind and that
    /// [`Self::attach`] then connects to — overrides this instead, so the
    /// value is decided once in `prepare_turn` and read back here.
    fn command_for_turn(&self, request: &TurnRequest, state: &AdapterState) -> Result<CommandSpec> {
        let _ = state;
        self.command(request)
    }

    /// Seed per-turn parser state from the validated request.
    ///
    /// The runtime calls this once, before [`Self::command_for_turn`] and
    /// before the first output line. Adapters whose protocol issues requests
    /// of its own (rather than encoding the whole turn in argv and stdin) use
    /// it to retain the turn parameters that [`Self::parse_line`] later needs.
    fn prepare_turn(&self, request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
        let _ = (request, state);
        Ok(())
    }

    /// Seed a retained turn, optionally reusing provider-specific process state.
    fn prepare_retained_turn(
        &self,
        request: &TurnRequest,
        state: &mut AdapterState,
        process_hint: Option<u64>,
    ) -> Result<()> {
        let _ = process_hint;
        self.prepare_turn(request, state)
    }

    /// Opaque provider-specific state needed to address this retained process.
    fn retained_process_hint(&self, state: &AdapterState) -> Option<u64> {
        let _ = state;
        None
    }

    /// Begin another turn on an already initialized retained process.
    ///
    /// Returning `None` means the adapter cannot safely reuse its process.
    fn retained_turn_start(&self, state: &AdapterState) -> Result<Option<Vec<u8>>> {
        let _ = state;
        Ok(None)
    }

    /// Mark parser state as belonging to a retained native process.
    fn mark_retained_turn(&self, state: &mut AdapterState) {
        let _ = state;
    }

    /// Whether an active retained turn may hand its live process to the next
    /// turn instead of rejecting that turn as busy.
    ///
    /// Return `true` only once the turn's own answer is complete and the
    /// process is just running background work (such as Claude background
    /// subagents) that continues across a new prompt. The runtime then
    /// completes this turn and the next turn submits its prompt to the same
    /// process, inheriting state through [`Self::inherit_retained_handoff`].
    /// The default never hands off.
    fn retained_handoff_ready(&self, state: &AdapterState) -> bool {
        let _ = state;
        false
    }

    /// Seed a turn that took over a live process from `previous`.
    ///
    /// Called after [`Self::prepare_turn`] and [`Self::mark_retained_turn`],
    /// before the new prompt is written. Background work the previous turn
    /// started keeps emitting frames that the new turn must recognize.
    fn inherit_retained_handoff(&self, previous: AdapterState, next: &mut AdapterState) {
        let _ = (previous, next);
    }

    /// Quiet period after which an open retained turn completes successfully.
    ///
    /// `Some` means the turn's work is done unless the provider produces more
    /// output within the returned duration; any frame re-evaluates it. Claude
    /// uses this for the follow-up answer it gives once background work
    /// drains. The default `None` waits for a terminal frame.
    fn retained_completion_grace(&self, state: &AdapterState) -> Option<std::time::Duration> {
        let _ = state;
        None
    }

    /// Encode another user message for a turn that is still running.
    ///
    /// The adapter records the message as belonging to the turn, so the turn
    /// ends only once the provider has answered it too. `Ok(None)` — the
    /// default — means the provider cannot accept input mid-turn.
    fn encode_user_message(&self, text: &str, state: &mut AdapterState) -> Result<Option<Vec<u8>>> {
        let _ = (text, state);
        Ok(None)
    }

    /// Whether a retained turn that was sent [`Self::interrupt_request`] has
    /// finished unwinding, so its process can be kept instead of terminated.
    ///
    /// `None` — the default — means the adapter does not keep a process
    /// across interruption: the runtime terminates it as soon as the turn is
    /// cancelled. `Some(false)` keeps reading, bounded by a short deadline.
    fn retained_interrupt_settled(&self, state: &AdapterState) -> Option<bool> {
        let _ = state;
        None
    }

    /// Whether the process still runs background work started by earlier
    /// turns. A retained process doing so is kept alive between turns, its
    /// output buffered for the next turn, instead of expiring when idle.
    fn retained_background_work(&self, state: &AdapterState) -> bool {
        let _ = state;
        false
    }

    /// Short descriptions of the background work
    /// [`retained_background_work`](Self::retained_background_work) reports,
    /// for telling a caller what replacing the process would stop.
    fn retained_background_summary(&self, state: &AdapterState) -> Vec<String> {
        let _ = state;
        Vec::new()
    }

    /// Encode a request asking an idle retained process whether it still
    /// owns live background work for `session_id`.
    ///
    /// Called when the idle timeout elapses. Returning `None` — the default —
    /// terminates the process on expiry. The response is recognized by
    /// [`Self::classify_idle_frame`].
    fn retained_keepalive_probe(&self, session_id: &str) -> Option<Vec<u8>> {
        let _ = session_id;
        None
    }

    /// Classify one frame read from a retained process between turns.
    ///
    /// The default treats every frame as [`IdleFrame::Unexpected`], which
    /// retires the process because the frame cannot be assigned to a turn.
    fn classify_idle_frame(&self, line: &str) -> IdleFrame {
        let _ = line;
        IdleFrame::Unexpected
    }

    /// Supply a protocol carrier to use instead of the child's stdout and stdin.
    ///
    /// Called once, after the provider process is spawned and before the first
    /// frame is read. Returning `None` — the default — keeps the ordinary
    /// stdio contract. Returning [`ProtocolStreams`] tells the runtime to read
    /// frames from, and write frames to, those streams instead; the child is
    /// still spawned, supervised, stderr-drained and terminated by the runtime
    /// exactly as before.
    ///
    /// This is how a provider whose protocol lives somewhere other than its
    /// own stdio joins the normal turn loop rather than growing a parallel
    /// one. The implementation typically spawns a task that translates the
    /// provider's native transport into newline-delimited frames.
    async fn attach(
        &self,
        request: &TurnRequest,
        state: &AdapterState,
    ) -> Result<Option<ProtocolStreams>> {
        let _ = (request, state);
        Ok(None)
    }

    /// Translate one stdout line and update accumulated state.
    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput>;

    /// Decide whether a frame longer than the event-line limit may be read
    /// past the limit instead of failing the turn.
    ///
    /// Called once, as soon as the frame grows past the limit, with its first
    /// few hundred bytes. Returning `false` — the default — fails the turn
    /// with [`crate::RuntimeError::Protocol`] at once, without reading the
    /// rest of the frame. Returning `true` makes the runtime consume the rest
    /// without buffering it and pass the frame to
    /// [`AgentAdapter::parse_oversized_frame`]. An adapter whose protocol
    /// repeats, in summary frames, data it already received in smaller ones
    /// accepts those, so a long turn does not fail at its very end merely
    /// because its summary outgrew the limit.
    fn accepts_oversized_frame(&self, prefix: &str) -> bool {
        let _ = prefix;
        false
    }

    /// Translate a frame accepted by
    /// [`AgentAdapter::accepts_oversized_frame`], from its first and last
    /// bytes.
    fn parse_oversized_frame(
        &self,
        frame: OversizedFrame<'_>,
        state: &mut AdapterState,
    ) -> Result<AdapterOutput> {
        let _ = (frame, state);
        Ok(AdapterOutput::default())
    }

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
    feature = "pi",
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

#[cfg(any(
    feature = "claude",
    feature = "codex",
    feature = "opencode",
    feature = "pi"
))]
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
