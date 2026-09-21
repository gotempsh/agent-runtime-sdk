//! Retained runtime host and in-process client.
//!
//! The retained API provides lifecycle and session continuity while delegating
//! provider execution to the existing bounded [`crate::AgentRuntime`]. Provider
//! drivers may later retain native processes without changing application-owned
//! persistence or the public client contract.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};
use tokio_util::sync::CancellationToken;

use crate::lifecycle::{
    ConfigurationImpact, DeliveryState, InterruptOutcome, InvocationId, RetryAdvice,
    RuntimeFailure, RuntimeFailureKind, RuntimeHealth, RuntimeId, RuntimeStatus,
};
use crate::{
    AgentRuntime, AutoCompactionPolicy, CompactionTrigger, EventSink, InteractionHandler,
    LaunchContext, PermissionMode, Provider, ProviderProcessErrorKind, RuntimeError,
    SandboxCapabilities, SandboxRequest, SecretString, ToolProcessPolicy, TransportErrorKind,
    TurnEvent, TurnProvenance, TurnRequest, TurnResult,
};

const EVENT_CHANNEL_CAPACITY: usize = 128;
const INTERRUPT_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const RECENT_INVOCATION_CAPACITY: usize = 1_024;
const MAX_ATTACHMENTS: usize = 64;
const MAX_ATTACHMENT_METADATA_BYTES: usize = 4_096;
const MAX_COMPACTION_INSTRUCTIONS_BYTES: usize = 16 * 1024;
const DEFAULT_RETAINED_RUNTIME_CAPACITY: usize = 1_024;

/// Result type returned by retained-runtime APIs.
pub type RetainedRuntimeResult<T> = std::result::Result<T, RuntimeFailure>;

/// Resource limits for an [`InProcessRuntimeClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedRuntimeLimits {
    /// Maximum number of acquired runtimes retained until explicit disposal.
    pub max_runtimes: usize,
}

impl Default for RetainedRuntimeLimits {
    fn default() -> Self {
        Self {
            max_runtimes: DEFAULT_RETAINED_RUNTIME_CAPACITY,
        }
    }
}

/// Immutable configuration used to acquire a retained runtime.
#[derive(Clone)]
pub struct RuntimeSpec {
    /// Application-supplied stable runtime identity.
    pub runtime_id: RuntimeId,
    /// Provider hosted by this runtime.
    pub provider: Provider,
    /// Default provider working directory.
    pub working_directory: PathBuf,
    /// Default provider model.
    pub model: Option<String>,
    /// Default reasoning effort or provider variant.
    pub reasoning: Option<String>,
    /// Default provider permission mode.
    pub permission_mode: PermissionMode,
    /// Default provider-native harness controls.
    pub harness_options: BTreeMap<String, String>,
    /// Default provider-neutral system, tool, and MCP launch context.
    pub launch_context: LaunchContext,
    /// Default automatic context-compaction policy.
    pub auto_compaction: AutoCompactionPolicy,
    /// Existing provider-native session to resume on the first invocation.
    pub provider_session_id: Option<String>,
    /// Default turn deadline.
    pub turn_timeout: Duration,
    /// Default interaction deadline.
    pub interaction_timeout: Duration,
    /// Default policy for provider child processes.
    pub tool_process_policy: ToolProcessPolicy,
    /// Environment applied to every invocation. Values remain redacted.
    pub environment: BTreeMap<String, SecretString>,
    /// Optional outer sandbox applied to every invocation.
    pub sandbox: Option<SandboxRequest>,
    /// Sandbox capabilities required by every invocation.
    pub required_sandbox_capabilities: SandboxCapabilities,
}

impl RuntimeSpec {
    /// Creates a retained runtime specification with safe defaults.
    pub fn new(
        runtime_id: RuntimeId,
        provider: Provider,
        working_directory: impl Into<PathBuf>,
    ) -> Self {
        Self {
            runtime_id,
            provider,
            working_directory: working_directory.into(),
            model: None,
            reasoning: None,
            permission_mode: PermissionMode::default(),
            harness_options: BTreeMap::new(),
            launch_context: LaunchContext::default(),
            auto_compaction: AutoCompactionPolicy::default(),
            provider_session_id: None,
            turn_timeout: Duration::from_secs(30 * 60),
            interaction_timeout: Duration::from_secs(10 * 60),
            tool_process_policy: ToolProcessPolicy::default(),
            environment: BTreeMap::new(),
            sandbox: None,
            required_sandbox_capabilities: SandboxCapabilities::NONE,
        }
    }
}

impl fmt::Debug for RuntimeSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeSpec")
            .field("runtime_id", &self.runtime_id)
            .field("provider", &self.provider)
            .field("working_directory", &self.working_directory)
            .field("model", &self.model)
            .field("reasoning", &self.reasoning)
            .field("permission_mode", &self.permission_mode)
            .field("harness_options", &self.harness_options)
            .field("launch_context", &self.launch_context)
            .field("auto_compaction", &self.auto_compaction)
            .field(
                "has_provider_session_id",
                &self.provider_session_id.is_some(),
            )
            .field("turn_timeout", &self.turn_timeout)
            .field("interaction_timeout", &self.interaction_timeout)
            .field("tool_process_policy", &self.tool_process_policy)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("sandbox", &self.sandbox)
            .field(
                "required_sandbox_capabilities",
                &self.required_sandbox_capabilities,
            )
            .finish_non_exhaustive()
    }
}

/// Semantic kind of work submitted through the retained invocation pipeline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuntimeInvocationKind {
    /// An ordinary user or agent turn.
    #[default]
    Turn,
    /// A provider-native manual context compaction.
    ManualCompaction,
}

/// One invocation submitted to a retained runtime.
#[derive(Clone)]
pub struct TurnInput {
    /// Stable invocation identity supplied by the application.
    pub invocation_id: InvocationId,
    /// Semantic operation represented by this invocation.
    pub invocation_kind: RuntimeInvocationKind,
    /// User prompt. Debug output reports only its byte length.
    pub prompt: String,
    /// Typed user-versus-agent provenance for this user-level prompt.
    pub provenance: TurnProvenance,
    /// Per-invocation model override.
    pub model: Option<String>,
    /// Per-invocation reasoning override.
    pub reasoning: Option<String>,
    /// Per-invocation permission override.
    pub permission_mode: Option<PermissionMode>,
    /// Per-invocation replacement for provider-native harness controls.
    pub harness_options: Option<BTreeMap<String, String>>,
    /// Per-invocation replacement for the runtime's launch context.
    pub launch_context: Option<LaunchContext>,
    /// Per-invocation automatic context-compaction override.
    pub auto_compaction: Option<AutoCompactionPolicy>,
    /// Optional provider turn limit.
    pub max_turns: Option<u32>,
    /// Per-invocation turn deadline override.
    pub timeout: Option<Duration>,
    /// Per-invocation interaction deadline override.
    pub interaction_timeout: Option<Duration>,
    /// Per-invocation child-process policy override.
    pub tool_process_policy: Option<ToolProcessPolicy>,
    /// Additional per-invocation environment. Values remain redacted.
    pub environment: BTreeMap<String, SecretString>,
    /// Files already present on the selected execution host and referenced by this turn.
    pub attachments: Vec<TurnAttachment>,
    /// How provider approvals and questions must be handled.
    pub interaction_policy: InteractionPolicy,
}

impl TurnInput {
    /// Creates an invocation with runtime defaults.
    pub fn new(invocation_id: InvocationId, prompt: impl Into<String>) -> Self {
        Self {
            invocation_id,
            invocation_kind: RuntimeInvocationKind::Turn,
            prompt: prompt.into(),
            provenance: TurnProvenance::default(),
            model: None,
            reasoning: None,
            permission_mode: None,
            harness_options: None,
            launch_context: None,
            auto_compaction: None,
            max_turns: None,
            timeout: None,
            interaction_timeout: None,
            tool_process_policy: None,
            environment: BTreeMap::new(),
            attachments: Vec::new(),
            interaction_policy: InteractionPolicy::Deny,
        }
    }
}

impl fmt::Debug for TurnInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnInput")
            .field("invocation_id", &self.invocation_id)
            .field("invocation_kind", &self.invocation_kind)
            .field("prompt_bytes", &self.prompt.len())
            .field("provenance", &self.provenance)
            .field("model", &self.model)
            .field("reasoning", &self.reasoning)
            .field("permission_mode", &self.permission_mode)
            .field("harness_options", &self.harness_options)
            .field("launch_context", &self.launch_context)
            .field("auto_compaction", &self.auto_compaction)
            .field("max_turns", &self.max_turns)
            .field("timeout", &self.timeout)
            .field("interaction_timeout", &self.interaction_timeout)
            .field("tool_process_policy", &self.tool_process_policy)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("attachments", &self.attachments)
            .field("interaction_policy", &self.interaction_policy)
            .finish_non_exhaustive()
    }
}

/// Application request for provider-native manual context compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionInput {
    /// Stable, durable operation identity supplied by the application.
    pub invocation_id: InvocationId,
    /// Optional provider-facing focus for the generated summary.
    pub instructions: Option<String>,
    /// Optional deadline override for this compaction invocation.
    pub timeout: Option<Duration>,
}

impl CompactionInput {
    /// Creates a manual compaction request without extra summary instructions.
    pub fn new(invocation_id: InvocationId) -> Self {
        Self {
            invocation_id,
            instructions: None,
            timeout: None,
        }
    }

    fn into_turn_input(self) -> TurnInput {
        let prompt = self.instructions.map_or_else(
            || "/compact".to_owned(),
            |instructions| format!("/compact {instructions}"),
        );
        let mut input = TurnInput::new(self.invocation_id, prompt);
        input.invocation_kind = RuntimeInvocationKind::ManualCompaction;
        input.timeout = self.timeout;
        input
    }
}

/// Reference to a file already available inside the execution target.
///
/// The SDK deliberately does not upload or persist file bytes. An application
/// stages an upload through its own authorized storage/transport boundary,
/// then submits the resulting execution-host path here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnAttachment {
    /// Absolute or working-directory-relative path meaningful on the execution host.
    pub path: PathBuf,
    /// Optional user-facing name independent from the host path.
    pub display_name: Option<String>,
    /// Optional media type supplied by the application.
    pub media_type: Option<String>,
}

impl TurnAttachment {
    /// Creates a file reference without optional metadata.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            display_name: None,
            media_type: None,
        }
    }
}

/// Required handling for provider approvals and questions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InteractionPolicy {
    /// Fail closed through the built-in deny/no-answer handler.
    #[default]
    Deny,
    /// Reject the invocation unless the caller supplies an interaction handler.
    RequireHandler,
}

/// Retention and interaction behavior implemented by a provider driver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeDriverCapabilities {
    /// The provider process itself remains alive between invocations.
    pub retained_process: bool,
    /// Provider-native session identifiers can continue a logical runtime.
    pub session_resume: bool,
    /// Provider questions and approvals can be answered while a turn is active.
    pub live_interactions: bool,
    /// The harness accepts an explicit automatic compaction policy.
    #[serde(default)]
    pub configurable_auto_compaction: bool,
    /// The harness supports provider-native manual compaction of an existing session.
    #[serde(default)]
    pub manual_compaction: bool,
    /// The driver emits active context-window occupancy snapshots when available.
    #[serde(default)]
    pub context_window_usage: bool,
    /// The driver delivers image attachments as native provider image inputs
    /// instead of describing their host paths in the prompt.
    #[serde(default)]
    pub native_image_attachments: bool,
}

/// Runtime setting whose update impact is being inspected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuntimeConfigurationKey {
    /// Provider implementation.
    Provider,
    /// Execution-host working directory.
    WorkingDirectory,
    /// Provider model.
    Model,
    /// Provider reasoning effort or mode.
    Reasoning,
    /// Permission policy.
    PermissionMode,
    /// Provider-native harness controls.
    HarnessOptions,
    /// Provider-neutral system, tool, and MCP launch context.
    LaunchContext,
    /// Automatic context-compaction policy.
    AutoCompaction,
    /// Process environment.
    Environment,
    /// Turn or interaction deadlines.
    Timeouts,
    /// Outer sandbox and required capabilities.
    Sandbox,
}

/// Versioned normalized event emitted by a retained invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Envelope schema version. Version one is the initial retained API.
    pub schema_version: u16,
    /// Runtime that emitted the event.
    pub runtime_id: RuntimeId,
    /// Invocation that emitted the event.
    pub invocation_id: InvocationId,
    /// One-based sequence number scoped to the invocation.
    pub sequence: u64,
    /// Milliseconds since the Unix epoch when the SDK observed the event.
    pub observed_at_unix_ms: u64,
    /// Provider-neutral lifecycle or provider event payload.
    pub event: RuntimeEvent,
}

/// Complete sequenced event stream for one retained invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuntimeEvent {
    /// The runtime accepted the invocation and began execution.
    InvocationStarted {
        /// Selected provider.
        provider: Provider,
    },
    /// Normalized provider activity.
    ProviderEvent {
        /// Existing provider-neutral event payload.
        event: TurnEvent,
    },
    /// The invocation completed successfully at the runtime boundary.
    InvocationCompleted {
        /// Provider-neutral terminal result.
        result: TurnResult,
    },
    /// The invocation failed at the runtime boundary.
    InvocationFailed {
        /// Typed failure including delivery and retry semantics.
        failure: RuntimeFailure,
    },
}

/// Outcome of disposing a retained runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DisposeOutcome {
    /// The runtime existed and disposal was initiated.
    Disposed,
    /// No runtime with that identity existed.
    NotFound,
}

/// Executor used by an in-process retained-runtime host.
///
/// Implementations must preserve the existing event backpressure and
/// interaction contracts. [`AgentRuntime`] implements this trait directly.
#[async_trait]
pub trait RuntimeTurnExecutor: Send + Sync {
    /// Reports driver behavior for the selected provider.
    fn capabilities(&self, provider: Provider) -> RuntimeDriverCapabilities;

    /// Reports how a setting change can be applied to an acquired runtime.
    fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact;

    /// Execute one fully assembled provider turn.
    async fn execute(
        &self,
        request: TurnRequest,
        events: &dyn EventSink,
        interactions: Option<&dyn InteractionHandler>,
    ) -> crate::Result<TurnResult>;
}

#[async_trait]
impl RuntimeTurnExecutor for AgentRuntime {
    fn capabilities(&self, provider: Provider) -> RuntimeDriverCapabilities {
        let permissions = self.permission_support(provider).ok();
        RuntimeDriverCapabilities {
            retained_process: false,
            session_resume: true,
            live_interactions: permissions
                .is_some_and(|support| support.live_approvals || support.live_questions),
            configurable_auto_compaction: provider == Provider::Claude,
            manual_compaction: provider == Provider::Claude,
            context_window_usage: provider == Provider::Claude
                || self
                    .turn_capabilities(provider)
                    .is_ok_and(|capabilities| capabilities.context_window_usage),
            native_image_attachments: self
                .turn_capabilities(provider)
                .is_ok_and(|capabilities| capabilities.native_image_attachments),
        }
    }

    fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact {
        match key {
            RuntimeConfigurationKey::Provider
            | RuntimeConfigurationKey::WorkingDirectory
            | RuntimeConfigurationKey::Sandbox => ConfigurationImpact::ReacquireRequired,
            RuntimeConfigurationKey::Model
            | RuntimeConfigurationKey::Reasoning
            | RuntimeConfigurationKey::PermissionMode
            | RuntimeConfigurationKey::HarnessOptions
            | RuntimeConfigurationKey::LaunchContext
            | RuntimeConfigurationKey::AutoCompaction
            | RuntimeConfigurationKey::Environment
            | RuntimeConfigurationKey::Timeouts => ConfigurationImpact::Live,
        }
    }

    async fn execute(
        &self,
        request: TurnRequest,
        events: &dyn EventSink,
        interactions: Option<&dyn InteractionHandler>,
    ) -> crate::Result<TurnResult> {
        self.run(request, events, interactions).await
    }
}

/// Client contract implemented by in-process and future remote runtime hosts.
#[async_trait]
pub trait RuntimeClient: Send + Sync {
    /// Acquire a new retained runtime.
    async fn acquire(&self, spec: RuntimeSpec) -> RetainedRuntimeResult<RuntimeHandle>;

    /// Attach to an existing retained runtime.
    async fn attach(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<RuntimeHandle>;

    /// Dispose a retained runtime and cancel its active invocation.
    async fn dispose(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<DisposeOutcome>;
}

/// In-process retained-runtime host backed by one bounded turn executor.
#[derive(Clone)]
pub struct InProcessRuntimeClient {
    executor: Arc<dyn RuntimeTurnExecutor>,
    runtimes: Arc<RwLock<HashMap<RuntimeId, Arc<RuntimeEntry>>>>,
    max_runtimes: usize,
}

impl InProcessRuntimeClient {
    /// Creates a retained client around the existing provider runtime.
    pub fn new(runtime: AgentRuntime) -> Self {
        Self::from_executor(Arc::new(runtime))
    }

    /// Creates a retained client with an explicit maximum runtime count.
    pub fn with_limits(
        runtime: AgentRuntime,
        limits: RetainedRuntimeLimits,
    ) -> RetainedRuntimeResult<Self> {
        Self::from_executor_with_limits(Arc::new(runtime), limits)
    }

    /// Creates a retained client around a custom executor.
    ///
    /// This constructor supports provider-driver conformance tests and custom
    /// embedded hosts without exposing application persistence to the SDK.
    ///
    /// # Panics
    ///
    /// Panics only if the crate's built-in default limits become internally invalid.
    pub fn from_executor(executor: Arc<dyn RuntimeTurnExecutor>) -> Self {
        Self::from_executor_with_limits(executor, RetainedRuntimeLimits::default())
            .expect("default retained runtime limits are valid")
    }

    /// Creates a retained client around a custom executor with explicit limits.
    pub fn from_executor_with_limits(
        executor: Arc<dyn RuntimeTurnExecutor>,
        limits: RetainedRuntimeLimits,
    ) -> RetainedRuntimeResult<Self> {
        if limits.max_runtimes == 0 {
            return Err(lifecycle_failure(
                None,
                None,
                RuntimeFailureKind::InvalidRequest,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                "retained runtime capacity must be greater than zero",
            ));
        }
        Ok(Self {
            executor,
            runtimes: Arc::new(RwLock::new(HashMap::new())),
            max_runtimes: limits.max_runtimes,
        })
    }
}

#[async_trait]
impl RuntimeClient for InProcessRuntimeClient {
    async fn acquire(&self, spec: RuntimeSpec) -> RetainedRuntimeResult<RuntimeHandle> {
        let runtime_id = spec.runtime_id.clone();
        let driver = self.executor.capabilities(spec.provider);
        if spec.auto_compaction != AutoCompactionPolicy::ProviderDefault
            && !driver.configurable_auto_compaction
        {
            return Err(lifecycle_failure(
                Some(runtime_id),
                None,
                RuntimeFailureKind::CapabilityUnavailable,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                format!(
                    "{} does not support configurable automatic compaction",
                    spec.provider
                ),
            ));
        }
        validate_auto_compaction_policy(spec.auto_compaction).map_err(|message| {
            lifecycle_failure(
                Some(spec.runtime_id.clone()),
                None,
                RuntimeFailureKind::InvalidRequest,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                message,
            )
        })?;
        let mut runtimes = self.runtimes.write().await;
        if runtimes.contains_key(&runtime_id) {
            return Err(lifecycle_failure(
                Some(runtime_id),
                None,
                RuntimeFailureKind::RuntimeAlreadyExists,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                "a runtime with this identifier already exists",
            ));
        }
        if runtimes.len() >= self.max_runtimes {
            return Err(lifecycle_failure(
                Some(runtime_id),
                None,
                RuntimeFailureKind::RuntimeBusy,
                RetryAdvice::After { milliseconds: 1_000 },
                DeliveryState::NotSent,
                format!(
                    "retained runtime capacity of {} is exhausted; dispose an inactive runtime before retrying",
                    self.max_runtimes
                ),
            ));
        }
        let entry = Arc::new(RuntimeEntry::new(spec, Arc::clone(&self.executor)));
        runtimes.insert(runtime_id, Arc::clone(&entry));
        Ok(RuntimeHandle::in_process(entry))
    }

    async fn attach(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<RuntimeHandle> {
        let runtimes = self.runtimes.read().await;
        let entry = runtimes.get(runtime_id).cloned().ok_or_else(|| {
            lifecycle_failure(
                Some(runtime_id.clone()),
                None,
                RuntimeFailureKind::RuntimeNotFound,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                "the retained runtime does not exist",
            )
        })?;
        Ok(RuntimeHandle::in_process(entry))
    }

    async fn dispose(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<DisposeOutcome> {
        let entry = self.runtimes.read().await.get(runtime_id).cloned();
        let Some(entry) = entry else {
            return Ok(DisposeOutcome::NotFound);
        };
        entry.dispose().await?;
        self.runtimes.write().await.remove(runtime_id);
        Ok(DisposeOutcome::Disposed)
    }
}

/// Attached capability for one retained runtime.
#[derive(Clone)]
pub struct RuntimeHandle {
    runtime_id: RuntimeId,
    provider: Provider,
    driver: RuntimeDriverCapabilities,
    backend: Arc<dyn RuntimeHandleBackend>,
}

impl RuntimeHandle {
    fn in_process(entry: Arc<RuntimeEntry>) -> Self {
        let provider = entry.spec.provider;
        let driver = entry.executor.capabilities(provider);
        Self::from_backend(entry.spec.runtime_id.clone(), provider, driver, entry)
    }

    pub(crate) fn from_backend(
        runtime_id: RuntimeId,
        provider: Provider,
        driver: RuntimeDriverCapabilities,
        backend: Arc<dyn RuntimeHandleBackend>,
    ) -> Self {
        Self {
            runtime_id,
            provider,
            driver,
            backend,
        }
    }

    /// Returns the retained runtime identity.
    pub fn runtime_id(&self) -> &RuntimeId {
        &self.runtime_id
    }

    /// Returns the provider hosted by this runtime.
    pub fn provider(&self) -> Provider {
        self.provider
    }

    /// Returns the provider driver's retention and interaction behavior.
    pub fn driver_capabilities(&self) -> RuntimeDriverCapabilities {
        self.driver
    }

    /// Reports whether changing one runtime setting is live or needs replacement.
    pub fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact {
        self.backend.configuration_impact(key)
    }

    /// Returns a point-in-time runtime health report.
    pub async fn health(&self) -> RuntimeHealth {
        self.backend.health().await
    }

    /// Starts an invocation with fail-closed interaction handling.
    pub async fn start_turn(&self, input: TurnInput) -> RetainedRuntimeResult<TurnHandle> {
        Arc::clone(&self.backend).start_turn(input, None).await
    }

    /// Starts a provider-native manual compaction using the same durable lifecycle as a turn.
    ///
    /// The runtime must already have a provider session. Applications should persist the
    /// returned event envelopes exactly like ordinary invocation events.
    pub async fn compact(&self, input: CompactionInput) -> RetainedRuntimeResult<TurnHandle> {
        if !self.driver.manual_compaction {
            return Err(lifecycle_failure(
                Some(self.runtime_id.clone()),
                Some(input.invocation_id),
                RuntimeFailureKind::CapabilityUnavailable,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                format!(
                    "{} does not support provider-native manual compaction",
                    self.provider
                ),
            ));
        }
        Arc::clone(&self.backend)
            .start_turn(input.into_turn_input(), None)
            .await
    }

    /// Starts an invocation with an application-owned interaction handler.
    pub async fn start_turn_with_interactions(
        &self,
        input: TurnInput,
        interactions: Arc<dyn InteractionHandler>,
    ) -> RetainedRuntimeResult<TurnHandle> {
        Arc::clone(&self.backend)
            .start_turn(input, Some(interactions))
            .await
    }
}

impl fmt::Debug for RuntimeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeHandle")
            .field("runtime_id", &self.runtime_id)
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub(crate) trait RuntimeHandleBackend: Send + Sync {
    fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact;

    async fn health(&self) -> RuntimeHealth;

    async fn start_turn(
        self: Arc<Self>,
        input: TurnInput,
        interactions: Option<Arc<dyn InteractionHandler>>,
    ) -> RetainedRuntimeResult<TurnHandle>;
}

/// Receiver for ordered invocation events.
pub struct TurnEventStream {
    receiver: mpsc::Receiver<EventEnvelope>,
}

impl TurnEventStream {
    /// Waits for the next event, returning `None` after the stream closes.
    pub async fn next(&mut self) -> Option<EventEnvelope> {
        self.receiver.recv().await
    }
}

/// Receiver for an invocation's terminal result.
pub struct TurnCompletion {
    runtime_id: RuntimeId,
    invocation_id: InvocationId,
    receiver: oneshot::Receiver<RetainedRuntimeResult<TurnResult>>,
}

impl TurnCompletion {
    /// Waits for the terminal result.
    pub async fn wait(self) -> RetainedRuntimeResult<TurnResult> {
        self.receiver.await.unwrap_or_else(|_| {
            Err(lifecycle_failure(
                Some(self.runtime_id),
                Some(self.invocation_id),
                RuntimeFailureKind::Indeterminate,
                RetryAdvice::RequiresUserAction,
                DeliveryState::PossiblySent,
                "the retained runtime task ended without a terminal result",
            ))
        })
    }
}

/// Active retained invocation with ordered events, completion, and interruption.
pub struct TurnHandle {
    /// Runtime that owns this invocation.
    pub runtime_id: RuntimeId,
    /// Stable invocation identity.
    pub invocation_id: InvocationId,
    events: TurnEventStream,
    completion: TurnCompletion,
    interrupt: TurnInterruptHandle,
}

/// Cloneable interruption capability independent from event/completion receivers.
#[derive(Clone)]
pub struct TurnInterruptHandle {
    backend: Arc<dyn TurnInterruptBackend>,
}

#[async_trait]
pub(crate) trait TurnInterruptBackend: Send + Sync {
    async fn interrupt(&self) -> InterruptOutcome;
}

struct InProcessTurnInterrupt {
    cancellation: CancellationToken,
    terminal: Arc<CompletionSignal>,
    disposed: Arc<AtomicBool>,
}

#[async_trait]
impl TurnInterruptBackend for InProcessTurnInterrupt {
    async fn interrupt(&self) -> InterruptOutcome {
        if self.disposed.load(Ordering::Acquire) {
            return InterruptOutcome::RuntimeDisposed;
        }
        if self.terminal.state.load(Ordering::Acquire) != CompletionSignal::RUNNING {
            return InterruptOutcome::AlreadyFinished;
        }
        self.cancellation.cancel();
        match tokio::time::timeout(
            INTERRUPT_CONFIRMATION_TIMEOUT,
            self.terminal.wait_for_completion(),
        )
        .await
        {
            Ok(CompletionKind::Cancelled) => InterruptOutcome::Interrupted,
            Ok(CompletionKind::Finished) => InterruptOutcome::AlreadyFinished,
            Err(_) => InterruptOutcome::Unconfirmed,
        }
    }
}

impl TurnInterruptHandle {
    pub(crate) fn from_backend(backend: Arc<dyn TurnInterruptBackend>) -> Self {
        Self { backend }
    }

    /// Requests interruption and waits briefly for provider termination confirmation.
    pub async fn interrupt(&self) -> InterruptOutcome {
        self.backend.interrupt().await
    }
}

impl fmt::Debug for TurnHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnHandle")
            .field("runtime_id", &self.runtime_id)
            .field("invocation_id", &self.invocation_id)
            .finish_non_exhaustive()
    }
}

impl TurnHandle {
    pub(crate) fn from_channels(
        runtime_id: RuntimeId,
        invocation_id: InvocationId,
        event_receiver: mpsc::Receiver<EventEnvelope>,
        completion_receiver: oneshot::Receiver<RetainedRuntimeResult<TurnResult>>,
        interrupt: TurnInterruptHandle,
    ) -> Self {
        Self {
            events: TurnEventStream {
                receiver: event_receiver,
            },
            completion: TurnCompletion {
                runtime_id: runtime_id.clone(),
                invocation_id: invocation_id.clone(),
                receiver: completion_receiver,
            },
            runtime_id,
            invocation_id,
            interrupt,
        }
    }

    /// Waits for the next normalized event.
    pub async fn next_event(&mut self) -> Option<EventEnvelope> {
        self.events.next().await
    }

    /// Splits the invocation into independently owned event and completion receivers.
    pub fn into_parts(self) -> (TurnEventStream, TurnCompletion) {
        (self.events, self.completion)
    }

    /// Returns a cloneable interruption capability for a supervising host.
    pub fn interrupt_handle(&self) -> TurnInterruptHandle {
        self.interrupt.clone()
    }

    /// Drains remaining events and waits for the terminal result.
    ///
    /// Use [`Self::into_parts`] when events and completion must be consumed by
    /// separate tasks.
    pub async fn wait(mut self) -> RetainedRuntimeResult<TurnResult> {
        while self.events.next().await.is_some() {}
        self.completion.wait().await
    }

    /// Requests interruption and waits briefly for provider termination confirmation.
    pub async fn interrupt(&self) -> InterruptOutcome {
        self.interrupt_handle().interrupt().await
    }
}

struct RuntimeEntry {
    spec: RuntimeSpec,
    executor: Arc<dyn RuntimeTurnExecutor>,
    state: Mutex<EntryState>,
    disposed: Arc<AtomicBool>,
}

struct EntryState {
    status: RuntimeStatus,
    provider_session_id: Option<String>,
    active: Option<ActiveTurn>,
    detail: Option<String>,
    recent_invocations: HashSet<InvocationId>,
    recent_invocation_order: VecDeque<InvocationId>,
}

struct ActiveTurn {
    invocation_id: InvocationId,
    cancellation: CancellationToken,
    terminal: Arc<CompletionSignal>,
}

impl RuntimeEntry {
    fn new(spec: RuntimeSpec, executor: Arc<dyn RuntimeTurnExecutor>) -> Self {
        let provider_session_id = spec.provider_session_id.clone();
        Self {
            spec,
            executor,
            state: Mutex::new(EntryState {
                status: RuntimeStatus::Ready,
                provider_session_id,
                active: None,
                detail: None,
                recent_invocations: HashSet::new(),
                recent_invocation_order: VecDeque::new(),
            }),
            disposed: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn health(&self) -> RuntimeHealth {
        let state = self.state.lock().await;
        RuntimeHealth {
            runtime_id: self.spec.runtime_id.clone(),
            status: state.status,
            observed_at_unix_ms: unix_time_ms(),
            detail: state.detail.clone(),
        }
    }

    async fn dispose(&self) -> RetainedRuntimeResult<()> {
        self.disposed.store(true, Ordering::Release);
        let mut state = self.state.lock().await;
        let terminal = if let Some(active) = &state.active {
            active.cancellation.cancel();
            let terminal = (active.invocation_id.clone(), Arc::clone(&active.terminal));
            state.status = RuntimeStatus::Stopping;
            Some(terminal)
        } else {
            state.status = RuntimeStatus::Stopped;
            None
        };
        drop(state);
        if let Some((invocation_id, terminal)) = terminal {
            tokio::time::timeout(
                INTERRUPT_CONFIRMATION_TIMEOUT,
                terminal.wait_for_completion(),
            )
            .await
            .map_err(|_| {
                lifecycle_failure(
                    Some(self.spec.runtime_id.clone()),
                    Some(invocation_id),
                    RuntimeFailureKind::Indeterminate,
                    RetryAdvice::RequiresUserAction,
                    DeliveryState::PossiblySent,
                    "retained provider termination was not confirmed during disposal",
                )
            })?;
        }
        Ok(())
    }

    async fn start_turn(
        self: &Arc<Self>,
        input: TurnInput,
        interactions: Option<Arc<dyn InteractionHandler>>,
    ) -> RetainedRuntimeResult<TurnHandle> {
        self.validate_input(&input, interactions.is_some())?;
        if self.disposed.load(Ordering::Acquire) {
            return Err(lifecycle_failure(
                Some(self.spec.runtime_id.clone()),
                Some(input.invocation_id),
                RuntimeFailureKind::RuntimeDisposed,
                RetryAdvice::Never,
                DeliveryState::NotSent,
                "the retained runtime has been disposed",
            ));
        }

        let cancellation = CancellationToken::new();
        let terminal = Arc::new(CompletionSignal::new());
        let provider_session_id = {
            let mut state = self.state.lock().await;
            if self.disposed.load(Ordering::Acquire) {
                return Err(lifecycle_failure(
                    Some(self.spec.runtime_id.clone()),
                    Some(input.invocation_id),
                    RuntimeFailureKind::RuntimeDisposed,
                    RetryAdvice::Never,
                    DeliveryState::NotSent,
                    "the retained runtime was disposed before invocation admission",
                ));
            }
            if state.recent_invocations.contains(&input.invocation_id) {
                return Err(lifecycle_failure(
                    Some(self.spec.runtime_id.clone()),
                    Some(input.invocation_id),
                    RuntimeFailureKind::InvocationAlreadyExists,
                    RetryAdvice::Never,
                    DeliveryState::Accepted,
                    "the invocation identifier was already accepted by this runtime",
                ));
            }
            if let Some(active) = &state.active {
                return Err(lifecycle_failure(
                    Some(self.spec.runtime_id.clone()),
                    Some(input.invocation_id),
                    RuntimeFailureKind::RuntimeBusy,
                    RetryAdvice::After { milliseconds: 100 },
                    DeliveryState::NotSent,
                    format!("runtime is processing invocation {}", active.invocation_id),
                ));
            }
            if input.invocation_kind == RuntimeInvocationKind::ManualCompaction
                && state.provider_session_id.is_none()
            {
                return Err(self.invalid_input(
                    &input.invocation_id,
                    "manual compaction requires an existing provider session",
                ));
            }
            state.status = RuntimeStatus::Busy;
            state.detail = None;
            state.active = Some(ActiveTurn {
                invocation_id: input.invocation_id.clone(),
                cancellation: cancellation.clone(),
                terminal: Arc::clone(&terminal),
            });
            state.recent_invocations.insert(input.invocation_id.clone());
            state
                .recent_invocation_order
                .push_back(input.invocation_id.clone());
            while state.recent_invocation_order.len() > RECENT_INVOCATION_CAPACITY {
                if let Some(expired) = state.recent_invocation_order.pop_front() {
                    state.recent_invocations.remove(&expired);
                }
            }
            state.provider_session_id.clone()
        };

        let request = self.build_request(&input, provider_session_id, cancellation.clone());
        let runtime_id = self.spec.runtime_id.clone();
        let invocation_id = input.invocation_id.clone();
        let (event_sender, event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (completion_sender, completion_receiver) = oneshot::channel();
        let sink = ChannelEventSink {
            provider: self.spec.provider,
            runtime_id: runtime_id.clone(),
            invocation_id: invocation_id.clone(),
            sequence: AtomicU64::new(0),
            sender: event_sender,
        };
        let entry = Arc::clone(self);
        let task_terminal = Arc::clone(&terminal);
        let task_invocation_id = invocation_id.clone();
        let invocation_kind = input.invocation_kind;
        tokio::spawn(async move {
            let started = sink
                .emit_runtime(RuntimeEvent::InvocationStarted {
                    provider: entry.spec.provider,
                })
                .await;
            let started = match (started, invocation_kind) {
                (Ok(()), RuntimeInvocationKind::ManualCompaction) => {
                    sink.emit(TurnEvent::CompactionStarted {
                        trigger: CompactionTrigger::Manual,
                    })
                    .await
                }
                (result, _) => result,
            };
            let mut result = match started {
                Ok(()) => entry
                    .executor
                    .execute(request, &sink, interactions.as_deref())
                    .await
                    .map_err(|error| {
                        runtime_error_to_failure(
                            error,
                            entry.spec.runtime_id.clone(),
                            task_invocation_id.clone(),
                        )
                    }),
                Err(error) => Err(runtime_error_to_failure(
                    error,
                    entry.spec.runtime_id.clone(),
                    task_invocation_id.clone(),
                )),
            };
            let terminal_event = match &result {
                Ok(turn) => RuntimeEvent::InvocationCompleted {
                    result: turn.clone(),
                },
                Err(failure) => RuntimeEvent::InvocationFailed {
                    failure: failure.clone(),
                },
            };
            if let Err(error) = sink.emit_runtime(terminal_event).await {
                if result.is_ok() {
                    result = Err(runtime_error_to_failure(
                        error,
                        entry.spec.runtime_id.clone(),
                        task_invocation_id.clone(),
                    ));
                }
            }
            let completion_kind = if result
                .as_ref()
                .is_err_and(|failure| failure.kind == RuntimeFailureKind::Cancelled)
            {
                CompletionKind::Cancelled
            } else {
                CompletionKind::Finished
            };
            {
                let mut state = entry.state.lock().await;
                if state
                    .active
                    .as_ref()
                    .is_some_and(|active| active.invocation_id == task_invocation_id)
                {
                    state.active = None;
                }
                if let Ok(turn) = &result {
                    if let Some(session_id) = &turn.session_id {
                        state.provider_session_id = Some(session_id.clone());
                    }
                    state.detail = None;
                } else if let Err(failure) = &result {
                    state.detail = Some(failure.message.clone());
                }
                state.status = if entry.disposed.load(Ordering::Acquire) {
                    RuntimeStatus::Stopped
                } else if result.as_ref().is_err_and(|failure| {
                    matches!(
                        failure.kind,
                        RuntimeFailureKind::Network
                            | RuntimeFailureKind::Transport
                            | RuntimeFailureKind::ProviderProcess
                            | RuntimeFailureKind::Indeterminate
                    )
                }) {
                    RuntimeStatus::Degraded
                } else {
                    RuntimeStatus::Ready
                };
            }
            task_terminal.complete(completion_kind);
            let _ = completion_sender.send(result);
        });

        let interrupt = TurnInterruptHandle::from_backend(Arc::new(InProcessTurnInterrupt {
            cancellation,
            terminal,
            disposed: Arc::clone(&self.disposed),
        }));
        Ok(TurnHandle::from_channels(
            runtime_id,
            invocation_id,
            event_receiver,
            completion_receiver,
            interrupt,
        ))
    }

    fn build_request(
        &self,
        input: &TurnInput,
        provider_session_id: Option<String>,
        cancellation: CancellationToken,
    ) -> TurnRequest {
        let mut environment = self.spec.environment.clone();
        environment.extend(input.environment.clone());
        let native_images = self
            .executor
            .capabilities(self.spec.provider)
            .native_image_attachments;
        TurnRequest {
            provider: self.spec.provider,
            working_directory: self.spec.working_directory.clone(),
            prompt: render_prompt(input, native_images),
            provenance: input.provenance.clone(),
            model: input.model.clone().or_else(|| self.spec.model.clone()),
            reasoning: input
                .reasoning
                .clone()
                .or_else(|| self.spec.reasoning.clone()),
            permission_mode: input
                .permission_mode
                .clone()
                .unwrap_or_else(|| self.spec.permission_mode.clone()),
            harness_options: input
                .harness_options
                .clone()
                .unwrap_or_else(|| self.spec.harness_options.clone()),
            launch_context: input
                .launch_context
                .clone()
                .unwrap_or_else(|| self.spec.launch_context.clone()),
            auto_compaction: input.auto_compaction.unwrap_or(self.spec.auto_compaction),
            session_id: provider_session_id,
            max_turns: input.max_turns,
            timeout: input.timeout.unwrap_or(self.spec.turn_timeout),
            interaction_timeout: input
                .interaction_timeout
                .unwrap_or(self.spec.interaction_timeout),
            tool_process_policy: input
                .tool_process_policy
                .unwrap_or(self.spec.tool_process_policy),
            environment,
            attachments: input.attachments.clone(),
            cancellation,
            sandbox: self.spec.sandbox.clone(),
            required_sandbox_capabilities: self.spec.required_sandbox_capabilities,
        }
    }

    fn validate_input(
        &self,
        input: &TurnInput,
        has_interaction_handler: bool,
    ) -> RetainedRuntimeResult<()> {
        if input.invocation_kind == RuntimeInvocationKind::ManualCompaction {
            if !input.attachments.is_empty() {
                return Err(self.invalid_input(
                    &input.invocation_id,
                    "manual compaction cannot include attachments",
                ));
            }
            if input.prompt.len() > MAX_COMPACTION_INSTRUCTIONS_BYTES + "/compact ".len()
                || input.prompt.contains('\0')
            {
                return Err(self.invalid_input(
                    &input.invocation_id,
                    format!(
                        "manual compaction instructions must be at most {MAX_COMPACTION_INSTRUCTIONS_BYTES} bytes and cannot contain NUL"
                    ),
                ));
            }
        }
        if input.auto_compaction.is_some()
            && !self
                .executor
                .capabilities(self.spec.provider)
                .configurable_auto_compaction
        {
            return Err(self.invalid_input(
                &input.invocation_id,
                format!(
                    "{} does not support configurable automatic compaction",
                    self.spec.provider
                ),
            ));
        }
        if let Some(policy) = input.auto_compaction {
            validate_auto_compaction_policy(policy)
                .map_err(|message| self.invalid_input(&input.invocation_id, message))?;
        }
        if input.attachments.len() > MAX_ATTACHMENTS {
            return Err(self.invalid_input(
                &input.invocation_id,
                format!("a turn may reference at most {MAX_ATTACHMENTS} attachments"),
            ));
        }
        if input.interaction_policy == InteractionPolicy::RequireHandler && !has_interaction_handler
        {
            return Err(self.invalid_input(
                &input.invocation_id,
                "this turn requires an interaction handler for approvals and questions",
            ));
        }
        for attachment in &input.attachments {
            let Some(path) = attachment.path.to_str() else {
                return Err(self.invalid_input(
                    &input.invocation_id,
                    "attachment paths must be valid UTF-8 for provider prompt delivery",
                ));
            };
            if path.is_empty() || path.contains(['\0', '\n', '\r']) {
                return Err(self.invalid_input(
                    &input.invocation_id,
                    "attachment paths must be non-empty and cannot contain NUL or newlines",
                ));
            }
            for (field, value) in [
                ("display name", attachment.display_name.as_deref()),
                ("media type", attachment.media_type.as_deref()),
            ] {
                if let Some(value) = value {
                    if value.len() > MAX_ATTACHMENT_METADATA_BYTES
                        || value.contains(['\0', '\n', '\r'])
                    {
                        return Err(self.invalid_input(
                            &input.invocation_id,
                            format!(
                                "attachment {field} must be at most {MAX_ATTACHMENT_METADATA_BYTES} bytes and cannot contain NUL or newlines"
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn invalid_input(
        &self,
        invocation_id: &InvocationId,
        message: impl Into<String>,
    ) -> RuntimeFailure {
        lifecycle_failure(
            Some(self.spec.runtime_id.clone()),
            Some(invocation_id.clone()),
            RuntimeFailureKind::InvalidRequest,
            RetryAdvice::Never,
            DeliveryState::NotSent,
            message,
        )
    }
}

fn validate_auto_compaction_policy(
    policy: AutoCompactionPolicy,
) -> std::result::Result<(), &'static str> {
    if let AutoCompactionPolicy::TokenThreshold { tokens } = policy {
        if !(100_000..=1_000_000).contains(&tokens) {
            return Err("automatic compaction threshold must be between 100000 and 1000000 tokens");
        }
    }
    Ok(())
}

#[async_trait]
impl RuntimeHandleBackend for RuntimeEntry {
    fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact {
        self.executor.configuration_impact(key)
    }

    async fn health(&self) -> RuntimeHealth {
        RuntimeEntry::health(self).await
    }

    async fn start_turn(
        self: Arc<Self>,
        input: TurnInput,
        interactions: Option<Arc<dyn InteractionHandler>>,
    ) -> RetainedRuntimeResult<TurnHandle> {
        RuntimeEntry::start_turn(&self, input, interactions).await
    }
}

/// Describe attachments the selected provider cannot read natively.
///
/// A provider that accepts native image inputs receives those files through
/// [`TurnRequest::attachments`] instead, so repeating their host paths here
/// would only spend context on a path the model does not need.
fn render_prompt(input: &TurnInput, native_images: bool) -> String {
    let described = input
        .attachments
        .iter()
        .filter(|attachment| !(native_images && is_image(attachment)))
        .collect::<Vec<_>>();
    if described.is_empty() {
        return input.prompt.clone();
    }
    let mut prompt = String::with_capacity(input.prompt.len() + described.len() * 80);
    prompt.push_str(&input.prompt);
    prompt.push_str("\n\nFiles attached to this request and available on the execution host:\n");
    for attachment in described {
        prompt.push_str("- path: ");
        prompt.push_str(attachment.path.to_string_lossy().as_ref());
        if let Some(display_name) = &attachment.display_name {
            prompt.push_str("; name: ");
            prompt.push_str(display_name);
        }
        if let Some(media_type) = &attachment.media_type {
            prompt.push_str("; media type: ");
            prompt.push_str(media_type);
        }
        prompt.push('\n');
    }
    prompt
}

/// Whether an attachment declares an image media type.
///
/// Only an explicit `image/*` media type counts: guessing from a file
/// extension would hand a provider a file it cannot decode.
pub(crate) fn is_image(attachment: &TurnAttachment) -> bool {
    attachment
        .media_type
        .as_deref()
        .is_some_and(|media_type| media_type.starts_with("image/"))
}

struct ChannelEventSink {
    provider: Provider,
    runtime_id: RuntimeId,
    invocation_id: InvocationId,
    sequence: AtomicU64,
    sender: mpsc::Sender<EventEnvelope>,
}

#[async_trait]
impl EventSink for ChannelEventSink {
    async fn emit(&self, event: TurnEvent) -> crate::Result<()> {
        self.emit_runtime(RuntimeEvent::ProviderEvent { event })
            .await
    }
}

impl ChannelEventSink {
    async fn emit_runtime(&self, event: RuntimeEvent) -> crate::Result<()> {
        let envelope = EventEnvelope {
            schema_version: 1,
            runtime_id: self.runtime_id.clone(),
            invocation_id: self.invocation_id.clone(),
            sequence: self.sequence.fetch_add(1, Ordering::AcqRel) + 1,
            observed_at_unix_ms: unix_time_ms(),
            event,
        };
        self.sender
            .send(envelope)
            .await
            .map_err(|_| RuntimeError::EventSink {
                provider: self.provider,
                message: "the retained invocation event receiver was dropped".to_owned(),
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionKind {
    Finished,
    Cancelled,
}

struct CompletionSignal {
    state: AtomicU8,
    notify: Notify,
}

impl CompletionSignal {
    const RUNNING: u8 = 0;
    const FINISHED: u8 = 1;
    const CANCELLED: u8 = 2;

    fn new() -> Self {
        Self {
            state: AtomicU8::new(Self::RUNNING),
            notify: Notify::new(),
        }
    }

    fn complete(&self, kind: CompletionKind) {
        self.state.store(
            match kind {
                CompletionKind::Finished => Self::FINISHED,
                CompletionKind::Cancelled => Self::CANCELLED,
            },
            Ordering::Release,
        );
        self.notify.notify_waiters();
    }

    async fn wait_for_completion(&self) -> CompletionKind {
        loop {
            let notified = self.notify.notified();
            match self.state.load(Ordering::Acquire) {
                Self::FINISHED => return CompletionKind::Finished,
                Self::CANCELLED => return CompletionKind::Cancelled,
                _ => notified.await,
            }
        }
    }
}

fn unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn lifecycle_failure(
    runtime_id: Option<RuntimeId>,
    invocation_id: Option<InvocationId>,
    kind: RuntimeFailureKind,
    retry: RetryAdvice,
    delivery: DeliveryState,
    message: impl Into<String>,
) -> RuntimeFailure {
    RuntimeFailure {
        runtime_id,
        invocation_id,
        kind,
        retry,
        delivery,
        message: message.into(),
        provider_code: None,
    }
}

fn runtime_error_to_failure(
    error: RuntimeError,
    runtime_id: RuntimeId,
    invocation_id: InvocationId,
) -> RuntimeFailure {
    let message = error.to_string();
    let (kind, retry, delivery, provider_code) = match error {
        RuntimeError::InvalidRequest { .. } => (
            RuntimeFailureKind::InvalidRequest,
            RetryAdvice::Never,
            DeliveryState::NotSent,
            None,
        ),
        RuntimeError::AdapterUnavailable { .. }
        | RuntimeError::ExecutableNotFound { .. }
        | RuntimeError::TransportCapabilityUnavailable { .. } => (
            RuntimeFailureKind::CapabilityUnavailable,
            RetryAdvice::RequiresUserAction,
            DeliveryState::NotSent,
            None,
        ),
        RuntimeError::Spawn { .. } => (
            RuntimeFailureKind::ProviderProcess,
            RetryAdvice::Immediate,
            DeliveryState::NotSent,
            None,
        ),
        RuntimeError::ProcessIo { .. } => (
            RuntimeFailureKind::Transport,
            RetryAdvice::Immediate,
            DeliveryState::PossiblySent,
            None,
        ),
        RuntimeError::Transport { source, .. } => transport_failure_contract(source),
        RuntimeError::Protocol { .. } => (
            RuntimeFailureKind::ProviderProtocol,
            RetryAdvice::Never,
            DeliveryState::Accepted,
            None,
        ),
        RuntimeError::EventSink { .. } => (
            RuntimeFailureKind::Indeterminate,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            None,
        ),
        RuntimeError::Timeout { .. } => (
            RuntimeFailureKind::Timeout,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            None,
        ),
        RuntimeError::Cancelled { .. } => (
            RuntimeFailureKind::Cancelled,
            RetryAdvice::Never,
            DeliveryState::Accepted,
            None,
        ),
        RuntimeError::ProcessFailed {
            kind,
            delivery,
            provider_code,
            ..
        } => {
            let (failure_kind, retry, _, fallback_code) = process_failure_contract(kind);
            (
                failure_kind,
                retry,
                delivery,
                provider_code.or(fallback_code),
            )
        }
        RuntimeError::Sandbox(_) => (
            RuntimeFailureKind::Permission,
            RetryAdvice::RequiresUserAction,
            DeliveryState::PossiblySent,
            None,
        ),
    };
    RuntimeFailure {
        runtime_id: Some(runtime_id),
        invocation_id: Some(invocation_id),
        kind,
        retry,
        delivery,
        message,
        provider_code,
    }
}

fn transport_failure_contract(
    error: crate::TransportError,
) -> (
    RuntimeFailureKind,
    RetryAdvice,
    DeliveryState,
    Option<String>,
) {
    let provider_code = Some(format!("transport::{:?}", error.kind).to_lowercase());
    match error.kind {
        TransportErrorKind::AuthenticationFailed
        | TransportErrorKind::HostKeyVerificationFailed => (
            RuntimeFailureKind::Authentication,
            RetryAdvice::RequiresUserAction,
            DeliveryState::NotSent,
            provider_code,
        ),
        TransportErrorKind::PermissionDenied => (
            RuntimeFailureKind::Permission,
            RetryAdvice::RequiresUserAction,
            DeliveryState::NotSent,
            provider_code,
        ),
        TransportErrorKind::InvalidConfiguration
        | TransportErrorKind::ExecutableNotFound
        | TransportErrorKind::WorkingDirectoryNotFound
        | TransportErrorKind::Unsupported => (
            RuntimeFailureKind::CapabilityUnavailable,
            RetryAdvice::RequiresUserAction,
            DeliveryState::NotSent,
            provider_code,
        ),
        TransportErrorKind::SpawnFailed => (
            RuntimeFailureKind::ProviderProcess,
            if error.retryable {
                RetryAdvice::Immediate
            } else {
                RetryAdvice::RequiresUserAction
            },
            DeliveryState::NotSent,
            provider_code,
        ),
        TransportErrorKind::NameResolutionFailed
        | TransportErrorKind::ConnectionRefused
        | TransportErrorKind::ConnectionTimedOut
        | TransportErrorKind::RemoteUnavailable => (
            RuntimeFailureKind::Network,
            RetryAdvice::ReconnectAndAttach,
            DeliveryState::PossiblySent,
            provider_code,
        ),
        TransportErrorKind::StreamFailed | TransportErrorKind::ProcessControlFailed => (
            RuntimeFailureKind::Transport,
            RetryAdvice::ReconnectAndAttach,
            DeliveryState::PossiblySent,
            provider_code,
        ),
        TransportErrorKind::Protocol => (
            RuntimeFailureKind::ProviderProtocol,
            RetryAdvice::Never,
            DeliveryState::PossiblySent,
            provider_code,
        ),
    }
}

fn process_failure_contract(
    kind: ProviderProcessErrorKind,
) -> (
    RuntimeFailureKind,
    RetryAdvice,
    DeliveryState,
    Option<String>,
) {
    let provider_code = Some(format!("process::{kind:?}").to_lowercase());
    match kind {
        ProviderProcessErrorKind::AuthenticationFailed => (
            RuntimeFailureKind::Authentication,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            provider_code,
        ),
        ProviderProcessErrorKind::PermissionDenied => (
            RuntimeFailureKind::Permission,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            provider_code,
        ),
        ProviderProcessErrorKind::ModelUnavailable => (
            RuntimeFailureKind::CapabilityUnavailable,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            provider_code,
        ),
        ProviderProcessErrorKind::RateLimited => (
            RuntimeFailureKind::RateLimited,
            RetryAdvice::After {
                milliseconds: 1_000,
            },
            DeliveryState::Accepted,
            provider_code,
        ),
        ProviderProcessErrorKind::Network => (
            RuntimeFailureKind::Network,
            RetryAdvice::ReconnectAndAttach,
            DeliveryState::Accepted,
            provider_code,
        ),
        ProviderProcessErrorKind::Unknown => (
            RuntimeFailureKind::ProviderProcess,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            provider_code,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    #[cfg(all(feature = "codex", unix))]
    use crate::McpServerConfig;
    use crate::{RunStatus, Usage};

    #[cfg(all(feature = "codex", unix))]
    #[tokio::test]
    async fn acquired_codex_runtime_accepts_temps_relay_turn() {
        let directory = tempfile::tempdir().unwrap();
        let mut builder = AgentRuntime::builder();
        builder.register(crate::providers::Codex::with_executable("/bin/false"));
        let client = InProcessRuntimeClient::new(builder.build().unwrap());
        let runtime_id = RuntimeId::new("temps-codex-relay-runtime").unwrap();
        let handle = client
            .acquire(RuntimeSpec::new(
                runtime_id.clone(),
                Provider::Codex,
                directory.path(),
            ))
            .await
            .unwrap();
        let mut input =
            TurnInput::new(InvocationId::new("temps-codex-relay-turn").unwrap(), "test");
        input.environment.insert(
            "TEMPS_MODEL_RELAY_TOKEN".into(),
            SecretString::new("test-secret"),
        );
        input.environment.insert(
            "TEMPS_CHAT_MCP_AUTHORIZATION".into(),
            SecretString::new("Bearer test-secret"),
        );
        input.harness_options = Some(BTreeMap::from([(
            "model_relay".into(),
            r#"{"base_url":"http://127.0.0.1:8000/v1","token_env":"TEMPS_MODEL_RELAY_TOKEN"}"#
                .into(),
        )]));
        let mut context = LaunchContext::default();
        context.mcp_servers.insert(
            "temps-chat".into(),
            McpServerConfig::Http {
                url: "http://127.0.0.1:8000/mcp".into(),
                headers_from: BTreeMap::from([(
                    "Authorization".into(),
                    "TEMPS_CHAT_MCP_AUTHORIZATION".into(),
                )]),
            },
        );
        input.launch_context = Some(context);
        let turn = handle.start_turn(input).await.unwrap();
        let failure = turn.wait().await.unwrap_err();
        assert_ne!(failure.kind, RuntimeFailureKind::InvalidRequest);
        client.dispose(&runtime_id).await.unwrap();
    }

    #[cfg(all(feature = "codex", unix))]
    #[tokio::test]
    async fn acquired_codex_runtime_rejects_invalid_temps_relay_descriptors() {
        let directory = tempfile::tempdir().unwrap();
        let mut builder = AgentRuntime::builder();
        builder.register(crate::providers::Codex::with_executable("/bin/false"));
        let client = InProcessRuntimeClient::new(builder.build().unwrap());
        let runtime_id = RuntimeId::new("temps-codex-invalid-relay-runtime").unwrap();
        let handle = client
            .acquire(RuntimeSpec::new(
                runtime_id.clone(),
                Provider::Codex,
                directory.path(),
            ))
            .await
            .unwrap();
        for (index, descriptor) in [
            r#"{"base_url":"https://user:pass@example.test/v1","token_env":"TEMPS_MODEL_RELAY_TOKEN"}"#,
            r#"{"base_url":"https://example.test/v1","token_env":"MISSING"}"#,
            r#"{"base_url":"https://example.test/v1","token_env":"TEMPS_MODEL_RELAY_TOKEN","extra":"unsafe"}"#,
        ].into_iter().enumerate() {
            let mut input = TurnInput::new(
                InvocationId::new(format!("invalid-relay-turn-{index}")).unwrap(),
                "test",
            );
            input.environment.insert(
                "TEMPS_MODEL_RELAY_TOKEN".into(),
                SecretString::new("test-secret"),
            );
            input.harness_options = Some(BTreeMap::from([("model_relay".into(), descriptor.into())]));
            let failure = handle.start_turn(input).await.unwrap().wait().await.unwrap_err();
            assert_eq!(failure.kind, RuntimeFailureKind::InvalidRequest);
        }
        client.dispose(&runtime_id).await.unwrap();
    }

    #[test]
    fn retained_failure_preserves_native_code_and_delivery_state() {
        let failure = runtime_error_to_failure(
            RuntimeError::ProcessFailed {
                provider: Provider::Claude,
                kind: ProviderProcessErrorKind::RateLimited,
                exit_code: Some(0),
                stderr: "usage limit reached".into(),
                provider_code: Some("claude::rate_limit_error".into()),
                delivery: DeliveryState::Accepted,
            },
            RuntimeId::new("runtime-native-failure").unwrap(),
            InvocationId::new("invocation-native-failure").unwrap(),
        );

        assert_eq!(failure.kind, RuntimeFailureKind::RateLimited);
        assert_eq!(failure.delivery, DeliveryState::Accepted);
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("claude::rate_limit_error")
        );
        assert_eq!(
            failure.retry,
            RetryAdvice::After {
                milliseconds: 1_000
            }
        );
    }

    struct RecordingExecutor {
        requests: StdMutex<Vec<TurnRequest>>,
    }

    impl RecordingExecutor {
        fn new() -> Self {
            Self {
                requests: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RuntimeTurnExecutor for RecordingExecutor {
        fn capabilities(&self, _provider: Provider) -> RuntimeDriverCapabilities {
            RuntimeDriverCapabilities {
                retained_process: true,
                session_resume: true,
                live_interactions: true,
                configurable_auto_compaction: true,
                manual_compaction: true,
                context_window_usage: true,
                native_image_attachments: true,
            }
        }

        fn configuration_impact(&self, _key: RuntimeConfigurationKey) -> ConfigurationImpact {
            ConfigurationImpact::Live
        }

        async fn execute(
            &self,
            request: TurnRequest,
            events: &dyn EventSink,
            _interactions: Option<&dyn InteractionHandler>,
        ) -> crate::Result<TurnResult> {
            let session_number = self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                + 1;
            let session_id = request
                .session_id
                .clone()
                .unwrap_or_else(|| format!("session-{session_number}"));
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            events
                .emit(TurnEvent::TextDelta {
                    text: "ok".to_owned(),
                })
                .await?;
            Ok(TurnResult {
                status: RunStatus::Succeeded,
                text: "ok".to_owned(),
                reasoning: None,
                session_id: Some(session_id),
                session_title: None,
                model: None,
                usage: Usage::default(),
            })
        }
    }

    struct BlockingExecutor;

    #[async_trait]
    impl RuntimeTurnExecutor for BlockingExecutor {
        fn capabilities(&self, _provider: Provider) -> RuntimeDriverCapabilities {
            RuntimeDriverCapabilities {
                retained_process: false,
                session_resume: true,
                live_interactions: false,
                ..RuntimeDriverCapabilities::default()
            }
        }

        fn configuration_impact(&self, _key: RuntimeConfigurationKey) -> ConfigurationImpact {
            ConfigurationImpact::ReacquireRequired
        }

        async fn execute(
            &self,
            request: TurnRequest,
            _events: &dyn EventSink,
            _interactions: Option<&dyn InteractionHandler>,
        ) -> crate::Result<TurnResult> {
            request.cancellation.cancelled().await;
            Err(RuntimeError::Cancelled {
                provider: request.provider,
            })
        }
    }

    fn runtime_spec(id: &str) -> RuntimeSpec {
        RuntimeSpec::new(
            RuntimeId::new(id).expect("valid runtime identifier"),
            Provider::Claude,
            ".",
        )
    }

    fn turn_input(id: &str, prompt: &str) -> TurnInput {
        TurnInput::new(
            InvocationId::new(id).expect("valid invocation identifier"),
            prompt,
        )
    }

    #[tokio::test]
    async fn retained_runtime_capacity_is_released_by_disposal() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor_with_limits(
            executor,
            RetainedRuntimeLimits { max_runtimes: 1 },
        )
        .expect("valid limits");
        client
            .acquire(runtime_spec("runtime-one"))
            .await
            .expect("first runtime");

        let failure = client
            .acquire(runtime_spec("runtime-two"))
            .await
            .expect_err("capacity must be enforced");

        assert_eq!(failure.kind, RuntimeFailureKind::RuntimeBusy);
        assert_eq!(failure.delivery, DeliveryState::NotSent);
        client
            .dispose(&RuntimeId::new("runtime-one").unwrap())
            .await
            .expect("dispose first runtime");
        client
            .acquire(runtime_spec("runtime-two"))
            .await
            .expect("capacity was released");
    }

    #[tokio::test]
    async fn acquire_stream_and_attach_preserve_provider_session() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let handle = client
            .acquire(runtime_spec("runtime-1"))
            .await
            .expect("acquire runtime");
        assert!(handle.driver_capabilities().session_resume);
        assert_eq!(
            handle.configuration_impact(RuntimeConfigurationKey::Model),
            ConfigurationImpact::Live
        );
        let mut first = handle
            .start_turn(turn_input("turn-1", "hello"))
            .await
            .expect("start first turn");
        let started = first.next_event().await.expect("started event");
        assert_eq!(started.sequence, 1);
        assert_eq!(started.runtime_id.as_str(), "runtime-1");
        assert!(matches!(
            started.event,
            RuntimeEvent::InvocationStarted {
                provider: Provider::Claude
            }
        ));
        let provider_event = first.next_event().await.expect("provider event");
        assert_eq!(provider_event.sequence, 2);
        assert!(matches!(
            provider_event.event,
            RuntimeEvent::ProviderEvent {
                event: TurnEvent::TextDelta { .. }
            }
        ));
        assert_eq!(first.wait().await.expect("first result").text, "ok");

        let attached = client
            .attach(&RuntimeId::new("runtime-1").expect("valid runtime identifier"))
            .await
            .expect("attach runtime");
        attached
            .start_turn(turn_input("turn-2", "continue"))
            .await
            .expect("start second turn")
            .wait()
            .await
            .expect("second result");

        let requests = executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].session_id, None);
        assert_eq!(requests[1].session_id.as_deref(), Some("session-1"));
    }

    #[tokio::test]
    async fn turn_launch_context_replaces_the_runtime_default() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let mut spec = runtime_spec("runtime-launch-context");
        spec.launch_context.system_prompt_append = Some("runtime default".into());
        let handle = client.acquire(spec).await.expect("acquire runtime");

        handle
            .start_turn(turn_input("turn-default", "inherit"))
            .await
            .expect("start inherited turn")
            .wait()
            .await
            .expect("inherited turn result");
        let mut overridden = turn_input("turn-override", "override");
        overridden.launch_context = Some(LaunchContext {
            system_prompt_append: Some("turn override".into()),
            allowed_tools: Some(vec!["Read".into()]),
            ..LaunchContext::default()
        });
        handle
            .start_turn(overridden)
            .await
            .expect("start overridden turn")
            .wait()
            .await
            .expect("overridden turn result");

        let requests = executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            requests[0].launch_context.system_prompt_append.as_deref(),
            Some("runtime default")
        );
        assert_eq!(
            requests[1].launch_context.system_prompt_append.as_deref(),
            Some("turn override")
        );
        assert_eq!(
            requests[1].launch_context.allowed_tools.as_deref(),
            Some(["Read".to_string()].as_slice())
        );
    }

    #[tokio::test]
    async fn runtime_rejects_parallel_turns_and_confirms_interruption() {
        let client = InProcessRuntimeClient::from_executor(Arc::new(BlockingExecutor));
        let handle = client
            .acquire(runtime_spec("runtime-busy"))
            .await
            .expect("acquire runtime");
        let first = handle
            .start_turn(turn_input("turn-active", "wait"))
            .await
            .expect("start active turn");
        let error = handle
            .start_turn(turn_input("turn-rejected", "overlap"))
            .await
            .expect_err("parallel turn must fail");
        assert_eq!(error.kind, RuntimeFailureKind::RuntimeBusy);
        assert_eq!(error.delivery, DeliveryState::NotSent);
        assert_eq!(first.interrupt().await, InterruptOutcome::Interrupted);
        let result = first.wait().await.expect_err("interrupted turn must fail");
        assert_eq!(result.kind, RuntimeFailureKind::Cancelled);
    }

    #[tokio::test]
    async fn dispose_cancels_active_turn_and_prevents_reattach() {
        let client = InProcessRuntimeClient::from_executor(Arc::new(BlockingExecutor));
        let runtime_id = RuntimeId::new("runtime-dispose").expect("valid runtime identifier");
        let handle = client
            .acquire(runtime_spec(runtime_id.as_str()))
            .await
            .expect("acquire runtime");
        let turn = handle
            .start_turn(turn_input("turn-dispose", "wait"))
            .await
            .expect("start turn");
        assert_eq!(
            client.dispose(&runtime_id).await.expect("dispose runtime"),
            DisposeOutcome::Disposed
        );
        assert_eq!(turn.interrupt().await, InterruptOutcome::RuntimeDisposed);
        let result = turn.wait().await.expect_err("disposed turn must fail");
        assert_eq!(result.kind, RuntimeFailureKind::Cancelled);
        let attach = client
            .attach(&runtime_id)
            .await
            .expect_err("disposed runtime must be removed");
        assert_eq!(attach.kind, RuntimeFailureKind::RuntimeNotFound);
        assert_eq!(handle.health().await.status, RuntimeStatus::Stopped);
        let after_dispose = handle
            .start_turn(turn_input("turn-after-dispose", "must not run"))
            .await
            .expect_err("disposed handle must not admit another invocation");
        assert_eq!(after_dispose.kind, RuntimeFailureKind::RuntimeDisposed);
    }

    #[tokio::test]
    async fn accepted_invocation_identifiers_cannot_be_replayed() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let handle = client
            .acquire(runtime_spec("runtime-deduplicate"))
            .await
            .expect("acquire runtime");
        handle
            .start_turn(turn_input("turn-once", "do this once"))
            .await
            .expect("start first turn")
            .wait()
            .await
            .expect("first result");

        let duplicate = handle
            .start_turn(turn_input("turn-once", "do this twice"))
            .await
            .expect_err("duplicate invocation must fail");
        assert_eq!(duplicate.kind, RuntimeFailureKind::InvocationAlreadyExists);
        assert_eq!(duplicate.delivery, DeliveryState::Accepted);
        assert_eq!(
            executor
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn attachment_references_are_validated_and_delivered_as_provider_context() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let handle = client
            .acquire(runtime_spec("runtime-attachments"))
            .await
            .expect("acquire runtime");
        let mut input = turn_input("turn-attachment", "Summarize the report");
        input.attachments.push(TurnAttachment {
            path: PathBuf::from("uploads/report.pdf"),
            display_name: Some("Quarterly report".to_owned()),
            media_type: Some("application/pdf".to_owned()),
        });
        handle
            .start_turn(input)
            .await
            .expect("start attachment turn")
            .wait()
            .await
            .expect("attachment result");

        let requests = executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(requests[0].prompt.contains("Summarize the report"));
        assert!(requests[0].prompt.contains("uploads/report.pdf"));
        assert!(requests[0].prompt.contains("Quarterly report"));
        assert!(requests[0].prompt.contains("application/pdf"));
        assert_eq!(requests[0].attachments.len(), 1);
    }

    #[tokio::test]
    async fn an_image_attachment_reaches_a_native_provider_without_prompt_path_text() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let handle = client
            .acquire(runtime_spec("runtime-image-attachments"))
            .await
            .expect("acquire runtime");
        let mut input = turn_input("turn-image", "What is in this screenshot?");
        input.attachments.push(TurnAttachment {
            path: PathBuf::from("uploads/screenshot.png"),
            display_name: Some("Screenshot".to_owned()),
            media_type: Some("image/png".to_owned()),
        });
        input.attachments.push(TurnAttachment {
            path: PathBuf::from("uploads/report.pdf"),
            display_name: None,
            media_type: Some("application/pdf".to_owned()),
        });
        handle
            .start_turn(input)
            .await
            .expect("start image turn")
            .wait()
            .await
            .expect("image result");

        let requests = executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!requests[0].prompt.contains("screenshot.png"));
        assert!(requests[0].prompt.contains("report.pdf"));
        assert_eq!(requests[0].attachments.len(), 2);
    }

    #[tokio::test]
    async fn manual_compaction_is_a_durable_invocation_with_ordered_events() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let mut spec = runtime_spec("runtime-compact");
        spec.provider_session_id = Some("session-existing".to_owned());
        let handle = client.acquire(spec).await.expect("acquire runtime");
        let mut input = CompactionInput::new(
            InvocationId::new("compact-1").expect("valid invocation identifier"),
        );
        input.instructions = Some("Preserve open decisions".to_owned());
        let turn = handle.compact(input).await.expect("start compaction");
        let (mut events, completion) = turn.into_parts();
        let mut observed = Vec::new();
        while let Some(event) = events.next().await {
            observed.push(event.event);
        }
        completion.wait().await.expect("compaction result");

        assert!(matches!(
            observed.as_slice(),
            [
                RuntimeEvent::InvocationStarted { .. },
                RuntimeEvent::ProviderEvent {
                    event: TurnEvent::CompactionStarted {
                        trigger: CompactionTrigger::Manual
                    }
                },
                RuntimeEvent::ProviderEvent {
                    event: TurnEvent::TextDelta { .. }
                },
                RuntimeEvent::InvocationCompleted { .. }
            ]
        ));
        let requests = executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests[0].prompt, "/compact Preserve open decisions");
        assert_eq!(requests[0].session_id.as_deref(), Some("session-existing"));
    }

    #[tokio::test]
    async fn manual_compaction_requires_an_existing_provider_session() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let handle = client
            .acquire(runtime_spec("runtime-compact-empty"))
            .await
            .expect("acquire runtime");

        let failure = handle
            .compact(CompactionInput::new(
                InvocationId::new("compact-empty").expect("valid invocation identifier"),
            ))
            .await
            .expect_err("sessionless compaction must fail");
        assert_eq!(failure.kind, RuntimeFailureKind::InvalidRequest);
        assert_eq!(failure.delivery, DeliveryState::NotSent);
        assert!(executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[tokio::test]
    async fn required_interactions_fail_before_delivery_without_a_handler() {
        let executor = Arc::new(RecordingExecutor::new());
        let client = InProcessRuntimeClient::from_executor(executor.clone());
        let handle = client
            .acquire(runtime_spec("runtime-interactions"))
            .await
            .expect("acquire runtime");
        let mut input = turn_input("turn-interactions", "ask before writing");
        input.interaction_policy = InteractionPolicy::RequireHandler;

        let failure = handle
            .start_turn(input)
            .await
            .expect_err("missing interaction handler must fail");
        assert_eq!(failure.kind, RuntimeFailureKind::InvalidRequest);
        assert_eq!(failure.delivery, DeliveryState::NotSent);
        assert!(executor
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }
}
