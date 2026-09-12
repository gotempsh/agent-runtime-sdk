//! Transport-aware provider harness discovery.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    AccountUsageSnapshot, PermissionSupport, Provider, ProviderReadiness, SecretString,
    TransportCapabilities, TransportError,
};

/// Explicit execution context for metadata and account probes.
///
/// The working directory and environment are interpreted inside the runtime's
/// configured transport. This lets local, SSH, container, and hosted targets
/// discover the same harness identity that will execute a turn without making
/// the SDK aware of an application's credential store.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderProbeContext {
    /// Directory from which provider configuration and project metadata are resolved.
    pub working_directory: PathBuf,
    /// Explicit secret environment injected only into provider probe processes.
    pub environment: BTreeMap<String, SecretString>,
}

/// Provider-native authentication evidence for one execution target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessAuthenticationStatus {
    /// The adapter cannot prove whether an actual provider request can authenticate.
    Unknown,
    /// The provider confirmed a usable authenticated identity.
    Authenticated,
    /// No provider identity is configured on this target.
    Required,
    /// Configured credentials were present but expired or were rejected.
    Rejected,
    /// Authentication could not be inspected because of a temporary or protocol failure.
    Unavailable,
}

/// Authentication readiness kept separate from executable and model discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessAuthentication {
    /// Evidence state reported by the provider-native probe.
    pub status: HarnessAuthenticationStatus,
    /// Stable provider-native source, such as `auth_status` or `login_status`.
    pub source: String,
    /// Bounded, redacted explanation when the status needs user action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether repeating the probe without changing credentials may help.
    pub retryable: bool,
}

impl HarnessAuthentication {
    /// No reliable provider-native authentication probe is available.
    pub fn unknown(source: impl Into<String>) -> Self {
        Self {
            status: HarnessAuthenticationStatus::Unknown,
            source: source.into(),
            reason: None,
            retryable: false,
        }
    }

    /// The provider confirmed a usable identity.
    pub fn authenticated(source: impl Into<String>) -> Self {
        Self {
            status: HarnessAuthenticationStatus::Authenticated,
            source: source.into(),
            reason: None,
            retryable: false,
        }
    }

    /// No identity is configured on the execution target.
    pub fn required(source: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            status: HarnessAuthenticationStatus::Required,
            source: source.into(),
            reason: Some(reason.into()),
            retryable: false,
        }
    }

    /// Configured credentials were rejected by the provider.
    pub fn rejected(source: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            status: HarnessAuthenticationStatus::Rejected,
            source: source.into(),
            reason: Some(reason.into()),
            retryable: false,
        }
    }

    /// The authentication probe did not produce reliable evidence.
    pub fn unavailable(
        source: impl Into<String>,
        reason: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status: HarnessAuthenticationStatus::Unavailable,
            source: source.into(),
            reason: Some(reason.into()),
            retryable,
        }
    }
}

impl ProviderProbeContext {
    /// Probe provider metadata from an execution-target-local directory.
    pub fn new(working_directory: impl Into<PathBuf>) -> Self {
        Self {
            working_directory: working_directory.into(),
            environment: BTreeMap::new(),
        }
    }

    /// Add one explicit secret environment value.
    pub fn with_environment(mut self, name: impl Into<String>, value: SecretString) -> Self {
        self.environment.insert(name.into(), value);
        self
    }
}

impl Default for ProviderProbeContext {
    fn default() -> Self {
        Self::new(".")
    }
}

impl fmt::Debug for ProviderProbeContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderProbeContext")
            .field("working_directory", &self.working_directory)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Provider-native control category advertised by a harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessControlKind {
    /// How the provider asks for or grants tool approval.
    Permission,
    /// Filesystem/process isolation applied by the provider itself.
    Sandbox,
    /// Provider interaction mode, such as Codex Plan mode.
    Collaboration,
    /// Provider-specific agent selection.
    Agent,
}

/// One selectable value in a provider-native control group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessControlOption {
    /// Stable provider-native value written to [`crate::TurnRequest::harness_options`].
    pub id: String,
    /// Short user-facing label.
    pub label: String,
    /// Concise behavior and trade-off description.
    pub description: String,
    /// Whether the harness uses this value when the caller does not select one.
    pub is_default: bool,
    /// Whether selecting this value removes an important safety boundary.
    pub dangerous: bool,
}

/// A provider-native set of mutually exclusive runtime controls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessControlGroup {
    /// Stable key accepted by [`crate::TurnRequest::harness_options`].
    pub id: String,
    /// User-facing group label.
    pub label: String,
    /// Semantic category for clients that choose a specialized presentation.
    pub kind: HarnessControlKind,
    /// Selectable values advertised by the running harness.
    pub options: Vec<HarnessControlOption>,
}

/// One reasoning-effort value supported by a discovered model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessReasoningEffort {
    /// Stable selection identifier accepted by [`crate::TurnRequest::reasoning`].
    ///
    /// Most values are provider-native effort identifiers. A harness may also expose a
    /// normalized session mode here when it belongs in the same user-facing thinking picker,
    /// such as Claude Code's `off` and `ultracode` choices.
    pub id: String,
    /// Provider-aware user-facing label, such as `Extra high` or `Ultra code`.
    pub label: String,
    /// Optional provider description.
    pub description: Option<String>,
    /// Whether this is the model's default effort.
    pub is_default: bool,
}

/// One service tier supported by a discovered model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessServiceTier {
    /// Provider-native service tier identifier.
    pub id: String,
    /// User-facing label, such as `Fast`.
    pub label: String,
    /// Optional provider description.
    pub description: Option<String>,
    /// Whether this is the model's default tier.
    pub is_default: bool,
}

/// Model metadata returned by the provider executable inside the selected transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModel {
    /// Provider-native model identifier accepted by [`crate::TurnRequest::model`].
    pub id: String,
    /// User-facing model name.
    pub label: String,
    /// Provider-supplied model description when available.
    pub description: Option<String>,
    /// Context-window size when the harness advertises it for this exact model selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_tokens: Option<u64>,
    /// Whether this is the harness's current default model.
    pub is_default: bool,
    /// Model-specific reasoning efforts advertised by the harness.
    pub reasoning_efforts: Vec<HarnessReasoningEffort>,
    /// Model-specific service tiers advertised by the harness.
    pub service_tiers: Vec<HarnessServiceTier>,
}

/// Completeness of a model catalog probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessCatalogStatus {
    /// The harness returned its configured model catalog.
    Ready,
    /// The CLI exposes only aliases or another incomplete model view.
    Partial,
    /// This harness version has no metadata-only model catalog command.
    Unsupported,
    /// The metadata probe failed without making the harness itself unusable.
    Failed,
}

/// Typed failure from a metadata-only harness catalog probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessCatalogError {
    /// Stable error category for UI recovery behavior.
    pub kind: HarnessCatalogErrorKind,
    /// Actionable, non-secret diagnostic.
    pub message: String,
    /// Whether retrying the same discovery operation may succeed.
    pub retryable: bool,
}

/// Stable categories for catalog probe failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessCatalogErrorKind {
    /// The provider requires credentials or rejected the supplied identity.
    Authentication,
    /// The authenticated identity cannot inspect the requested metadata.
    Permission,
    /// The requested or configured model is unavailable to this identity.
    ModelUnavailable,
    /// The provider rejected the probe because an account quota was exhausted.
    RateLimited,
    /// The provider could not reach its upstream service.
    Network,
    /// The execution target failed to start or stream the probe.
    Transport,
    /// The provider returned malformed or incomplete metadata.
    Protocol,
    /// The probe exceeded its bounded deadline.
    Timeout,
    /// The provider command exited unsuccessfully.
    CommandFailed,
}

/// Models discovered from one running harness without starting an agent turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelCatalog {
    /// Completeness of this catalog.
    pub status: HarnessCatalogStatus,
    /// Stable provider-native source, such as `app_server` or `models_command`.
    pub source: String,
    /// Models returned by the harness.
    pub models: Vec<HarnessModel>,
    /// Typed failure when [`HarnessCatalogStatus::Failed`] is returned.
    pub error: Option<HarnessCatalogError>,
}

impl HarnessModelCatalog {
    /// Return a catalog for a harness without a metadata-only model endpoint.
    pub fn unsupported(source: impl Into<String>) -> Self {
        Self {
            status: HarnessCatalogStatus::Unsupported,
            source: source.into(),
            models: Vec::new(),
            error: None,
        }
    }
}

/// Aggregate readiness for every provider adapter registered on one runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessInventory {
    /// Stable name of the configured execution transport.
    pub transport: String,
    /// Process and isolation behavior supplied by that transport.
    pub transport_capabilities: TransportCapabilities,
    /// Canonically ordered provider probes.
    pub harnesses: Vec<HarnessReadiness>,
}

impl HarnessInventory {
    /// Return whether at least one provider can run on this target.
    pub fn has_ready_harness(&self) -> bool {
        self.harnesses
            .iter()
            .any(|harness| harness.status == HarnessStatus::Ready)
    }
}

/// Outcome of probing one registered provider inside the configured transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessReadiness {
    /// Provider adapter that was inspected.
    pub provider: Provider,
    /// Overall result after executable and transport compatibility checks.
    pub status: HarnessStatus,
    /// Executable/version result when the transport probe completed.
    pub readiness: Option<ProviderReadiness>,
    /// Provider-native evidence that an actual request can authenticate.
    pub authentication: HarnessAuthentication,
    /// Static permission behavior implemented by the provider adapter.
    pub permissions: PermissionSupport,
    /// Provider-neutral launch-context fields enforced by this adapter.
    pub launch_context: crate::LaunchContextCapabilities,
    /// Provider-native controls, kept separate by semantic responsibility.
    pub control_groups: Vec<HarnessControlGroup>,
    /// Model metadata fetched from the provider executable in this transport.
    pub models: HarnessModelCatalog,
    /// Latest account quota snapshot returned by the same bounded metadata probe.
    ///
    /// `None` means the harness did not expose account usage; it does not mean
    /// that the account has zero usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_usage: Option<AccountUsageSnapshot>,
    /// Typed reasons an installed provider cannot use every required channel.
    pub limitations: Vec<HarnessLimitation>,
    /// Typed transport failure when the target could not be inspected.
    pub error: Option<TransportError>,
}

/// Stable aggregate harness state for onboarding and recovery UIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessStatus {
    /// The executable exists and the transport satisfies its runtime contract.
    Ready,
    /// The execution target was reachable but the executable was absent.
    NotInstalled,
    /// The executable exists but the transport lacks a required capability.
    Incompatible,
    /// The target could not be inspected successfully.
    Unavailable,
}

/// A transport capability required by a registered provider adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessLimitation {
    /// The adapter needs bidirectional stdin for approvals or questions.
    InteractiveStdinUnavailable,
    /// The runtime cannot guarantee cleanup of the provider's descendants.
    ProcessTreeTerminationUnavailable,
}
