//! Provider-neutral sandbox selection and backend extension points.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CommandSpec, Provider, TurnEvent};

/// Controls a sandbox backend can enforce for a turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SandboxCapabilities {
    /// OS-enforced filesystem restrictions.
    pub filesystem: bool,
    /// Complete outbound network blocking.
    pub network_block: bool,
    /// Destination-level outbound network allowlists.
    pub network_allowlist: bool,
    /// Credential injection without exposing secret values to the child.
    pub credentials: bool,
    /// Durable or inspectable sandbox audit information.
    pub audit: bool,
    /// Isolation that applies to provider tool subprocesses as well as the
    /// immediate provider process.
    pub process_isolation: bool,
}

impl SandboxCapabilities {
    /// No required or advertised controls.
    pub const NONE: Self = Self {
        filesystem: false,
        network_block: false,
        network_allowlist: false,
        credentials: false,
        audit: false,
        process_isolation: false,
    };

    /// Return true when this capability set satisfies every required control.
    pub fn satisfies(self, required: Self) -> bool {
        (!required.filesystem || self.filesystem)
            && (!required.network_block || self.network_block)
            && (!required.network_allowlist || self.network_allowlist)
            && (!required.credentials || self.credentials)
            && (!required.audit || self.audit)
            && (!required.process_isolation || self.process_isolation)
    }

    /// Combine controls enforced by nested execution and sandbox layers.
    pub fn union(self, other: Self) -> Self {
        Self {
            filesystem: self.filesystem || other.filesystem,
            network_block: self.network_block || other.network_block,
            network_allowlist: self.network_allowlist || other.network_allowlist,
            credentials: self.credentials || other.credentials,
            audit: self.audit || other.audit,
            process_isolation: self.process_isolation || other.process_isolation,
        }
    }

    pub(crate) fn missing(self, required: Self) -> Vec<&'static str> {
        [
            (required.filesystem && !self.filesystem, "filesystem"),
            (
                required.network_block && !self.network_block,
                "network_block",
            ),
            (
                required.network_allowlist && !self.network_allowlist,
                "network_allowlist",
            ),
            (required.credentials && !self.credentials, "credentials"),
            (required.audit && !self.audit, "audit"),
            (
                required.process_isolation && !self.process_isolation,
                "process_isolation",
            ),
        ]
        .into_iter()
        .filter_map(|(missing, name)| missing.then_some(name))
        .collect()
    }
}

/// Non-secret context supplied to a sandbox backend for one turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxContext {
    /// Provider whose command will be sandboxed.
    pub provider: Provider,
    /// Validated working directory for the turn.
    pub working_directory: PathBuf,
}

/// Opaque identity and revision of an application-managed sandbox profile.
///
/// Revisions are strings so a store can use an integer, UUID, content hash,
/// or database-native concurrency token without converting it to a lossy
/// common representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxProfileRef {
    /// Stable profile identity understood by its manager.
    pub id: String,
    /// Exact materialized revision, when known.
    pub revision: Option<String>,
}

impl SandboxProfileRef {
    /// Select a profile and let its manager resolve the current revision.
    pub fn current(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            revision: None,
        }
    }

    /// Select one exact profile revision.
    pub fn at_revision(id: impl Into<String>, revision: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            revision: Some(revision.into()),
        }
    }
}

/// Portable filesystem access used by managed profile changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SandboxPathAccess {
    /// Read-only access.
    Read,
    /// Write access. Backends may also grant the reads required to write.
    Write,
    /// Read and write access.
    ReadWrite,
}

/// Resource a sandbox denied while an agent step was running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SandboxResource {
    /// Filesystem path denied by the sandbox.
    Path {
        /// Absolute or backend-supported symbolic path.
        path: PathBuf,
        /// Access inferred from the provider output, when reliable.
        access: Option<SandboxPathAccess>,
    },
    /// Outbound destination denied by a network policy.
    NetworkDestination {
        /// Host, domain pattern, or backend-native destination.
        destination: String,
    },
    /// Named credential unavailable to the sandboxed process.
    Credential {
        /// Non-secret credential name.
        name: String,
    },
    /// Backend-specific resource safe to display and persist.
    Other {
        /// Stable resource kind.
        resource_kind: String,
        /// Bounded, non-secret resource value.
        value: String,
    },
}

/// Structured sandbox failure associated with one provider tool step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxViolation {
    /// Provider-native tool/step identifier, when the protocol supplies one.
    pub step_id: Option<String>,
    /// Tool name that encountered the denial, when known.
    pub tool_name: Option<String>,
    /// Denied resource.
    pub resource: SandboxResource,
    /// Bounded diagnostic suitable for an audit trail.
    pub message: String,
}

/// Portable change an approved recovery can apply to a managed profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SandboxProfileChange {
    /// Grant access to one path.
    GrantPath {
        /// Path to grant.
        path: PathBuf,
        /// Narrowest access the operator approved.
        access: SandboxPathAccess,
        /// Override a backend protection rule when supported.
        bypass_protection: bool,
    },
    /// Add one outbound destination.
    AllowNetworkDestination {
        /// Host, domain pattern, or backend-native destination.
        destination: String,
    },
    /// Make one already-configured named credential available.
    AddCredential {
        /// Non-secret credential name. Secret values never belong here.
        name: String,
    },
    /// Explicit backend extension. Values must not contain secrets.
    Custom {
        /// Stable backend-specific change name.
        name: String,
        /// Backend-specific non-secret payload.
        value: Value,
    },
}

/// Compare-and-swap update requested from a sandbox profile manager.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SandboxProfileUpdate {
    /// Profile revision used by the failed step.
    pub profile: SandboxProfileRef,
    /// Approved policy change.
    pub change: SandboxProfileChange,
    /// Failure that motivated the change.
    pub violation: SandboxViolation,
}

/// A profile revision resolved to an executable sandbox backend.
#[derive(Clone)]
pub struct ResolvedSandboxProfile {
    /// Exact profile revision represented by `backend`.
    pub profile: SandboxProfileRef,
    pub(crate) backend: Arc<dyn SandboxBackend>,
}

impl ResolvedSandboxProfile {
    /// Associate an exact profile revision with its configured backend.
    pub fn new(profile: SandboxProfileRef, backend: impl SandboxBackend + 'static) -> Self {
        Self {
            profile,
            backend: Arc::new(backend),
        }
    }

    /// Associate an exact profile revision with an already shared backend.
    pub fn from_arc(profile: SandboxProfileRef, backend: Arc<dyn SandboxBackend>) -> Self {
        Self { profile, backend }
    }

    /// Backend capabilities for this exact revision.
    pub fn capabilities(&self) -> SandboxCapabilities {
        self.backend.capabilities()
    }
}

impl fmt::Debug for ResolvedSandboxProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedSandboxProfile")
            .field("profile", &self.profile)
            .field("backend", &self.backend.name())
            .field("capabilities", &self.backend.capabilities())
            .finish()
    }
}

/// Application-owned persistence and materialization for sandbox profiles.
///
/// `update` must use `request.profile.revision` as an optimistic concurrency
/// token when it is present. It must durably commit and validate the new
/// revision before returning it. The runtime never mutates a profile itself.
#[async_trait]
pub trait SandboxProfileManager: Send + Sync {
    /// Stable diagnostic name for this manager.
    fn name(&self) -> &'static str;

    /// Controls every profile materialized by this configured manager can
    /// enforce. Exact revisions are checked again after resolution.
    fn capabilities(&self) -> SandboxCapabilities;

    /// Resolve a reference to one exact, validated sandbox revision.
    async fn resolve(
        &self,
        profile: &SandboxProfileRef,
        context: &SandboxContext,
    ) -> Result<ResolvedSandboxProfile, SandboxError>;

    /// Atomically update and resolve a new profile revision.
    async fn update(
        &self,
        request: SandboxProfileUpdate,
        context: &SandboxContext,
    ) -> Result<ResolvedSandboxProfile, SandboxError>;
}

/// Context shown to the host before a sandbox profile is widened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SandboxRecoveryRequest {
    /// Exact profile revision used by the denied step.
    pub profile: SandboxProfileRef,
    /// Structured denial.
    pub violation: SandboxViolation,
    /// One-based recovery attempt number.
    pub attempt: u8,
}

/// Host decision for a sandbox denial.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SandboxRecoveryDecision {
    /// Leave the profile unchanged and do not retry.
    Deny {
        /// Optional reason for the audit trail.
        reason: Option<String>,
    },
    /// Apply this exact change and retry in the same provider session.
    Retry {
        /// Least-privilege change approved by the host or operator.
        change: SandboxProfileChange,
    },
}

/// Application-owned approval bridge for sandbox profile recovery.
#[async_trait]
pub trait SandboxRecoveryHandler: Send + Sync {
    /// Decide whether and how to widen a profile after one denied step.
    async fn recover(&self, request: SandboxRecoveryRequest) -> SandboxRecoveryDecision;
}

/// Bound for opt-in managed sandbox retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxRecoveryPolicy {
    /// Maximum number of profile updates and provider-session retries.
    pub max_retries: u8,
}

impl Default for SandboxRecoveryPolicy {
    fn default() -> Self {
        Self { max_retries: 1 }
    }
}

/// Failure while selecting or preparing a sandbox backend.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SandboxError {
    /// The selected backend cannot satisfy required controls.
    #[error("sandbox backend `{backend}` is missing required capabilities: {missing:?}")]
    MissingCapabilities {
        /// Selected backend.
        backend: String,
        /// Required controls the backend does not advertise.
        missing: Vec<&'static str>,
    },
    /// Backend-specific preparation failed.
    #[error("sandbox backend `{backend}` failed: {source}")]
    Backend {
        /// Selected backend.
        backend: String,
        /// Typed backend error retained as the source.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A profile manager returned a revision that does not match the request.
    #[error("sandbox profile manager `{manager}` returned invalid profile `{profile}`: {message}")]
    InvalidProfile {
        /// Profile manager.
        manager: String,
        /// Requested profile identity.
        profile: String,
        /// Validation failure.
        message: String,
    },
    /// Profile resolution or update exceeded its bounded deadline.
    #[error(
        "sandbox profile manager `{manager}` timed out during {operation} after {seconds} seconds"
    )]
    ProfileTimeout {
        /// Profile manager.
        manager: String,
        /// Bounded operation.
        operation: &'static str,
        /// Configured deadline.
        seconds: u64,
    },
}

impl SandboxError {
    /// Wrap a backend-specific error without discarding its source chain.
    pub fn backend(
        backend: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Backend {
            backend: backend.into(),
            source: Box::new(source),
        }
    }
}

/// Extension point for OS sandboxes, containers, and remote execution guards.
///
/// Implementations are trusted application components. They must keep command
/// arguments separated, preserve stdin/environment requirements, and return an
/// error rather than weakening a requested policy.
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    /// Stable diagnostic name for this backend.
    fn name(&self) -> &'static str;

    /// Controls this configured backend instance actually enforces.
    fn capabilities(&self) -> SandboxCapabilities;

    /// Prepare or wrap a provider command before it is spawned.
    async fn prepare(
        &self,
        context: SandboxContext,
        command: CommandSpec,
    ) -> Result<CommandSpec, SandboxError>;

    /// Classify a provider event as a sandbox denial when the backend can do
    /// so reliably. The default supports backends without runtime auditing.
    fn classify_event(&self, _event: &TurnEvent) -> Option<SandboxViolation> {
        None
    }
}

/// Per-turn selection of a configured sandbox backend and required controls.
#[derive(Clone)]
pub struct SandboxRequest {
    source: SandboxSource,
    required: SandboxCapabilities,
}

#[derive(Clone)]
enum SandboxSource {
    Direct(Arc<dyn SandboxBackend>),
    Managed {
        manager: Arc<dyn SandboxProfileManager>,
        profile: SandboxProfileRef,
    },
}

pub(crate) struct ResolvedSandbox {
    pub(crate) backend: Arc<dyn SandboxBackend>,
    pub(crate) profile: Option<SandboxProfileRef>,
    pub(crate) required: SandboxCapabilities,
}

impl ResolvedSandbox {
    pub(crate) fn as_request(&self) -> SandboxRequest {
        SandboxRequest {
            source: SandboxSource::Direct(self.backend.clone()),
            required: self.required,
        }
    }

    pub(crate) fn classify_event(&self, event: &TurnEvent) -> Option<SandboxViolation> {
        self.backend.classify_event(event)
    }
}

impl SandboxRequest {
    /// Select a configured backend without additional capability requirements.
    pub fn new(backend: impl SandboxBackend + 'static) -> Self {
        Self {
            source: SandboxSource::Direct(Arc::new(backend)),
            required: SandboxCapabilities::NONE,
        }
    }

    /// Select an already shared backend.
    pub fn from_arc(backend: Arc<dyn SandboxBackend>) -> Self {
        Self {
            source: SandboxSource::Direct(backend),
            required: SandboxCapabilities::NONE,
        }
    }

    /// Resolve a profile through an application-owned manager for each turn.
    pub fn managed(
        manager: impl SandboxProfileManager + 'static,
        profile: SandboxProfileRef,
    ) -> Self {
        Self::managed_from_arc(Arc::new(manager), profile)
    }

    /// Resolve a profile through an already shared manager for each turn.
    pub fn managed_from_arc(
        manager: Arc<dyn SandboxProfileManager>,
        profile: SandboxProfileRef,
    ) -> Self {
        Self {
            source: SandboxSource::Managed { manager, profile },
            required: SandboxCapabilities::NONE,
        }
    }

    /// Require controls before backend preparation or provider execution.
    pub fn requiring(mut self, required: SandboxCapabilities) -> Self {
        self.required = required;
        self
    }

    /// Selected backend name.
    pub fn backend_name(&self) -> &'static str {
        match &self.source {
            SandboxSource::Direct(backend) => backend.name(),
            SandboxSource::Managed { manager, .. } => manager.name(),
        }
    }

    /// Controls advertised by the configured backend instance.
    pub fn capabilities(&self) -> SandboxCapabilities {
        match &self.source {
            SandboxSource::Direct(backend) => backend.capabilities(),
            SandboxSource::Managed { manager, .. } => manager.capabilities(),
        }
    }

    /// Controls the caller requires for this turn.
    pub fn required_capabilities(&self) -> SandboxCapabilities {
        self.required
    }

    fn validate_capabilities(&self, available: SandboxCapabilities) -> Result<(), SandboxError> {
        if available.satisfies(self.required) {
            Ok(())
        } else {
            Err(SandboxError::MissingCapabilities {
                backend: self.backend_name().to_string(),
                missing: available.missing(self.required),
            })
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SandboxError> {
        self.validate_capabilities(self.capabilities())
    }

    pub(crate) async fn resolve(
        &self,
        context: &SandboxContext,
    ) -> Result<ResolvedSandbox, SandboxError> {
        let (backend, profile) = match &self.source {
            SandboxSource::Direct(backend) => (backend.clone(), None),
            SandboxSource::Managed { manager, profile } => {
                let resolved = manager.resolve(profile, context).await?;
                if resolved.profile.id != profile.id {
                    return Err(SandboxError::InvalidProfile {
                        manager: manager.name().to_string(),
                        profile: profile.id.clone(),
                        message: "resolved profile identity changed".to_string(),
                    });
                }
                (resolved.backend, Some(resolved.profile))
            }
        };
        self.validate_capabilities(backend.capabilities())?;
        Ok(ResolvedSandbox {
            backend,
            profile,
            required: self.required,
        })
    }

    pub(crate) fn managed_parts(
        &self,
    ) -> Option<(Arc<dyn SandboxProfileManager>, SandboxProfileRef)> {
        match &self.source {
            SandboxSource::Managed { manager, profile } => Some((manager.clone(), profile.clone())),
            SandboxSource::Direct(_) => None,
        }
    }

    pub(crate) async fn prepare(
        &self,
        context: SandboxContext,
        command: CommandSpec,
    ) -> Result<CommandSpec, SandboxError> {
        let resolved = self.resolve(&context).await?;
        resolved.backend.prepare(context, command).await
    }
}

impl fmt::Debug for SandboxRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SandboxRequest")
            .field("backend", &self.backend_name())
            .field("capabilities", &self.capabilities())
            .field(
                "profile",
                &match &self.source {
                    SandboxSource::Managed { profile, .. } => Some(profile),
                    SandboxSource::Direct(_) => None,
                },
            )
            .field("required", &self.required)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_each_missing_required_capability() {
        let available = SandboxCapabilities {
            filesystem: true,
            ..SandboxCapabilities::NONE
        };
        let required = SandboxCapabilities {
            filesystem: true,
            network_allowlist: true,
            audit: true,
            ..SandboxCapabilities::NONE
        };
        assert!(!available.satisfies(required));
        assert_eq!(available.missing(required), ["network_allowlist", "audit"]);
    }
}
