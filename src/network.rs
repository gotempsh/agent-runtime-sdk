//! Provider-neutral private-network lifecycle contracts.
//!
//! Providers may use very different mechanisms: Tailscale and Headscale can
//! expose a userspace daemon and interactive login, while WireGuard may own a
//! system interface and route table. The object-safe contracts in this module
//! let hosts register and select those providers at runtime without requiring
//! every implementation to expose Tailscale-specific sockets or commands.

use std::any::Any;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Component, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::CommandSpec;

const MAX_PROVIDER_ID_LEN: usize = 64;
const MAX_INSTANCE_NAME_LEN: usize = 128;
const MAX_NODE_NAME_LEN: usize = 253;
const MAX_REGISTERED_PROVIDERS: usize = 32;

static ACTIVE_STATE_DIRS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

/// Stable, validated identity used to register and persist a network provider.
///
/// IDs contain lowercase ASCII letters, digits, and inner hyphens. This makes
/// them safe to use as map keys and telemetry dimensions. Hosts must still not
/// concatenate an ID into a filesystem path or shell command without using the
/// relevant structured API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkProviderId(Cow<'static, str>);

impl NetworkProviderId {
    /// Validate a provider ID supplied by an external implementation.
    pub fn new(value: impl Into<String>) -> Result<Self, NetworkProviderIdError> {
        let value = value.into();
        validate_provider_id(&value)?;
        Ok(Self(Cow::Owned(value)))
    }

    #[cfg(feature = "tailnet")]
    pub(crate) const fn from_validated_static(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }

    /// Borrow the provider ID as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NetworkProviderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why a network provider ID was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NetworkProviderIdError {
    /// The ID was empty or exceeded the supported bound.
    #[error("provider ID must contain 1 to {MAX_PROVIDER_ID_LEN} characters")]
    InvalidLength,
    /// The ID did not begin and end with an ASCII letter or digit.
    #[error("provider ID must begin and end with a lowercase ASCII letter or digit")]
    InvalidBoundary,
    /// The ID contained an unsupported character.
    #[error("provider ID contains invalid character {character:?} at byte {index}")]
    InvalidCharacter {
        /// Unsupported character.
        character: char,
        /// Byte offset of the unsupported character.
        index: usize,
    },
}

fn validate_provider_id(value: &str) -> Result<(), NetworkProviderIdError> {
    if value.is_empty() || value.len() > MAX_PROVIDER_ID_LEN {
        return Err(NetworkProviderIdError::InvalidLength);
    }
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let bytes = value.as_bytes();
    if !is_alphanumeric(bytes[0]) || !is_alphanumeric(bytes[bytes.len() - 1]) {
        return Err(NetworkProviderIdError::InvalidBoundary);
    }
    for (index, byte) in bytes.iter().copied().enumerate() {
        if !is_alphanumeric(byte) && byte != b'-' {
            return Err(NetworkProviderIdError::InvalidCharacter {
                character: char::from(byte),
                index,
            });
        }
    }
    Ok(())
}

/// Discovery hints a network provider exposes to its host application.
///
/// Providers are trusted in-process code and self-report these values. They
/// are suitable for choosing UI and onboarding, never for authorization,
/// privilege decisions, or proof that traffic is isolated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetworkProviderCapabilities {
    /// The provider supports an interactive authentication operation.
    pub interactive_authentication: bool,
    /// The provider can run without creating a privileged kernel interface.
    pub userspace_networking: bool,
    /// The provider supplies a per-session proxy for selective routing.
    pub split_proxy: bool,
}

impl NetworkProviderCapabilities {
    /// Construct explicit provider discovery hints.
    pub const fn new(
        interactive_authentication: bool,
        userspace_networking: bool,
        split_proxy: bool,
    ) -> Self {
        Self {
            interactive_authentication,
            userspace_networking,
            split_proxy,
        }
    }
}

/// Provider-neutral configuration for one isolated network identity.
#[derive(Clone)]
#[non_exhaustive]
pub struct NetworkInstanceSpec {
    /// Application label for the connection.
    pub name: String,
    /// Private, absolute directory for durable provider state.
    pub state_dir: PathBuf,
    /// Optional node name requested from providers that advertise one.
    pub node_name: Option<String>,
    /// Provider-specific, in-process configuration. This may contain secrets
    /// and is deliberately omitted from `Debug` output.
    provider_configuration: Option<Arc<dyn Any + Send + Sync>>,
}

impl NetworkInstanceSpec {
    /// Describe one network identity with a provider-derived node name.
    pub fn new(
        name: impl Into<String>,
        state_dir: impl Into<PathBuf>,
    ) -> Result<Self, NetworkError> {
        let name = name.into();
        let name = name.trim();
        if name.is_empty()
            || name.chars().count() > MAX_INSTANCE_NAME_LEN
            || name.chars().any(char::is_control)
        {
            return Err(NetworkError::InvalidConfiguration {
                field: "name",
                message: format!(
                    "must contain 1 to {MAX_INSTANCE_NAME_LEN} non-control characters"
                ),
            });
        }
        let state_dir = state_dir.into();
        if !state_dir.is_absolute() {
            return Err(NetworkError::InvalidConfiguration {
                field: "state_dir",
                message: format!("{} is not absolute", state_dir.display()),
            });
        }
        Ok(Self {
            name: name.to_string(),
            state_dir,
            node_name: None,
            provider_configuration: None,
        })
    }

    /// Request an explicit provider node name.
    pub fn with_node_name(mut self, node_name: impl Into<String>) -> Result<Self, NetworkError> {
        let node_name = node_name.into();
        let node_name = node_name.trim();
        if node_name.is_empty()
            || node_name.chars().count() > MAX_NODE_NAME_LEN
            || node_name.chars().any(char::is_control)
        {
            return Err(NetworkError::InvalidConfiguration {
                field: "node_name",
                message: format!("must contain 1 to {MAX_NODE_NAME_LEN} non-control characters"),
            });
        }
        self.node_name = Some(node_name.to_string());
        Ok(self)
    }

    /// Attach typed provider-specific configuration for this instance.
    ///
    /// Hosts reconstruct this value after authorizing and decrypting persisted
    /// configuration. Do not accept arbitrary serialized types from untrusted
    /// input. Providers retrieve it with [`Self::provider_configuration`].
    pub fn with_provider_configuration<T>(mut self, configuration: T) -> Self
    where
        T: Any + Send + Sync,
    {
        self.provider_configuration = Some(Arc::new(configuration));
        self
    }

    /// Downcast the provider-specific configuration to the expected type.
    pub fn provider_configuration<T>(&self) -> Option<&T>
    where
        T: Any + Send + Sync,
    {
        self.provider_configuration.as_deref()?.downcast_ref()
    }
}

impl std::fmt::Debug for NetworkInstanceSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkInstanceSpec")
            .field("name", &self.name)
            .field("state_dir", &self.state_dir)
            .field("node_name", &self.node_name)
            .field(
                "provider_configuration",
                &self.provider_configuration.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Provider-neutral lifecycle state of one network session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetworkSessionState {
    /// No provider resources are active.
    Stopped,
    /// Provider resources are starting or reconnecting.
    Starting,
    /// User authentication is required.
    NeedsAuthentication,
    /// The network is ready for agent launches.
    Running,
    /// The provider failed; `detail` contains the actionable reason.
    Failed,
}

/// Common status suitable for provider-neutral UI and orchestration.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetworkSessionStatus {
    /// Current lifecycle state.
    pub state: NetworkSessionState,
    /// Current operation or actionable failure, safe for user display.
    /// Providers must redact credentials and keep this value bounded.
    pub detail: String,
    /// Browser URL for interactive authentication, when one is pending.
    /// Treat it as ephemeral sensitive data and never persist or log it.
    pub authentication_url: Option<String>,
    /// Provider-reported network or organization name.
    pub network_name: Option<String>,
    /// Provider-managed DNS suffix.
    pub dns_suffix: Option<String>,
    /// Addresses assigned to this session.
    pub self_addresses: Vec<String>,
    /// Online peers when the provider exposes peer discovery.
    pub online_peers: Option<u32>,
    /// Known peers when the provider exposes peer discovery.
    pub total_peers: Option<u32>,
    /// Last state-change time as Unix milliseconds.
    pub updated_at_ms: u64,
}

impl std::fmt::Debug for NetworkSessionStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkSessionStatus")
            .field("state", &self.state)
            .field("detail", &self.detail)
            .field("authentication_pending", &self.authentication_url.is_some())
            .field("network_name", &self.network_name)
            .field("dns_suffix", &self.dns_suffix)
            .field("self_addresses", &self.self_addresses)
            .field("online_peers", &self.online_peers)
            .field("total_peers", &self.total_peers)
            .field("updated_at_ms", &self.updated_at_ms)
            .finish()
    }
}

impl NetworkSessionStatus {
    /// Construct the minimum provider-neutral status.
    pub fn new(state: NetworkSessionState, detail: impl Into<String>) -> Self {
        Self {
            state,
            detail: detail.into(),
            authentication_url: None,
            network_name: None,
            dns_suffix: None,
            self_addresses: Vec::new(),
            online_peers: None,
            total_peers: None,
            updated_at_ms: 0,
        }
    }
}

/// Sandbox resources needed to use one network access handle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetworkSandboxRequirements {
    /// Loopback TCP ports the sandboxed process may connect to.
    pub loopback_ports: Vec<u16>,
    /// Unix sockets the sandboxed process may access.
    pub unix_sockets: Vec<PathBuf>,
    /// Provider-generated files the sandboxed process may read.
    pub readable_files: Vec<PathBuf>,
    /// Optional upstream proxy for a sandbox filtering proxy.
    pub upstream_proxy: Option<String>,
}

/// Launch-time access produced by a connected private-network session.
///
/// Implementations must not place raw credentials in returned environment or
/// guidance. Hosts must treat all values as sensitive configuration and avoid
/// logging them indiscriminately. Environment, guidance, and sandbox
/// requirements are privileged outputs: accept them only from trusted,
/// explicitly registered provider implementations.
pub trait NetworkAccess: Send + Sync {
    /// Environment needed by an agent process.
    fn environment(&self) -> BTreeMap<OsString, OsString>;

    /// Short provider-specific instructions for the agent system prompt.
    fn guidance(&self) -> String;

    /// Resources that a sandbox must expose for this access handle.
    fn sandbox_requirements(&self) -> NetworkSandboxRequirements;

    /// Apply this network environment to an agent command.
    fn apply(&self, mut command: CommandSpec) -> CommandSpec {
        command.environment.extend(self.environment());
        command
    }
}

/// One active provider connection.
///
/// Implementations own every process, interface, route, and credential they
/// create. Startup must be cancellation-safe: cancelling `start` may not leave
/// connectivity or privileged resources behind. Dropping the last session must
/// clean up ephemeral resources. `stop` must revoke connectivity before it
/// succeeds; `deauthenticate` must revoke provider identity before it succeeds.
/// Hosts that need to reuse durable state call [`ManagedNetworkSession::shutdown`]
/// instead of dropping the managed wrapper.
#[async_trait::async_trait]
pub trait NetworkSession: Send + Sync {
    /// Return the latest cached status without forcing remote discovery.
    async fn status(&self) -> NetworkSessionStatus;

    /// Start or resume interactive authentication.
    async fn authenticate(&self) -> Result<NetworkSessionStatus, NetworkError>;

    /// Revoke the provider identity while retaining reusable local state.
    async fn deauthenticate(&self) -> Result<(), NetworkError>;

    /// Revoke connectivity while preserving durable provider state.
    async fn stop(&self) -> Result<NetworkSessionStatus, NetworkError>;

    /// Relaunch the connection with its existing configuration and state.
    async fn restart(&self) -> Result<NetworkSessionStatus, NetworkError>;

    /// Resolve launch-time access for a connected session.
    async fn access(&self) -> Result<Arc<dyn NetworkAccess>, NetworkError>;
}

/// Object-safe factory for one kind of private-network connection.
///
/// Implementations are trusted in-process code. Provider-specific installation
/// and credential configuration belongs on the provider value; instance names,
/// state directories, and node names arrive through [`NetworkInstanceSpec`].
#[async_trait::async_trait]
pub trait NetworkProvider: Send + Sync {
    /// Stable provider identity used for runtime selection and persistence.
    fn id(&self) -> NetworkProviderId;

    /// UI/discovery hints. Never use these values as an authorization signal.
    fn capabilities(&self) -> NetworkProviderCapabilities;

    /// Start one isolated connection.
    async fn start(
        &self,
        spec: NetworkInstanceSpec,
    ) -> Result<Arc<dyn NetworkSession>, NetworkError>;
}

/// Typed failures shared by provider-neutral network orchestration.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetworkError {
    /// Instance configuration was rejected before provider startup.
    #[error("invalid network {field}: {message}")]
    InvalidConfiguration {
        /// Rejected field.
        field: &'static str,
        /// Actionable explanation.
        message: String,
    },
    /// A provider does not implement the requested lifecycle operation.
    #[error("network provider {provider} does not support {operation}")]
    Unsupported {
        /// Provider that rejected the operation.
        provider: NetworkProviderId,
        /// Stable operation name.
        operation: &'static str,
    },
    /// A registered provider failed an operation. Providers must redact
    /// credentials and bound externally sourced diagnostics before wrapping
    /// them here.
    #[error("network provider {provider} failed to {operation}: {source}")]
    Provider {
        /// Provider that failed.
        provider: NetworkProviderId,
        /// Stable operation name.
        operation: &'static str,
        /// Provider-specific typed source.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// No provider is registered under the requested ID.
    #[error("network provider {provider} is not registered")]
    NotRegistered {
        /// Requested provider ID.
        provider: NetworkProviderId,
    },
    /// A second provider tried to claim an existing ID.
    #[error("network provider {provider} is already registered")]
    AlreadyRegistered {
        /// Conflicting provider ID.
        provider: NetworkProviderId,
    },
    /// The bounded registry has reached its provider limit.
    #[error("network provider registry is full (maximum {MAX_REGISTERED_PROVIDERS})")]
    RegistryFull,
    /// Another live session owns the same canonical state directory.
    #[error("network state directory {path} is already owned by a live session")]
    StateDirectoryInUse {
        /// Conflicting canonical path.
        path: PathBuf,
    },
}

impl NetworkError {
    /// Wrap a provider-specific failure with operation context.
    pub fn provider(
        provider: NetworkProviderId,
        operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Provider {
            provider,
            operation,
            source: Box::new(source),
        }
    }
}

/// Runtime registry for heterogeneous private-network providers.
#[derive(Default)]
pub struct NetworkProviderRegistry {
    providers: BTreeMap<NetworkProviderId, Arc<dyn NetworkProvider>>,
}

impl NetworkProviderRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a trusted provider, rejecting identity collisions.
    pub fn register(&mut self, provider: Arc<dyn NetworkProvider>) -> Result<(), NetworkError> {
        let id = provider.id();
        if self.providers.contains_key(&id) {
            return Err(NetworkError::AlreadyRegistered { provider: id });
        }
        if self.providers.len() >= MAX_REGISTERED_PROVIDERS {
            return Err(NetworkError::RegistryFull);
        }
        self.providers.insert(id, provider);
        Ok(())
    }

    /// List registered providers and their UI/discovery hints.
    ///
    /// The result is bounded by the registry's provider limit.
    pub fn providers(&self) -> Vec<NetworkProviderDescriptor> {
        self.providers
            .iter()
            .map(|(id, provider)| NetworkProviderDescriptor {
                id: id.clone(),
                capabilities: provider.capabilities(),
            })
            .collect()
    }

    /// Return discovery hints for a registered provider.
    pub fn capabilities(
        &self,
        provider: &NetworkProviderId,
    ) -> Result<NetworkProviderCapabilities, NetworkError> {
        self.providers
            .get(provider)
            .map(|registered| registered.capabilities())
            .ok_or_else(|| NetworkError::NotRegistered {
                provider: provider.clone(),
            })
    }

    /// Start a session through the selected provider.
    pub async fn start(
        &self,
        provider: &NetworkProviderId,
        spec: NetworkInstanceSpec,
    ) -> Result<ManagedNetworkSession, NetworkError> {
        let implementation =
            self.providers
                .get(provider)
                .ok_or_else(|| NetworkError::NotRegistered {
                    provider: provider.clone(),
                })?;
        let state_dir_key = canonical_state_dir_key(&spec.state_dir).await?;
        {
            let mut active = active_state_dirs()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !active.insert(state_dir_key.clone()) {
                return Err(NetworkError::StateDirectoryInUse {
                    path: state_dir_key,
                });
            }
        }
        let reservation = StateDirectoryReservation {
            path: state_dir_key,
        };
        let session = match implementation.start(spec).await {
            Ok(session) => session,
            Err(error) => {
                reservation.release();
                return Err(error);
            }
        };
        Ok(ManagedNetworkSession {
            provider: provider.clone(),
            session,
            state_dir_reservation: reservation,
        })
    }
}

/// One registered provider exposed for selection UI.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetworkProviderDescriptor {
    /// Validated provider identity.
    pub id: NetworkProviderId,
    /// Self-reported UI/discovery hints.
    pub capabilities: NetworkProviderCapabilities,
}

/// A provider session whose identity is assigned by the SDK registry.
pub struct ManagedNetworkSession {
    provider: NetworkProviderId,
    session: Arc<dyn NetworkSession>,
    state_dir_reservation: StateDirectoryReservation,
}

impl ManagedNetworkSession {
    /// Provider selected by the registry for this session.
    pub fn provider_id(&self) -> &NetworkProviderId {
        &self.provider
    }

    /// Borrow the provider-neutral session lifecycle.
    pub fn session(&self) -> &dyn NetworkSession {
        self.session.as_ref()
    }

    /// Stop the provider, confirm teardown, and release its state directory.
    ///
    /// Consume sessions through this method whenever their state directory may
    /// be reused. Dropping without shutdown keeps the process-wide reservation
    /// until process exit: the provider still performs best-effort cleanup, but
    /// the SDK will not risk starting a second identity before teardown is
    /// confirmed.
    pub async fn shutdown(self) -> Result<NetworkSessionStatus, NetworkError> {
        let status = self.session.stop().await?;
        self.state_dir_reservation.release();
        Ok(status)
    }
}

struct StateDirectoryReservation {
    path: PathBuf,
}

impl StateDirectoryReservation {
    fn release(&self) {
        active_state_dirs()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.path);
    }
}

fn active_state_dirs() -> &'static Mutex<BTreeSet<PathBuf>> {
    ACTIVE_STATE_DIRS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

async fn canonical_state_dir_key(path: &std::path::Path) -> Result<PathBuf, NetworkError> {
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(NetworkError::InvalidConfiguration {
            field: "state_dir",
            message: "must not contain . or .. components".into(),
        });
    }
    let mut existing = path;
    let mut missing = Vec::new();
    loop {
        match tokio::fs::canonicalize(existing).await {
            Ok(mut canonical) => {
                for component in missing.iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = existing.file_name() else {
                    return Err(NetworkError::InvalidConfiguration {
                        field: "state_dir",
                        message: format!("{} cannot be resolved", path.display()),
                    });
                };
                missing.push(name.to_os_string());
                let Some(parent) = existing.parent() else {
                    return Err(NetworkError::InvalidConfiguration {
                        field: "state_dir",
                        message: format!("{} has no existing ancestor", path.display()),
                    });
                };
                existing = parent;
            }
            Err(error) => {
                return Err(NetworkError::InvalidConfiguration {
                    field: "state_dir",
                    message: format!("{} cannot be resolved: {error}", path.display()),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_ids_are_bounded_and_path_safe() {
        assert_eq!(
            NetworkProviderId::new("wireguard-1").unwrap().as_str(),
            "wireguard-1"
        );
        for invalid in [
            "",
            "WireGuard",
            "-wireguard",
            "wireguard-",
            "wg/config",
            "wg\n",
        ] {
            assert!(NetworkProviderId::new(invalid).is_err(), "{invalid:?}");
        }
        assert!(NetworkProviderId::new("x".repeat(65)).is_err());
    }

    #[test]
    fn instance_names_are_bounded_before_provider_startup() {
        let root = if cfg!(windows) { "C:\\state" } else { "/state" };
        let spec = NetworkInstanceSpec::new(" client ", root)
            .unwrap()
            .with_node_name(" node ")
            .unwrap();
        assert_eq!(spec.name, "client");
        assert_eq!(spec.node_name.as_deref(), Some("node"));
        assert!(NetworkInstanceSpec::new("x".repeat(MAX_INSTANCE_NAME_LEN + 1), root).is_err());
        assert!(NetworkInstanceSpec::new("client", root)
            .unwrap()
            .with_node_name("x".repeat(MAX_NODE_NAME_LEN + 1))
            .is_err());
    }
}
