//! Provider-neutral private-network lifecycle contracts.
//!
//! Providers may use very different mechanisms: Tailscale and Headscale can
//! expose a userspace daemon and interactive login, while WireGuard may own a
//! static interface and route table. These traits share lifecycle semantics
//! without requiring provider-specific sockets, commands, or credentials.

/// Capabilities a network provider exposes to its host application.
///
/// Hosts can use these flags to present the right onboarding flow without
/// assuming that every VPN has a browser login, a userspace daemon, or an
/// application-level split proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetworkProviderCapabilities {
    /// The provider may ask the user to complete authentication in a browser.
    pub interactive_authentication: bool,
    /// The provider can run without creating a privileged kernel interface.
    pub userspace_networking: bool,
    /// The provider supplies a per-session proxy for selective routing.
    pub split_proxy: bool,
}

/// Factory for one kind of private-network connection.
///
/// The associated types intentionally let providers use differently shaped
/// configuration and access handles. Tailscale and Headscale can share a
/// userspace daemon and SOCKS endpoint, while a future WireGuard provider may
/// instead own an interface and route table without pretending it has a
/// Tailscale control socket.
#[async_trait::async_trait]
pub trait NetworkProvider: std::fmt::Debug + Send + Sync {
    /// Provider-specific configuration used to start one connection.
    type Specification: Send;
    /// A running connection owned by this provider.
    type Session: NetworkSession<Error = Self::Error>;
    /// Provider-specific, typed failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Stable machine-readable provider identifier.
    fn id(&self) -> &'static str;

    /// Features callers may safely expose for this provider.
    fn capabilities(&self) -> NetworkProviderCapabilities;

    /// Start one isolated network connection.
    async fn start(&self, spec: Self::Specification) -> Result<Self::Session, Self::Error>;
}

/// Common lifecycle of a running private-network connection.
///
/// Authentication is a separate trait because static WireGuard
/// configurations, for example, do not have an interactive login lifecycle.
#[async_trait::async_trait]
pub trait NetworkSession: std::fmt::Debug + Clone + Send + Sync + 'static {
    /// Provider-specific status snapshot.
    type Status: Send;
    /// Launch-time access passed to an agent runtime.
    type Access: Send;
    /// Provider-specific, typed failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Stable identifier of the provider that created this session.
    fn provider_id(&self) -> &'static str;

    /// Return the latest cached status without forcing remote discovery.
    async fn status(&self) -> Self::Status;

    /// Stop the connection while preserving provider state on disk.
    async fn stop(&self) -> Result<Self::Status, Self::Error>;

    /// Relaunch the connection with its existing configuration and state.
    async fn restart(&self) -> Result<Self::Status, Self::Error>;

    /// Resolve the launch-time access for a connected session.
    async fn access(&self) -> Result<Self::Access, Self::Error>;
}

/// Optional interactive identity lifecycle for providers that require login.
#[async_trait::async_trait]
pub trait InteractiveNetworkSession: NetworkSession {
    /// Start or resume the provider's interactive authentication flow.
    async fn authenticate(&self) -> Result<Self::Status, Self::Error>;

    /// Remove the provider identity while keeping local connection state.
    async fn deauthenticate(&self) -> Result<(), Self::Error>;
}
