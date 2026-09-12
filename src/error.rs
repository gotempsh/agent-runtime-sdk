use std::path::PathBuf;

use crate::lifecycle::DeliveryState;
use crate::Provider;

/// Stable category for an unsuccessfully completed provider process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ProviderProcessErrorKind {
    /// Provider credentials are absent, expired, or rejected.
    AuthenticationFailed,
    /// The configured identity cannot perform the requested provider action.
    PermissionDenied,
    /// The requested model is missing or unavailable to this identity.
    ModelUnavailable,
    /// The provider rejected the request due to a usage or rate limit.
    RateLimited,
    /// The provider endpoint could not be reached successfully.
    Network,
    /// The process failed without a recognized safe category.
    Unknown,
}

/// Result type returned by this crate.
pub type Result<T> = std::result::Result<T, RuntimeError>;

/// Typed failures from discovery, sandbox preparation, or a running turn.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    /// The caller supplied a request that cannot be executed safely.
    #[error("invalid {field}: {message}")]
    InvalidRequest {
        /// Field whose value was rejected.
        field: &'static str,
        /// Actionable validation message.
        message: String,
    },
    /// No adapter for the requested provider was compiled or registered.
    #[error("the {provider} adapter is not enabled")]
    AdapterUnavailable {
        /// Requested provider.
        provider: Provider,
    },
    /// The provider executable could not be found.
    #[error("{provider} is not installed; expected executable `{executable}` on PATH")]
    ExecutableNotFound {
        /// Requested provider.
        provider: Provider,
        /// Expected executable name or path.
        executable: String,
    },
    /// A child process could not be started.
    #[error("failed to start {provider} using {program}: {source}")]
    Spawn {
        /// Requested provider.
        provider: Provider,
        /// Program that failed to start.
        program: PathBuf,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
    /// Communication with a child process failed.
    #[error("{provider} {stream} failed: {source}")]
    ProcessIo {
        /// Requested provider.
        provider: Provider,
        /// Stream or operation that failed.
        stream: &'static str,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
    /// The configured local or remote execution transport failed.
    #[error("{provider} execution transport failed: {source}")]
    Transport {
        /// Provider whose turn was being prepared or executed.
        provider: Provider,
        /// Typed transport failure suitable for application recovery UI.
        #[source]
        source: crate::TransportError,
    },
    /// A transport cannot provide a capability required by this provider turn.
    #[error("{transport} cannot run {provider}: missing {capability} ({message})")]
    TransportCapabilityUnavailable {
        /// Provider whose command requires the capability.
        provider: Provider,
        /// Configured transport name.
        transport: String,
        /// Stable capability identifier suitable for application control flow.
        capability: &'static str,
        /// Actionable explanation for the user.
        message: String,
    },
    /// A provider emitted a malformed or unsupported protocol frame.
    #[error("{provider} protocol error: {message}")]
    Protocol {
        /// Provider that emitted the frame.
        provider: Provider,
        /// Bounded, non-secret diagnostic.
        message: String,
    },
    /// The application event sink stopped accepting events.
    #[error("event sink rejected a {provider} event: {message}")]
    EventSink {
        /// Provider whose event was rejected.
        provider: Provider,
        /// Sink-provided context.
        message: String,
    },
    /// A turn exceeded its configured deadline.
    #[error("{provider} turn timed out after {seconds} seconds")]
    Timeout {
        /// Timed-out provider.
        provider: Provider,
        /// Configured deadline.
        seconds: u64,
    },
    /// A turn was cancelled by its owner.
    #[error("{provider} turn was cancelled")]
    Cancelled {
        /// Cancelled provider.
        provider: Provider,
    },
    /// The provider process exited unsuccessfully.
    #[error("{provider} failed with code {exit_code:?} ({kind:?}): {stderr}")]
    ProcessFailed {
        /// Failed provider.
        provider: Provider,
        /// Stable user-actionable failure category.
        kind: ProviderProcessErrorKind,
        /// Exit code, or `None` when terminated by a signal.
        exit_code: Option<i32>,
        /// Bounded, redacted diagnostic from a native failure frame or stderr.
        stderr: String,
        /// Optional bounded provider-native error code.
        provider_code: Option<String>,
        /// Whether the provider acknowledged or may have received the turn.
        delivery: DeliveryState,
    },
    /// Sandbox capability validation or preparation failed.
    #[error("sandbox error: {0}")]
    Sandbox(#[from] crate::SandboxError),
}

pub(crate) fn classify_provider_failure(diagnostic: &str) -> ProviderProcessErrorKind {
    let diagnostic = diagnostic.to_ascii_lowercase();
    if diagnostic.contains("401 unauthorized")
        || diagnostic.contains("not logged in")
        || diagnostic.contains("authentication failed")
        || diagnostic.contains("invalid api key")
        || diagnostic.contains("authentication_error")
    {
        ProviderProcessErrorKind::AuthenticationFailed
    } else if diagnostic.contains("403 forbidden") || diagnostic.contains("permission denied") {
        ProviderProcessErrorKind::PermissionDenied
    } else if diagnostic.contains("model not found")
        || diagnostic.contains("unknown model")
        || diagnostic.contains("does not have access to model")
        || diagnostic.contains("model_not_found")
    {
        ProviderProcessErrorKind::ModelUnavailable
    } else if diagnostic.contains("429")
        || diagnostic.contains("rate limit")
        || diagnostic.contains("usage limit")
        || diagnostic.contains("rate_limit")
    {
        ProviderProcessErrorKind::RateLimited
    } else if diagnostic.contains("could not resolve")
        || diagnostic.contains("connection refused")
        || diagnostic.contains("connection timed out")
        || diagnostic.contains("failed to connect")
        || diagnostic.contains("invalid peer certificate")
    {
        ProviderProcessErrorKind::Network
    } else {
        ProviderProcessErrorKind::Unknown
    }
}
