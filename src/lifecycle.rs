//! Provider-neutral runtime lifecycle and failure contracts.
//!
//! These types describe execution without assigning persistence, tenancy, or
//! user-interface responsibilities to the runtime crate.

use std::fmt;

use serde::{de, Deserialize, Deserializer, Serialize};

const MAX_IDENTIFIER_BYTES: usize = 256;

/// An error returned when a runtime-owned identifier is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeIdentifierError {
    /// The identifier was empty.
    #[error("{kind} identifier cannot be empty")]
    Empty {
        /// The identifier kind.
        kind: &'static str,
    },
    /// The identifier contained a NUL byte.
    #[error("{kind} identifier cannot contain a NUL byte")]
    ContainsNul {
        /// The identifier kind.
        kind: &'static str,
    },
    /// The identifier exceeded the protocol-safe size limit.
    #[error("{kind} identifier is {actual} bytes; the maximum is {maximum}")]
    TooLong {
        /// The identifier kind.
        kind: &'static str,
        /// The maximum accepted byte length.
        maximum: usize,
        /// The provided byte length.
        actual: usize,
    },
}

fn validate_identifier(
    kind: &'static str,
    value: String,
) -> Result<String, RuntimeIdentifierError> {
    if value.is_empty() {
        return Err(RuntimeIdentifierError::Empty { kind });
    }
    if value.contains('\0') {
        return Err(RuntimeIdentifierError::ContainsNul { kind });
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(RuntimeIdentifierError::TooLong {
            kind,
            maximum: MAX_IDENTIFIER_BYTES,
            actual: value.len(),
        });
    }
    Ok(value)
}

/// Stable identity for one retained provider runtime.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RuntimeId(String);

impl RuntimeId {
    /// Creates a validated runtime identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeIdentifierError> {
        validate_identifier("runtime", value.into()).map(Self)
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RuntimeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Stable identity for one turn invocation within a runtime.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct InvocationId(String);

impl InvocationId {
    /// Creates a validated invocation identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeIdentifierError> {
        validate_identifier("invocation", value.into()).map(Self)
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InvocationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for InvocationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Current state of a retained runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuntimeStatus {
    /// The provider process is being started.
    Starting,
    /// The runtime can accept a turn.
    Ready,
    /// The runtime is processing a turn.
    Busy,
    /// The runtime remains usable with reduced capability.
    Degraded,
    /// The runtime is shutting down.
    Stopping,
    /// The runtime has stopped cleanly.
    Stopped,
    /// The runtime cannot continue without intervention.
    Failed,
}

/// A point-in-time health report for a retained runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeHealth {
    /// Runtime being described.
    pub runtime_id: RuntimeId,
    /// Current lifecycle status.
    pub status: RuntimeStatus,
    /// Milliseconds since the Unix epoch when this report was observed.
    pub observed_at_unix_ms: u64,
    /// Optional bounded, user-safe status detail.
    pub detail: Option<String>,
}

/// Whether a requested configuration change can be applied to a runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ConfigurationImpact {
    /// The change is applied without restarting the provider process.
    Live,
    /// The provider runtime must be restarted before the change takes effect.
    RestartRequired,
    /// A new runtime must be acquired with the requested configuration.
    ReacquireRequired,
    /// The provider does not support this configuration.
    Unsupported,
}

/// Result of requesting interruption of an invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InterruptOutcome {
    /// The runtime confirmed that the invocation was interrupted.
    Interrupted,
    /// The invocation had already reached a terminal state.
    AlreadyFinished,
    /// The owning runtime had already been disposed.
    RuntimeDisposed,
    /// Interruption was requested but could not be confirmed.
    Unconfirmed,
}

/// How far a turn request progressed before a failure was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryState {
    /// The SDK knows the provider did not receive the request.
    NotSent,
    /// The SDK cannot determine whether the provider accepted the request.
    PossiblySent,
    /// The provider acknowledged or began processing the request.
    Accepted,
}

/// Machine-readable recovery guidance for a runtime failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RetryAdvice {
    /// Repeating the operation cannot resolve this failure.
    Never,
    /// Repeating the operation immediately is safe.
    Immediate,
    /// Retry after the indicated minimum delay.
    After {
        /// Minimum delay in milliseconds.
        milliseconds: u64,
    },
    /// Reconnect to the execution target and reattach to the runtime first.
    ReconnectAndAttach,
    /// User input or an external configuration change is required.
    RequiresUserAction,
}

/// Provider-neutral category for a runtime failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuntimeFailureKind {
    /// A runtime with the requested identity already exists.
    RuntimeAlreadyExists,
    /// The requested runtime could not be found.
    RuntimeNotFound,
    /// The runtime has already been disposed.
    RuntimeDisposed,
    /// The runtime is already processing another invocation.
    RuntimeBusy,
    /// This invocation identity was already accepted by the runtime.
    InvocationAlreadyExists,
    /// The request was structurally or semantically invalid.
    InvalidRequest,
    /// Authentication was missing, invalid, or expired.
    Authentication,
    /// The operation was not permitted.
    Permission,
    /// The selected model or provider capability was unavailable.
    CapabilityUnavailable,
    /// The provider rejected work because of a rate limit.
    RateLimited,
    /// The execution target could not be reached.
    Network,
    /// The execution transport failed after connecting.
    Transport,
    /// Provider output violated the expected protocol.
    ProviderProtocol,
    /// The provider process failed to start or exited unexpectedly.
    ProviderProcess,
    /// A bounded operation exceeded its deadline.
    Timeout,
    /// The invocation was cancelled or interrupted.
    Cancelled,
    /// The SDK cannot safely classify the failure further.
    Indeterminate,
}

/// Typed failure returned by lifecycle and turn operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct RuntimeFailure {
    /// Runtime associated with the failure, if one had been acquired.
    pub runtime_id: Option<RuntimeId>,
    /// Invocation associated with the failure, if one had been created.
    pub invocation_id: Option<InvocationId>,
    /// Stable provider-neutral failure category.
    pub kind: RuntimeFailureKind,
    /// Recovery guidance for callers and durable schedulers.
    pub retry: RetryAdvice,
    /// Whether the failed turn may already have reached the provider.
    pub delivery: DeliveryState,
    /// Bounded, redacted message suitable for logs and user feedback.
    pub message: String,
    /// Optional provider-native code retained for diagnostics.
    pub provider_code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_id_rejects_invalid_values() {
        assert!(matches!(
            RuntimeId::new(""),
            Err(RuntimeIdentifierError::Empty { kind: "runtime" })
        ));
        assert!(matches!(
            RuntimeId::new("bad\0id"),
            Err(RuntimeIdentifierError::ContainsNul { kind: "runtime" })
        ));
        assert!(matches!(
            RuntimeId::new("x".repeat(MAX_IDENTIFIER_BYTES + 1)),
            Err(RuntimeIdentifierError::TooLong {
                kind: "runtime",
                maximum: MAX_IDENTIFIER_BYTES,
                actual
            }) if actual == MAX_IDENTIFIER_BYTES + 1
        ));
    }

    #[test]
    fn deserialization_preserves_identifier_validation() {
        let valid = serde_json::from_str::<InvocationId>("\"turn-42\"");
        assert_eq!(
            valid.expect("valid invocation identifier").as_str(),
            "turn-42"
        );

        let empty = serde_json::from_str::<InvocationId>("\"\"");
        assert!(empty.is_err());
    }

    #[test]
    fn runtime_failure_round_trips_with_retry_and_delivery_context() {
        let failure = RuntimeFailure {
            runtime_id: Some(RuntimeId::new("runtime-1").expect("valid runtime identifier")),
            invocation_id: Some(
                InvocationId::new("invocation-7").expect("valid invocation identifier"),
            ),
            kind: RuntimeFailureKind::Network,
            retry: RetryAdvice::ReconnectAndAttach,
            delivery: DeliveryState::PossiblySent,
            message: "SSH connection ended before acknowledgement".to_owned(),
            provider_code: Some("connection_lost".to_owned()),
        };

        let encoded = serde_json::to_string(&failure).expect("serialize runtime failure");
        let decoded =
            serde_json::from_str::<RuntimeFailure>(&encoded).expect("deserialize runtime failure");
        assert_eq!(decoded, failure);
    }
}
