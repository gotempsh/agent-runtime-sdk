//! Versioned wire contract for a remote runtime host.
//!
//! This module defines bounded, serializable messages independently from HTTP,
//! WebSocket, Unix-socket, or SSH forwarding choices. Authentication and
//! authorization remain responsibilities of the server embedding the host.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::lifecycle::{
    ConfigurationImpact, InterruptOutcome, InvocationId, RuntimeFailure, RuntimeHealth, RuntimeId,
};
use crate::retained::{
    DisposeOutcome, EventEnvelope, InteractionPolicy, RuntimeConfigurationKey,
    RuntimeDriverCapabilities, RuntimeInvocationKind, TurnAttachment,
};
use crate::{
    ApprovalDecision, AutoCompactionPolicy, LaunchContext, PermissionMode, Provider,
    QuestionAnswer, SandboxCapabilities, ToolProcessPolicy, TurnProvenance, TurnResult,
};

/// Initial remote-host protocol version.
pub const PROTOCOL_VERSION_V1: u16 = 1;
/// Adds provider-neutral system, tool, and MCP launch context.
pub const PROTOCOL_VERSION_V2: u16 = 2;
/// Adds typed user-versus-agent turn provenance.
pub const PROTOCOL_VERSION_V3: u16 = 3;
/// Adds context-window usage and automatic/manual compaction controls.
pub const PROTOCOL_VERSION_V4: u16 = 4;
/// Versions implemented by this SDK, ordered by preference.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[u16] = &[
    PROTOCOL_VERSION_V4,
    PROTOCOL_VERSION_V3,
    PROTOCOL_VERSION_V2,
    PROTOCOL_VERSION_V1,
];
/// Maximum encoded client or host frame size.
pub const MAX_PROTOCOL_FRAME_BYTES: usize = 2 * 1024 * 1024;
/// Default maximum events returned by one attach replay.
pub const DEFAULT_PROTOCOL_REPLAY_EVENTS: usize = 2_048;
const MAX_REQUEST_ID_BYTES: usize = 256;

/// Secret value explicitly intended for an authenticated protocol frame.
///
/// Debug output is always redacted. Serialization necessarily exposes the
/// value to the configured wire codec; callers must never send frames over an
/// unauthenticated or unencrypted channel.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolSecret(String);

impl ProtocolSecret {
    /// Wraps a value for explicit wire transport.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the secret to an authenticated remote-host implementation.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProtocolSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtocolSecret([REDACTED])")
    }
}

/// One environment variable transported to a remote runtime host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolEnvironmentVariable {
    /// Environment variable name.
    pub name: String,
    /// Secret-bearing value with redacted debug output.
    pub value: ProtocolSecret,
}

/// Host-resolved sandbox selection safe to put on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolSandboxSelection {
    /// Stable backend name configured on the remote host.
    pub backend: String,
    /// Optional application-managed profile identity.
    pub profile_id: Option<String>,
    /// Exact profile revision when the caller requires one.
    pub profile_revision: Option<String>,
    /// Capabilities the resolved backend must enforce.
    pub required_capabilities: SandboxCapabilities,
}

/// Serializable configuration for acquiring a remote retained runtime.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolRuntimeSpec {
    /// Stable runtime identity.
    pub runtime_id: RuntimeId,
    /// Provider hosted by the runtime.
    pub provider: Provider,
    /// Working directory meaningful on the remote execution host.
    pub working_directory: PathBuf,
    /// Default provider model.
    pub model: Option<String>,
    /// Default provider reasoning effort or mode.
    pub reasoning: Option<String>,
    /// Default permission policy.
    pub permission_mode: PermissionMode,
    /// Provider-native harness controls.
    pub harness_options: BTreeMap<String, String>,
    /// Default provider-neutral system, tool, and MCP launch context.
    #[serde(default)]
    pub launch_context: LaunchContext,
    /// Default automatic context-compaction policy.
    #[serde(default)]
    pub auto_compaction: AutoCompactionPolicy,
    /// Existing provider-native session to resume.
    pub provider_session_id: Option<String>,
    /// Default turn deadline in milliseconds.
    pub turn_timeout_ms: u64,
    /// Default interaction deadline in milliseconds.
    pub interaction_timeout_ms: u64,
    /// Default child-process lifetime policy.
    pub tool_process_policy: ToolProcessPolicy,
    /// Environment explicitly sent to the remote host.
    pub environment: Vec<ProtocolEnvironmentVariable>,
    /// Optional host-resolved sandbox selection.
    pub sandbox: Option<ProtocolSandboxSelection>,
}

impl fmt::Debug for ProtocolRuntimeSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtocolRuntimeSpec")
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
            .field("turn_timeout_ms", &self.turn_timeout_ms)
            .field("interaction_timeout_ms", &self.interaction_timeout_ms)
            .field("tool_process_policy", &self.tool_process_policy)
            .field(
                "environment_keys",
                &self
                    .environment
                    .iter()
                    .map(|variable| variable.name.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("sandbox", &self.sandbox)
            .finish_non_exhaustive()
    }
}

/// Serializable request for one remote invocation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolTurnInput {
    /// Stable invocation identity.
    pub invocation_id: InvocationId,
    /// Semantic operation represented by this invocation.
    #[serde(default)]
    pub invocation_kind: RuntimeInvocationKind,
    /// User prompt. Protocol logging must redact the full frame.
    pub prompt: String,
    /// Typed user-versus-agent provenance for this user-level prompt.
    #[serde(default)]
    pub provenance: TurnProvenance,
    /// Per-invocation model override.
    pub model: Option<String>,
    /// Per-invocation reasoning override.
    pub reasoning: Option<String>,
    /// Per-invocation permission override.
    pub permission_mode: Option<PermissionMode>,
    /// Per-invocation replacement for provider-native controls.
    pub harness_options: Option<BTreeMap<String, String>>,
    /// Per-invocation replacement for the runtime's launch context.
    #[serde(default)]
    pub launch_context: Option<LaunchContext>,
    /// Per-invocation automatic compaction override.
    #[serde(default)]
    pub auto_compaction: Option<AutoCompactionPolicy>,
    /// Optional provider turn limit.
    pub max_turns: Option<u32>,
    /// Per-invocation turn deadline override in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Per-invocation interaction deadline override in milliseconds.
    pub interaction_timeout_ms: Option<u64>,
    /// Per-invocation child-process lifetime override.
    pub tool_process_policy: Option<ToolProcessPolicy>,
    /// Additional environment explicitly sent for this invocation.
    pub environment: Vec<ProtocolEnvironmentVariable>,
    /// Files already staged on the remote execution host.
    pub attachments: Vec<TurnAttachment>,
    /// Required approval/question handling.
    pub interaction_policy: InteractionPolicy,
}

impl fmt::Debug for ProtocolTurnInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtocolTurnInput")
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
            .field("timeout_ms", &self.timeout_ms)
            .field("interaction_timeout_ms", &self.interaction_timeout_ms)
            .field("tool_process_policy", &self.tool_process_policy)
            .field(
                "environment_keys",
                &self
                    .environment
                    .iter()
                    .map(|variable| variable.name.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("attachment_count", &self.attachments.len())
            .field("interaction_policy", &self.interaction_policy)
            .finish_non_exhaustive()
    }
}

impl ProtocolTurnInput {
    /// Converts the optional turn deadline to a duration.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout_ms.map(Duration::from_millis)
    }

    /// Converts the optional interaction deadline to a duration.
    pub fn interaction_timeout(&self) -> Option<Duration> {
        self.interaction_timeout_ms.map(Duration::from_millis)
    }
}

/// Initial negotiation sent before lifecycle frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolHandshake {
    /// Protocol versions supported by the client, ordered by preference.
    pub supported_versions: Vec<u16>,
    /// Bounded client implementation name for diagnostics.
    pub client_name: String,
}

/// Negotiated host protocol and bounded host metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolHandshakeAccepted {
    /// Version selected by the host.
    pub selected_version: u16,
    /// Bounded host implementation name for diagnostics.
    pub host_name: String,
}

/// Client request carried after protocol negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClientRequest {
    /// Acquire a new retained runtime.
    Acquire {
        /// Serializable runtime configuration.
        spec: ProtocolRuntimeSpec,
    },
    /// Attach to a retained runtime and replay events after supplied cursors.
    Attach {
        /// Runtime to attach.
        runtime_id: RuntimeId,
        /// Last durably observed sequence for each invocation.
        #[serde(default)]
        replay_after: BTreeMap<InvocationId, u64>,
        /// Maximum replay envelopes to return before `ReplayComplete`.
        #[serde(default = "default_replay_events")]
        replay_limit: usize,
    },
    /// Start one invocation.
    StartTurn {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Invocation request.
        input: ProtocolTurnInput,
    },
    /// Interrupt one active invocation.
    Interrupt {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Invocation to interrupt.
        invocation_id: InvocationId,
    },
    /// Dispose one runtime.
    Dispose {
        /// Runtime to dispose.
        runtime_id: RuntimeId,
    },
    /// Fetch point-in-time runtime health.
    Health {
        /// Runtime to inspect.
        runtime_id: RuntimeId,
    },
    /// Resolve a pending provider approval.
    ResolveApproval {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Owning invocation.
        invocation_id: InvocationId,
        /// Provider interaction identifier.
        interaction_id: String,
        /// Authorized application decision.
        decision: ApprovalDecision,
    },
    /// Resolve a pending provider question.
    ResolveQuestion {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Owning invocation.
        invocation_id: InvocationId,
        /// Provider interaction identifier.
        interaction_id: String,
        /// Authorized application answer.
        answer: QuestionAnswer,
    },
    /// Decline a pending provider question.
    DeclineQuestion {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Owning invocation.
        invocation_id: InvocationId,
        /// Provider interaction identifier.
        interaction_id: String,
    },
}

/// Versioned request frame with an idempotent request identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFrame {
    /// Negotiated protocol version.
    pub version: u16,
    /// Client-generated idempotency and response-correlation identifier.
    pub request_id: String,
    /// Lifecycle request.
    pub request: ClientRequest,
}

/// Successful response to a lifecycle request.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
// Wire frames are encoded immediately; boxing the terminal result would make
// the public protocol API less ergonomic without improving transport bounds.
#[allow(clippy::large_enum_variant)]
pub enum HostResponse {
    /// Runtime was acquired or attached.
    RuntimeReady {
        /// Provider owned by the retained runtime.
        provider: Provider,
        /// Current runtime health.
        health: RuntimeHealth,
        /// Provider-driver behavior.
        driver: RuntimeDriverCapabilities,
        /// Update behavior for every runtime configuration field.
        configuration_impacts: BTreeMap<RuntimeConfigurationKey, ConfigurationImpact>,
    },
    /// Invocation was accepted and will emit events.
    TurnAccepted {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Accepted invocation.
        invocation_id: InvocationId,
    },
    /// Invocation reached its terminal result.
    TurnCompleted {
        /// Owning runtime.
        runtime_id: RuntimeId,
        /// Completed invocation.
        invocation_id: InvocationId,
        /// Provider-neutral terminal result.
        result: TurnResult,
    },
    /// Interruption request reached a terminal outcome.
    Interrupted {
        /// Interruption outcome.
        outcome: InterruptOutcome,
    },
    /// Runtime disposal result.
    Disposed {
        /// Disposal outcome.
        outcome: DisposeOutcome,
    },
    /// Point-in-time runtime health.
    Health {
        /// Current runtime health.
        health: RuntimeHealth,
    },
    /// Replay has caught up to the host's current event tail.
    ReplayComplete {
        /// Runtime whose replay completed.
        runtime_id: RuntimeId,
        /// More matching events remain after this bounded page.
        truncated: bool,
    },
    /// An approval or question response reached the live provider waiter.
    InteractionResolved {
        /// Provider interaction identifier.
        interaction_id: String,
    },
}

impl fmt::Debug for HostResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeReady {
                provider,
                health,
                driver,
                configuration_impacts,
            } => formatter
                .debug_struct("RuntimeReady")
                .field("runtime_id", &health.runtime_id)
                .field("provider", provider)
                .field("status", &health.status)
                .field("driver", driver)
                .field("configuration_impacts", configuration_impacts)
                .finish(),
            Self::TurnAccepted {
                runtime_id,
                invocation_id,
            } => formatter
                .debug_struct("TurnAccepted")
                .field("runtime_id", runtime_id)
                .field("invocation_id", invocation_id)
                .finish(),
            Self::TurnCompleted {
                runtime_id,
                invocation_id,
                ..
            } => formatter
                .debug_struct("TurnCompleted")
                .field("runtime_id", runtime_id)
                .field("invocation_id", invocation_id)
                .field("result", &"[REDACTED]")
                .finish(),
            Self::Interrupted { outcome } => formatter
                .debug_struct("Interrupted")
                .field("outcome", outcome)
                .finish(),
            Self::Disposed { outcome } => formatter
                .debug_struct("Disposed")
                .field("outcome", outcome)
                .finish(),
            Self::Health { health } => formatter
                .debug_struct("Health")
                .field("runtime_id", &health.runtime_id)
                .field("status", &health.status)
                .finish(),
            Self::ReplayComplete {
                runtime_id,
                truncated,
            } => formatter
                .debug_struct("ReplayComplete")
                .field("runtime_id", runtime_id)
                .field("truncated", truncated)
                .finish(),
            Self::InteractionResolved { interaction_id } => formatter
                .debug_struct("InteractionResolved")
                .field("interaction_id", interaction_id)
                .finish(),
        }
    }
}

/// Host frame correlated to a client request or emitted asynchronously.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum HostFrame {
    /// Successful response to one client frame.
    Response {
        /// Matching client request identifier.
        request_id: String,
        /// Typed response.
        response: HostResponse,
    },
    /// Typed failure to one client frame.
    Failure {
        /// Matching client request identifier.
        request_id: String,
        /// Failure including delivery and retry semantics.
        failure: RuntimeFailure,
    },
    /// Ordered invocation event independent from request/response completion.
    Event {
        /// Versioned normalized event.
        envelope: EventEnvelope,
    },
}

impl fmt::Debug for HostFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response {
                request_id,
                response,
            } => formatter
                .debug_struct("Response")
                .field("request_id", request_id)
                .field("response", response)
                .finish(),
            Self::Failure {
                request_id,
                failure,
            } => formatter
                .debug_struct("Failure")
                .field("request_id", request_id)
                .field("kind", &failure.kind)
                .field("delivery", &failure.delivery)
                .field("retry", &failure.retry)
                .finish_non_exhaustive(),
            Self::Event { envelope } => formatter
                .debug_struct("Event")
                .field("runtime_id", &envelope.runtime_id)
                .field("invocation_id", &envelope.invocation_id)
                .field("sequence", &envelope.sequence)
                .field("payload", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Typed failure while encoding or decoding a remote-host frame.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolCodecError {
    /// A frame exceeds the SDK's bounded maximum.
    #[error("protocol frame is {actual} bytes; the maximum is {maximum}")]
    FrameTooLarge {
        /// Maximum accepted size.
        maximum: usize,
        /// Actual encoded size.
        actual: usize,
    },
    /// JSON encoding failed.
    #[error("could not encode protocol frame: {source}")]
    Encode {
        /// Serialization failure.
        #[source]
        source: serde_json::Error,
    },
    /// JSON decoding failed.
    #[error("could not decode protocol frame: {source}")]
    Decode {
        /// Deserialization failure.
        #[source]
        source: serde_json::Error,
    },
    /// The frame does not use a negotiated version.
    #[error("protocol version {version} is not supported")]
    UnsupportedVersion {
        /// Unsupported frame version.
        version: u16,
    },
    /// A request uses a feature that an older negotiated protocol cannot enforce.
    #[error(
        "protocol feature `{feature}` requires version {required}, but the frame uses {actual}"
    )]
    FeatureRequiresVersion {
        /// Stable feature name.
        feature: &'static str,
        /// First protocol version implementing the feature.
        required: u16,
        /// Frame version supplied by the client.
        actual: u16,
    },
    /// The request correlation identifier is invalid.
    #[error("protocol request id {message}")]
    InvalidRequestId {
        /// Actionable validation detail.
        message: &'static str,
    },
}

/// Selects the first client-preferred version supported by this SDK.
pub fn negotiate_version(client_versions: &[u16]) -> Option<u16> {
    client_versions
        .iter()
        .copied()
        .find(|version| SUPPORTED_PROTOCOL_VERSIONS.contains(version))
}

/// Encodes a bounded client frame as JSON.
pub fn encode_client_frame(frame: &ClientFrame) -> Result<Vec<u8>, ProtocolCodecError> {
    validate_client_frame(frame)?;
    encode_bounded(frame)
}

/// Decodes and validates a bounded client frame.
pub fn decode_client_frame(bytes: &[u8]) -> Result<ClientFrame, ProtocolCodecError> {
    validate_frame_size(bytes.len())?;
    let frame = serde_json::from_slice::<ClientFrame>(bytes)
        .map_err(|source| ProtocolCodecError::Decode { source })?;
    validate_client_frame(&frame)?;
    Ok(frame)
}

/// Encodes a bounded host frame as JSON.
pub fn encode_host_frame(frame: &HostFrame) -> Result<Vec<u8>, ProtocolCodecError> {
    encode_bounded(frame)
}

/// Decodes a bounded host frame.
pub fn decode_host_frame(bytes: &[u8]) -> Result<HostFrame, ProtocolCodecError> {
    validate_frame_size(bytes.len())?;
    serde_json::from_slice(bytes).map_err(|source| ProtocolCodecError::Decode { source })
}

fn validate_client_frame(frame: &ClientFrame) -> Result<(), ProtocolCodecError> {
    if !SUPPORTED_PROTOCOL_VERSIONS.contains(&frame.version) {
        return Err(ProtocolCodecError::UnsupportedVersion {
            version: frame.version,
        });
    }
    if frame.request_id.is_empty() {
        return Err(ProtocolCodecError::InvalidRequestId {
            message: "cannot be empty",
        });
    }
    if frame.request_id.len() > MAX_REQUEST_ID_BYTES {
        return Err(ProtocolCodecError::InvalidRequestId {
            message: "exceeds 256 bytes",
        });
    }
    if frame.request_id.contains('\0') {
        return Err(ProtocolCodecError::InvalidRequestId {
            message: "cannot contain a NUL byte",
        });
    }
    let has_launch_context = match &frame.request {
        ClientRequest::Acquire { spec } => spec.launch_context != LaunchContext::default(),
        ClientRequest::StartTurn { input, .. } => input
            .launch_context
            .as_ref()
            .is_some_and(|context| context != &LaunchContext::default()),
        _ => false,
    };
    if has_launch_context && frame.version < PROTOCOL_VERSION_V2 {
        return Err(ProtocolCodecError::FeatureRequiresVersion {
            feature: "launch_context",
            required: PROTOCOL_VERSION_V2,
            actual: frame.version,
        });
    }
    let has_agent_provenance = matches!(
        &frame.request,
        ClientRequest::StartTurn { input, .. }
            if matches!(input.provenance, TurnProvenance::Agent(_))
    );
    if has_agent_provenance && frame.version < PROTOCOL_VERSION_V3 {
        return Err(ProtocolCodecError::FeatureRequiresVersion {
            feature: "agent_relay_provenance",
            required: PROTOCOL_VERSION_V3,
            actual: frame.version,
        });
    }
    let has_context_compaction = match &frame.request {
        ClientRequest::Acquire { spec } => {
            spec.auto_compaction != AutoCompactionPolicy::ProviderDefault
        }
        ClientRequest::StartTurn { input, .. } => {
            input.invocation_kind == RuntimeInvocationKind::ManualCompaction
                || input.auto_compaction.is_some()
        }
        _ => false,
    };
    if has_context_compaction && frame.version < PROTOCOL_VERSION_V4 {
        return Err(ProtocolCodecError::FeatureRequiresVersion {
            feature: "context_compaction",
            required: PROTOCOL_VERSION_V4,
            actual: frame.version,
        });
    }
    Ok(())
}

fn encode_bounded<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolCodecError> {
    let bytes =
        serde_json::to_vec(value).map_err(|source| ProtocolCodecError::Encode { source })?;
    validate_frame_size(bytes.len())?;
    Ok(bytes)
}

fn validate_frame_size(actual: usize) -> Result<(), ProtocolCodecError> {
    if actual > MAX_PROTOCOL_FRAME_BYTES {
        return Err(ProtocolCodecError::FrameTooLarge {
            maximum: MAX_PROTOCOL_FRAME_BYTES,
            actual,
        });
    }
    Ok(())
}

const fn default_replay_events() -> usize {
    DEFAULT_PROTOCOL_REPLAY_EVENTS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_id() -> RuntimeId {
        RuntimeId::new("runtime-1").expect("valid runtime identifier")
    }

    fn invocation_id() -> InvocationId {
        InvocationId::new("invocation-1").expect("valid invocation identifier")
    }

    fn turn_frame() -> ClientFrame {
        ClientFrame {
            version: PROTOCOL_VERSION_V2,
            request_id: "request-1".to_owned(),
            request: ClientRequest::StartTurn {
                runtime_id: runtime_id(),
                input: ProtocolTurnInput {
                    invocation_id: invocation_id(),
                    invocation_kind: RuntimeInvocationKind::Turn,
                    prompt: "inspect the runtime".to_owned(),
                    provenance: TurnProvenance::default(),
                    model: Some("opus".to_owned()),
                    reasoning: Some("high".to_owned()),
                    permission_mode: Some(PermissionMode::Default),
                    harness_options: None,
                    launch_context: Some(LaunchContext {
                        system_prompt_append: Some("private standing instructions".into()),
                        allowed_tools: Some(vec!["Read".into()]),
                        mcp_servers: BTreeMap::from([(
                            "fleet".into(),
                            crate::McpServerConfig::Http {
                                url: "https://runtime.invalid/mcp?tenant=private".into(),
                                headers_from: BTreeMap::from([(
                                    "Authorization".into(),
                                    "API_TOKEN".into(),
                                )]),
                            },
                        )]),
                        strict_mcp_config: true,
                    }),
                    auto_compaction: None,
                    max_turns: None,
                    timeout_ms: Some(5_000),
                    interaction_timeout_ms: Some(30_000),
                    tool_process_policy: None,
                    environment: vec![ProtocolEnvironmentVariable {
                        name: "API_TOKEN".to_owned(),
                        value: ProtocolSecret::new("wire-secret"),
                    }],
                    attachments: vec![TurnAttachment::new("uploads/report.pdf")],
                    interaction_policy: InteractionPolicy::RequireHandler,
                },
            },
        }
    }

    #[test]
    fn negotiates_the_client_preferred_supported_version() {
        assert_eq!(negotiate_version(&[9, 2, 1]), Some(2));
        assert_eq!(negotiate_version(&[9, 1, 0]), Some(1));
        assert_eq!(negotiate_version(&[9, 8]), None);
    }

    #[test]
    fn client_frames_round_trip_without_debugging_secrets() {
        let frame = turn_frame();
        let debug = format!("{frame:?}");
        assert!(!debug.contains("wire-secret"));
        assert!(!debug.contains("inspect the runtime"));
        assert!(!debug.contains("uploads/report.pdf"));
        assert!(!debug.contains("private standing instructions"));
        assert!(!debug.contains("runtime.invalid"));

        let bytes = encode_client_frame(&frame).expect("encode client frame");
        assert!(String::from_utf8_lossy(&bytes).contains("wire-secret"));
        let decoded = decode_client_frame(&bytes).expect("decode client frame");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn rejects_unsupported_versions_and_invalid_request_ids() {
        let mut frame = turn_frame();
        frame.version = 9;
        assert!(matches!(
            encode_client_frame(&frame),
            Err(ProtocolCodecError::UnsupportedVersion { version: 9 })
        ));

        frame.version = PROTOCOL_VERSION_V2;
        frame.request_id.clear();
        assert!(matches!(
            encode_client_frame(&frame),
            Err(ProtocolCodecError::InvalidRequestId { .. })
        ));
    }

    #[test]
    fn protocol_v1_rejects_launch_context_instead_of_ignoring_enforcement() {
        let mut frame = turn_frame();
        frame.version = PROTOCOL_VERSION_V1;

        assert!(matches!(
            encode_client_frame(&frame),
            Err(ProtocolCodecError::FeatureRequiresVersion {
                feature: "launch_context",
                required: PROTOCOL_VERSION_V2,
                actual: PROTOCOL_VERSION_V1,
            })
        ));
    }

    #[test]
    fn protocol_v2_rejects_agent_provenance_instead_of_dropping_it() {
        let mut frame = turn_frame();
        frame.version = PROTOCOL_VERSION_V2;
        let ClientRequest::StartTurn { input, .. } = &mut frame.request else {
            panic!("expected start turn frame")
        };
        input.launch_context = None;
        input.provenance = TurnProvenance::Agent(crate::relay::AgentMessageProvenance {
            message_id: crate::relay::AgentMessageId::new("message-1").unwrap(),
            thread_id: crate::relay::AgentThreadId::new("thread-1").unwrap(),
            sender: crate::relay::AgentAddress::new("project-a/agent-a").unwrap(),
            recipient: crate::relay::AgentAddress::new("project-b/agent-b").unwrap(),
            reply_to: None,
            hop_count: 1,
            hop_limit: 4,
        });

        assert!(matches!(
            encode_client_frame(&frame),
            Err(ProtocolCodecError::FeatureRequiresVersion {
                feature: "agent_relay_provenance",
                required: PROTOCOL_VERSION_V3,
                actual: PROTOCOL_VERSION_V2,
            })
        ));
    }

    #[test]
    fn protocol_v3_rejects_context_compaction_controls_instead_of_dropping_them() {
        let mut frame = turn_frame();
        frame.version = PROTOCOL_VERSION_V3;
        let ClientRequest::StartTurn { input, .. } = &mut frame.request else {
            panic!("expected start turn frame")
        };
        input.launch_context = None;
        input.invocation_kind = RuntimeInvocationKind::ManualCompaction;

        assert!(matches!(
            encode_client_frame(&frame),
            Err(ProtocolCodecError::FeatureRequiresVersion {
                feature: "context_compaction",
                required: PROTOCOL_VERSION_V4,
                actual: PROTOCOL_VERSION_V3,
            })
        ));
    }

    #[test]
    fn protocol_v4_round_trips_context_compaction_controls() {
        let mut frame = turn_frame();
        frame.version = PROTOCOL_VERSION_V4;
        let ClientRequest::StartTurn { input, .. } = &mut frame.request else {
            panic!("expected start turn frame")
        };
        input.invocation_kind = RuntimeInvocationKind::ManualCompaction;
        input.auto_compaction = Some(AutoCompactionPolicy::TokenThreshold { tokens: 250_000 });

        let encoded = encode_client_frame(&frame).expect("encode protocol v4");
        let decoded = decode_client_frame(&encoded).expect("decode protocol v4");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn rejects_oversized_frames_before_json_decoding() {
        let oversized = vec![b'x'; MAX_PROTOCOL_FRAME_BYTES + 1];
        assert!(matches!(
            decode_client_frame(&oversized),
            Err(ProtocolCodecError::FrameTooLarge { .. })
        ));
    }
}
