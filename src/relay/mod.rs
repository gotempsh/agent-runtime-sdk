//! Provider-neutral contracts for application-owned agent-to-agent relay.
//!
//! The SDK defines messages, grants, delivery state, and the router boundary.
//! It does not store an agent directory, persist messages, schedule recipient
//! turns, or connect agents directly. Applications implement
//! [`AgentMessageRouter`] and expose a scoped capability only for turns that
//! should be able to use relay tools.

use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

mod inbound;
mod mcp;

pub use inbound::{inbound_turn_input, inbound_turn_request, render_inbound_agent_message};
pub use mcp::{
    AgentRelayActivitySink, AgentRelayMcpBridge, AgentRelayMcpExposure, AgentRelayMcpToolResult,
    AGENT_RELAY_MCP_SERVER_NAME, AGENT_RELAY_TOOL_DISCOVER, AGENT_RELAY_TOOL_REPLY,
    AGENT_RELAY_TOOL_SEND, AGENT_RELAY_TOOL_STATUS,
};

/// Initial schema version for durable relay envelopes.
pub const AGENT_MESSAGE_SCHEMA_VERSION: u16 = 1;
/// Maximum size of a relay identifier.
pub const MAX_RELAY_IDENTIFIER_BYTES: usize = 256;
/// Maximum entries in relay metadata.
pub const MAX_RELAY_METADATA_ENTRIES: usize = 32;
/// Maximum encoded size of relay metadata.
pub const MAX_RELAY_METADATA_BYTES: usize = 8 * 1024;
/// Maximum number of attachment references on one message.
pub const MAX_AGENT_MESSAGE_ATTACHMENTS: usize = 16;
/// Absolute SDK ceiling for one message body. Grants can impose a lower limit.
pub const MAX_AGENT_MESSAGE_BYTES: usize = 256 * 1024;
/// Absolute SDK ceiling for a message TTL.
pub const MAX_AGENT_MESSAGE_TTL_SECONDS: u32 = 7 * 24 * 60 * 60;

/// Validation error for a provider-neutral relay contract.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AgentRelayValidationError {
    /// A required identifier was empty.
    #[error("{kind} cannot be empty")]
    EmptyIdentifier {
        /// Identifier category.
        kind: &'static str,
    },
    /// An identifier was not safe to carry across line-oriented protocols.
    #[error("{kind} cannot contain NUL or newline characters")]
    InvalidIdentifierCharacters {
        /// Identifier category.
        kind: &'static str,
    },
    /// An identifier exceeded the wire-safe size ceiling.
    #[error("{kind} is {actual} bytes; the maximum is {maximum}")]
    IdentifierTooLong {
        /// Identifier category.
        kind: &'static str,
        /// Maximum accepted bytes.
        maximum: usize,
        /// Actual bytes.
        actual: usize,
    },
    /// Metadata has too many keys.
    #[error("relay metadata has {actual} entries; the maximum is {maximum}")]
    TooManyMetadataEntries {
        /// Maximum accepted entries.
        maximum: usize,
        /// Actual entries.
        actual: usize,
    },
    /// Metadata exceeded its encoded size limit.
    #[error("relay metadata is {actual} bytes; the maximum is {maximum}")]
    MetadataTooLarge {
        /// Maximum accepted bytes.
        maximum: usize,
        /// Actual encoded bytes.
        actual: usize,
    },
    /// A message field violated a bounded relay contract.
    #[error("invalid relay field `{field}`: {message}")]
    InvalidField {
        /// Stable field name.
        field: &'static str,
        /// Actionable validation detail.
        message: String,
    },
}

fn validate_identifier(
    kind: &'static str,
    value: String,
) -> Result<String, AgentRelayValidationError> {
    if value.is_empty() {
        return Err(AgentRelayValidationError::EmptyIdentifier { kind });
    }
    if value.contains(['\0', '\n', '\r']) {
        return Err(AgentRelayValidationError::InvalidIdentifierCharacters { kind });
    }
    if value.len() > MAX_RELAY_IDENTIFIER_BYTES {
        return Err(AgentRelayValidationError::IdentifierTooLong {
            kind,
            maximum: MAX_RELAY_IDENTIFIER_BYTES,
            actual: value.len(),
        });
    }
    Ok(value)
}

macro_rules! relay_identifier {
    ($name:ident, $kind:literal, $description:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a validated identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, AgentRelayValidationError> {
                validate_identifier($kind, value.into()).map(Self)
            }

            /// Returns the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

relay_identifier!(
    AgentAddress,
    "agent address",
    "Stable logical address for a top-level agent. This is intentionally distinct from a runtime or provider session identifier."
);
relay_identifier!(
    AgentMessageId,
    "agent message id",
    "Application-assigned durable identity for one relay message."
);
relay_identifier!(
    AgentThreadId,
    "agent thread id",
    "Application-assigned identity used to correlate a relay conversation."
);
relay_identifier!(
    AgentIdempotencyKey,
    "agent message idempotency key",
    "Sender-scoped key used to reconcile retries after an ambiguous broker response."
);

/// Bounded, application-defined metadata carried by relay records.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgentRelayMetadata(BTreeMap<String, Value>);

impl AgentRelayMetadata {
    /// Validates and wraps application metadata.
    pub fn new(values: BTreeMap<String, Value>) -> Result<Self, AgentRelayValidationError> {
        if values.len() > MAX_RELAY_METADATA_ENTRIES {
            return Err(AgentRelayValidationError::TooManyMetadataEntries {
                maximum: MAX_RELAY_METADATA_ENTRIES,
                actual: values.len(),
            });
        }
        for key in values.keys() {
            validate_identifier("relay metadata key", key.clone())?;
        }
        let bytes = serde_json::to_vec(&values).map_err(|error| {
            AgentRelayValidationError::InvalidField {
                field: "metadata",
                message: error.to_string(),
            }
        })?;
        if bytes.len() > MAX_RELAY_METADATA_BYTES {
            return Err(AgentRelayValidationError::MetadataTooLarge {
                maximum: MAX_RELAY_METADATA_BYTES,
                actual: bytes.len(),
            });
        }
        Ok(Self(values))
    }

    /// Returns the validated metadata map.
    pub fn as_map(&self) -> &BTreeMap<String, Value> {
        &self.0
    }

    /// Consumes the wrapper and returns the metadata map.
    pub fn into_map(self) -> BTreeMap<String, Value> {
        self.0
    }
}

impl Serialize for AgentRelayMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AgentRelayMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = BTreeMap::<String, Value>::deserialize(deserializer)?;
        Self::new(values).map_err(de::Error::custom)
    }
}

/// Opaque attachment reference resolved by the host application.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMessageAttachment {
    /// Stable application attachment identity.
    pub id: String,
    /// Application-resolvable URI or opaque reference. The SDK never fetches it.
    pub uri: String,
    /// Optional display name.
    pub name: Option<String>,
    /// Optional media type.
    pub media_type: Option<String>,
    /// Optional content digest for reconciliation.
    pub digest: Option<String>,
    /// Bounded application metadata.
    #[serde(default)]
    pub metadata: AgentRelayMetadata,
}

impl AgentMessageAttachment {
    fn validate(&self) -> Result<(), AgentRelayValidationError> {
        validate_identifier("attachment id", self.id.clone())?;
        if self.uri.is_empty() || self.uri.len() > 4_096 || self.uri.contains(['\0', '\n', '\r']) {
            return Err(AgentRelayValidationError::InvalidField {
                field: "message.attachments.uri",
                message: "must be non-empty, at most 4096 bytes, and contain no NUL or newlines"
                    .to_owned(),
            });
        }
        for (field, value) in [
            ("message.attachments.name", self.name.as_deref()),
            ("message.attachments.media_type", self.media_type.as_deref()),
            ("message.attachments.digest", self.digest.as_deref()),
        ] {
            if value.is_some_and(|value| value.len() > 512 || value.contains(['\0', '\n', '\r'])) {
                return Err(AgentRelayValidationError::InvalidField {
                    field,
                    message: "must be at most 512 bytes and contain no NUL or newlines".to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// User-level content carried between independent top-level agents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMessage {
    /// Message body. It is never a system instruction.
    pub content: String,
    /// Host-resolved attachment references.
    #[serde(default)]
    pub attachments: Vec<AgentMessageAttachment>,
    /// Bounded application metadata.
    #[serde(default)]
    pub metadata: AgentRelayMetadata,
}

impl AgentMessage {
    /// Creates a text-only agent message.
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            attachments: Vec::new(),
            metadata: AgentRelayMetadata::default(),
        }
    }

    /// Validates absolute SDK bounds. A [`MessagingGrant`] may be stricter.
    pub fn validate(&self) -> Result<(), AgentRelayValidationError> {
        if self.content.is_empty() || self.content.len() > MAX_AGENT_MESSAGE_BYTES {
            return Err(AgentRelayValidationError::InvalidField {
                field: "message.content",
                message: format!("must be non-empty and at most {MAX_AGENT_MESSAGE_BYTES} bytes"),
            });
        }
        if self.content.contains('\0') {
            return Err(AgentRelayValidationError::InvalidField {
                field: "message.content",
                message: "cannot contain NUL".to_owned(),
            });
        }
        if self.attachments.len() > MAX_AGENT_MESSAGE_ATTACHMENTS {
            return Err(AgentRelayValidationError::InvalidField {
                field: "message.attachments",
                message: format!("may contain at most {MAX_AGENT_MESSAGE_ATTACHMENTS} references"),
            });
        }
        for attachment in &self.attachments {
            attachment.validate()?;
        }
        Ok(())
    }
}

/// Durable envelope produced and persisted by the host application.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMessageEnvelope {
    /// Envelope schema version.
    pub schema_version: u16,
    /// Durable application-assigned message identity.
    pub message_id: AgentMessageId,
    /// Sender-scoped retry reconciliation key.
    pub idempotency_key: AgentIdempotencyKey,
    /// Stable relay thread.
    pub thread_id: AgentThreadId,
    /// Earlier message being answered, when any.
    pub reply_to: Option<AgentMessageId>,
    /// Logical sender derived from a host capability.
    pub sender: AgentAddress,
    /// Logical recipient authorized by the host.
    pub recipient: AgentAddress,
    /// User-level message content.
    pub message: AgentMessage,
    /// Host observation time in Unix milliseconds.
    pub created_at_unix_ms: u64,
    /// Time-to-live selected for this message.
    pub ttl_seconds: u32,
    /// Number of relay edges traversed so far.
    pub hop_count: u8,
    /// Maximum relay edges allowed for this chain.
    pub hop_limit: u8,
}

impl AgentMessageEnvelope {
    /// Validates the portable envelope and its absolute bounds.
    pub fn validate(&self) -> Result<(), AgentRelayValidationError> {
        if self.schema_version != AGENT_MESSAGE_SCHEMA_VERSION {
            return Err(AgentRelayValidationError::InvalidField {
                field: "schema_version",
                message: format!(
                    "unsupported version {}; expected {AGENT_MESSAGE_SCHEMA_VERSION}",
                    self.schema_version
                ),
            });
        }
        self.message.validate()?;
        if self.ttl_seconds == 0 || self.ttl_seconds > MAX_AGENT_MESSAGE_TTL_SECONDS {
            return Err(AgentRelayValidationError::InvalidField {
                field: "ttl_seconds",
                message: format!("must be between 1 and {MAX_AGENT_MESSAGE_TTL_SECONDS} seconds"),
            });
        }
        if self.hop_limit == 0 || self.hop_count > self.hop_limit {
            return Err(AgentRelayValidationError::InvalidField {
                field: "hop_count",
                message: "hop limit must be positive and cannot be lower than hop count".to_owned(),
            });
        }
        Ok(())
    }

    /// Returns whether the TTL had elapsed at the supplied Unix time.
    pub fn is_expired_at(&self, now_unix_ms: u64) -> bool {
        let ttl_ms = u64::from(self.ttl_seconds).saturating_mul(1_000);
        now_unix_ms >= self.created_at_unix_ms.saturating_add(ttl_ms)
    }
}

/// Typed provenance attached to a recipient turn without message content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessageProvenance {
    /// Durable relay message identity.
    pub message_id: AgentMessageId,
    /// Relay thread identity.
    pub thread_id: AgentThreadId,
    /// Logical sender.
    pub sender: AgentAddress,
    /// Logical recipient.
    pub recipient: AgentAddress,
    /// Earlier message being answered, when any.
    pub reply_to: Option<AgentMessageId>,
    /// Current relay hop count.
    pub hop_count: u8,
    /// Relay hop ceiling.
    pub hop_limit: u8,
}

impl From<&AgentMessageEnvelope> for AgentMessageProvenance {
    fn from(envelope: &AgentMessageEnvelope) -> Self {
        Self {
            message_id: envelope.message_id.clone(),
            thread_id: envelope.thread_id.clone(),
            sender: envelope.sender.clone(),
            recipient: envelope.recipient.clone(),
            reply_to: envelope.reply_to.clone(),
            hop_count: envelope.hop_count,
            hop_limit: envelope.hop_limit,
        }
    }
}

/// Current durable delivery state. Relay delivery is at least once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentDeliveryStatus {
    /// The application durably accepted and queued the message.
    Queued,
    /// A host worker is attempting recipient dispatch.
    Dispatching,
    /// The recipient runtime acknowledged the inbound turn.
    Delivered,
    /// Delivery was permanently rejected.
    Rejected,
    /// The TTL elapsed before delivery.
    Expired,
}

/// Reconciled durable receipt returned by send, reply, and status operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDeliveryReceipt {
    /// Canonical durable message identity.
    pub message_id: AgentMessageId,
    /// Sender-scoped retry reconciliation key.
    pub idempotency_key: AgentIdempotencyKey,
    /// Relay thread identity.
    pub thread_id: AgentThreadId,
    /// Latest durable delivery state.
    pub status: AgentDeliveryStatus,
    /// Number of dispatch attempts recorded by the host.
    pub attempts: u32,
    /// Host observation time in Unix milliseconds.
    pub updated_at_unix_ms: u64,
    /// Bounded, redacted operational detail.
    pub detail: Option<String>,
}

/// Normalized relay lifecycle operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRelayActivityKind {
    /// An authorized directory query completed.
    Discovery,
    /// A new message was submitted to the durable router.
    Send,
    /// An explicit reply was submitted to the durable router.
    Reply,
    /// Delivery state was reconciled.
    Status,
    /// A grant, authorization, or application policy rejected an operation.
    Rejected,
    /// A hop, message, byte, or rate ceiling stopped an operation.
    LimitReached,
}

/// Bounded relay activity suitable for an application event stream.
///
/// Message content, attachment URIs, idempotency keys, and capability secrets
/// are deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRelayActivity {
    /// Operation that was observed.
    pub kind: AgentRelayActivityKind,
    /// Host-authenticated logical sender.
    pub sender: AgentAddress,
    /// Recipient when the operation names one.
    pub recipient: Option<AgentAddress>,
    /// Canonical message identity when known.
    pub message_id: Option<AgentMessageId>,
    /// Thread identity when known.
    pub thread_id: Option<AgentThreadId>,
    /// Latest durable delivery state when known.
    pub delivery_status: Option<AgentDeliveryStatus>,
    /// Typed failure category when the operation failed.
    pub error_kind: Option<AgentRelayErrorKind>,
    /// Bounded, redacted detail suitable for a user-facing activity timeline.
    pub detail: Option<String>,
}

/// Address selector used by discovery, send, and receive grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentAddressPattern {
    /// Match one exact logical address.
    Exact(AgentAddress),
    /// Match logical addresses beginning with this validated prefix.
    Prefix(String),
}

impl AgentAddressPattern {
    /// Creates a validated prefix selector.
    pub fn prefix(value: impl Into<String>) -> Result<Self, AgentRelayValidationError> {
        validate_identifier("agent address prefix", value.into()).map(Self::Prefix)
    }

    /// Returns whether an address is inside this selector.
    pub fn matches(&self, address: &AgentAddress) -> bool {
        match self {
            Self::Exact(exact) => exact == address,
            Self::Prefix(prefix) => address.as_str().starts_with(prefix),
        }
    }

    fn validate(&self) -> Result<(), AgentRelayValidationError> {
        if let Self::Prefix(prefix) = self {
            validate_identifier("agent address prefix", prefix.clone())?;
        }
        Ok(())
    }
}

/// Scoped discovery permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDiscoveryGrant {
    /// Address ranges visible to the caller.
    pub addresses: Vec<AgentAddressPattern>,
    /// Maximum entries returned by one query.
    pub max_results: u16,
}

/// Scoped sending permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSendGrant {
    /// Authorized recipient address ranges.
    pub recipients: Vec<AgentAddressPattern>,
}

/// Scoped receive permission applied before dispatching an inbound turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReceiveGrant {
    /// Authorized sender address ranges.
    pub senders: Vec<AgentAddressPattern>,
}

/// Approval policy attached to a messaging capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentMessageApproval {
    /// The grant itself is sufficient for the scoped operation.
    NotRequired,
    /// The application decides whether a particular operation needs approval.
    #[default]
    HostPolicy,
    /// The application must record approval for every send or reply.
    Required,
}

/// Loop, message, rate, and byte ceilings for one scoped capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessagingLimits {
    /// Maximum hop count accepted or emitted.
    pub max_hops: u8,
    /// Maximum UTF-8 message body bytes.
    pub max_message_bytes: usize,
    /// Maximum send/reply operations per turn capability.
    pub max_messages_per_turn: u32,
    /// Maximum cumulative message bytes per turn capability.
    pub max_bytes_per_turn: usize,
    /// Optional rolling message rate ceiling.
    pub max_messages_per_minute: Option<u32>,
    /// Maximum message TTL.
    pub max_ttl_seconds: u32,
}

impl Default for AgentMessagingLimits {
    fn default() -> Self {
        Self {
            max_hops: 4,
            max_message_bytes: 64 * 1024,
            max_messages_per_turn: 16,
            max_bytes_per_turn: 256 * 1024,
            max_messages_per_minute: Some(30),
            max_ttl_seconds: 24 * 60 * 60,
        }
    }
}

/// Complete host-issued messaging grant. The default grants no relay access.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagingGrant {
    /// Optional scoped discovery access. `None` hides discovery tools.
    pub discovery: Option<AgentDiscoveryGrant>,
    /// Optional scoped send access. `None` hides send and reply tools.
    pub send: Option<AgentSendGrant>,
    /// Optional scoped receive access. `None` rejects inbound relay turns.
    pub receive: Option<AgentReceiveGrant>,
    /// Application approval behavior.
    pub approval: AgentMessageApproval,
    /// Mandatory loop and budget boundaries.
    pub limits: AgentMessagingLimits,
}

impl MessagingGrant {
    /// Validates grant selectors and non-zero safety ceilings.
    pub fn validate(&self) -> Result<(), AgentRelayValidationError> {
        for pattern in self
            .discovery
            .iter()
            .flat_map(|grant| grant.addresses.iter())
            .chain(self.send.iter().flat_map(|grant| grant.recipients.iter()))
            .chain(self.receive.iter().flat_map(|grant| grant.senders.iter()))
        {
            pattern.validate()?;
        }
        if self
            .discovery
            .as_ref()
            .is_some_and(|grant| grant.max_results == 0)
        {
            return Err(AgentRelayValidationError::InvalidField {
                field: "discovery.max_results",
                message: "must be greater than zero".to_owned(),
            });
        }
        let limits = &self.limits;
        if limits.max_hops == 0
            || limits.max_message_bytes == 0
            || limits.max_message_bytes > MAX_AGENT_MESSAGE_BYTES
            || limits.max_messages_per_turn == 0
            || limits.max_bytes_per_turn < limits.max_message_bytes
            || limits.max_messages_per_minute == Some(0)
            || limits.max_ttl_seconds == 0
            || limits.max_ttl_seconds > MAX_AGENT_MESSAGE_TTL_SECONDS
        {
            return Err(AgentRelayValidationError::InvalidField {
                field: "limits",
                message: "limits must be non-zero, internally consistent, and within SDK ceilings"
                    .to_owned(),
            });
        }
        Ok(())
    }

    /// Returns whether discovery may reveal this address.
    pub fn permits_discovery(&self, address: &AgentAddress) -> bool {
        self.discovery.as_ref().is_some_and(|grant| {
            grant
                .addresses
                .iter()
                .any(|pattern| pattern.matches(address))
        })
    }

    /// Returns whether the scoped sender may address this recipient.
    pub fn permits_send_to(&self, recipient: &AgentAddress) -> bool {
        self.send.as_ref().is_some_and(|grant| {
            grant
                .recipients
                .iter()
                .any(|pattern| pattern.matches(recipient))
        })
    }

    /// Returns whether the recipient capability accepts this sender.
    pub fn permits_receive_from(&self, sender: &AgentAddress) -> bool {
        self.receive
            .as_ref()
            .is_some_and(|grant| grant.senders.iter().any(|pattern| pattern.matches(sender)))
    }
}

/// Availability hint returned by application-owned discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentAvailability {
    /// The host can schedule a turn immediately.
    Online,
    /// The recipient is executing another turn; durable messages remain queued.
    Busy,
    /// No harness is currently reachable; durable messages remain queued.
    Offline,
    /// The directory cannot currently determine availability.
    Unknown,
}

/// One application-owned agent directory entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDirectoryEntry {
    /// Stable logical address.
    pub address: AgentAddress,
    /// Human-readable label.
    pub display_name: String,
    /// Bounded description of the agent's role or capability.
    pub description: Option<String>,
    /// Non-authoritative scheduling hint.
    pub availability: AgentAvailability,
    /// Bounded application metadata.
    #[serde(default)]
    pub metadata: AgentRelayMetadata,
}

/// Bounded, cursor-based agent discovery query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDiscoveryQuery {
    /// Optional application-defined search text.
    pub query: Option<String>,
    /// Opaque application cursor.
    pub cursor: Option<String>,
    /// Requested result limit, bounded again by the grant and router.
    pub limit: u16,
}

/// Page returned by application-owned discovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDirectoryPage {
    /// Authorized directory entries.
    pub agents: Vec<AgentDirectoryEntry>,
    /// Opaque cursor for another page.
    pub next_cursor: Option<String>,
}

/// Host-derived routing context supplied to [`AgentMessageRouter`].
///
/// Tool arguments never contain this value. A bridge binds it after authenticating
/// the turn capability, which prevents a harness from selecting its own sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRelayContext {
    sender: AgentAddress,
    capability_id: String,
    inbound: Option<AgentMessageProvenance>,
    approval: AgentMessageApproval,
}

impl AgentRelayContext {
    /// Creates context for a user-originated turn.
    pub fn for_turn(
        sender: AgentAddress,
        capability_id: impl Into<String>,
        approval: AgentMessageApproval,
    ) -> Result<Self, AgentRelayValidationError> {
        let capability_id = validate_identifier("relay capability id", capability_id.into())?;
        Ok(Self {
            sender,
            capability_id,
            inbound: None,
            approval,
        })
    }

    /// Creates context for a turn triggered by a received relay envelope.
    pub fn for_inbound_turn(
        sender: AgentAddress,
        capability_id: impl Into<String>,
        approval: AgentMessageApproval,
        inbound: AgentMessageProvenance,
    ) -> Result<Self, AgentRelayValidationError> {
        if inbound.recipient != sender {
            return Err(AgentRelayValidationError::InvalidField {
                field: "inbound.recipient",
                message: "must match the scoped agent address".to_owned(),
            });
        }
        let mut context = Self::for_turn(sender, capability_id, approval)?;
        context.inbound = Some(inbound);
        Ok(context)
    }

    /// Returns the host-authenticated logical sender.
    pub fn sender(&self) -> &AgentAddress {
        &self.sender
    }

    /// Returns the non-secret capability audit identifier.
    pub fn capability_id(&self) -> &str {
        &self.capability_id
    }

    /// Returns provenance for the inbound message that caused this turn.
    pub fn inbound(&self) -> Option<&AgentMessageProvenance> {
        self.inbound.as_ref()
    }

    /// Returns the host-selected approval policy.
    pub fn approval(&self) -> AgentMessageApproval {
        self.approval
    }

    /// Returns the next host-derived hop count for an outbound message.
    pub fn next_hop_count(&self) -> u8 {
        self.inbound
            .as_ref()
            .map_or(0, |inbound| inbound.hop_count.saturating_add(1))
    }
}

/// Application request to durably enqueue a new agent message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendAgentMessageRequest {
    /// Authorized recipient.
    pub recipient: AgentAddress,
    /// Sender-scoped idempotency key. Retries must reuse it.
    pub idempotency_key: AgentIdempotencyKey,
    /// Optional existing thread. The router creates one when absent.
    pub thread_id: Option<AgentThreadId>,
    /// Optional message being correlated as a reply.
    pub reply_to: Option<AgentMessageId>,
    /// User-level message body and attachment references.
    pub message: AgentMessage,
    /// Requested TTL in seconds.
    pub ttl_seconds: u32,
    /// Host-derived hop count. MCP callers cannot set this field.
    pub hop_count: u8,
    /// Grant-derived hop ceiling. MCP callers cannot set this field.
    pub hop_limit: u8,
}

/// Application request to reply to a durable inbound message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplyToAgentMessageRequest {
    /// Message being answered. The router resolves its sender and thread.
    pub in_reply_to: AgentMessageId,
    /// Sender-scoped idempotency key. Retries must reuse it.
    pub idempotency_key: AgentIdempotencyKey,
    /// User-level reply body and attachment references.
    pub message: AgentMessage,
    /// Requested TTL in seconds.
    pub ttl_seconds: u32,
    /// Host-derived hop count. MCP callers cannot set this field.
    pub hop_count: u8,
    /// Grant-derived hop ceiling. MCP callers cannot set this field.
    pub hop_limit: u8,
}

/// Reconciliation query accepted after a safe retry or ambiguous response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentDeliveryQuery {
    /// Query the canonical message identity.
    MessageId(AgentMessageId),
    /// Query the sender-scoped idempotency key.
    IdempotencyKey(AgentIdempotencyKey),
}

/// How far a failed relay operation may have progressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRelayDeliveryState {
    /// The router confirms nothing durable was accepted.
    NotAccepted,
    /// The router durably accepted the operation.
    Accepted,
    /// The caller must reconcile because acceptance is ambiguous.
    PossiblyAccepted,
}

/// Machine-readable retry guidance for relay failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRelayRetryAdvice {
    /// Repeating the operation cannot resolve the failure.
    Never,
    /// Repeating with the same idempotency key is safe immediately.
    Immediate,
    /// Retry with the same idempotency key after a minimum delay.
    After {
        /// Minimum delay in milliseconds.
        milliseconds: u64,
    },
    /// Look up status by message ID or idempotency key before retrying.
    Reconcile,
    /// Host approval or another external change is required.
    RequiresUserAction,
}

/// Stable provider-neutral relay failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRelayErrorKind {
    /// Tool input or a portable contract was invalid.
    InvalidRequest,
    /// The grant or host policy does not authorize this recipient.
    UnauthorizedRecipient,
    /// The recipient's receive grant does not authorize this sender.
    UnauthorizedSender,
    /// A required host approval is absent.
    ApprovalRequired,
    /// The directory has no matching recipient.
    RecipientNotFound,
    /// A requested message or idempotency record was not found.
    MessageNotFound,
    /// The capability exceeded its rolling rate boundary.
    RateLimited,
    /// The per-turn message or byte budget was exhausted.
    BudgetExceeded,
    /// Another relay edge would exceed the hop ceiling.
    HopLimitExceeded,
    /// The message TTL elapsed.
    Expired,
    /// The application relay or target harness is temporarily unavailable.
    Unavailable,
    /// Acceptance is ambiguous and requires reconciliation.
    Indeterminate,
}

/// Typed error returned by an application-owned relay router or tool bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct AgentRelayError {
    /// Stable failure category.
    pub kind: AgentRelayErrorKind,
    /// Retry guidance.
    pub retry: AgentRelayRetryAdvice,
    /// Whether durable acceptance may have occurred.
    pub delivery: AgentRelayDeliveryState,
    /// Bounded, redacted explanation.
    pub message: String,
    /// Canonical message ID when one is known.
    pub message_id: Option<AgentMessageId>,
}

impl AgentRelayError {
    /// Creates a permanent, pre-acceptance rejection.
    pub fn permanent(kind: AgentRelayErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            retry: AgentRelayRetryAdvice::Never,
            delivery: AgentRelayDeliveryState::NotAccepted,
            message: message.into(),
            message_id: None,
        }
    }
}

/// Result returned by relay contracts.
pub type AgentRelayResult<T> = Result<T, AgentRelayError>;

/// Durable routing boundary implemented by the embedding application.
///
/// Implementations own the agent directory, authorization, optional approval,
/// idempotency index, queues, dispatch scheduling, delivery attempts, and
/// persistence. `send` and `reply` must durably record their idempotency key
/// before reporting `Queued`. Delivery is at least once; implementations must
/// never claim exactly-once side effects at the recipient.
#[async_trait]
pub trait AgentMessageRouter: Send + Sync {
    /// Discovers agents visible to this scoped sender.
    async fn discover(
        &self,
        context: &AgentRelayContext,
        query: AgentDiscoveryQuery,
    ) -> AgentRelayResult<AgentDirectoryPage>;

    /// Authorizes and durably enqueues a new message.
    async fn send(
        &self,
        context: &AgentRelayContext,
        request: SendAgentMessageRequest,
    ) -> AgentRelayResult<AgentDeliveryReceipt>;

    /// Authorizes and durably enqueues an explicit reply.
    async fn reply(
        &self,
        context: &AgentRelayContext,
        request: ReplyToAgentMessageRequest,
    ) -> AgentRelayResult<AgentDeliveryReceipt>;

    /// Reconciles the latest durable state by canonical ID or idempotency key.
    async fn delivery_status(
        &self,
        context: &AgentRelayContext,
        query: AgentDeliveryQuery,
    ) -> AgentRelayResult<AgentDeliveryReceipt>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(value: &str) -> AgentAddress {
        AgentAddress::new(value).expect("valid address")
    }

    #[test]
    fn logical_addresses_are_validated_and_distinct_from_runtime_ids() {
        assert_eq!(
            address("tenant-a/project-b/reviewer").as_str(),
            "tenant-a/project-b/reviewer"
        );
        assert!(AgentAddress::new("").is_err());
        assert!(AgentAddress::new("bad\naddress").is_err());
    }

    #[test]
    fn metadata_bounds_are_enforced_during_deserialization() {
        let oversized = serde_json::json!({"value": "x".repeat(MAX_RELAY_METADATA_BYTES)});
        assert!(serde_json::from_value::<AgentRelayMetadata>(oversized).is_err());
    }

    #[test]
    fn deny_by_default_grant_requires_explicit_scopes() {
        let grant = MessagingGrant::default();
        assert!(!grant.permits_send_to(&address("team/agent-b")));
        assert!(!grant.permits_receive_from(&address("team/agent-a")));
        assert!(!grant.permits_discovery(&address("team/agent-b")));
        grant.validate().expect("default safety limits are valid");
    }

    #[test]
    fn envelope_checks_ttl_hops_and_expiration() {
        let envelope = AgentMessageEnvelope {
            schema_version: AGENT_MESSAGE_SCHEMA_VERSION,
            message_id: AgentMessageId::new("message-1").unwrap(),
            idempotency_key: AgentIdempotencyKey::new("attempt-1").unwrap(),
            thread_id: AgentThreadId::new("thread-1").unwrap(),
            reply_to: None,
            sender: address("project-a/agent"),
            recipient: address("project-b/agent"),
            message: AgentMessage::text("Please inspect the API boundary."),
            created_at_unix_ms: 1_000,
            ttl_seconds: 10,
            hop_count: 0,
            hop_limit: 4,
        };
        envelope.validate().expect("valid envelope");
        assert!(!envelope.is_expired_at(10_999));
        assert!(envelope.is_expired_at(11_000));
    }

    #[test]
    fn inbound_context_derives_identity_and_next_hop() {
        let recipient = address("project-b/agent");
        let provenance = AgentMessageProvenance {
            message_id: AgentMessageId::new("message-1").unwrap(),
            thread_id: AgentThreadId::new("thread-1").unwrap(),
            sender: address("project-a/agent"),
            recipient: recipient.clone(),
            reply_to: None,
            hop_count: 2,
            hop_limit: 4,
        };
        let context = AgentRelayContext::for_inbound_turn(
            recipient,
            "capability-1",
            AgentMessageApproval::HostPolicy,
            provenance,
        )
        .expect("valid scoped context");
        assert_eq!(context.sender().as_str(), "project-b/agent");
        assert_eq!(context.next_hop_count(), 3);
    }
}
