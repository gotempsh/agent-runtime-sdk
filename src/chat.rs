//! Durable, application-owned chat persistence contracts.
//!
//! The runtime executes provider turns, but applications normally expose and
//! persist chats. A chat can contain any number of user and assistant messages
//! while retaining the provider session required to continue the conversation.

use std::collections::BTreeMap;
use std::error::Error;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ApprovalRequest, Provider, TurnEvent, TurnResult};

/// Durable state of the latest response in a chat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChatStatus {
    /// The chat has no response in flight.
    Idle,
    /// A response is waiting for an executor.
    Queued,
    /// A provider is producing a response.
    Running,
    /// The provider is waiting for an approval decision.
    ApprovalNeeded,
    /// The provider is waiting for an answer to a question.
    InputNeeded,
    /// The latest response completed successfully.
    Succeeded,
    /// The latest response failed.
    Failed,
    /// The latest response was cancelled.
    Cancelled,
    /// The latest response exceeded its deadline.
    TimedOut,
}

/// Durable chat metadata independent of any database implementation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chat {
    /// Application-assigned stable identifier.
    pub id: String,
    /// Human-readable title.
    pub title: String,
    /// Provider used by the latest response.
    pub provider: Provider,
    /// Latest response status.
    pub status: ChatStatus,
    /// Provider-native session used to continue this chat.
    pub session_id: Option<String>,
    /// Model reported or selected for the latest response.
    pub model: Option<String>,
    /// Monotonic revision used for optimistic concurrency.
    pub revision: u64,
    /// Creation time in Unix milliseconds.
    pub created_at_unix_ms: u64,
    /// Last durable update time in Unix milliseconds.
    pub updated_at_unix_ms: u64,
    /// Application-specific indexed or materialized metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

/// Author of a persisted chat message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChatRole {
    /// Human-authored input.
    User,
    /// Provider-authored response.
    Assistant,
    /// Application-authored context or diagnostic.
    System,
}

/// One durable message in a chat transcript.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Monotonic sequence within the chat.
    pub sequence: u64,
    /// Message author.
    pub role: ChatRole,
    /// Message body.
    pub content: String,
    /// Application-resolved attachment references associated with the message.
    #[serde(default)]
    pub attachments: Vec<ChatAttachment>,
    /// Creation time in Unix milliseconds.
    pub created_at_unix_ms: u64,
}

/// Attachment reference carried by a persisted or queued chat message.
///
/// The runtime does not fetch or upload the URI. Applications decide whether
/// it identifies a local path, object-store object, sandbox resource, or other
/// provider-visible input and resolve it before execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatAttachment {
    /// Application-assigned stable identifier.
    pub id: String,
    /// Human-readable filename or label.
    pub name: String,
    /// Application-resolvable URI or opaque resource reference.
    pub uri: String,
    /// Optional media type, such as `image/png` or `text/plain`.
    pub media_type: Option<String>,
    /// Application-specific attachment metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

/// One durable user message waiting for application-defined delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedChatMessage {
    /// Application-assigned stable queue item identifier.
    pub id: String,
    /// Chat that will receive the message.
    pub chat_id: String,
    /// User-authored message body.
    pub content: String,
    /// Replaceable attachment references.
    #[serde(default)]
    pub attachments: Vec<ChatAttachment>,
    /// Monotonic revision used for optimistic edits.
    pub revision: u64,
    /// Creation time in Unix milliseconds.
    pub created_at_unix_ms: u64,
    /// Last edit time in Unix milliseconds.
    pub updated_at_unix_ms: u64,
    /// Application-owned routing or scheduling metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

/// Cursor-based page of messages waiting for delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedChatMessagePage {
    /// Queue items in store-defined stable delivery order.
    pub messages: Vec<QueuedChatMessage>,
    /// Opaque cursor for the next page.
    pub next_cursor: Option<String>,
}

/// Application-visible event stored in the ordered chat stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChatEventData {
    /// Latest response status changed.
    StatusChanged {
        /// New status.
        status: ChatStatus,
    },
    /// Normalized event emitted by a provider invocation.
    Runtime {
        /// Provider-neutral runtime event.
        event: TurnEvent,
    },
    /// A provider invocation completed.
    ResponseCompleted {
        /// Provider-neutral terminal result.
        result: TurnResult,
    },
    /// A response failed outside the provider result protocol.
    ResponseFailed {
        /// Typed error code suitable for application handling.
        code: String,
        /// Safe human-readable diagnostic.
        message: String,
        /// Whether retry may succeed without changing the request.
        retryable: bool,
    },
    /// An application-derived plan was created.
    PlanCreated {
        /// Plan title.
        title: String,
        /// Ordered plan steps.
        steps: Vec<String>,
    },
    /// A message was appended to the transcript.
    MessageAppended {
        /// Sequence of the appended message.
        message_sequence: u64,
    },
    /// Application-specific normalized event.
    Application {
        /// Stable application event name.
        name: String,
        /// Structured event payload.
        payload: Value,
    },
}

/// One replayable event in a chat-scoped stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatEvent {
    /// Monotonic sequence within the chat.
    pub sequence: u64,
    /// Internal provider invocation identifier.
    ///
    /// This disambiguates approvals and repeated native identifiers without
    /// making invocations the public persistence unit.
    pub invocation_id: String,
    /// Event time in Unix milliseconds.
    pub occurred_at_unix_ms: u64,
    /// Normalized event payload.
    pub data: ChatEventData,
}

/// Durable approval audit record scoped to one provider invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatApproval {
    /// Internal provider invocation identifier.
    pub invocation_id: String,
    /// Provider approval request.
    pub request: ApprovalRequest,
    /// Persisted decision, or `None` while waiting.
    pub decision: Option<StoredApprovalDecision>,
    /// Last update time in Unix milliseconds.
    pub updated_at_unix_ms: u64,
}

/// Serializable approval decision for audit storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredApprovalDecision {
    /// The operation was allowed.
    Allowed,
    /// The operation was denied.
    Denied,
}

/// Complete bounded view returned when a chat is retrieved.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredChat {
    /// Chat metadata.
    pub chat: Chat,
    /// Ordered retained transcript.
    pub messages: Vec<ChatMessage>,
    /// Ordered retained event stream.
    pub events: Vec<ChatEvent>,
    /// Retained approval audit records.
    pub approvals: Vec<ChatApproval>,
}

/// Cursor-based page of chat summaries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatPage {
    /// Chat summaries in store-defined stable order.
    pub chats: Vec<Chat>,
    /// Opaque cursor for the next page.
    pub next_cursor: Option<String>,
}

/// Atomic mutation applied to an existing chat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatCommit {
    /// Materialized chat state after this commit.
    pub chat: Chat,
    /// Revision that must still be current before the commit.
    pub expected_revision: u64,
    /// Messages appended by this commit.
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    /// Events appended by this commit.
    #[serde(default)]
    pub events: Vec<ChatEvent>,
    /// Approval rows inserted or updated by this commit.
    #[serde(default)]
    pub approvals: Vec<ChatApproval>,
}

/// Application-supplied durable storage for persistent agent chats.
///
/// Implementations should atomically validate `expected_revision`, append all
/// rows in a [`ChatCommit`], and replace the materialized chat state. A stale
/// revision must return an implementation-specific conflict error.
#[async_trait]
pub trait ChatStore: Send + Sync {
    /// Store-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Create a chat and its first durable message atomically.
    async fn create_chat(
        &self,
        chat: &Chat,
        initial_message: &ChatMessage,
    ) -> Result<(), Self::Error>;

    /// Retrieve one chat with its retained transcript, events, and approvals.
    async fn load_chat(&self, chat_id: &str) -> Result<Option<StoredChat>, Self::Error>;

    /// List bounded chat summaries using an opaque cursor.
    async fn list_chats(&self, cursor: Option<&str>, limit: usize)
        -> Result<ChatPage, Self::Error>;

    /// Atomically update materialized state and append durable chat data.
    async fn commit(&self, commit: &ChatCommit) -> Result<(), Self::Error>;

    /// Replay chat events strictly after `sequence`, up to `limit` rows.
    async fn events_after(
        &self,
        chat_id: &str,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChatEvent>, Self::Error>;
}

/// Application-supplied durable queue for chat messages.
///
/// The SDK defines persistence and concurrency semantics but intentionally does
/// not choose a database, worker, transport, or destination. `pop` operations
/// must atomically remove and return an item so only one application worker can
/// deliver it. Applications that need leases can implement them behind this
/// trait and return an item only after successfully acquiring their lease.
#[async_trait]
pub trait ChatQueueStore: Send + Sync {
    /// Store-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Append a new item to its chat's durable queue.
    async fn enqueue(&self, message: &QueuedChatMessage) -> Result<(), Self::Error>;

    /// Retrieve bounded queued messages in stable delivery order.
    async fn list_queued(
        &self,
        chat_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<QueuedChatMessagePage, Self::Error>;

    /// Retrieve one queued message without removing it.
    async fn load_queued(
        &self,
        chat_id: &str,
        message_id: &str,
    ) -> Result<Option<QueuedChatMessage>, Self::Error>;

    /// Replace editable content and attachments using optimistic concurrency.
    ///
    /// `message.revision` is the new materialized revision and
    /// `expected_revision` must still be current when the update commits.
    async fn update_queued(
        &self,
        message: &QueuedChatMessage,
        expected_revision: u64,
    ) -> Result<(), Self::Error>;

    /// Atomically remove and return a specific item for immediate delivery.
    async fn pop_queued(
        &self,
        chat_id: &str,
        message_id: &str,
    ) -> Result<Option<QueuedChatMessage>, Self::Error>;

    /// Atomically remove and return the next item in delivery order.
    async fn pop_next_queued(
        &self,
        chat_id: &str,
    ) -> Result<Option<QueuedChatMessage>, Self::Error>;
}
