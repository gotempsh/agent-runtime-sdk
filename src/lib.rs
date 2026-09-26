//! A provider-neutral runtime for installed coding-agent CLIs.
//!
//! `temps-agent-runtime` supervises Claude Code, Codex, and OpenCode as child
//! processes and translates their provider-specific JSON streams into one
//! typed [`TurnEvent`] model. Applications retain ownership of persistence,
//! UI, HTTP APIs, authorization, and scheduling.
//!
//! The [`network`] module defines provider-neutral private-network lifecycle
//! contracts. The optional [`nono`] module manages and applies Nono profiles without
//! silently falling back to unsandboxed execution. The optional [`tailnet`]
//! module implements those contracts for Tailscale and gives an agent its own
//! network identity through a supervised userspace `tailscaled`.

mod adapter;
mod chat;
mod discovery;
mod error;
mod extensions;
mod interactions;
pub mod journal;
pub mod lifecycle;
pub mod network;
mod process;
pub mod protocol;
pub mod protocol_client;
pub mod protocol_host;
pub mod protocol_stream;
pub mod relay;
pub mod retained;
mod runtime;
mod sandbox;
pub mod services;
pub mod startup;
pub mod transport;
mod types;
mod url_security;

#[cfg(feature = "ssh")]
mod ssh;

#[cfg(feature = "temps-sandbox")]
mod temps_sandbox;

pub mod providers;

#[cfg(feature = "nono")]
pub mod nono;

#[cfg(feature = "tailnet")]
pub mod tailnet;

pub use adapter::{
    AccountUsageProbeSpec, AdapterOutput, AdapterState, AgentAdapter, AuthenticationProbeSpec,
    CatalogProbeSpec, CommandSpec, InteractionRequest, ProtocolStreams, ProviderTerminalFailure,
};
pub use chat::{
    Chat, ChatApproval, ChatAttachment, ChatCommit, ChatEvent, ChatEventData, ChatMessage,
    ChatPage, ChatQueueStore, ChatRole, ChatStatus, ChatStore, QueuedChatMessage,
    QueuedChatMessagePage, StoredApprovalDecision, StoredChat,
};
pub use discovery::{
    HarnessAuthentication, HarnessAuthenticationStatus, HarnessCatalogError,
    HarnessCatalogErrorKind, HarnessCatalogStatus, HarnessControlGroup, HarnessControlKind,
    HarnessControlOption, HarnessInventory, HarnessLimitation, HarnessModel, HarnessModelCatalog,
    HarnessReadiness, HarnessReasoningEffort, HarnessServiceTier, HarnessStatus,
    ProviderProbeContext,
};
pub use error::{ProviderProcessErrorKind, Result, RuntimeError};
pub use extensions::{
    HarnessExtensionAccess, HarnessExtensionDenialKind, HarnessExtensionInventory,
    HarnessExtensionQuery, HarnessExtensionScope, HarnessExtensionWarning, HarnessMcpDefinition,
    HarnessMcpServer, HarnessSkill, McpServerManagementRequest, SkillManagementRequest,
};
pub use interactions::{InteractionBroker, InteractionBrokerError, InteractionResolution};
pub use runtime::{AgentRuntime, AgentRuntimeBuilder, CodexProcessRetention};
pub use sandbox::{
    ResolvedSandboxProfile, SandboxBackend, SandboxCapabilities, SandboxContext, SandboxError,
    SandboxPathAccess, SandboxProfileChange, SandboxProfileManager, SandboxProfileRef,
    SandboxProfileUpdate, SandboxRecoveryDecision, SandboxRecoveryHandler, SandboxRecoveryPolicy,
    SandboxRecoveryRequest, SandboxRequest, SandboxResource, SandboxViolation,
};
pub use services::{
    ManagedProcessError, ManagedProcessEvent, ManagedProcessEventStream, ManagedProcessHandle,
    ManagedProcessId, ManagedProcessLogLine, ManagedProcessResult, ManagedProcessSnapshot,
    ManagedProcessSpec, ManagedProcessStatus, ManagedProcessStreamError, ManagedProcessSupervisor,
    ManagedProcessSupervisorBuilder, RestartPolicy,
};
#[cfg(feature = "ssh")]
pub use ssh::{SshAuthentication, SshHostKeyPolicy, SshTransport, SshTransportBuilder};
#[cfg(feature = "temps-sandbox")]
pub use temps_sandbox::{TempsSandboxAuth, TempsSandboxTransport, TempsSandboxTransportBuilder};
pub use transport::{
    ExecutionTransport, LocalTransport, TransportCapabilities, TransportError, TransportErrorKind,
    TransportExitStatus, TransportProcess, TransportProcessControl, TransportProcessHandle,
    TransportReader, TransportReadinessRequest, TransportResult, TransportSpawnRequest,
    TransportWriter, WorkingDirectoryCandidates, WorkingDirectoryQuery,
};
pub use types::{
    AccountCredits, AccountUsageReport, AccountUsageSnapshot, AccountUsageStatus,
    AccountUsageWindow, AccountUsageWindowKind, AgentTask, AgentTaskActivity,
    AgentTaskActivityKind, AgentTaskUsage, ApprovalDecision, ApprovalRequest, AutoCompactionPolicy,
    CompactionTrigger, ContextCompaction, ContextWindowUsage, DenyAll, EventSink,
    InteractionHandler, LaunchContext, LaunchContextCapabilities, McpServerConfig, NoopEventSink,
    PermissionMode, PermissionSupport, Provider, ProviderReadiness, QuestionAnswer, QuestionOption,
    QuestionPrompt, QuestionRequest, RunStatus, SecretString, ToolCallStatus, ToolProcessPolicy,
    TurnCapabilities, TurnEvent, TurnProvenance, TurnRequest, TurnResult, Usage,
};

pub use startup::{StartupObserver, StartupStage, StartupTiming};
