mod store;

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use temps_agent_runtime::lifecycle::{InvocationId, RuntimeFailure, RuntimeFailureKind, RuntimeId};
use temps_agent_runtime::retained::{
    CompactionInput, EventEnvelope, InProcessRuntimeClient, RuntimeClient, RuntimeEvent,
    RuntimeHandle, RuntimeSpec,
};
use temps_agent_runtime::{
    AccountUsageSnapshot, AdapterOutput, AdapterState, AgentAdapter, AgentRuntime,
    ApprovalDecision, ApprovalRequest, ChatAttachment, ChatQueueStore as DurableChatQueueStore,
    CommandSpec, ContextWindowUsage, EventSink,
    ExecutionTransport, HarnessExtensionInventory, HarnessExtensionQuery, HarnessExtensionScope,
    HarnessInventory, HarnessMcpDefinition, InteractionBroker, InteractionHandler,
    InteractionRequest, LocalTransport, McpServerManagementRequest, PermissionMode,
    PermissionSupport, Provider, ProviderReadiness, QuestionAnswer, QuestionRequest,
    QueuedChatMessage, RunStatus, RuntimeError, SecretString, SkillManagementRequest,
    SshHostKeyPolicy, SshTransport, TempsSandboxAuth, TempsSandboxTransport, ToolCallStatus,
    TransportError, TransportErrorKind, TransportSpawnRequest, TurnEvent, TurnRequest, TurnResult,
    Usage,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use store::{ChatStore, SqliteChatStore, StoreError};

const MAX_EVENTS_PER_CHAT: usize = 2_000;
const MAX_LISTED_CHATS: usize = 200;
const MAX_UPLOAD_BYTES: usize = 10 * 1024 * 1024;

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("__demo_provider") {
        return demo_provider_process().await;
    }
    if std::env::args().any(|argument| argument == "--version") {
        println!("agent-runtime-demo-provider 0.1.0");
        return ExitCode::SUCCESS;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "agent_runtime_fullstack_example=info,tower_http=info".into()),
        )
        .init();

    let state = match AppState::new().await {
        Ok(state) => state,
        Err(error) => {
            eprintln!("could not initialize runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let web_dist = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web/dist");
    let static_files =
        ServeDir::new(&web_dist).not_found_service(ServeFile::new(web_dist.join("index.html")));
    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/discovery", post(discover_harnesses))
        .route("/api/extensions", post(discover_extensions))
        .route("/api/extensions/skills", patch(manage_skill))
        .route("/api/extensions/mcp-servers", patch(manage_mcp_server))
        .route(
            "/api/ssh-connections",
            get(list_ssh_connections).post(save_ssh_connection),
        )
        .route(
            "/api/ssh-connections/{connection_id}",
            delete(delete_ssh_connection),
        )
        .route("/api/attachments", post(upload_attachment))
        .route(
            "/api/working-directories",
            post(suggest_working_directories),
        )
        .route("/api/chats", get(list_chats).post(create_chat))
        .route("/api/chats/{chat_id}", get(get_chat))
        .route("/api/chats/{chat_id}/messages", post(send_message))
        .route("/api/chats/{chat_id}/compact", post(compact_chat))
        .route(
            "/api/chats/{chat_id}/queue",
            get(list_queued_messages).post(enqueue_message),
        )
        .route(
            "/api/chats/{chat_id}/queue/{message_id}",
            patch(update_queued_message),
        )
        .route(
            "/api/chats/{chat_id}/queue/{message_id}/send",
            post(send_queued_message_now),
        )
        .route("/api/chats/{chat_id}/events", get(stream_events))
        .route("/api/chats/{chat_id}/cancel", post(cancel_chat))
        .route(
            "/api/chats/{chat_id}/approvals/{approval_id}",
            post(resolve_approval),
        )
        .route(
            "/api/chats/{chat_id}/questions/{question_id}",
            post(resolve_question),
        )
        .fallback_service(static_files)
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES + 64 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let address = std::env::var("AGENT_RUNTIME_EXAMPLE_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:7788".to_string());
    let listener = match tokio::net::TcpListener::bind(&address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("could not bind {address}: {error}");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(%address, "full-stack runtime example ready");
    if let Err(error) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("server failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Clone)]
struct AppState {
    active_chats: Arc<RwLock<HashMap<String, Arc<ChatRecord>>>>,
    chat_creation: Arc<Mutex<()>>,
    store: Arc<dyn ChatStore>,
    queue_store: Arc<dyn DurableChatQueueStore<Error = StoreError>>,
    send_now: Arc<Mutex<HashMap<String, (String, ChatMessageRequest)>>>,
    upload_sequence: Arc<AtomicU64>,
    operation_sequence: Arc<AtomicU64>,
    demo_runtime: AgentRuntime,
    workspace: PathBuf,
}

impl AppState {
    async fn new() -> Result<Self, AppInitError> {
        let mut demo_builder = AgentRuntime::builder().concurrency_limit(4);
        demo_builder.register(DemoAdapter);
        let database_path = std::env::var_os("AGENT_RUNTIME_EXAMPLE_DB").map_or_else(
            || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".data/runtime.sqlite3"),
            PathBuf::from,
        );
        let sqlite_store = Arc::new(SqliteChatStore::open(&database_path).await?);
        let store: Arc<dyn ChatStore> = sqlite_store.clone();
        let queue_store: Arc<dyn DurableChatQueueStore<Error = StoreError>> = sqlite_store;
        let recovered = store.recover_interrupted_chats().await?;
        tracing::info!(database = %database_path.display(), recovered, "SQLite chat store ready");
        Ok(Self {
            active_chats: Arc::new(RwLock::new(HashMap::new())),
            chat_creation: Arc::new(Mutex::new(())),
            store,
            queue_store,
            send_now: Arc::new(Mutex::new(HashMap::new())),
            upload_sequence: Arc::new(AtomicU64::new(1)),
            operation_sequence: Arc::new(AtomicU64::new(1)),
            demo_runtime: demo_builder.build()?,
            workspace: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum AppInitError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

struct ChatRecord {
    snapshot: RwLock<ChatSnapshot>,
    messages: RwLock<Vec<ChatMessage>>,
    events: RwLock<Vec<ChatEvent>>,
    next_message_sequence: AtomicU64,
    current_message_sequence: AtomicU64,
    assistant_text_observed: AtomicBool,
    next_sequence: AtomicU64,
    sender: broadcast::Sender<ChatEvent>,
    cancellation: CancellationToken,
    interactions: InteractionBroker,
    store: Arc<dyn ChatStore>,
}

impl ChatRecord {
    fn new(
        snapshot: ChatSnapshot,
        messages: Vec<ChatMessage>,
        events: Vec<ChatEvent>,
        cancellation: CancellationToken,
        store: Arc<dyn ChatStore>,
    ) -> Arc<Self> {
        let (sender, _) = broadcast::channel(128);
        let next_message_sequence = messages.last().map_or(1, |message| message.sequence + 1);
        let current_message_sequence = messages
            .iter()
            .rev()
            .find(|message| message.role == "user")
            .map_or(0, |message| message.sequence);
        let next_sequence = events.last().map_or(1, |event| event.sequence + 1);
        Arc::new(Self {
            snapshot: RwLock::new(snapshot),
            messages: RwLock::new(messages),
            events: RwLock::new(events),
            next_message_sequence: AtomicU64::new(next_message_sequence),
            current_message_sequence: AtomicU64::new(current_message_sequence),
            assistant_text_observed: AtomicBool::new(false),
            next_sequence: AtomicU64::new(next_sequence),
            sender,
            cancellation,
            interactions: InteractionBroker::default(),
            store,
        })
    }

    async fn push(&self, kind: &str, payload: Value) -> Result<ChatEvent, StoreError> {
        let event = ChatEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            timestamp_ms: now_ms(),
            kind: kind.to_string(),
            payload,
        };
        {
            let mut events = self.events.write().await;
            events.push(event.clone());
            if events.len() > MAX_EVENTS_PER_CHAT {
                let overflow = events.len() - MAX_EVENTS_PER_CHAT;
                events.drain(..overflow);
            }
        }
        let snapshot = {
            let mut snapshot = self.snapshot.write().await;
            snapshot.updated_at_ms = event.timestamp_ms;
            snapshot.clone()
        };
        self.store
            .save_snapshot_and_event(&snapshot, &event)
            .await?;
        let _ = self.sender.send(event.clone());
        Ok(event)
    }

    async fn set_status(&self, status: ChatStatus) -> Result<(), StoreError> {
        self.snapshot.write().await.status = status;
        self.push("chat_status", json!({ "status": status }))
            .await?;
        Ok(())
    }

    async fn append_message(
        &self,
        role: &str,
        content: String,
        attachments: Vec<ChatAttachment>,
    ) -> Result<ChatMessage, StoreError> {
        let message = ChatMessage {
            sequence: self.next_message_sequence.fetch_add(1, Ordering::Relaxed),
            role: role.to_string(),
            content,
            attachments,
            created_at_ms: now_ms(),
        };
        if role == "user" {
            self.current_message_sequence
                .store(message.sequence, Ordering::Relaxed);
        }
        let event = ChatEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            timestamp_ms: message.created_at_ms,
            kind: "message_appended".to_string(),
            payload: serde_json::to_value(&message).unwrap_or(Value::Null),
        };
        let snapshot = {
            let mut snapshot = self.snapshot.write().await;
            snapshot.updated_at_ms = event.timestamp_ms;
            snapshot.clone()
        };
        self.store
            .save_snapshot_message_and_event(&snapshot, &message, &event)
            .await?;
        self.messages.write().await.push(message.clone());
        self.events.write().await.push(event.clone());
        let _ = self.sender.send(event);
        Ok(message)
    }

    async fn view(&self) -> ChatView {
        ChatView {
            chat: self.snapshot.read().await.clone(),
            messages: self.messages.read().await.clone(),
            events: self.events.read().await.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ChatStatus {
    Queued,
    Running,
    ApprovalNeeded,
    InputNeeded,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatSnapshot {
    id: String,
    title: String,
    mode: ExecutionMode,
    provider: ProviderChoice,
    permission: PermissionChoice,
    scenario: DemoScenario,
    target: String,
    #[serde(default)]
    connection_kind: ConnectionKind,
    #[serde(default)]
    connection_id: Option<String>,
    #[serde(default)]
    connection_key: String,
    #[serde(default = "default_working_directory")]
    working_directory: String,
    status: ChatStatus,
    draft: String,
    session_id: Option<String>,
    model: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    usage: Usage,
    #[serde(default)]
    account_usage: Option<AccountUsageSnapshot>,
    #[serde(default)]
    harness_options: BTreeMap<String, String>,
    error: Option<String>,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatView {
    chat: ChatSnapshot,
    messages: Vec<ChatMessage>,
    events: Vec<ChatEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessage {
    sequence: u64,
    role: String,
    content: String,
    #[serde(default)]
    attachments: Vec<ChatAttachment>,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatEvent {
    sequence: u64,
    timestamp_ms: u64,
    kind: String,
    payload: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExecutionMode {
    Demo,
    Installed,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ConnectionKind {
    #[default]
    Local,
    Ssh,
    TempsSandbox,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderChoice {
    Claude,
    Codex,
    Opencode,
}

impl ProviderChoice {
    fn provider(self) -> Provider {
        match self {
            Self::Claude => Provider::Claude,
            Self::Codex => Provider::Codex,
            Self::Opencode => Provider::OpenCode,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PermissionChoice {
    Default,
    AcceptEdits,
    Plan,
    FullAccess,
}

impl PermissionChoice {
    fn permission(self) -> PermissionMode {
        match self {
            Self::Default => PermissionMode::Default,
            Self::AcceptEdits => PermissionMode::AcceptEdits,
            Self::Plan => PermissionMode::Plan,
            Self::FullAccess => PermissionMode::FullAccess,
        }
    }

    fn from_effective_mode(mode: &PermissionMode) -> Option<Self> {
        match mode {
            PermissionMode::Default => Some(Self::Default),
            PermissionMode::AcceptEdits => Some(Self::AcceptEdits),
            PermissionMode::Plan => Some(Self::Plan),
            PermissionMode::FullAccess => Some(Self::FullAccess),
            PermissionMode::Custom(_) => None,
            _ => None,
        }
    }
}

fn claude_permission_option(mode: &PermissionMode) -> String {
    match mode {
        PermissionMode::Default => "manual".to_string(),
        PermissionMode::AcceptEdits => "acceptEdits".to_string(),
        PermissionMode::Plan => "plan".to_string(),
        PermissionMode::FullAccess => "bypassPermissions".to_string(),
        PermissionMode::Custom(value) => value.clone(),
        _ => "manual".to_string(),
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DemoScenario {
    Approval,
    Failure,
}

#[derive(Clone, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExecutionTargetRequest {
    #[default]
    Local,
    Ssh {
        #[serde(default)]
        connection_id: Option<String>,
        host: String,
        user: Option<String>,
        port: Option<u16>,
        #[serde(default)]
        authentication: SshAuthenticationRequest,
        known_hosts_file: Option<PathBuf>,
        #[serde(default)]
        accept_new_host_key: bool,
    },
    TempsSandbox {
        base_url: String,
        sandbox_id: String,
        auth: TempsSandboxAuthRequest,
    },
}

#[derive(Clone, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SshAuthenticationRequest {
    #[default]
    Agent,
    IdentityFile {
        path: PathBuf,
    },
    Password {
        password: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StoredSshAuthentication {
    Agent,
    IdentityFile,
    Password,
}

#[derive(Debug, Clone)]
struct StoredSshConnection {
    id: String,
    label: String,
    host: String,
    user: Option<String>,
    port: Option<u16>,
    authentication: StoredSshAuthentication,
    identity_file: Option<PathBuf>,
    password: Option<String>,
    known_hosts_file: Option<PathBuf>,
    accept_new_host_key: bool,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SshConnectionSummary {
    id: String,
    label: String,
    host: String,
    user: Option<String>,
    port: Option<u16>,
    authentication: StoredSshAuthentication,
    identity_file: Option<PathBuf>,
    known_hosts_file: Option<PathBuf>,
    accept_new_host_key: bool,
    has_password: bool,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl From<&StoredSshConnection> for SshConnectionSummary {
    fn from(connection: &StoredSshConnection) -> Self {
        Self {
            id: connection.id.clone(),
            label: connection.label.clone(),
            host: connection.host.clone(),
            user: connection.user.clone(),
            port: connection.port,
            authentication: connection.authentication,
            identity_file: connection.identity_file.clone(),
            known_hosts_file: connection.known_hosts_file.clone(),
            accept_new_host_key: connection.accept_new_host_key,
            has_password: connection.password.is_some(),
            created_at_ms: connection.created_at_ms,
            updated_at_ms: connection.updated_at_ms,
        }
    }
}

#[derive(Deserialize)]
struct SaveSshConnectionRequest {
    label: String,
    target: ExecutionTargetRequest,
}

impl ExecutionTargetRequest {
    const fn label(&self) -> &'static str {
        match self {
            Self::Local => "Local machine",
            Self::Ssh { .. } => "SSH target",
            Self::TempsSandbox { .. } => "Temps sandbox",
        }
    }
}

struct ConnectionIdentity {
    kind: ConnectionKind,
    id: Option<String>,
    key: String,
    label: String,
}

fn connection_identity(target: &ExecutionTargetRequest) -> ConnectionIdentity {
    match target {
        ExecutionTargetRequest::Local => ConnectionIdentity {
            kind: ConnectionKind::Local,
            id: None,
            key: "local".to_string(),
            label: "Local machine".to_string(),
        },
        ExecutionTargetRequest::Ssh {
            connection_id,
            host,
            user,
            port,
            authentication,
            known_hosts_file,
            accept_new_host_key,
        } => {
            let label = user
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map_or_else(|| host.clone(), |user| format!("{user}@{host}"));
            let key = if let Some(connection_id) = connection_id {
                format!("ssh:saved:{connection_id}")
            } else {
                let authentication = match authentication {
                    SshAuthenticationRequest::Agent => "agent".to_string(),
                    SshAuthenticationRequest::IdentityFile { path } => {
                        format!("identity:{}", path.display())
                    }
                    // Password material is deliberately excluded from the persisted identity.
                    SshAuthenticationRequest::Password { .. } => "password".to_string(),
                };
                format!(
                    "ssh:raw:{}:{}:{}:{}:{}:{}",
                    host,
                    user.as_deref().unwrap_or_default(),
                    port.unwrap_or(22),
                    authentication,
                    known_hosts_file
                        .as_deref()
                        .map(|path| path.to_string_lossy())
                        .unwrap_or_default(),
                    accept_new_host_key,
                )
            };
            ConnectionIdentity {
                kind: ConnectionKind::Ssh,
                id: connection_id.clone(),
                key,
                label,
            }
        }
        ExecutionTargetRequest::TempsSandbox {
            base_url,
            sandbox_id,
            auth,
        } => {
            let authentication = match auth {
                TempsSandboxAuthRequest::Bearer { .. } => "bearer",
                TempsSandboxAuthRequest::SessionCookie { .. } => "session_cookie",
            };
            ConnectionIdentity {
                kind: ConnectionKind::TempsSandbox,
                id: None,
                key: format!("temps_sandbox:{base_url}:{sandbox_id}:{authentication}"),
                label: format!("Temps sandbox {sandbox_id}"),
            }
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TempsSandboxAuthRequest {
    Bearer { token: String },
    SessionCookie { cookie: String },
}

#[derive(Deserialize)]
struct DiscoveryRequest {
    #[serde(default)]
    target: ExecutionTargetRequest,
}

#[derive(Default, Deserialize)]
struct ChatListQuery {
    connection_key: Option<String>,
}

#[derive(Deserialize)]
struct WorkingDirectoryRequest {
    #[serde(default)]
    target: ExecutionTargetRequest,
    #[serde(default)]
    input: String,
}

#[derive(Deserialize)]
struct ExtensionDiscoveryRequest {
    #[serde(default)]
    target: ExecutionTargetRequest,
    provider: ProviderChoice,
    working_directory: PathBuf,
    skill_query: Option<String>,
    #[serde(default = "default_extension_limit")]
    limit: usize,
}

const fn default_extension_limit() -> usize {
    100
}

#[derive(Deserialize)]
struct ManageSkillRequest {
    #[serde(default)]
    target: ExecutionTargetRequest,
    provider: ProviderChoice,
    working_directory: PathBuf,
    scope: HarnessExtensionScope,
    name: String,
    content: Option<String>,
}

#[derive(Deserialize)]
struct ManageMcpServerRequest {
    #[serde(default)]
    target: ExecutionTargetRequest,
    provider: ProviderChoice,
    working_directory: PathBuf,
    scope: HarnessExtensionScope,
    name: String,
    definition: Option<HarnessMcpDefinition>,
}

#[derive(Clone, Deserialize)]
struct ChatMessageRequest {
    prompt: String,
    #[serde(default)]
    attachments: Vec<ChatAttachment>,
    #[serde(default = "default_mode")]
    mode: ExecutionMode,
    #[serde(default = "default_provider")]
    provider: ProviderChoice,
    #[serde(default = "default_permission")]
    permission: PermissionChoice,
    #[serde(default = "default_scenario")]
    scenario: DemoScenario,
    model: Option<String>,
    reasoning: Option<String>,
    #[serde(default)]
    harness_options: BTreeMap<String, String>,
    #[serde(default)]
    target: ExecutionTargetRequest,
    working_directory: Option<PathBuf>,
}

#[derive(Clone, Deserialize)]
struct CompactChatRequest {
    #[serde(default = "default_mode")]
    mode: ExecutionMode,
    #[serde(default = "default_provider")]
    provider: ProviderChoice,
    #[serde(default = "default_permission")]
    permission: PermissionChoice,
    #[serde(default = "default_scenario")]
    scenario: DemoScenario,
    model: Option<String>,
    reasoning: Option<String>,
    #[serde(default)]
    harness_options: BTreeMap<String, String>,
    #[serde(default)]
    target: ExecutionTargetRequest,
    working_directory: Option<PathBuf>,
}

impl CompactChatRequest {
    fn queued_message_fallback(&self) -> ChatMessageRequest {
        ChatMessageRequest {
            prompt: String::new(),
            attachments: Vec::new(),
            mode: self.mode,
            provider: self.provider,
            permission: self.permission,
            scenario: self.scenario,
            model: self.model.clone(),
            reasoning: self.reasoning.clone(),
            harness_options: self.harness_options.clone(),
            target: self.target.clone(),
            working_directory: self.working_directory.clone(),
        }
    }
}

#[derive(Deserialize)]
struct QueueMessageRequest {
    content: String,
    #[serde(default)]
    attachments: Vec<ChatAttachment>,
}

#[derive(Deserialize)]
struct QueueMessageUpdate {
    content: String,
    #[serde(default)]
    attachments: Vec<ChatAttachment>,
    expected_revision: u64,
}

fn default_working_directory() -> String {
    ".".to_string()
}

fn default_mode() -> ExecutionMode {
    ExecutionMode::Demo
}

fn default_provider() -> ProviderChoice {
    ProviderChoice::Claude
}

fn default_permission() -> PermissionChoice {
    PermissionChoice::Plan
}

fn default_scenario() -> DemoScenario {
    DemoScenario::Approval
}

#[derive(Debug, Deserialize)]
struct ApprovalBody {
    decision: ApprovalChoice,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ApprovalChoice {
    Allow,
    Deny,
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    #[serde(default)]
    after: u64,
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "storage": "sqlite" }))
}

async fn list_ssh_connections(
    State(state): State<AppState>,
) -> Result<Json<Vec<SshConnectionSummary>>, ApiError> {
    state
        .store
        .list_ssh_connections()
        .await
        .map(Json)
        .map_err(store_api_error)
}

async fn save_ssh_connection(
    State(state): State<AppState>,
    Json(body): Json<SaveSshConnectionRequest>,
) -> Result<(StatusCode, Json<SshConnectionSummary>), ApiError> {
    let label = body.label.trim();
    if label.is_empty() || label.len() > 120 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Connection names must contain 1 to 120 characters." })),
        ));
    }
    let ExecutionTargetRequest::Ssh {
        host,
        user,
        port,
        authentication,
        known_hosts_file,
        accept_new_host_key,
        ..
    } = body.target
    else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Only SSH targets can be saved as SSH connections." })),
        ));
    };
    if host.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "An SSH host is required." })),
        ));
    }
    let (authentication, identity_file, password) = match authentication {
        SshAuthenticationRequest::Agent => (StoredSshAuthentication::Agent, None, None),
        SshAuthenticationRequest::IdentityFile { path } => {
            (StoredSshAuthentication::IdentityFile, Some(path), None)
        }
        SshAuthenticationRequest::Password { password } => {
            if password.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({ "error": "A password is required when saving password authentication." }),
                    ),
                ));
            }
            (StoredSshAuthentication::Password, None, Some(password))
        }
    };
    let now = now_ms();
    let connection = StoredSshConnection {
        id: state
            .store
            .allocate_ssh_connection_id()
            .await
            .map_err(store_api_error)?,
        label: label.to_string(),
        host: host.trim().to_string(),
        user: user.filter(|value| !value.trim().is_empty()),
        port,
        authentication,
        identity_file,
        password,
        known_hosts_file,
        accept_new_host_key,
        created_at_ms: now,
        updated_at_ms: now,
    };
    state
        .store
        .save_ssh_connection(&connection)
        .await
        .map_err(store_api_error)?;
    Ok((
        StatusCode::CREATED,
        Json(SshConnectionSummary::from(&connection)),
    ))
}

async fn delete_ssh_connection(
    State(state): State<AppState>,
    AxumPath(connection_id): AxumPath<String>,
) -> Result<StatusCode, ApiError> {
    if state
        .store
        .delete_ssh_connection(&connection_id)
        .await
        .map_err(store_api_error)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Saved SSH connection not found." })),
        ))
    }
}

type ApiError = (StatusCode, Json<Value>);

fn runtime_for_target(target: &ExecutionTargetRequest) -> Result<AgentRuntime, TransportError> {
    AgentRuntime::builder()
        .concurrency_limit(4)
        .transport_from_arc(transport_for_target(target)?)
        .build()
        .map_err(|error| {
            TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                target.label(),
                "configure",
                error.to_string(),
                false,
            )
        })
}

fn transport_for_target(
    target: &ExecutionTargetRequest,
) -> Result<Arc<dyn ExecutionTransport>, TransportError> {
    match target {
        ExecutionTargetRequest::Local => Ok(Arc::new(LocalTransport)),
        ExecutionTargetRequest::Ssh {
            connection_id: _,
            host,
            user,
            port,
            authentication,
            known_hosts_file,
            accept_new_host_key,
        } => {
            let mut transport = SshTransport::builder(host);
            if let Some(user) = user.as_deref() {
                transport = transport.user(user);
            }
            if let Some(port) = port {
                transport = transport.port(*port);
            }
            transport = match authentication {
                SshAuthenticationRequest::Agent => transport.agent(),
                SshAuthenticationRequest::IdentityFile { path } => transport.identity_file(path),
                SshAuthenticationRequest::Password { password } => {
                    transport.password(SecretString::new(password))
                }
            };
            if let Some(path) = known_hosts_file {
                transport = transport.known_hosts_file(path);
            }
            transport = transport.host_key_policy(if *accept_new_host_key {
                SshHostKeyPolicy::AcceptNew
            } else {
                SshHostKeyPolicy::Strict
            });
            Ok(Arc::new(transport.build()?))
        }
        ExecutionTargetRequest::TempsSandbox {
            base_url,
            sandbox_id,
            auth,
        } => {
            let auth = match auth {
                TempsSandboxAuthRequest::Bearer { token } => {
                    TempsSandboxAuth::Bearer(SecretString::new(token))
                }
                TempsSandboxAuthRequest::SessionCookie { cookie } => {
                    TempsSandboxAuth::SessionCookie(SecretString::new(cookie))
                }
            };
            Ok(Arc::new(
                TempsSandboxTransport::builder(base_url, sandbox_id, auth).build()?,
            ))
        }
    }
}

async fn resolve_execution_target(
    state: &AppState,
    target: &ExecutionTargetRequest,
) -> Result<ExecutionTargetRequest, ApiError> {
    let ExecutionTargetRequest::Ssh {
        connection_id: Some(connection_id),
        ..
    } = target
    else {
        return Ok(target.clone());
    };
    let connection = state
        .store
        .load_ssh_connection(connection_id)
        .await
        .map_err(store_api_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Saved SSH connection not found." })),
            )
        })?;
    let authentication = match connection.authentication {
        StoredSshAuthentication::Agent => SshAuthenticationRequest::Agent,
        StoredSshAuthentication::IdentityFile => SshAuthenticationRequest::IdentityFile {
            path: connection.identity_file.ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "The saved identity-file connection is incomplete." })),
                )
            })?,
        },
        StoredSshAuthentication::Password => SshAuthenticationRequest::Password {
            password: connection.password.ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "The saved password connection is incomplete." })),
                )
            })?,
        },
    };
    Ok(ExecutionTargetRequest::Ssh {
        connection_id: None,
        host: connection.host,
        user: connection.user,
        port: connection.port,
        authentication,
        known_hosts_file: connection.known_hosts_file,
        accept_new_host_key: connection.accept_new_host_key,
    })
}

fn transport_api_error(error: TransportError) -> ApiError {
    let TransportError {
        kind,
        transport,
        operation,
        message,
        retryable,
    } = error;
    let status = match kind {
        TransportErrorKind::InvalidConfiguration => StatusCode::BAD_REQUEST,
        TransportErrorKind::AuthenticationFailed => StatusCode::UNAUTHORIZED,
        TransportErrorKind::PermissionDenied | TransportErrorKind::HostKeyVerificationFailed => {
            StatusCode::FORBIDDEN
        }
        _ => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(json!({
            "error": message,
            "kind": kind,
            "transport": transport,
            "operation": operation,
            "retryable": retryable,
        })),
    )
}

fn store_api_error(error: StoreError) -> ApiError {
    tracing::error!(%error, "SQLite chat store operation failed");
    drop(error);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "The chat store could not complete this operation." })),
    )
}

fn not_found_api_error() -> ApiError {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "Chat not found." })),
    )
}

fn conflict_api_error(message: &str) -> ApiError {
    (StatusCode::CONFLICT, Json(json!({ "error": message })))
}

fn retained_api_error(error: RuntimeFailure) -> ApiError {
    let RuntimeFailure {
        runtime_id,
        invocation_id,
        kind,
        retry,
        delivery,
        message,
        provider_code,
    } = error;
    let status = match kind {
        RuntimeFailureKind::InvalidRequest => StatusCode::BAD_REQUEST,
        RuntimeFailureKind::Authentication => StatusCode::UNAUTHORIZED,
        RuntimeFailureKind::Permission => StatusCode::FORBIDDEN,
        RuntimeFailureKind::RuntimeBusy
        | RuntimeFailureKind::RuntimeAlreadyExists
        | RuntimeFailureKind::InvocationAlreadyExists => StatusCode::CONFLICT,
        RuntimeFailureKind::CapabilityUnavailable => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(json!({
            "error": message,
            "kind": kind,
            "retry": retry,
            "delivery": delivery,
            "runtime_id": runtime_id,
            "invocation_id": invocation_id,
            "provider_code": provider_code,
        })),
    )
}

fn queue_store_api_error(error: StoreError) -> ApiError {
    match error {
        StoreError::QueueMessageNotFound(message_id) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Queued message `{message_id}` was not found.") })),
        ),
        StoreError::QueueConflict(message_id) => (
            StatusCode::CONFLICT,
            Json(
                json!({ "error": format!("Queued message `{message_id}` changed. Reload it before editing again.") }),
            ),
        ),
        error => store_api_error(error),
    }
}

async fn discover_harnesses(
    State(state): State<AppState>,
    Json(body): Json<DiscoveryRequest>,
) -> Result<Json<HarnessInventory>, ApiError> {
    let target = resolve_execution_target(&state, &body.target).await?;
    let runtime = runtime_for_target(&target).map_err(transport_api_error)?;
    Ok(Json(runtime.discover_harnesses().await))
}

async fn suggest_working_directories(
    State(state): State<AppState>,
    Json(body): Json<WorkingDirectoryRequest>,
) -> Result<Json<temps_agent_runtime::WorkingDirectoryCandidates>, ApiError> {
    let target = resolve_execution_target(&state, &body.target).await?;
    let runtime = runtime_for_target(&target).map_err(transport_api_error)?;
    runtime
        .suggest_working_directories(body.input, 20)
        .await
        .map(Json)
        .map_err(transport_api_error)
}

async fn discover_extensions(
    State(state): State<AppState>,
    Json(body): Json<ExtensionDiscoveryRequest>,
) -> Result<Json<HarnessExtensionInventory>, ApiError> {
    let target = resolve_execution_target(&state, &body.target).await?;
    let runtime = runtime_for_target(&target).map_err(transport_api_error)?;
    runtime
        .discover_harness_extensions(HarnessExtensionQuery {
            provider: body.provider.provider(),
            working_directory: body.working_directory,
            skill_query: body.skill_query,
            limit: body.limit,
        })
        .await
        .map(Json)
        .map_err(transport_api_error)
}

async fn manage_skill(
    State(state): State<AppState>,
    Json(body): Json<ManageSkillRequest>,
) -> Result<StatusCode, ApiError> {
    let target = resolve_execution_target(&state, &body.target).await?;
    let runtime = runtime_for_target(&target).map_err(transport_api_error)?;
    runtime
        .manage_skill(SkillManagementRequest {
            provider: body.provider.provider(),
            scope: body.scope,
            name: body.name,
            working_directory: body.working_directory,
            content: body.content,
        })
        .await
        .map_err(transport_api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn manage_mcp_server(
    State(state): State<AppState>,
    Json(body): Json<ManageMcpServerRequest>,
) -> Result<StatusCode, ApiError> {
    let target = resolve_execution_target(&state, &body.target).await?;
    let runtime = runtime_for_target(&target).map_err(transport_api_error)?;
    runtime
        .manage_mcp_server(McpServerManagementRequest {
            provider: body.provider.provider(),
            scope: body.scope,
            name: body.name,
            working_directory: body.working_directory,
            definition: body.definition,
        })
        .await
        .map_err(transport_api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn upload_attachment(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<ChatAttachment>), ApiError> {
    let mut target = None;
    let mut working_directory = None;
    let mut upload = None;

    while let Some(field) = multipart.next_field().await.map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Could not read the upload form: {error}") })),
        )
    })? {
        match field.name() {
            Some("target") => {
                let value = field.text().await.map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("Could not read the upload target: {error}") })),
                    )
                })?;
                target = Some(
                    serde_json::from_str::<ExecutionTargetRequest>(&value).map_err(|_| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({ "error": "The upload target is invalid." })),
                        )
                    })?,
                );
            }
            Some("working_directory") => {
                working_directory = Some(PathBuf::from(field.text().await.map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("Could not read the working directory: {error}") })),
                    )
                })?));
            }
            Some("file") => {
                let name = safe_upload_name(field.file_name().unwrap_or("upload.bin"));
                let media_type = field.content_type().map(ToString::to_string);
                let bytes = field.bytes().await.map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("Could not read the uploaded file: {error}") })),
                    )
                })?;
                if bytes.len() > MAX_UPLOAD_BYTES {
                    return Err((
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(json!({ "error": "Attachments cannot exceed 10 MiB." })),
                    ));
                }
                upload = Some((name, media_type, bytes));
            }
            _ => {}
        }
    }

    let target = target.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "The upload target is required." })),
        )
    })?;
    let working_directory = working_directory.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "The chat working directory is required." })),
        )
    })?;
    let (name, media_type, bytes) = upload.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Choose a file to upload." })),
        )
    })?;

    let target = resolve_execution_target(&state, &target).await?;
    let transport = transport_for_target(&target).map_err(transport_api_error)?;
    transport
        .validate_working_directory(&working_directory)
        .await
        .map_err(transport_api_error)?;
    let id = format!(
        "upload_{:x}_{:x}",
        now_ms(),
        state.upload_sequence.fetch_add(1, Ordering::Relaxed)
    );
    let relative_directory = PathBuf::from(".temps-agent-runtime")
        .join("uploads")
        .join(&id);
    let relative_path = relative_directory.join(&name);
    store_upload(
        Arc::clone(&transport),
        working_directory.clone(),
        &relative_directory,
        &relative_path,
        &bytes,
    )
    .await
    .map_err(transport_api_error)?;

    let size = bytes.len();
    let uri = working_directory.join(relative_path).display().to_string();
    Ok((
        StatusCode::CREATED,
        Json(ChatAttachment {
            id,
            name,
            uri,
            media_type,
            metadata: BTreeMap::from([
                ("size_bytes".to_string(), json!(size)),
                ("source".to_string(), json!("upload")),
            ]),
        }),
    ))
}

async fn store_upload(
    transport: Arc<dyn ExecutionTransport>,
    working_directory: PathBuf,
    relative_directory: &std::path::Path,
    relative_path: &std::path::Path,
    bytes: &[u8],
) -> Result<(), TransportError> {
    let first = store_upload_once(
        Arc::clone(&transport),
        working_directory.clone(),
        relative_directory,
        relative_path,
        bytes,
    )
    .await;
    match first {
        Err(error) if error.retryable => {
            store_upload_once(
                transport,
                working_directory,
                relative_directory,
                relative_path,
                bytes,
            )
            .await
        }
        result => result,
    }
}

async fn store_upload_once(
    transport: Arc<dyn ExecutionTransport>,
    working_directory: PathBuf,
    relative_directory: &std::path::Path,
    relative_path: &std::path::Path,
    bytes: &[u8],
) -> Result<(), TransportError> {
    let mut command = CommandSpec::new("sh");
    command.args = vec![
        "-c".into(),
        "umask 077; mkdir -p -- \"$1\" && cat > \"$2\"".into(),
        "attachment-upload".into(),
        relative_directory.as_os_str().to_owned(),
        relative_path.as_os_str().to_owned(),
    ];
    command.interactive_stdin = true;
    let mut process = transport
        .spawn(TransportSpawnRequest {
            command,
            working_directory: working_directory.clone(),
        })
        .await?;
    let mut stdin = process.take_stdin().ok_or_else(|| {
        TransportError::new(
            TransportErrorKind::StreamFailed,
            transport.name(),
            "upload_attachment",
            "the target process did not expose stdin",
            false,
        )
    })?;
    stdin.write_all(bytes).await.map_err(|error| {
        TransportError::new(
            TransportErrorKind::StreamFailed,
            transport.name(),
            "upload_attachment",
            error.to_string(),
            true,
        )
    })?;
    stdin.shutdown().await.map_err(|error| {
        TransportError::new(
            TransportErrorKind::StreamFailed,
            transport.name(),
            "upload_attachment",
            error.to_string(),
            true,
        )
    })?;
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), process.wait())
        .await
        .map_err(|_| {
            TransportError::new(
                TransportErrorKind::ConnectionTimedOut,
                transport.name(),
                "upload_attachment",
                "the upload exceeded 30 seconds",
                true,
            )
        })??;
    if !status.success {
        let mut detail = String::new();
        if let Some(mut stderr) = process.take_stderr() {
            let _ = stderr.read_to_string(&mut detail).await;
        }
        return Err(TransportError::new(
            TransportErrorKind::StreamFailed,
            transport.name(),
            "upload_attachment",
            if detail.trim().is_empty() {
                "the target could not store the attachment".to_string()
            } else {
                detail.trim().chars().take(512).collect()
            },
            false,
        ));
    }
    Ok(())
}

fn safe_upload_name(name: &str) -> String {
    let basename = std::path::Path::new(name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("upload.bin");
    let cleaned = basename
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(160)
        .collect::<String>();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "upload.bin".to_string()
    } else {
        cleaned
    }
}

async fn list_chats(
    State(state): State<AppState>,
    Query(query): Query<ChatListQuery>,
) -> Result<Json<Vec<ChatSnapshot>>, ApiError> {
    if query
        .connection_key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > 1_024 || key.contains('\0'))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "connection_key is invalid." })),
        ));
    }
    state
        .store
        .list_chats(query.connection_key.as_deref(), MAX_LISTED_CHATS)
        .await
        .map(Json)
        .map_err(store_api_error)
}

async fn get_chat(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
) -> Result<Json<ChatView>, ApiError> {
    if let Some(record) = find_active_chat(&state, &chat_id).await {
        return Ok(Json(record.view().await));
    }
    state
        .store
        .load_chat(&chat_id)
        .await
        .map_err(store_api_error)?
        .map(Json)
        .ok_or_else(not_found_api_error)
}

async fn list_queued_messages(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
) -> Result<Json<Vec<QueuedChatMessage>>, ApiError> {
    let page = state
        .queue_store
        .list_queued(&chat_id, None, 200)
        .await
        .map_err(queue_store_api_error)?;
    Ok(Json(page.messages))
}

async fn enqueue_message(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
    Json(body): Json<QueueMessageRequest>,
) -> Result<(StatusCode, Json<QueuedChatMessage>), ApiError> {
    let content = body.content.trim().to_string();
    validate_message(&content, &body.attachments)?;
    if state
        .store
        .load_chat(&chat_id)
        .await
        .map_err(store_api_error)?
        .is_none()
    {
        return Err(not_found_api_error());
    }
    let now = now_ms();
    let message = QueuedChatMessage {
        id: state
            .store
            .allocate_queued_message_id()
            .await
            .map_err(store_api_error)?,
        chat_id,
        content,
        attachments: body.attachments,
        revision: 1,
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
        metadata: BTreeMap::new(),
    };
    state
        .queue_store
        .enqueue(&message)
        .await
        .map_err(queue_store_api_error)?;
    Ok((StatusCode::CREATED, Json(message)))
}

async fn update_queued_message(
    State(state): State<AppState>,
    AxumPath((chat_id, message_id)): AxumPath<(String, String)>,
    Json(body): Json<QueueMessageUpdate>,
) -> Result<Json<QueuedChatMessage>, ApiError> {
    let content = body.content.trim().to_string();
    validate_message(&content, &body.attachments)?;
    let current = state
        .queue_store
        .load_queued(&chat_id, &message_id)
        .await
        .map_err(queue_store_api_error)?
        .ok_or_else(|| {
            queue_store_api_error(StoreError::QueueMessageNotFound(message_id.clone()))
        })?;
    let updated = QueuedChatMessage {
        content,
        attachments: body.attachments,
        revision: current.revision.saturating_add(1),
        updated_at_unix_ms: now_ms(),
        ..current
    };
    state
        .queue_store
        .update_queued(&updated, body.expected_revision)
        .await
        .map_err(queue_store_api_error)?;
    Ok(Json(updated))
}

async fn send_queued_message_now(
    State(state): State<AppState>,
    AxumPath((chat_id, message_id)): AxumPath<(String, String)>,
    Json(body): Json<ChatMessageRequest>,
) -> Result<(StatusCode, Json<QueuedChatMessage>), ApiError> {
    let queued = state
        .queue_store
        .load_queued(&chat_id, &message_id)
        .await
        .map_err(queue_store_api_error)?
        .ok_or_else(|| {
            queue_store_api_error(StoreError::QueueMessageNotFound(message_id.clone()))
        })?;

    if let Some(record) = find_active_chat(&state, &chat_id).await {
        let mut send_now = state.send_now.lock().await;
        if send_now.contains_key(&chat_id) {
            return Err(conflict_api_error(
                "Another queued message is already waiting to be sent now.",
            ));
        }
        send_now.insert(chat_id, (message_id, body));
        drop(send_now);
        record
            .set_status(ChatStatus::Cancelling)
            .await
            .map_err(store_api_error)?;
        record.cancellation.cancel();
        return Ok((StatusCode::ACCEPTED, Json(queued)));
    }

    let Some(message) = state
        .queue_store
        .pop_queued(&chat_id, &message_id)
        .await
        .map_err(queue_store_api_error)?
    else {
        return Err(queue_store_api_error(StoreError::QueueMessageNotFound(
            message_id,
        )));
    };
    let mut request = body;
    request.prompt.clone_from(&message.content);
    request.attachments.clone_from(&message.attachments);
    if let Err(error) = start_chat_message(state.clone(), chat_id, request).await {
        state
            .queue_store
            .enqueue(&message)
            .await
            .map_err(queue_store_api_error)?;
        return Err(error);
    }
    Ok((StatusCode::ACCEPTED, Json(message)))
}

// Keep `from_secs` so this example remains compatible with the SDK's Rust 1.82 MSRV.
#[allow(clippy::duration_suboptimal_units)]
async fn compact_chat(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
    Json(body): Json<CompactChatRequest>,
) -> Result<(StatusCode, Json<ChatView>), ApiError> {
    let creation_guard = state.chat_creation.lock().await;
    if find_active_chat(&state, &chat_id).await.is_some() {
        return Err(conflict_api_error(
            "Wait for the current operation to finish before compacting this chat.",
        ));
    }
    let previous = state
        .store
        .load_chat(&chat_id)
        .await
        .map_err(store_api_error)?
        .ok_or_else(not_found_api_error)?;
    if previous.chat.mode != ExecutionMode::Installed {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "error": "Manual compaction requires an installed Claude harness." })),
        ));
    }
    if previous.chat.provider != ProviderChoice::Claude {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                json!({ "error": "Manual compaction is currently available only for Claude sessions." }),
            ),
        ));
    }
    if body.mode != previous.chat.mode || body.provider != previous.chat.provider {
        return Err(conflict_api_error(
            "Compaction must use the harness that owns this chat's provider session.",
        ));
    }
    let provider_session_id = previous.chat.session_id.clone().ok_or_else(|| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                json!({ "error": "Claude has not created a resumable session for this chat yet." }),
            ),
        )
    })?;
    let connection = connection_identity(&body.target);
    if !previous.chat.connection_key.is_empty() && previous.chat.connection_key != connection.key {
        return Err(conflict_api_error(
            "A chat cannot change execution connection during compaction.",
        ));
    }
    let working_directory = body.working_directory.clone().unwrap_or_else(|| {
        if matches!(body.target, ExecutionTargetRequest::Local) {
            state.workspace.clone()
        } else {
            PathBuf::from(".")
        }
    });
    if previous.chat.working_directory != working_directory.display().to_string() {
        return Err(conflict_api_error(
            "A chat cannot change working directory during compaction.",
        ));
    }

    let target = resolve_execution_target(&state, &body.target).await?;
    let runtime = runtime_for_target(&target).map_err(transport_api_error)?;
    let operation_sequence = state.operation_sequence.fetch_add(1, Ordering::Relaxed);
    let invocation_id = InvocationId::new(format!(
        "compact-{chat_id}-{}-{operation_sequence}",
        now_ms()
    ))
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Could not create a compaction operation: {error}") })),
        )
    })?;
    let runtime_id = RuntimeId::new(format!("compact-runtime-{chat_id}-{operation_sequence}"))
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Could not create a compaction runtime: {error}") })),
            )
        })?;
    let mut spec = RuntimeSpec::new(runtime_id, Provider::Claude, working_directory.clone());
    spec.provider_session_id = Some(provider_session_id);
    spec.model.clone_from(&body.model);
    spec.reasoning.clone_from(&body.reasoning);
    spec.permission_mode = body.permission.permission();
    spec.harness_options.clone_from(&body.harness_options);
    spec.turn_timeout = Duration::from_secs(5 * 60);
    spec.interaction_timeout = Duration::from_secs(2 * 60);
    let retained = InProcessRuntimeClient::new(runtime);
    let runtime_handle = retained.acquire(spec).await.map_err(retained_api_error)?;
    if !runtime_handle.driver_capabilities().manual_compaction {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                json!({ "error": "The selected Claude harness does not support manual compaction." }),
            ),
        ));
    }

    let cancellation = CancellationToken::new();
    let mut snapshot = previous.chat;
    snapshot.permission = body.permission;
    snapshot.model.clone_from(&body.model);
    snapshot.reasoning.clone_from(&body.reasoning);
    snapshot.harness_options.clone_from(&body.harness_options);
    snapshot.status = ChatStatus::Queued;
    snapshot.draft.clear();
    snapshot.error = None;
    snapshot.updated_at_ms = now_ms();
    let record = ChatRecord::new(
        snapshot,
        previous.messages,
        previous.events,
        cancellation.clone(),
        Arc::clone(&state.store),
    );
    record
        .push(
            "compaction_requested",
            json!({ "invocation_id": invocation_id.as_str() }),
        )
        .await
        .map_err(store_api_error)?;
    record
        .set_status(ChatStatus::Queued)
        .await
        .map_err(store_api_error)?;
    state
        .active_chats
        .write()
        .await
        .insert(chat_id.clone(), Arc::clone(&record));
    drop(creation_guard);

    spawn_compaction_chain(
        state,
        chat_id,
        Arc::clone(&record),
        body.queued_message_fallback(),
        runtime_handle,
        CompactionInput::new(invocation_id),
        cancellation,
    );
    Ok((StatusCode::ACCEPTED, Json(record.view().await)))
}

async fn create_chat(
    State(state): State<AppState>,
    Json(body): Json<ChatMessageRequest>,
) -> Result<(StatusCode, Json<ChatView>), (StatusCode, Json<Value>)> {
    let prompt = body.prompt.trim().to_string();
    validate_message(&prompt, &body.attachments)?;
    let creation_guard = state.chat_creation.lock().await;
    let connection = connection_identity(&body.target);
    let live_runtime = if matches!(body.mode, ExecutionMode::Installed) {
        let target = resolve_execution_target(&state, &body.target).await?;
        Some(runtime_for_target(&target).map_err(transport_api_error)?)
    } else {
        None
    };
    let working_directory = request_working_directory(&state, &body);
    let chat_id = state
        .store
        .allocate_chat_id()
        .await
        .map_err(store_api_error)?;
    let now = now_ms();
    let initial_message = ChatMessage {
        sequence: 1,
        role: "user".to_string(),
        content: prompt.clone(),
        attachments: body.attachments.clone(),
        created_at_ms: now,
    };
    let cancellation = CancellationToken::new();
    let record = ChatRecord::new(
        ChatSnapshot {
            id: chat_id.clone(),
            title: chat_title(&prompt),
            mode: body.mode,
            provider: body.provider,
            permission: body.permission,
            scenario: body.scenario,
            target: if matches!(body.mode, ExecutionMode::Demo) {
                "Deterministic subprocess".to_string()
            } else {
                connection.label.clone()
            },
            connection_kind: connection.kind,
            connection_id: connection.id,
            connection_key: connection.key,
            working_directory: working_directory.display().to_string(),
            status: ChatStatus::Queued,
            draft: String::new(),
            session_id: None,
            model: body.model.clone(),
            reasoning: body.reasoning.clone(),
            usage: Usage::default(),
            account_usage: None,
            harness_options: body.harness_options.clone(),
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
        vec![initial_message.clone()],
        Vec::new(),
        cancellation.clone(),
        Arc::clone(&state.store),
    );
    let initial_snapshot = record.snapshot.read().await.clone();
    state
        .store
        .create_chat(&initial_snapshot, &initial_message)
        .await
        .map_err(store_api_error)?;
    record
        .push("message_started", json!({ "message_sequence": 1 }))
        .await
        .map_err(store_api_error)?;
    record
        .set_status(ChatStatus::Queued)
        .await
        .map_err(store_api_error)?;
    state
        .active_chats
        .write()
        .await
        .insert(chat_id.clone(), Arc::clone(&record));
    drop(creation_guard);

    spawn_turn_chain(
        state,
        chat_id.clone(),
        Arc::clone(&record),
        body,
        prompt,
        cancellation,
        live_runtime,
        None,
    );
    Ok((StatusCode::CREATED, Json(record.view().await)))
}

async fn send_message(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
    Json(body): Json<ChatMessageRequest>,
) -> Result<(StatusCode, Json<ChatView>), ApiError> {
    let view = start_chat_message(state, chat_id, body).await?;
    Ok((StatusCode::ACCEPTED, Json(view)))
}

async fn start_chat_message(
    state: AppState,
    chat_id: String,
    body: ChatMessageRequest,
) -> Result<ChatView, ApiError> {
    let prompt = body.prompt.trim().to_string();
    validate_message(&prompt, &body.attachments)?;
    let creation_guard = state.chat_creation.lock().await;
    if find_active_chat(&state, &chat_id).await.is_some() {
        return Err(conflict_api_error(
            "Wait for the current response before sending another message.",
        ));
    }
    let previous = state
        .store
        .load_chat(&chat_id)
        .await
        .map_err(store_api_error)?
        .ok_or_else(not_found_api_error)?;
    let connection = connection_identity(&body.target);
    if !previous.chat.connection_key.is_empty() && previous.chat.connection_key != connection.key {
        return Err(conflict_api_error(
            "A chat cannot change execution connection; start a new chat to use Local or another remote.",
        ));
    }
    let working_directory = request_working_directory(&state, &body);
    if previous.chat.working_directory != working_directory.display().to_string() {
        return Err(conflict_api_error(
            "A chat cannot change working directory; start a new chat for another folder.",
        ));
    }
    let live_runtime = if matches!(body.mode, ExecutionMode::Installed) {
        let target = resolve_execution_target(&state, &body.target).await?;
        Some(runtime_for_target(&target).map_err(transport_api_error)?)
    } else {
        None
    };
    let resume_session_id = (previous.chat.mode == body.mode
        && previous.chat.provider == body.provider)
        .then(|| previous.chat.session_id.clone())
        .flatten();
    let cancellation = CancellationToken::new();
    let mut snapshot = previous.chat;
    snapshot.mode = body.mode;
    snapshot.provider = body.provider;
    snapshot.permission = body.permission;
    snapshot.scenario = body.scenario;
    if snapshot.connection_key.is_empty() {
        snapshot.target = connection.label;
        snapshot.connection_kind = connection.kind;
        snapshot.connection_id = connection.id;
        snapshot.connection_key = connection.key;
    }
    snapshot.working_directory = working_directory.display().to_string();
    snapshot.status = ChatStatus::Queued;
    snapshot.draft.clear();
    snapshot.session_id.clone_from(&resume_session_id);
    snapshot.model.clone_from(&body.model);
    snapshot.reasoning.clone_from(&body.reasoning);
    snapshot.harness_options.clone_from(&body.harness_options);
    snapshot.error = None;
    snapshot.updated_at_ms = now_ms();
    let record = ChatRecord::new(
        snapshot,
        previous.messages,
        previous.events,
        cancellation.clone(),
        Arc::clone(&state.store),
    );
    let message = record
        .append_message("user", prompt.clone(), body.attachments.clone())
        .await
        .map_err(store_api_error)?;
    record
        .push(
            "message_started",
            json!({ "message_sequence": message.sequence }),
        )
        .await
        .map_err(store_api_error)?;
    record
        .set_status(ChatStatus::Queued)
        .await
        .map_err(store_api_error)?;
    state
        .active_chats
        .write()
        .await
        .insert(chat_id.clone(), Arc::clone(&record));
    drop(creation_guard);

    spawn_turn_chain(
        state,
        chat_id,
        Arc::clone(&record),
        body,
        prompt,
        cancellation,
        live_runtime,
        resume_session_id,
    );
    Ok(record.view().await)
}

fn spawn_turn_chain(
    state: AppState,
    chat_id: String,
    record: Arc<ChatRecord>,
    body: ChatMessageRequest,
    prompt: String,
    cancellation: CancellationToken,
    live_runtime: Option<AgentRuntime>,
    resume_session_id: Option<String>,
) {
    tokio::spawn(async move {
        let fallback_body = body.clone();
        execute_turn(
            state.clone(),
            Arc::clone(&record),
            body,
            prompt,
            cancellation,
            live_runtime,
            resume_session_id,
        )
        .await;
        dispatch_after_operation(state, chat_id, record, fallback_body).await;
    });
}

fn spawn_compaction_chain(
    state: AppState,
    chat_id: String,
    record: Arc<ChatRecord>,
    fallback_body: ChatMessageRequest,
    runtime: RuntimeHandle,
    input: CompactionInput,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        execute_compaction(Arc::clone(&record), runtime, input, cancellation).await;
        dispatch_after_operation(state, chat_id, record, fallback_body).await;
    });
}

async fn execute_compaction(
    record: Arc<ChatRecord>,
    runtime: RuntimeHandle,
    input: CompactionInput,
    cancellation: CancellationToken,
) {
    let chat_id = record.snapshot.read().await.id.clone();
    if let Err(error) = record.set_status(ChatStatus::Running).await {
        tracing::error!(%chat_id, %error, "could not persist running compaction status");
        return;
    }
    let turn = match runtime.compact(input).await {
        Ok(turn) => turn,
        Err(failure) => {
            fail_compaction(&record, failure).await;
            return;
        }
    };
    let interrupt = turn.interrupt_handle();
    let interrupt_record = Arc::clone(&record);
    let interrupt_task = tokio::spawn(async move {
        cancellation.cancelled().await;
        let outcome = interrupt.interrupt().await;
        if let Err(error) = interrupt_record
            .push("compaction_interrupt", json!({ "outcome": outcome }))
            .await
        {
            tracing::error!(%error, "could not persist compaction interrupt outcome");
        }
    });
    let (mut events, completion) = turn.into_parts();
    while let Some(envelope) = events.next().await {
        if let Err(error) = persist_compaction_envelope(&record, envelope).await {
            tracing::error!(%chat_id, %error, "could not persist compaction event");
        }
    }
    let result = completion.wait().await;
    interrupt_task.abort();
    match result {
        Ok(result) => {
            {
                let mut snapshot = record.snapshot.write().await;
                snapshot.session_id.clone_from(&result.session_id);
                if result.model.is_some() {
                    snapshot.model.clone_from(&result.model);
                }
            }
            if let Err(error) = record
                .push(
                    "compaction_result",
                    serde_json::to_value(&result).unwrap_or(Value::Null),
                )
                .await
            {
                tracing::error!(%chat_id, %error, "could not persist compaction result");
                return;
            }
            if let Err(error) = record
                .set_status(match result.status {
                    RunStatus::Succeeded => ChatStatus::Succeeded,
                    _ => ChatStatus::Failed,
                })
                .await
            {
                tracing::error!(%chat_id, %error, "could not persist terminal compaction status");
            }
        }
        Err(failure) => fail_compaction(&record, failure).await,
    }
}

async fn persist_compaction_envelope(
    record: &ChatRecord,
    envelope: EventEnvelope,
) -> Result<(), StoreError> {
    let invocation_id = envelope.invocation_id.to_string();
    let runtime_id = envelope.runtime_id.to_string();
    let runtime_sequence = envelope.sequence;
    match envelope.event {
        RuntimeEvent::InvocationStarted { provider } => {
            record
                .push(
                    "compaction_invocation_started",
                    json!({
                        "provider": provider,
                        "runtime_id": runtime_id,
                        "invocation_id": invocation_id,
                        "runtime_sequence": runtime_sequence,
                    }),
                )
                .await?;
        }
        RuntimeEvent::ProviderEvent { event } => match event {
            TurnEvent::TextDelta { text } | TurnEvent::ReasoningDelta { text } => {
                record
                    .push(
                        "compaction_progress",
                        json!({
                            "message": text,
                            "runtime_id": runtime_id,
                            "invocation_id": invocation_id,
                            "runtime_sequence": runtime_sequence,
                        }),
                    )
                    .await?;
            }
            event => {
                record
                    .push(
                        "turn_event",
                        serde_json::to_value(event).unwrap_or(Value::Null),
                    )
                    .await?;
            }
        },
        RuntimeEvent::InvocationCompleted { result } => {
            record
                .push(
                    "compaction_invocation_completed",
                    json!({
                        "result": result,
                        "runtime_id": runtime_id,
                        "invocation_id": invocation_id,
                        "runtime_sequence": runtime_sequence,
                    }),
                )
                .await?;
        }
        RuntimeEvent::InvocationFailed { failure } => {
            record
                .push(
                    "compaction_invocation_failed",
                    json!({
                        "message": failure.message.clone(),
                        "failure": failure,
                        "runtime_id": runtime_id,
                        "invocation_id": invocation_id,
                        "runtime_sequence": runtime_sequence,
                    }),
                )
                .await?;
        }
        _ => {}
    }
    Ok(())
}

async fn fail_compaction(record: &ChatRecord, failure: RuntimeFailure) {
    let chat_id = record.snapshot.read().await.id.clone();
    let message = failure.message.clone();
    record.snapshot.write().await.error = Some(message.clone());
    if let Err(error) = record
        .push(
            "chat_error",
            json!({
                "message": message,
                "kind": failure.kind,
                "retry": failure.retry,
                "delivery": failure.delivery,
            }),
        )
        .await
    {
        tracing::error!(%chat_id, %error, "could not persist compaction failure");
        return;
    }
    let status = if failure.kind == RuntimeFailureKind::Cancelled {
        ChatStatus::Cancelled
    } else {
        ChatStatus::Failed
    };
    if let Err(error) = record.set_status(status).await {
        tracing::error!(%chat_id, %error, "could not persist failed compaction status");
    }
}

async fn dispatch_after_operation(
    state: AppState,
    chat_id: String,
    record: Arc<ChatRecord>,
    fallback_body: ChatMessageRequest,
) {
    let removed = {
        let mut active = state.active_chats.write().await;
        let is_current = active
            .get(&chat_id)
            .is_some_and(|current| Arc::ptr_eq(current, &record));
        if is_current {
            active.remove(&chat_id);
        }
        is_current
    };
    if !removed {
        return;
    }

    let preferred = state.send_now.lock().await.remove(&chat_id);
    let queued = match preferred.as_ref() {
        Some((message_id, _)) => state.queue_store.pop_queued(&chat_id, message_id).await,
        None => state.queue_store.pop_next_queued(&chat_id).await,
    };
    let queued = match queued {
        Ok(message) => message,
        Err(error) => {
            tracing::error!(%chat_id, %error, "could not pop the next queued message");
            return;
        }
    };
    let Some(message) = queued else {
        return;
    };

    let mut request = preferred.map_or(fallback_body, |(_, request)| request);
    request.prompt.clone_from(&message.content);
    request.attachments.clone_from(&message.attachments);
    if start_chat_message(state.clone(), chat_id.clone(), request)
        .await
        .is_err()
    {
        tracing::error!(%chat_id, message_id = %message.id, "could not dispatch queued message; returning it to the durable queue");
        if let Err(error) = state.queue_store.enqueue(&message).await {
            tracing::error!(%chat_id, message_id = %message.id, %error, "could not restore queued message");
        }
    }
}

fn validate_message(content: &str, attachments: &[ChatAttachment]) -> Result<(), ApiError> {
    if (content.is_empty() && attachments.is_empty()) || content.len() > 32 * 1024 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "A message needs text or an attachment, and text cannot exceed 32768 characters." }),
            ),
        ));
    }
    if attachments.len() > 32
        || attachments.iter().any(|attachment| {
            attachment.id.trim().is_empty()
                || attachment.name.trim().is_empty()
                || attachment.uri.trim().is_empty()
                || attachment.id.len() > 256
                || attachment.name.len() > 1_024
                || attachment.uri.len() > 8_192
        })
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "Messages support up to 32 attachments with non-empty IDs, names, and URIs." }),
            ),
        ));
    }
    Ok(())
}

fn prompt_with_attachments(content: &str, attachments: &[ChatAttachment]) -> String {
    if attachments.is_empty() {
        return content.to_string();
    }
    let mut prompt = content.to_string();
    if !prompt.is_empty() {
        prompt.push_str("\n\n");
    }
    prompt.push_str("Attachments supplied with this message:\n");
    for attachment in attachments {
        let media_type = attachment
            .media_type
            .as_deref()
            .map_or(String::new(), |value| format!(" [{value}]"));
        prompt.push_str(&format!(
            "- {}{}: {}\n",
            attachment.name, media_type, attachment.uri
        ));
    }
    prompt
}

fn request_working_directory(state: &AppState, body: &ChatMessageRequest) -> PathBuf {
    body.working_directory.clone().unwrap_or_else(|| {
        if matches!(body.target, ExecutionTargetRequest::Local) {
            state.workspace.clone()
        } else {
            PathBuf::from(".")
        }
    })
}

fn chat_title(prompt: &str) -> String {
    normalize_session_title(prompt).unwrap_or_else(|| "New agent chat".to_string())
}

fn normalize_session_title(value: &str) -> Option<String> {
    let first_line = value.lines().next().unwrap_or(value).trim();
    if first_line.is_empty() {
        return None;
    }
    let mut title = first_line.chars().take(72).collect::<String>();
    if first_line.chars().count() > 72 {
        title.push('…');
    }
    Some(title)
}

// Keep `from_secs` so this example remains compatible with the SDK's Rust 1.82 MSRV.
#[allow(clippy::duration_suboptimal_units)]
async fn execute_turn(
    state: AppState,
    record: Arc<ChatRecord>,
    body: ChatMessageRequest,
    prompt: String,
    cancellation: CancellationToken,
    live_runtime: Option<AgentRuntime>,
    resume_session_id: Option<String>,
) {
    let chat_id = record.snapshot.read().await.id.clone();
    if let Err(error) = record.set_status(ChatStatus::Running).await {
        tracing::error!(%chat_id, %error, "could not persist running status");
        return;
    }
    if matches!(body.mode, ExecutionMode::Demo) {
        if let Err(error) = record
            .push(
                "plan_created",
                json!({
                    "title": "Verify the runtime boundary",
                    "steps": [
                        "Open a supervised provider process",
                        "Pause for application approval",
                        "Stream the terminal result"
                    ]
                }),
            )
            .await
        {
            tracing::error!(%chat_id, %error, "could not persist plan event");
            return;
        }
    }

    let provider = if matches!(body.mode, ExecutionMode::Demo) {
        Provider::Claude
    } else {
        body.provider.provider()
    };
    let working_directory = request_working_directory(&state, &body);
    let provider_prompt = prompt_with_attachments(&prompt, &body.attachments);
    let mut request = TurnRequest::new(provider, working_directory, provider_prompt);
    request.permission_mode = body.permission.permission();
    request.harness_options = body.harness_options;
    request.reasoning = body.reasoning;
    request.model = if matches!(body.mode, ExecutionMode::Demo) {
        Some(match body.scenario {
            DemoScenario::Approval => "approval".to_string(),
            DemoScenario::Failure => "failure".to_string(),
        })
    } else {
        body.model
    };
    request.session_id = resume_session_id;
    request.timeout = Duration::from_secs(5 * 60);
    request.interaction_timeout = Duration::from_secs(2 * 60);
    request.cancellation = cancellation;

    let sink = ChatSink {
        record: Arc::clone(&record),
    };
    let interactions = ChatInteractions {
        record: Arc::clone(&record),
    };
    let runtime = if matches!(body.mode, ExecutionMode::Demo) {
        &state.demo_runtime
    } else {
        live_runtime
            .as_ref()
            .expect("an installed chat message is created with a configured runtime")
    };

    match runtime.run(request, &sink, Some(&interactions)).await {
        Ok(result) => {
            if let Err(error) = finish_turn(&record, result).await {
                tracing::error!(%chat_id, %error, "could not persist turn result");
            }
        }
        Err(RuntimeError::Cancelled { .. }) => {
            if let Err(error) = record.set_status(ChatStatus::Cancelled).await {
                tracing::error!(%chat_id, %error, "could not persist cancellation");
            }
        }
        Err(error) => {
            let message = error.to_string();
            record.snapshot.write().await.error = Some(message.clone());
            record.snapshot.write().await.draft.clear();
            if let Err(store_error) = record
                .push("chat_error", json!({ "message": message }))
                .await
            {
                tracing::error!(%chat_id, %store_error, "could not persist runtime error");
                return;
            }
            if let Err(store_error) = record.set_status(ChatStatus::Failed).await {
                tracing::error!(%chat_id, %store_error, "could not persist failed status");
            }
        }
    }
}

async fn finish_turn(record: &ChatRecord, result: TurnResult) -> Result<(), StoreError> {
    let remaining_text = {
        let mut snapshot = record.snapshot.write().await;
        snapshot.session_id.clone_from(&result.session_id);
        if let Some(title) = result
            .session_title
            .as_deref()
            .and_then(normalize_session_title)
        {
            snapshot.title = title;
        }
        if result.model.is_some() {
            snapshot.model.clone_from(&result.model);
        }
        std::mem::take(&mut snapshot.draft)
    };
    record
        .push(
            "turn_result",
            serde_json::to_value(&result).unwrap_or(Value::Null),
        )
        .await?;
    let final_text = if !remaining_text.is_empty() {
        remaining_text
    } else if !record.assistant_text_observed.load(Ordering::Acquire) {
        result.text.clone()
    } else {
        String::new()
    };
    if !final_text.is_empty() {
        record
            .append_message("assistant", final_text, Vec::new())
            .await?;
    }
    record
        .set_status(match result.status {
            RunStatus::Succeeded => ChatStatus::Succeeded,
            _ => ChatStatus::Failed,
        })
        .await?;
    Ok(())
}

async fn cancel_chat(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
) -> Result<StatusCode, ApiError> {
    let Some(record) = find_active_chat(&state, &chat_id).await else {
        return match state
            .store
            .load_chat(&chat_id)
            .await
            .map_err(store_api_error)?
        {
            Some(view) if is_terminal_status(view.chat.status) => Ok(StatusCode::NO_CONTENT),
            Some(_) => Err(conflict_api_error(
                "This chat is not attached to the current server process.",
            )),
            None => Err(not_found_api_error()),
        };
    };
    let status = record.snapshot.read().await.status;
    if matches!(
        status,
        ChatStatus::Succeeded | ChatStatus::Failed | ChatStatus::Cancelled
    ) {
        return Ok(StatusCode::NO_CONTENT);
    }
    record
        .set_status(ChatStatus::Cancelling)
        .await
        .map_err(store_api_error)?;
    record.cancellation.cancel();
    Ok(StatusCode::ACCEPTED)
}

async fn resolve_approval(
    State(state): State<AppState>,
    AxumPath((chat_id, approval_id)): AxumPath<(String, String)>,
    Json(body): Json<ApprovalBody>,
) -> Result<StatusCode, ApiError> {
    let record = find_active_chat(&state, &chat_id)
        .await
        .ok_or_else(not_found_api_error)?;
    let (decision, decision_name) = match body.decision {
        ApprovalChoice::Allow => (ApprovalDecision::Allow, "allow"),
        ApprovalChoice::Deny => (
            ApprovalDecision::Deny {
                reason: Some("Denied from the example UI.".to_string()),
            },
            "deny",
        ),
    };
    state
        .store
        .resolve_approval(
            &chat_id,
            record.current_message_sequence.load(Ordering::Relaxed),
            &approval_id,
            decision_name,
        )
        .await
        .map_err(store_api_error)?;
    record
        .interactions
        .resolve_approval(&approval_id, decision)
        .map_err(|error| conflict_api_error(&error.to_string()))?;
    record
        .set_status(ChatStatus::Running)
        .await
        .map_err(store_api_error)?;
    Ok(StatusCode::ACCEPTED)
}

async fn resolve_question(
    State(state): State<AppState>,
    AxumPath((chat_id, question_id)): AxumPath<(String, String)>,
    Json(answer): Json<QuestionAnswer>,
) -> Result<StatusCode, ApiError> {
    let Some(answers) = answer.answers.as_object() else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Question answers must be a JSON object keyed by question text."
            })),
        ));
    };
    if answers.is_empty()
        || answers.len() > 8
        || answers.iter().any(|(question, value)| {
            question.is_empty()
                || question.len() > 2_048
                || value
                    .as_str()
                    .is_none_or(|answer| answer.is_empty() || answer.len() > 8_192)
        })
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Submit between 1 and 8 non-empty string answers within the size limits."
            })),
        ));
    }
    let record = find_active_chat(&state, &chat_id)
        .await
        .ok_or_else(not_found_api_error)?;
    if record.snapshot.read().await.status != ChatStatus::InputNeeded {
        return Err(conflict_api_error(
            "This chat is not waiting for an answer.",
        ));
    }
    let pending = record.events.read().await.iter().rev().any(|event| {
        event.kind == "turn_event"
            && event.payload.get("type").and_then(Value::as_str) == Some("question_requested")
            && event.payload.get("id").and_then(Value::as_str) == Some(question_id.as_str())
    });
    if !pending {
        return Err(conflict_api_error(
            "The requested question is not pending for this chat.",
        ));
    }
    record
        .interactions
        .resolve_question(&question_id, answer.clone())
        .map_err(|error| conflict_api_error(&error.to_string()))?;
    record
        .push(
            "question_answered",
            json!({ "id": question_id, "answers": answer.answers }),
        )
        .await
        .map_err(store_api_error)?;
    record
        .set_status(ChatStatus::Running)
        .await
        .map_err(store_api_error)?;
    Ok(StatusCode::ACCEPTED)
}

async fn stream_events(
    State(state): State<AppState>,
    AxumPath(chat_id): AxumPath<String>,
    Query(query): Query<EventQuery>,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let active = find_active_chat(&state, &chat_id).await;
    let mut receiver = active.as_ref().map(|record| record.sender.subscribe());
    let replay = if let Some(record) = active.as_ref() {
        record.events.read().await.clone()
    } else {
        state
            .store
            .load_chat(&chat_id)
            .await
            .map_err(store_api_error)?
            .ok_or_else(not_found_api_error)?
            .events
    }
    .into_iter()
    .filter(|event| event.sequence > query.after)
    .collect::<Vec<_>>();
    let stream = async_stream::stream! {
        let mut last_sequence = query.after;
        for item in replay {
            last_sequence = item.sequence;
            yield Ok(sse_event(&item));
            if event_is_terminal(&item) {
                return;
            }
        }
        while let Some(active_receiver) = receiver.as_mut() {
            match active_receiver.recv().await {
                Ok(item) if item.sequence > last_sequence => {
                    last_sequence = item.sequence;
                    yield Ok(sse_event(&item));
                    if event_is_terminal(&item) {
                        break;
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let missing = active.as_ref().expect("receiver only exists for active chats").events.read().await
                        .iter()
                        .filter(|event| event.sequence > last_sequence)
                        .cloned()
                        .collect::<Vec<_>>();
                    for item in missing {
                        last_sequence = item.sequence;
                        yield Ok(sse_event(&item));
                        if event_is_terminal(&item) {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn sse_event(event: &ChatEvent) -> Event {
    Event::default()
        .id(event.sequence.to_string())
        .event(&event.kind)
        .json_data(event)
        .unwrap_or_else(|_| Event::default().event("serialization_error"))
}

fn event_is_terminal(event: &ChatEvent) -> bool {
    event.kind == "chat_status"
        && matches!(
            event.payload.get("status").and_then(Value::as_str),
            Some("succeeded" | "failed" | "cancelled")
        )
}

const fn is_terminal_status(status: ChatStatus) -> bool {
    matches!(
        status,
        ChatStatus::Succeeded | ChatStatus::Failed | ChatStatus::Cancelled
    )
}

async fn find_active_chat(state: &AppState, chat_id: &str) -> Option<Arc<ChatRecord>> {
    state.active_chats.read().await.get(chat_id).cloned()
}

struct ChatSink {
    record: Arc<ChatRecord>,
}

#[async_trait]
impl EventSink for ChatSink {
    async fn emit(&self, event: TurnEvent) -> temps_agent_runtime::Result<()> {
        let (provider, approval, assistant_segment) = {
            let mut snapshot = self.record.snapshot.write().await;
            let approval = match &event {
                TurnEvent::ApprovalRequested(request)
                | TurnEvent::PlanApprovalRequested(request) => {
                    Some((snapshot.id.clone(), request.clone()))
                }
                _ => None,
            };
            match &event {
                TurnEvent::TextDelta { text } => {
                    self.record
                        .assistant_text_observed
                        .store(true, Ordering::Release);
                    snapshot.draft.push_str(text);
                }
                TurnEvent::SessionStarted { session_id, title } => {
                    snapshot.session_id = Some(session_id.clone());
                    if let Some(title) = title.as_deref().and_then(normalize_session_title) {
                        snapshot.title = title;
                    }
                }
                TurnEvent::PermissionModeChanged { mode } => {
                    snapshot.harness_options.insert(
                        "permission_mode".to_string(),
                        claude_permission_option(mode),
                    );
                    if let Some(permission) = PermissionChoice::from_effective_mode(mode) {
                        snapshot.permission = permission;
                    }
                }
                TurnEvent::Usage(usage) => {
                    snapshot.usage.input_tokens = usage.input_tokens.or(snapshot.usage.input_tokens);
                    snapshot.usage.output_tokens =
                        usage.output_tokens.or(snapshot.usage.output_tokens);
                    snapshot.usage.cache_creation_input_tokens = usage
                        .cache_creation_input_tokens
                        .or(snapshot.usage.cache_creation_input_tokens);
                    snapshot.usage.cache_read_input_tokens = usage
                        .cache_read_input_tokens
                        .or(snapshot.usage.cache_read_input_tokens);
                    snapshot.usage.context_window = usage
                        .context_window
                        .clone()
                        .or_else(|| snapshot.usage.context_window.clone());
                    snapshot.usage.cost_usd = usage.cost_usd.or(snapshot.usage.cost_usd);
                }
                TurnEvent::AccountUsageUpdated { usage } => {
                    snapshot.account_usage = Some(usage.clone());
                }
                TurnEvent::ApprovalRequested(_) | TurnEvent::PlanApprovalRequested(_) => {
                    snapshot.status = ChatStatus::ApprovalNeeded;
                }
                TurnEvent::QuestionRequested(_) => {
                    snapshot.status = ChatStatus::InputNeeded;
                }
                _ => {}
            }
            let boundary = matches!(
                &event,
                TurnEvent::ToolCall {
                    status: ToolCallStatus::Started,
                    ..
                } | TurnEvent::ApprovalRequested(_)
                    | TurnEvent::PlanApprovalRequested(_)
                    | TurnEvent::QuestionRequested(_)
            );
            let assistant_segment = boundary
                .then(|| std::mem::take(&mut snapshot.draft))
                .filter(|segment| !segment.is_empty());
            (snapshot.provider.provider(), approval, assistant_segment)
        };
        if let Some(segment) = assistant_segment {
            self.record
                .append_message("assistant", segment, Vec::new())
                .await
                .map_err(|error| RuntimeError::EventSink {
                    provider,
                    message: error.to_string(),
                })?;
        }
        if let Some((chat_id, request)) = approval {
            self.record
                .store
                .save_approval(
                    &chat_id,
                    self.record.current_message_sequence.load(Ordering::Relaxed),
                    &request,
                )
                .await
                .map_err(|error| RuntimeError::EventSink {
                    provider,
                    message: error.to_string(),
                })?;
        }
        self.record
            .push(
                "turn_event",
                serde_json::to_value(event).unwrap_or(Value::Null),
            )
            .await
            .map_err(|error| RuntimeError::EventSink {
                provider,
                message: error.to_string(),
            })?;
        Ok(())
    }
}

struct ChatInteractions {
    record: Arc<ChatRecord>,
}

#[async_trait]
impl InteractionHandler for ChatInteractions {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        if let Err(error) = self.record.set_status(ChatStatus::ApprovalNeeded).await {
            return ApprovalDecision::Deny {
                reason: Some(format!("Could not persist approval state: {error}")),
            };
        }
        self.record.interactions.approve(request).await
    }

    async fn answer(&self, request: QuestionRequest) -> Option<QuestionAnswer> {
        if self
            .record
            .set_status(ChatStatus::InputNeeded)
            .await
            .is_err()
        {
            return None;
        }
        self.record.interactions.answer(request).await
    }
}

struct DemoAdapter;

#[async_trait]
impl AgentAdapter for DemoAdapter {
    fn provider(&self) -> Provider {
        Provider::Claude
    }

    fn executable(&self) -> PathBuf {
        std::env::current_exe().unwrap_or_else(|_| PathBuf::from("agent-runtime-fullstack-example"))
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            default: true,
            accept_edits: true,
            plan: true,
            full_access: true,
            custom: false,
            live_approvals: true,
            live_questions: false,
        }
    }

    async fn readiness(&self) -> ProviderReadiness {
        ProviderReadiness {
            provider: Provider::Claude,
            installed: true,
            executable: Some(self.executable()),
            version: Some("demo-provider 0.1.0".to_string()),
            detail: "Deterministic provider embedded in the example server.".to_string(),
        }
    }

    fn command(&self, request: &TurnRequest) -> temps_agent_runtime::Result<CommandSpec> {
        let mut command = CommandSpec::new(self.executable());
        command.args.extend([
            "__demo_provider".into(),
            "--scenario".into(),
            request.model.as_deref().unwrap_or("approval").into(),
        ]);
        command.initial_stdin = Some(request.prompt.as_bytes().to_vec());
        command.interactive_stdin = true;
        Ok(command)
    }

    fn parse_line(
        &self,
        line: &str,
        state: &mut AdapterState,
    ) -> temps_agent_runtime::Result<AdapterOutput> {
        let value: Value = serde_json::from_str(line).map_err(|error| RuntimeError::Protocol {
            provider: Provider::Claude,
            message: format!("invalid demo frame: {error}"),
        })?;
        let mut output = AdapterOutput::default();
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "session" => {
                let session_id = value["id"].as_str().unwrap_or("demo-session").to_string();
                state.result.session_title = value["title"].as_str().map(str::to_string);
                if state.result.session_id.as_deref() != Some(session_id.as_str()) {
                    state.result.session_id = Some(session_id.clone());
                    output.events.push(TurnEvent::SessionStarted {
                        session_id,
                        title: state.result.session_title.clone(),
                    });
                }
            }
            "reasoning" => {
                let text = value["text"].as_str().unwrap_or_default().to_string();
                state
                    .result
                    .reasoning
                    .get_or_insert_with(String::new)
                    .push_str(&text);
                output.events.push(TurnEvent::ReasoningDelta { text });
            }
            "approval" => {
                let request = ApprovalRequest {
                    id: value["id"].as_str().unwrap_or("approval_demo").to_string(),
                    tool_name: value["tool"].as_str().unwrap_or("command").to_string(),
                    input: value.get("input").cloned().unwrap_or(Value::Null),
                    description: value["description"].as_str().map(str::to_string),
                };
                output
                    .events
                    .push(TurnEvent::ApprovalRequested(request.clone()));
                output.interaction = Some(InteractionRequest::Approval {
                    request,
                    original: value,
                });
            }
            "tool" => {
                let status = match value["status"].as_str() {
                    Some("started") => ToolCallStatus::Started,
                    Some("failed") => ToolCallStatus::Failed,
                    _ => ToolCallStatus::Succeeded,
                };
                output.events.push(TurnEvent::ToolCall {
                    id: value["id"].as_str().map(str::to_string),
                    name: value["name"].as_str().unwrap_or("command").to_string(),
                    status,
                    input: value.get("input").cloned(),
                    output: value["output"].as_str().map(str::to_string),
                    error: value["error"].as_str().map(str::to_string),
                    task_id: None,
                });
            }
            "text" => {
                let text = value["text"].as_str().unwrap_or_default().to_string();
                state.result.text.push_str(&text);
                output.events.push(TurnEvent::TextDelta { text });
            }
            "usage" => {
                let usage = Usage {
                    input_tokens: value["input_tokens"].as_u64(),
                    output_tokens: value["output_tokens"].as_u64(),
                    context_window: Some(ContextWindowUsage {
                        used_tokens: value["context_used_tokens"].as_u64(),
                        limit_tokens: value["context_limit_tokens"].as_u64(),
                        model: value["model"].as_str().map(str::to_owned),
                        estimated: false,
                    }),
                    cost_usd: value["cost_usd"].as_f64(),
                    ..Usage::default()
                };
                state.result.usage = usage.clone();
                output.events.push(TurnEvent::Usage(usage));
            }
            "account_usage" => {
                if let Ok(usage) = serde_json::from_value::<AccountUsageSnapshot>(value["usage"].clone()) {
                    output.events.push(TurnEvent::AccountUsageUpdated { usage });
                }
            }
            "completed" => output.terminal = true,
            "error" => {
                state.result.status = RunStatus::Failed;
                output.events.push(TurnEvent::Warning {
                    message: value["message"]
                        .as_str()
                        .unwrap_or("Demo failure")
                        .to_string(),
                });
                output.terminal = true;
            }
            _ => {}
        }
        Ok(output)
    }

    fn approval_response(
        &self,
        _request: &ApprovalRequest,
        _original: &Value,
        decision: ApprovalDecision,
    ) -> temps_agent_runtime::Result<Option<Vec<u8>>> {
        let decision = match decision {
            ApprovalDecision::Allow => "allow",
            _ => "deny",
        };
        Ok(Some(
            json!({ "decision": decision }).to_string().into_bytes(),
        ))
    }

    fn question_response(
        &self,
        _request: &QuestionRequest,
        _original: &Value,
        _answer: Option<QuestionAnswer>,
    ) -> temps_agent_runtime::Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

async fn demo_provider_process() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    let scenario = args
        .windows(2)
        .find(|pair| pair[0] == "--scenario")
        .map_or("approval", |pair| pair[1].as_str());
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    let prompt = input.next_line().await.ok().flatten().unwrap_or_default();
    let mut output = tokio::io::stdout();
    let session_id = format!("demo-{}", std::process::id());
    emit_frame(
        &mut output,
        json!({ "type": "session", "id": session_id, "title": chat_title(&prompt) }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(90)).await;
    emit_frame(
        &mut output,
        json!({
            "type": "reasoning",
            "text": format!("Prepared a bounded plan for {} prompt bytes.", prompt.len())
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(90)).await;

    if scenario == "failure" {
        emit_frame(
            &mut output,
            json!({ "type": "error", "message": "The deterministic provider failure was requested." }),
        )
        .await;
        return ExitCode::from(7);
    }

    emit_frame(
        &mut output,
        json!({
            "type": "approval",
            "id": "approval_demo_command",
            "tool": "cargo test",
            "description": "Allow the deterministic provider to simulate a read-only test command.",
            "input": { "command": "cargo test --doc" }
        }),
    )
    .await;
    let response = input
        .next_line()
        .await
        .ok()
        .flatten()
        .and_then(|line| serde_json::from_str::<Value>(&line).ok());
    let allowed = response
        .as_ref()
        .and_then(|value| value["decision"].as_str())
        == Some("allow");
    if !allowed {
        emit_frame(
            &mut output,
            json!({
                "type": "tool",
                "id": "tool_demo_command",
                "name": "cargo test",
                "status": "failed",
                "error": "Approval denied."
            }),
        )
        .await;
        emit_frame(
            &mut output,
            json!({ "type": "text", "text": "The command was denied, so no tool process was started." }),
        )
        .await;
        emit_frame(&mut output, json!({ "type": "completed" })).await;
        return ExitCode::SUCCESS;
    }

    emit_frame(
        &mut output,
        json!({
            "type": "tool",
            "id": "tool_demo_command",
            "name": "cargo test",
            "status": "started",
            "input": { "command": "cargo test --doc" }
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(180)).await;
    emit_frame(
        &mut output,
        json!({
            "type": "tool",
            "id": "tool_demo_command",
            "name": "cargo test",
            "status": "succeeded",
            "output": "Doc tests passed."
        }),
    )
    .await;
    emit_frame(
        &mut output,
        json!({ "type": "text", "text": "The SDK stream is live. " }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    emit_frame(
        &mut output,
        json!({ "type": "text", "text": "Approval completed and the turn finished safely." }),
    )
    .await;
    emit_frame(
        &mut output,
        json!({ "type": "usage", "input_tokens": 48, "output_tokens": 19, "context_used_tokens": 128000, "context_limit_tokens": 200000, "model": "claude-sonnet-5", "cost_usd": 0.0 }),
    )
    .await;
    emit_frame(
        &mut output,
        json!({
            "type": "account_usage",
            "usage": {
                "provider": "claude",
                "plan": "pro",
                "windows": [
                    { "id": "five_hour", "kind": "session", "used_percent": 63.0, "duration_minutes": 300, "resets_at_unix_seconds": now_secs() + 7200 },
                    { "id": "seven_day", "kind": "weekly", "used_percent": 41.0, "duration_minutes": 10080, "resets_at_unix_seconds": now_secs() + 432000 }
                ],
                "credits": null
            }
        }),
    )
    .await;
    emit_frame(&mut output, json!({ "type": "completed" })).await;
    ExitCode::SUCCESS
}

async fn emit_frame(output: &mut tokio::io::Stdout, value: Value) {
    let _ = output.write_all(value.to_string().as_bytes()).await;
    let _ = output.write_all(b"\n").await;
    let _ = output.flush().await;
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod upload_tests {
    use super::*;

    async fn test_chat_record(model: Option<&str>) -> (tempfile::TempDir, Arc<ChatRecord>) {
        let directory = tempfile::tempdir().expect("temporary chat directory");
        let store = Arc::new(
            SqliteChatStore::open(&directory.path().join("runtime.sqlite3"))
                .await
                .expect("open chat store"),
        );
        let snapshot = ChatSnapshot {
            id: "chat_projection".to_owned(),
            title: "Projection test".to_owned(),
            mode: ExecutionMode::Installed,
            provider: ProviderChoice::Claude,
            permission: PermissionChoice::Plan,
            scenario: DemoScenario::Approval,
            target: "Local machine".to_owned(),
            connection_kind: ConnectionKind::Local,
            connection_id: None,
            connection_key: "local".to_owned(),
            working_directory: ".".to_owned(),
            status: ChatStatus::Running,
            draft: String::new(),
            session_id: None,
            model: model.map(str::to_owned),
            reasoning: None,
            usage: Usage::default(),
            account_usage: None,
            harness_options: BTreeMap::new(),
            error: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        let message = ChatMessage {
            sequence: 1,
            role: "user".to_owned(),
            content: "Ask me a question".to_owned(),
            attachments: Vec::new(),
            created_at_ms: 1,
        };
        store
            .create_chat(&snapshot, &message)
            .await
            .expect("create chat");
        let store: Arc<dyn ChatStore> = store;
        let record = ChatRecord::new(
            snapshot,
            vec![message],
            Vec::new(),
            CancellationToken::new(),
            store,
        );
        (directory, record)
    }

    #[test]
    fn saved_connection_identity_is_stable_and_secret_free() {
        let target = ExecutionTargetRequest::Ssh {
            connection_id: Some("ssh_0042".to_string()),
            host: "agent.example.com".to_string(),
            user: Some("runner".to_string()),
            port: Some(22),
            authentication: SshAuthenticationRequest::Password {
                password: "do-not-persist-this".to_string(),
            },
            known_hosts_file: None,
            accept_new_host_key: false,
        };

        let identity = connection_identity(&target);
        assert_eq!(identity.kind, ConnectionKind::Ssh);
        assert_eq!(identity.id.as_deref(), Some("ssh_0042"));
        assert_eq!(identity.key, "ssh:saved:ssh_0042");
        assert_eq!(identity.label, "runner@agent.example.com");
        assert!(!identity.key.contains("do-not-persist-this"));
    }

    #[test]
    fn upload_names_cannot_escape_the_target_directory() {
        assert_eq!(safe_upload_name("../../private key.txt"), "private_key.txt");
        assert_eq!(safe_upload_name(".."), "upload.bin");
    }

    #[tokio::test]
    async fn local_upload_preserves_binary_bytes_exactly() {
        let temporary = tempfile::tempdir().expect("temporary upload directory");
        let relative_directory = PathBuf::from(".temps-agent-runtime/uploads/upload_test");
        let relative_path = relative_directory.join("payload.bin");
        let bytes = b"binary\0payload\nwithout-an-appended-newline";

        store_upload(
            Arc::new(LocalTransport),
            temporary.path().to_path_buf(),
            &relative_directory,
            &relative_path,
            bytes,
        )
        .await
        .expect("store upload");

        assert_eq!(
            tokio::fs::read(temporary.path().join(relative_path))
                .await
                .expect("read uploaded bytes"),
            bytes
        );
    }

    #[tokio::test]
    async fn assistant_text_is_persisted_on_both_sides_of_a_question() {
        let (_directory, record) = test_chat_record(Some("claude-opus-5")).await;
        let sink = ChatSink {
            record: Arc::clone(&record),
        };
        sink.emit(TurnEvent::TextDelta {
            text: "Before the question.".to_owned(),
        })
        .await
        .expect("emit pre-question text");
        sink.emit(TurnEvent::ToolCall {
            id: Some("tool-question".to_owned()),
            name: "AskUserQuestion".to_owned(),
            status: ToolCallStatus::Started,
            input: None,
            output: None,
            error: None,
            task_id: None,
        })
        .await
        .expect("emit question tool");
        sink.emit(TurnEvent::QuestionRequested(QuestionRequest::new(
            "question-fruit",
            json!([]),
        )))
        .await
        .expect("emit question request");
        sink.emit(TurnEvent::TextDelta {
            text: "After the answer.".to_owned(),
        })
        .await
        .expect("emit post-answer text");
        finish_turn(
            &record,
            TurnResult {
                text: "Before the question.After the answer.".to_owned(),
                model: Some("claude-opus-5".to_owned()),
                ..TurnResult::default()
            },
        )
        .await
        .expect("finish projected turn");

        let view = record.view().await;
        assert_eq!(
            view.messages
                .iter()
                .map(|message| (message.role.as_str(), message.content.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("user", "Ask me a question"),
                ("assistant", "Before the question."),
                ("assistant", "After the answer."),
            ]
        );
        let before_event = view
            .events
            .iter()
            .find(|event| {
                event.kind == "message_appended"
                    && event.payload.get("content") == Some(&json!("Before the question."))
            })
            .expect("pre-question message event");
        let tool_event = view
            .events
            .iter()
            .find(|event| {
                event.kind == "turn_event" && event.payload.get("type") == Some(&json!("tool_call"))
            })
            .expect("question tool event");
        assert!(before_event.sequence < tool_event.sequence);
    }

    #[tokio::test]
    async fn effective_permission_mode_updates_the_persisted_chat_projection() {
        let (_directory, record) = test_chat_record(Some("claude-opus-5")).await;
        let sink = ChatSink {
            record: Arc::clone(&record),
        };

        sink.emit(TurnEvent::PermissionModeChanged {
            mode: PermissionMode::Plan,
        })
        .await
        .expect("persist plan mode");

        let view = record.view().await;
        assert_eq!(
            view.chat.harness_options.get("permission_mode"),
            Some(&"plan".to_owned())
        );
        assert!(matches!(view.chat.permission, PermissionChoice::Plan));
        assert!(view.events.iter().any(|event| {
            event.kind == "turn_event"
                && event.payload.get("type") == Some(&json!("permission_mode_changed"))
                && event.payload.get("mode") == Some(&json!("plan"))
        }));
    }

    #[tokio::test]
    async fn missing_provider_model_does_not_erase_the_selected_chat_model() {
        let (_directory, record) = test_chat_record(Some("claude-opus-5")).await;

        finish_turn(&record, TurnResult::default())
            .await
            .expect("finish turn without a reported model");

        assert_eq!(
            record.snapshot.read().await.model.as_deref(),
            Some("claude-opus-5")
        );
    }
}
