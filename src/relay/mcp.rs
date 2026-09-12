use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{LaunchContext, McpServerConfig};

use super::{
    AgentAddress, AgentDeliveryQuery, AgentDeliveryReceipt, AgentDiscoveryQuery,
    AgentIdempotencyKey, AgentMessage, AgentMessageAttachment, AgentMessageId,
    AgentMessageProvenance, AgentMessageRouter, AgentRelayActivity, AgentRelayActivityKind,
    AgentRelayContext, AgentRelayDeliveryState, AgentRelayError, AgentRelayErrorKind,
    AgentRelayMetadata, AgentRelayResult, AgentRelayRetryAdvice, AgentRelayValidationError,
    AgentThreadId, MessagingGrant, ReplyToAgentMessageRequest, SendAgentMessageRequest,
};

/// Default provider-visible MCP server name.
pub const AGENT_RELAY_MCP_SERVER_NAME: &str = "agent_relay";
/// Provider-neutral discovery tool name.
pub const AGENT_RELAY_TOOL_DISCOVER: &str = "agent_relay_discover";
/// Provider-neutral send tool name.
pub const AGENT_RELAY_TOOL_SEND: &str = "agent_relay_send";
/// Provider-neutral reply tool name.
pub const AGENT_RELAY_TOOL_REPLY: &str = "agent_relay_reply";
/// Provider-neutral delivery reconciliation tool name.
pub const AGENT_RELAY_TOOL_STATUS: &str = "agent_relay_status";

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const DEFAULT_DISCOVERY_LIMIT: u16 = 20;
const DEFAULT_TTL_SECONDS: u32 = 3_600;

/// Explicit LaunchContext exposure for an application-hosted relay MCP bridge.
///
/// Constructing a bridge does not expose it to a harness. The application must
/// install this value into a runtime or turn launch context and supply every
/// referenced secret separately through the redacted environment map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRelayMcpExposure {
    server_name: String,
    server: McpServerConfig,
}

impl AgentRelayMcpExposure {
    /// Creates an HTTP MCP exposure using an environment-sourced Authorization header.
    ///
    /// The referenced environment value should contain the complete header value,
    /// normally `Bearer <short-lived-token>`. No token is stored in this config.
    pub fn http(
        url: impl Into<String>,
        authorization_source_env: impl Into<String>,
    ) -> Result<Self, AgentRelayValidationError> {
        Self::http_named(AGENT_RELAY_MCP_SERVER_NAME, url, authorization_source_env)
    }

    /// Creates a named HTTP MCP exposure.
    pub fn http_named(
        server_name: impl Into<String>,
        url: impl Into<String>,
        authorization_source_env: impl Into<String>,
    ) -> Result<Self, AgentRelayValidationError> {
        let server_name = validate_server_name(server_name.into())?;
        let url = url.into();
        let parsed = crate::url_security::validate_http_endpoint(&url).map_err(|message| {
            AgentRelayValidationError::InvalidField {
                field: "relay_mcp.url",
                message: message.to_owned(),
            }
        })?;
        if parsed.scheme() == "http" && !crate::url_security::is_loopback_endpoint(&parsed) {
            return Err(AgentRelayValidationError::InvalidField {
                field: "relay_mcp.url",
                message: "authenticated relay MCP requires HTTPS or loopback HTTP".to_owned(),
            });
        }
        let source = validate_environment_name(authorization_source_env.into())?;
        Ok(Self {
            server_name,
            server: McpServerConfig::Http {
                url,
                headers_from: BTreeMap::from([("Authorization".to_owned(), source)]),
            },
        })
    }

    /// Creates a stdio MCP exposure for an application-provided bridge executable.
    ///
    /// The executable is responsible for authenticating to the application router.
    /// Secrets must be referenced through `environment_from`, never placed in `args`.
    pub fn stdio(
        command: impl Into<PathBuf>,
        args: Vec<String>,
        environment_from: BTreeMap<String, String>,
    ) -> Result<Self, AgentRelayValidationError> {
        Self::stdio_named(AGENT_RELAY_MCP_SERVER_NAME, command, args, environment_from)
    }

    /// Creates a named stdio MCP exposure.
    pub fn stdio_named(
        server_name: impl Into<String>,
        command: impl Into<PathBuf>,
        args: Vec<String>,
        environment_from: BTreeMap<String, String>,
    ) -> Result<Self, AgentRelayValidationError> {
        let server_name = validate_server_name(server_name.into())?;
        for (target, source) in &environment_from {
            validate_environment_name(target.clone())?;
            validate_environment_name(source.clone())?;
        }
        Ok(Self {
            server_name,
            server: McpServerConfig::Stdio {
                command: command.into(),
                args,
                environment_from,
            },
        })
    }

    /// Returns the provider-visible MCP server name.
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Returns a provider-visible Claude MCP tool name for an adapter tool.
    pub fn claude_tool_name(&self, tool_name: &str) -> String {
        format!("mcp__{}__{tool_name}", self.server_name)
    }

    /// Explicitly installs this relay endpoint into a launch context.
    pub fn install(&self, context: &mut LaunchContext) -> Result<(), AgentRelayValidationError> {
        if context.mcp_servers.contains_key(&self.server_name) {
            return Err(AgentRelayValidationError::InvalidField {
                field: "launch_context.mcp_servers",
                message: format!(
                    "an MCP server named `{}` is already configured",
                    self.server_name
                ),
            });
        }
        context
            .mcp_servers
            .insert(self.server_name.clone(), self.server.clone());
        Ok(())
    }
}

fn validate_server_name(value: String) -> Result<String, AgentRelayValidationError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(AgentRelayValidationError::InvalidField {
            field: "relay_mcp.server_name",
            message: "must use 1-64 ASCII letters, numbers, `-`, or `_`".to_owned(),
        });
    }
    Ok(value)
}

fn validate_environment_name(value: String) -> Result<String, AgentRelayValidationError> {
    if value.is_empty() || value.contains(['=', '\0']) {
        return Err(AgentRelayValidationError::InvalidField {
            field: "relay_mcp.environment",
            message: "names must be non-empty and contain no `=` or NUL".to_owned(),
        });
    }
    Ok(value)
}

/// Optional destination for normalized, content-free relay activity.
#[async_trait]
pub trait AgentRelayActivitySink: Send + Sync {
    /// Observes one relay lifecycle operation.
    async fn emit(&self, activity: AgentRelayActivity);
}

struct NoopActivitySink;

#[async_trait]
impl AgentRelayActivitySink for NoopActivitySink {
    async fn emit(&self, _activity: AgentRelayActivity) {}
}

/// Provider-neutral result for one Agent Relay MCP tool call.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRelayMcpToolResult {
    /// Whether MCP should present the result as a tool failure.
    pub is_error: bool,
    /// Short user-safe text content.
    pub content: String,
    /// Typed structured result or error.
    pub structured_content: Value,
}

impl AgentRelayMcpToolResult {
    fn success<T: serde::Serialize>(summary: String, value: &T) -> Self {
        Self {
            is_error: false,
            content: summary,
            structured_content: serde_json::to_value(value)
                .unwrap_or_else(|_| json!({"error": "relay result serialization failed"})),
        }
    }

    fn error(error: &AgentRelayError) -> Self {
        Self {
            is_error: true,
            content: error.message.clone(),
            structured_content: json!({"error": error}),
        }
    }

    fn into_mcp_value(self) -> Value {
        json!({
            "content": [{"type": "text", "text": self.content}],
            "structuredContent": self.structured_content,
            "isError": self.is_error,
        })
    }
}

struct BridgeBudget {
    message_count: u32,
    message_bytes: usize,
    recent_messages: VecDeque<Instant>,
}

impl BridgeBudget {
    fn new() -> Self {
        Self {
            message_count: 0,
            message_bytes: 0,
            recent_messages: VecDeque::new(),
        }
    }
}

/// Reusable Agent Relay tool adapter with a host-scoped sender capability.
///
/// This type implements the MCP request semantics but intentionally does not
/// open a socket or own a process. An application mounts [`Self::handle_json_rpc`]
/// in its authenticated local HTTP/stdio MCP boundary. One bridge should be
/// scoped to one runtime or turn capability so its budgets cannot be shared or
/// reset by harness arguments.
pub struct AgentRelayMcpBridge {
    context: AgentRelayContext,
    grant: MessagingGrant,
    router: Arc<dyn AgentMessageRouter>,
    activity: Arc<dyn AgentRelayActivitySink>,
    budget: Mutex<BridgeBudget>,
}

impl AgentRelayMcpBridge {
    /// Creates a bridge for a user-originated turn.
    pub fn for_turn(
        sender: AgentAddress,
        capability_id: impl Into<String>,
        grant: MessagingGrant,
        router: Arc<dyn AgentMessageRouter>,
    ) -> Result<Self, AgentRelayValidationError> {
        grant.validate()?;
        let context = AgentRelayContext::for_turn(sender, capability_id, grant.approval)?;
        Ok(Self::new(context, grant, router))
    }

    /// Creates a bridge for a turn triggered by an inbound relay message.
    pub fn for_inbound_turn(
        sender: AgentAddress,
        capability_id: impl Into<String>,
        grant: MessagingGrant,
        inbound: AgentMessageProvenance,
        router: Arc<dyn AgentMessageRouter>,
    ) -> Result<Self, AgentRelayValidationError> {
        grant.validate()?;
        let context =
            AgentRelayContext::for_inbound_turn(sender, capability_id, grant.approval, inbound)?;
        Ok(Self::new(context, grant, router))
    }

    fn new(
        context: AgentRelayContext,
        grant: MessagingGrant,
        router: Arc<dyn AgentMessageRouter>,
    ) -> Self {
        Self {
            context,
            grant,
            router,
            activity: Arc::new(NoopActivitySink),
            budget: Mutex::new(BridgeBudget::new()),
        }
    }

    /// Adds a normalized activity destination without exposing message content.
    pub fn with_activity_sink(mut self, sink: Arc<dyn AgentRelayActivitySink>) -> Self {
        self.activity = sink;
        self
    }

    /// Returns the bound host-authenticated sender.
    pub fn sender(&self) -> &AgentAddress {
        self.context.sender()
    }

    /// Returns MCP tool definitions authorized by this grant.
    pub fn tool_definitions(&self) -> Vec<Value> {
        let mut tools = Vec::new();
        if self.grant.discovery.is_some() {
            tools.push(discovery_definition());
        }
        if self.grant.send.is_some() {
            tools.extend([send_definition(), reply_definition(), status_definition()]);
        }
        tools
    }

    /// Executes one authorized Agent Relay tool call.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> AgentRelayMcpToolResult {
        let result = match name {
            AGENT_RELAY_TOOL_DISCOVER if self.grant.discovery.is_some() => {
                self.call_discover(arguments).await
            }
            AGENT_RELAY_TOOL_SEND if self.grant.send.is_some() => self.call_send(arguments).await,
            AGENT_RELAY_TOOL_REPLY if self.grant.send.is_some() => self.call_reply(arguments).await,
            AGENT_RELAY_TOOL_STATUS if self.grant.send.is_some() => {
                self.call_status(arguments).await
            }
            _ => Err(AgentRelayError::permanent(
                AgentRelayErrorKind::InvalidRequest,
                format!("relay tool `{name}` is not exposed by this capability"),
            )),
        };

        match result {
            Ok(result) => result,
            Err(error) => {
                self.emit_error_activity(&error).await;
                AgentRelayMcpToolResult::error(&error)
            }
        }
    }

    /// Handles one minimal MCP JSON-RPC request.
    ///
    /// Supported methods are `initialize`, `ping`, `tools/list`, and
    /// `tools/call`. Notifications return `None`. Transport framing, sessions,
    /// authentication, and network listeners stay in the host application.
    pub async fn handle_json_rpc(&self, request: Value) -> Option<Value> {
        let id = request.get("id").cloned();
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            return id.map(|id| json_rpc_error(id, -32600, "invalid MCP request"));
        };
        let id = id?;
        let result = match method {
            "initialize" => json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {
                    "name": "temps-agent-runtime-relay",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            }),
            "ping" => json!({}),
            "tools/list" => json!({"tools": self.tool_definitions()}),
            "tools/call" => {
                let Some(name) = request.pointer("/params/name").and_then(Value::as_str) else {
                    return Some(json_rpc_error(id, -32602, "tool name is required"));
                };
                let arguments = request
                    .pointer("/params/arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                self.call_tool(name, arguments).await.into_mcp_value()
            }
            _ => return Some(json_rpc_error(id, -32601, "MCP method not found")),
        };
        Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
    }

    async fn call_discover(&self, arguments: Value) -> AgentRelayResult<AgentRelayMcpToolResult> {
        let input: DiscoverToolInput = parse_arguments(arguments)?;
        let discovery = self.grant.discovery.as_ref().ok_or_else(|| {
            AgentRelayError::permanent(
                AgentRelayErrorKind::InvalidRequest,
                "agent discovery is not granted",
            )
        })?;
        let limit = input
            .limit
            .unwrap_or(DEFAULT_DISCOVERY_LIMIT)
            .min(discovery.max_results);
        if limit == 0 {
            return Err(invalid_tool_input(
                "discovery limit must be greater than zero",
            ));
        }
        let query = AgentDiscoveryQuery {
            query: input.query,
            cursor: input.cursor,
            limit,
        };
        let mut page = self.router.discover(&self.context, query).await?;
        page.agents
            .retain(|entry| self.grant.permits_discovery(&entry.address));
        page.agents.truncate(usize::from(limit));
        self.emit_activity(AgentRelayActivity {
            kind: AgentRelayActivityKind::Discovery,
            sender: self.context.sender().clone(),
            recipient: None,
            message_id: None,
            thread_id: None,
            delivery_status: None,
            error_kind: None,
            detail: Some(format!(
                "{} authorized agent(s) returned",
                page.agents.len()
            )),
        })
        .await;
        Ok(AgentRelayMcpToolResult::success(
            format!("Found {} authorized agent(s).", page.agents.len()),
            &page,
        ))
    }

    async fn call_send(&self, arguments: Value) -> AgentRelayResult<AgentRelayMcpToolResult> {
        let input: SendToolInput = parse_arguments(arguments)?;
        let (recipient, idempotency_key, thread_id, reply_to, message, ttl_seconds) =
            input.into_parts();
        if !self.grant.permits_send_to(&recipient) {
            return Err(AgentRelayError::permanent(
                AgentRelayErrorKind::UnauthorizedRecipient,
                format!("the messaging grant does not authorize recipient `{recipient}`"),
            ));
        }
        self.validate_and_reserve(&message, ttl_seconds)?;
        let hop_count = self.context.next_hop_count();
        let hop_limit = self.effective_hop_limit();
        validate_hop(hop_count, hop_limit)?;
        let request = SendAgentMessageRequest {
            recipient: recipient.clone(),
            idempotency_key,
            thread_id,
            reply_to,
            message,
            ttl_seconds,
            hop_count,
            hop_limit,
        };
        let receipt = self.router.send(&self.context, request).await?;
        self.emit_receipt_activity(AgentRelayActivityKind::Send, Some(recipient), &receipt)
            .await;
        Ok(receipt_result(&receipt))
    }

    async fn call_reply(&self, arguments: Value) -> AgentRelayResult<AgentRelayMcpToolResult> {
        let input: ReplyToolInput = parse_arguments(arguments)?;
        let (in_reply_to, idempotency_key, message, ttl_seconds) = input.into_parts();
        self.validate_and_reserve(&message, ttl_seconds)?;
        let hop_count = self.context.next_hop_count();
        let hop_limit = self.effective_hop_limit();
        validate_hop(hop_count, hop_limit)?;
        let request = ReplyToAgentMessageRequest {
            in_reply_to,
            idempotency_key,
            message,
            ttl_seconds,
            hop_count,
            hop_limit,
        };
        let receipt = self.router.reply(&self.context, request).await?;
        self.emit_receipt_activity(AgentRelayActivityKind::Reply, None, &receipt)
            .await;
        Ok(receipt_result(&receipt))
    }

    async fn call_status(&self, arguments: Value) -> AgentRelayResult<AgentRelayMcpToolResult> {
        let input: StatusToolInput = parse_arguments(arguments)?;
        let query = match (input.message_id, input.idempotency_key) {
            (Some(message_id), None) => AgentDeliveryQuery::MessageId(message_id),
            (None, Some(idempotency_key)) => AgentDeliveryQuery::IdempotencyKey(idempotency_key),
            _ => {
                return Err(invalid_tool_input(
                    "provide exactly one of `message_id` or `idempotency_key`",
                ))
            }
        };
        let receipt = self.router.delivery_status(&self.context, query).await?;
        self.emit_receipt_activity(AgentRelayActivityKind::Status, None, &receipt)
            .await;
        Ok(receipt_result(&receipt))
    }

    fn validate_and_reserve(
        &self,
        message: &AgentMessage,
        ttl_seconds: u32,
    ) -> AgentRelayResult<()> {
        message
            .validate()
            .map_err(|error| invalid_tool_input(format!("invalid agent message: {error}")))?;
        if message.content.len() > self.grant.limits.max_message_bytes {
            return Err(limit_error(
                AgentRelayErrorKind::BudgetExceeded,
                format!(
                    "message is {} bytes; this capability permits {} bytes per message",
                    message.content.len(),
                    self.grant.limits.max_message_bytes
                ),
            ));
        }
        if ttl_seconds == 0 || ttl_seconds > self.grant.limits.max_ttl_seconds {
            return Err(limit_error(
                AgentRelayErrorKind::BudgetExceeded,
                format!(
                    "TTL must be between 1 and {} seconds",
                    self.grant.limits.max_ttl_seconds
                ),
            ));
        }
        let mut budget = lock_budget(&self.budget);
        let now = Instant::now();
        while budget
            .recent_messages
            .front()
            .is_some_and(|created| now.duration_since(*created) >= Duration::from_secs(60))
        {
            budget.recent_messages.pop_front();
        }
        if budget.message_count >= self.grant.limits.max_messages_per_turn {
            return Err(limit_error(
                AgentRelayErrorKind::BudgetExceeded,
                format!(
                    "turn message budget of {} operations is exhausted",
                    self.grant.limits.max_messages_per_turn
                ),
            ));
        }
        if budget.message_bytes.saturating_add(message.content.len())
            > self.grant.limits.max_bytes_per_turn
        {
            return Err(limit_error(
                AgentRelayErrorKind::BudgetExceeded,
                format!(
                    "turn byte budget of {} bytes would be exceeded",
                    self.grant.limits.max_bytes_per_turn
                ),
            ));
        }
        if self
            .grant
            .limits
            .max_messages_per_minute
            .is_some_and(|limit| budget.recent_messages.len() >= limit as usize)
        {
            return Err(AgentRelayError {
                kind: AgentRelayErrorKind::RateLimited,
                retry: AgentRelayRetryAdvice::After {
                    milliseconds: 60_000,
                },
                delivery: AgentRelayDeliveryState::NotAccepted,
                message: "relay message rate boundary reached; retry after 60000ms".to_owned(),
                message_id: None,
            });
        }
        budget.message_count = budget.message_count.saturating_add(1);
        budget.message_bytes = budget.message_bytes.saturating_add(message.content.len());
        budget.recent_messages.push_back(now);
        Ok(())
    }

    fn effective_hop_limit(&self) -> u8 {
        self.context
            .inbound()
            .map_or(self.grant.limits.max_hops, |inbound| {
                inbound.hop_limit.min(self.grant.limits.max_hops)
            })
    }

    async fn emit_receipt_activity(
        &self,
        kind: AgentRelayActivityKind,
        recipient: Option<AgentAddress>,
        receipt: &AgentDeliveryReceipt,
    ) {
        self.emit_activity(AgentRelayActivity {
            kind,
            sender: self.context.sender().clone(),
            recipient,
            message_id: Some(receipt.message_id.clone()),
            thread_id: Some(receipt.thread_id.clone()),
            delivery_status: Some(receipt.status),
            error_kind: None,
            detail: receipt.detail.clone().map(|detail| bounded_detail(&detail)),
        })
        .await;
    }

    async fn emit_error_activity(&self, error: &AgentRelayError) {
        let kind = match error.kind {
            AgentRelayErrorKind::RateLimited
            | AgentRelayErrorKind::BudgetExceeded
            | AgentRelayErrorKind::HopLimitExceeded => AgentRelayActivityKind::LimitReached,
            _ => AgentRelayActivityKind::Rejected,
        };
        self.emit_activity(AgentRelayActivity {
            kind,
            sender: self.context.sender().clone(),
            recipient: None,
            message_id: error.message_id.clone(),
            thread_id: None,
            delivery_status: None,
            error_kind: Some(error.kind),
            detail: Some(bounded_detail(&error.message)),
        })
        .await;
    }

    async fn emit_activity(&self, activity: AgentRelayActivity) {
        self.activity.emit(activity).await;
    }
}

fn lock_budget(budget: &Mutex<BridgeBudget>) -> MutexGuard<'_, BridgeBudget> {
    budget
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn bounded_detail(detail: &str) -> String {
    detail.chars().take(1_024).collect()
}

fn validate_hop(hop_count: u8, hop_limit: u8) -> AgentRelayResult<()> {
    if hop_count > hop_limit {
        return Err(limit_error(
            AgentRelayErrorKind::HopLimitExceeded,
            format!("relay hop {hop_count} exceeds the chain limit {hop_limit}"),
        ));
    }
    Ok(())
}

fn receipt_result(receipt: &AgentDeliveryReceipt) -> AgentRelayMcpToolResult {
    AgentRelayMcpToolResult::success(
        format!(
            "Message {} is {:?}; reconcile with agent_relay_status if delivery becomes ambiguous.",
            receipt.message_id, receipt.status
        ),
        receipt,
    )
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(arguments: Value) -> AgentRelayResult<T> {
    serde_json::from_value(arguments)
        .map_err(|error| invalid_tool_input(format!("invalid relay tool arguments: {error}")))
}

fn invalid_tool_input(message: impl Into<String>) -> AgentRelayError {
    AgentRelayError::permanent(AgentRelayErrorKind::InvalidRequest, message)
}

fn limit_error(kind: AgentRelayErrorKind, message: impl Into<String>) -> AgentRelayError {
    AgentRelayError::permanent(kind, message)
}

fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoverToolInput {
    query: Option<String>,
    cursor: Option<String>,
    limit: Option<u16>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendToolInput {
    recipient: AgentAddress,
    idempotency_key: AgentIdempotencyKey,
    thread_id: Option<AgentThreadId>,
    reply_to: Option<AgentMessageId>,
    content: String,
    #[serde(default)]
    attachments: Vec<AgentMessageAttachment>,
    #[serde(default)]
    metadata: AgentRelayMetadata,
    #[serde(default = "default_ttl_seconds")]
    ttl_seconds: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyToolInput {
    in_reply_to: AgentMessageId,
    idempotency_key: AgentIdempotencyKey,
    content: String,
    #[serde(default)]
    attachments: Vec<AgentMessageAttachment>,
    #[serde(default)]
    metadata: AgentRelayMetadata,
    #[serde(default = "default_ttl_seconds")]
    ttl_seconds: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusToolInput {
    message_id: Option<AgentMessageId>,
    idempotency_key: Option<AgentIdempotencyKey>,
}

const fn default_ttl_seconds() -> u32 {
    DEFAULT_TTL_SECONDS
}

fn discovery_definition() -> Value {
    json!({
        "name": AGENT_RELAY_TOOL_DISCOVER,
        "description": "Discover only agents visible to this host-scoped relay capability.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "cursor": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1}
            },
            "additionalProperties": false
        }
    })
}

fn message_properties() -> Value {
    json!({
        "idempotency_key": {"type": "string", "minLength": 1},
        "content": {"type": "string", "minLength": 1},
        "attachments": {
            "type": "array",
            "items": {"type": "object"}
        },
        "metadata": {"type": "object"},
        "ttl_seconds": {"type": "integer", "minimum": 1}
    })
}

fn send_definition() -> Value {
    let mut properties = message_properties();
    let object = properties
        .as_object_mut()
        .expect("static message properties are an object");
    object.insert(
        "recipient".to_owned(),
        json!({"type": "string", "minLength": 1}),
    );
    object.insert("thread_id".to_owned(), json!({"type": "string"}));
    object.insert("reply_to".to_owned(), json!({"type": "string"}));
    json!({
        "name": AGENT_RELAY_TOOL_SEND,
        "description": "Durably enqueue a user-level message to an authorized top-level agent. Reuse idempotency_key on retry.",
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": ["recipient", "idempotency_key", "content"],
            "additionalProperties": false
        }
    })
}

fn reply_definition() -> Value {
    let mut properties = message_properties();
    properties
        .as_object_mut()
        .expect("static message properties are an object")
        .insert(
            "in_reply_to".to_owned(),
            json!({"type": "string", "minLength": 1}),
        );
    json!({
        "name": AGENT_RELAY_TOOL_REPLY,
        "description": "Explicitly reply to a durable agent message. The host resolves its sender and thread. Reuse idempotency_key on retry.",
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": ["in_reply_to", "idempotency_key", "content"],
            "additionalProperties": false
        }
    })
}

fn status_definition() -> Value {
    json!({
        "name": AGENT_RELAY_TOOL_STATUS,
        "description": "Reconcile durable delivery by canonical message ID or sender-scoped idempotency key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message_id": {"type": "string", "minLength": 1},
                "idempotency_key": {"type": "string", "minLength": 1}
            },
            "oneOf": [
                {"required": ["message_id"]},
                {"required": ["idempotency_key"]}
            ],
            "additionalProperties": false
        }
    })
}

// Keep construction in the call paths explicit so host-derived routing fields
// can never be deserialized from MCP arguments.
impl SendToolInput {
    fn into_parts(
        self,
    ) -> (
        AgentAddress,
        AgentIdempotencyKey,
        Option<AgentThreadId>,
        Option<AgentMessageId>,
        AgentMessage,
        u32,
    ) {
        let message = AgentMessage {
            content: self.content,
            attachments: self.attachments,
            metadata: self.metadata,
        };
        (
            self.recipient,
            self.idempotency_key,
            self.thread_id,
            self.reply_to,
            message,
            self.ttl_seconds,
        )
    }
}

impl ReplyToolInput {
    fn into_parts(self) -> (AgentMessageId, AgentIdempotencyKey, AgentMessage, u32) {
        let message = AgentMessage {
            content: self.content,
            attachments: self.attachments,
            metadata: self.metadata,
        };
        (
            self.in_reply_to,
            self.idempotency_key,
            message,
            self.ttl_seconds,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;

    #[test]
    fn authenticated_http_exposure_requires_tls_except_on_loopback() {
        for url in [
            "http://example.test/mcp",
            "http://localhost.example.test/mcp",
        ] {
            assert!(AgentRelayMcpExposure::http(url, "RELAY_AUTH").is_err());
        }
        for url in [
            "https://example.test/mcp",
            "http://localhost:8787/mcp",
            "http://[::1]:8787/mcp",
        ] {
            assert!(AgentRelayMcpExposure::http(url, "RELAY_AUTH").is_ok());
        }
    }
    use crate::relay::{
        AgentAddressPattern, AgentAvailability, AgentDeliveryStatus, AgentDirectoryEntry,
        AgentDirectoryPage, AgentDiscoveryGrant, AgentMessagingLimits, AgentSendGrant,
    };

    struct Router {
        senders: StdMutex<Vec<AgentAddress>>,
    }

    #[async_trait]
    impl AgentMessageRouter for Router {
        async fn discover(
            &self,
            context: &AgentRelayContext,
            _query: AgentDiscoveryQuery,
        ) -> AgentRelayResult<AgentDirectoryPage> {
            self.senders.lock().unwrap().push(context.sender().clone());
            Ok(AgentDirectoryPage {
                agents: vec![
                    AgentDirectoryEntry {
                        address: AgentAddress::new("team/agent-b").unwrap(),
                        display_name: "B".to_owned(),
                        description: None,
                        availability: AgentAvailability::Busy,
                        metadata: AgentRelayMetadata::default(),
                    },
                    AgentDirectoryEntry {
                        address: AgentAddress::new("private/agent-c").unwrap(),
                        display_name: "C".to_owned(),
                        description: None,
                        availability: AgentAvailability::Online,
                        metadata: AgentRelayMetadata::default(),
                    },
                ],
                next_cursor: None,
            })
        }

        async fn send(
            &self,
            context: &AgentRelayContext,
            request: SendAgentMessageRequest,
        ) -> AgentRelayResult<AgentDeliveryReceipt> {
            self.senders.lock().unwrap().push(context.sender().clone());
            Ok(receipt(request.idempotency_key, "message-send"))
        }

        async fn reply(
            &self,
            context: &AgentRelayContext,
            request: ReplyToAgentMessageRequest,
        ) -> AgentRelayResult<AgentDeliveryReceipt> {
            self.senders.lock().unwrap().push(context.sender().clone());
            Ok(receipt(request.idempotency_key, "message-reply"))
        }

        async fn delivery_status(
            &self,
            context: &AgentRelayContext,
            _query: AgentDeliveryQuery,
        ) -> AgentRelayResult<AgentDeliveryReceipt> {
            self.senders.lock().unwrap().push(context.sender().clone());
            Ok(receipt(
                AgentIdempotencyKey::new("status-key").unwrap(),
                "message-status",
            ))
        }
    }

    fn receipt(key: AgentIdempotencyKey, id: &str) -> AgentDeliveryReceipt {
        AgentDeliveryReceipt {
            message_id: AgentMessageId::new(id).unwrap(),
            idempotency_key: key,
            thread_id: AgentThreadId::new("thread-1").unwrap(),
            status: AgentDeliveryStatus::Queued,
            attempts: 0,
            updated_at_unix_ms: 1,
            detail: Some("recipient busy; durable message remains queued".to_owned()),
        }
    }

    fn grant(max_messages: u32) -> MessagingGrant {
        MessagingGrant {
            discovery: Some(AgentDiscoveryGrant {
                addresses: vec![AgentAddressPattern::prefix("team/").unwrap()],
                max_results: 10,
            }),
            send: Some(AgentSendGrant {
                recipients: vec![AgentAddressPattern::prefix("team/").unwrap()],
            }),
            limits: AgentMessagingLimits {
                max_messages_per_turn: max_messages,
                max_messages_per_minute: None,
                ..AgentMessagingLimits::default()
            },
            ..MessagingGrant::default()
        }
    }

    fn bridge(max_messages: u32) -> (AgentRelayMcpBridge, Arc<Router>) {
        let router = Arc::new(Router {
            senders: StdMutex::new(Vec::new()),
        });
        let bridge = AgentRelayMcpBridge::for_turn(
            AgentAddress::new("team/agent-a").unwrap(),
            "capability-1",
            grant(max_messages),
            router.clone(),
        )
        .unwrap();
        (bridge, router)
    }

    #[tokio::test]
    async fn tool_schema_and_calls_never_accept_sender_identity() {
        let (bridge, router) = bridge(2);
        let definitions = serde_json::to_string(&bridge.tool_definitions()).unwrap();
        assert!(!definitions.contains("\"sender\":"));

        let result = bridge
            .call_tool(
                AGENT_RELAY_TOOL_SEND,
                json!({
                    "recipient": "team/agent-b",
                    "idempotency_key": "attempt-1",
                    "content": "Please review the API."
                }),
            )
            .await;
        assert!(!result.is_error);
        assert_eq!(router.senders.lock().unwrap()[0].as_str(), "team/agent-a");

        let spoof = bridge
            .call_tool(
                AGENT_RELAY_TOOL_SEND,
                json!({
                    "sender": "private/spoofed",
                    "recipient": "team/agent-b",
                    "idempotency_key": "attempt-2",
                    "content": "spoof"
                }),
            )
            .await;
        assert!(spoof.is_error);
    }

    #[tokio::test]
    async fn discovery_is_post_filtered_by_the_grant() {
        let (bridge, _) = bridge(1);
        let result = bridge
            .call_tool(AGENT_RELAY_TOOL_DISCOVER, json!({"limit": 10}))
            .await;
        assert!(!result.is_error);
        assert_eq!(
            result.structured_content["agents"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            result.structured_content["agents"][0]["address"],
            "team/agent-b"
        );
    }

    #[tokio::test]
    async fn message_budget_stops_agent_loops() {
        let (bridge, _) = bridge(1);
        let first = bridge
            .call_tool(
                AGENT_RELAY_TOOL_SEND,
                json!({
                    "recipient": "team/agent-b",
                    "idempotency_key": "attempt-1",
                    "content": "first"
                }),
            )
            .await;
        assert!(!first.is_error);
        let second = bridge
            .call_tool(
                AGENT_RELAY_TOOL_SEND,
                json!({
                    "recipient": "team/agent-b",
                    "idempotency_key": "attempt-2",
                    "content": "second"
                }),
            )
            .await;
        assert!(second.is_error);
        assert_eq!(
            second.structured_content["error"]["kind"],
            "budget_exceeded"
        );
    }

    #[tokio::test]
    async fn minimal_json_rpc_surface_lists_and_calls_tools() {
        let (bridge, _) = bridge(1);
        let listed = bridge
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {}
            }))
            .await
            .unwrap();
        assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 4);

        let called = bridge
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": "call-1",
                "method": "tools/call",
                "params": {
                    "name": AGENT_RELAY_TOOL_STATUS,
                    "arguments": {"idempotency_key": "attempt-1"}
                }
            }))
            .await
            .unwrap();
        assert_eq!(called["result"]["isError"], false);
        assert_eq!(called["result"]["structuredContent"]["status"], "queued");
    }

    #[test]
    fn exposure_is_explicit_and_secret_values_are_not_configuration() {
        let exposure = AgentRelayMcpExposure::http(
            "http://127.0.0.1:8787/mcp/relay",
            "AGENT_RELAY_TURN_TOKEN",
        )
        .unwrap();
        let debug = format!("{exposure:?}");
        assert!(!debug.contains("short-lived-token"));
        let mut context = LaunchContext::default();
        exposure.install(&mut context).unwrap();
        assert!(context
            .mcp_servers
            .contains_key(AGENT_RELAY_MCP_SERVER_NAME));
    }
}
